//! Pool::scrub_ld — periodic parity-verify + bad-chunklet quarantine.
//!
//! Walks every stripe in the LD and recomputes the expected parity / mirror
//! relationship from the live data. Where the comparison can pinpoint the
//! corrupt chunklet (e.g. RAID-5 parity disagrees with data → parity is the
//! suspect; RAID-6 P-only or Q-only mismatch), the offending chunklet is
//! marked `Bad` in its PD bitmap. After scrub, `Pool::rebuild_ld` will
//! treat those Bad chunklets like Failed members and route around them.
//!
//! Where the scrub finds a mismatch but cannot identify the culprit
//! (Mirror N=2, RAID-5 silent data corruption, RAID-6 P+Q both wrong), the
//! mismatch is logged in `ScrubReport::mismatches` and **no** Bad mark is
//! applied — the user must look at the report and decide.
//!
//! Scrub uses the same 1 MiB batched IO as rebuild (see `rebuild::REBUILD_BATCH_BYTES`).
//! For Plain / Raid0, scrub is a no-op (no redundancy → no comparison possible).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::error::{ChunkletError, ChunkletResult};
use crate::ld::descriptor::LdDescriptor;
use crate::ld::{gf256, LdMirror, LdRaid5, LdRaid6};
use crate::pd::PhysicalDisk;
use crate::pool::Pool;
use crate::types::{
    ChunkletState, LdId, LdRole, PdId, RaidLevel, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const SCRUB_BATCH_BYTES: u64 = 1024 * 1024;
const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

#[derive(Clone, Debug)]
pub struct ScrubReport {
    pub ld_id: LdId,
    pub batches_checked: u64,
    pub mismatches: Vec<ScrubMismatch>,
    pub marked_bad: usize,
    /// Sets that scrub skipped because a redundancy member was missing
    /// (Failed PD or Bad chunklet). Without this counter, the scrub of a
    /// fully-degraded LD reads as "0 batches checked, 0 mismatches" — the
    /// operator can't tell whether the LD is healthy or just unscrubbable.
    pub sets_skipped_degraded: usize,
}

#[derive(Clone, Debug)]
pub struct ScrubMismatch {
    pub set_idx: usize,
    pub batch_offset: u64,
    pub kind: ScrubMismatchKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScrubMismatchKind {
    /// Mirror copies disagree. `divergent_count` says how many copies don't
    /// match the most-frequent value at this offset.
    MirrorDivergence { divergent_count: usize, total_copies: usize },
    /// RAID-5 parity didn't equal XOR of data. Marked the parity chunklet Bad.
    Raid5ParityMismatch,
    /// RAID-6: P didn't match (Q did). Marked the P chunklet Bad.
    Raid6P,
    /// RAID-6: Q didn't match (P did). Marked the Q chunklet Bad.
    Raid6Q,
    /// RAID-6: both P and Q mismatched. Cannot identify culprit; logged only.
    Raid6Both,
    /// Plain/Raid0 IO error during scrub (corruption surfacing as read error).
    IoError(String),
}

impl Pool {
    /// Run a scrub pass on the LD. Returns a report summarizing checked
    /// batches and any mismatches; chunklets identified as the unique
    /// culprit are marked `Bad` in their PDs' bitmaps before returning.
    ///
    /// Scrub does NOT rebuild — the caller follows up with
    /// `Pool::rebuild_ld(ld_id)` to restore redundancy onto fresh chunklets.
    pub fn scrub_ld(&self, ld_id: LdId) -> ChunkletResult<ScrubReport> {
        let _commit = self.manifest_lock.lock();

        let desc = self
            .find_ld(ld_id)
            .ok_or_else(|| ChunkletError::Invariant(format!("LD {} not found", ld_id)))?;
        let pds_snapshot = self.state.read().pds.clone();

        let mut report = ScrubReport {
            ld_id,
            batches_checked: 0,
            mismatches: Vec::new(),
            marked_bad: 0,
            sets_skipped_degraded: 0,
        };

        match desc.raid_level {
            RaidLevel::Plain | RaidLevel::Raid0 => {
                // No parity → no scrub. Just walk every member and read one
                // batch to confirm the chunklet is at least IO-readable; any
                // IO error gets logged.
                for (i, m) in desc.members.iter().enumerate() {
                    if let Some(pd) = pds_snapshot.get(&m.pd) {
                        let mut buf = vec![0u8; SCRUB_BATCH_BYTES as usize];
                        if let Err(e) = pd.read_chunklet_user(m.chunklet_index, 0, &mut buf) {
                            report.mismatches.push(ScrubMismatch {
                                set_idx: i,
                                batch_offset: 0,
                                kind: ScrubMismatchKind::IoError(e.to_string()),
                            });
                        }
                    }
                    report.batches_checked += 1;
                }
                return Ok(report);
            }
            RaidLevel::Mirror => {
                self.scrub_mirror(&desc, &pds_snapshot, &mut report)?;
            }
            RaidLevel::Raid5 => {
                self.scrub_raid5(&desc, &pds_snapshot, &mut report)?;
            }
            RaidLevel::Raid6 => {
                self.scrub_raid6(&desc, &pds_snapshot, &mut report)?;
            }
        }

        Ok(report)
    }

    fn scrub_mirror(
        &self,
        desc: &LdDescriptor,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        report: &mut ScrubReport,
    ) -> ChunkletResult<()> {
        let _ld = LdMirror::open(desc.clone(), pds_snapshot)?;
        let n = desc.set_size as usize;
        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let batches = batches_per_chunklet();

        // For each set, for each batch, read all live copies and compare.
        let mut bad_marks: BTreeMap<PdId, BTreeSet<u32>> = BTreeMap::new();
        for set_idx in 0..n_sets {
            let base = set_idx * n;
            for batch_n in 0..batches {
                let off = batch_n * SCRUB_BATCH_BYTES;
                let take = batch_take(off);
                let mut copies: Vec<Vec<u8>> = Vec::with_capacity(n);
                let mut alive_idx: Vec<usize> = Vec::with_capacity(n);
                for pos in 0..n {
                    let m = &desc.members[base + pos];
                    if let Some(pd) = pds_snapshot.get(&m.pd) {
                        let mut buf = vec![0u8; take];
                        if pd.read_chunklet_user(m.chunklet_index, off, &mut buf).is_ok() {
                            copies.push(buf);
                            alive_idx.push(pos);
                        }
                    }
                }
                report.batches_checked += 1;
                if copies.len() < 2 {
                    // Need at least 2 alive copies to compare. Bump
                    // skipped-degraded once per set (not per batch);
                    // we only care about whether this set was scrubbable.
                    if batch_n == 0 {
                        report.sets_skipped_degraded += 1;
                    }
                    continue;
                }
                // Find the most common value pattern (by byte equality).
                let mut counts: Vec<usize> = vec![1; copies.len()];
                for i in 0..copies.len() {
                    for j in (i + 1)..copies.len() {
                        if copies[i] == copies[j] {
                            counts[i] += 1;
                            counts[j] += 1;
                        }
                    }
                }
                let max = *counts.iter().max().unwrap();
                let majority_idx = counts.iter().position(|&c| c == max).unwrap();
                let divergent: Vec<usize> = (0..copies.len())
                    .filter(|&i| copies[i] != copies[majority_idx])
                    .collect();
                if divergent.is_empty() {
                    continue;
                }
                let kind = ScrubMismatchKind::MirrorDivergence {
                    divergent_count: divergent.len(),
                    total_copies: copies.len(),
                };
                report.mismatches.push(ScrubMismatch {
                    set_idx,
                    batch_offset: off,
                    kind,
                });
                // For N >= 3, mark divergent copies Bad. For N = 2 we cannot
                // tell which is right, so we log only.
                if n >= 3 && copies.len() - divergent.len() >= 2 {
                    for &local_idx in &divergent {
                        let global_pos = alive_idx[local_idx];
                        let m = &desc.members[base + global_pos];
                        bad_marks.entry(m.pd).or_default().insert(m.chunklet_index);
                    }
                }
            }
        }
        report.marked_bad = self.commit_bad_marks(pds_snapshot, &bad_marks)?;
        Ok(())
    }

    fn scrub_raid5(
        &self,
        desc: &LdDescriptor,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        report: &mut ScrubReport,
    ) -> ChunkletResult<()> {
        let ld = LdRaid5::open(desc.clone(), pds_snapshot)?;
        let n = desc.set_size as usize;
        let k = ld.data_per_set();
        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let batches = batches_per_chunklet();
        let mut bad_marks: BTreeMap<PdId, BTreeSet<u32>> = BTreeMap::new();

        for set_idx in 0..n_sets {
            let base = set_idx * n;
            // Need parity + all data alive to do the comparison. If anything's
            // already dead, skip — rebuild handles those. Bump
            // sets_skipped_degraded so the report distinguishes
            // "scrub clean" from "scrub couldn't run".
            let parity_member = &desc.members[base + k];
            if pds_snapshot.get(&parity_member.pd).is_none() {
                report.sets_skipped_degraded += 1;
                continue;
            }
            let mut data_pds = Vec::with_capacity(k);
            let mut all_alive = true;
            for pos in 0..k {
                let m = &desc.members[base + pos];
                match pds_snapshot.get(&m.pd) {
                    None => {
                        all_alive = false;
                        break;
                    }
                    Some(pd) => data_pds.push((pd.clone(), m.chunklet_index)),
                }
            }
            if !all_alive {
                report.sets_skipped_degraded += 1;
                continue;
            }

            for batch_n in 0..batches {
                let off = batch_n * SCRUB_BATCH_BYTES;
                let take = batch_take(off);
                let mut expected = vec![0u8; take];
                let mut tmp = vec![0u8; take];
                for (pd, chunklet_idx) in &data_pds {
                    pd.read_chunklet_user(*chunklet_idx, off, &mut tmp)?;
                    for i in 0..take {
                        expected[i] ^= tmp[i];
                    }
                }
                let parity_pd = pds_snapshot.get(&parity_member.pd).unwrap();
                let mut stored_parity = vec![0u8; take];
                parity_pd.read_chunklet_user(
                    parity_member.chunklet_index,
                    off,
                    &mut stored_parity,
                )?;
                report.batches_checked += 1;
                if stored_parity != expected {
                    report.mismatches.push(ScrubMismatch {
                        set_idx,
                        batch_offset: off,
                        kind: ScrubMismatchKind::Raid5ParityMismatch,
                    });
                    bad_marks
                        .entry(parity_member.pd)
                        .or_default()
                        .insert(parity_member.chunklet_index);
                    // Mark + skip remaining batches for this set; whole parity
                    // chunklet will be rebuilt.
                    break;
                }
            }
        }
        report.marked_bad = self.commit_bad_marks(pds_snapshot, &bad_marks)?;
        Ok(())
    }

    fn scrub_raid6(
        &self,
        desc: &LdDescriptor,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        report: &mut ScrubReport,
    ) -> ChunkletResult<()> {
        let ld = LdRaid6::open(desc.clone(), pds_snapshot)?;
        let n = desc.set_size as usize;
        let k = ld.data_per_set();
        let n_sets = (desc.row_size as usize) * (desc.num_rows as usize);
        let batches = batches_per_chunklet();
        let mut bad_marks: BTreeMap<PdId, BTreeSet<u32>> = BTreeMap::new();

        for set_idx in 0..n_sets {
            let base = set_idx * n;
            let p_member = &desc.members[base + k];
            let q_member = &desc.members[base + k + 1];
            if pds_snapshot.get(&p_member.pd).is_none()
                || pds_snapshot.get(&q_member.pd).is_none()
            {
                report.sets_skipped_degraded += 1;
                continue;
            }
            let mut data_pds = Vec::with_capacity(k);
            let mut all_alive = true;
            for pos in 0..k {
                let m = &desc.members[base + pos];
                match pds_snapshot.get(&m.pd) {
                    None => {
                        all_alive = false;
                        break;
                    }
                    Some(pd) => data_pds.push((pd.clone(), m.chunklet_index, pos)),
                }
            }
            if !all_alive {
                report.sets_skipped_degraded += 1;
                continue;
            }

            let p_pd = pds_snapshot.get(&p_member.pd).unwrap();
            let q_pd = pds_snapshot.get(&q_member.pd).unwrap();
            let mut p_marked = false;
            let mut q_marked = false;
            for batch_n in 0..batches {
                let off = batch_n * SCRUB_BATCH_BYTES;
                let take = batch_take(off);
                let mut expected_p = vec![0u8; take];
                let mut expected_q = vec![0u8; take];
                let mut tmp = vec![0u8; take];
                for (pd, chunklet_idx, pos) in &data_pds {
                    pd.read_chunklet_user(*chunklet_idx, off, &mut tmp)?;
                    for i in 0..take {
                        expected_p[i] ^= tmp[i];
                    }
                    gf256::mul_xor_into(&mut expected_q, &tmp, gf256::g_pow(*pos));
                }
                let mut stored_p = vec![0u8; take];
                let mut stored_q = vec![0u8; take];
                p_pd.read_chunklet_user(p_member.chunklet_index, off, &mut stored_p)?;
                q_pd.read_chunklet_user(q_member.chunklet_index, off, &mut stored_q)?;
                report.batches_checked += 1;
                let p_ok = stored_p == expected_p;
                let q_ok = stored_q == expected_q;
                let kind = match (p_ok, q_ok) {
                    (true, true) => continue,
                    (false, true) => Some(ScrubMismatchKind::Raid6P),
                    (true, false) => Some(ScrubMismatchKind::Raid6Q),
                    (false, false) => Some(ScrubMismatchKind::Raid6Both),
                };
                if let Some(kind) = kind {
                    report.mismatches.push(ScrubMismatch {
                        set_idx,
                        batch_offset: off,
                        kind: kind.clone(),
                    });
                    match kind {
                        ScrubMismatchKind::Raid6P if !p_marked => {
                            bad_marks
                                .entry(p_member.pd)
                                .or_default()
                                .insert(p_member.chunklet_index);
                            p_marked = true;
                        }
                        ScrubMismatchKind::Raid6Q if !q_marked => {
                            bad_marks
                                .entry(q_member.pd)
                                .or_default()
                                .insert(q_member.chunklet_index);
                            q_marked = true;
                        }
                        // Raid6Both: ambiguous — log only.
                        _ => {}
                    }
                    if p_marked && q_marked {
                        break;
                    }
                }
            }
        }
        report.marked_bad = self.commit_bad_marks(pds_snapshot, &bad_marks)?;
        Ok(())
    }

    fn commit_bad_marks(
        &self,
        pds_snapshot: &BTreeMap<PdId, Arc<PhysicalDisk>>,
        bad_marks: &BTreeMap<PdId, BTreeSet<u32>>,
    ) -> ChunkletResult<usize> {
        let mut total = 0;
        for (pd_id, idxs) in bad_marks {
            let pd = pds_snapshot.get(pd_id).ok_or_else(|| {
                ChunkletError::Invariant(format!("scrub: unknown PD {}", pd_id))
            })?;
            let idxs = idxs.clone();
            let count = idxs.len();
            pd.commit_manifest(move |_body, bitmap| {
                for &idx in &idxs {
                    bitmap.set(idx, ChunkletState::Bad)?;
                }
                Ok(())
            })?;
            total += count;
        }
        Ok(total)
    }
}

fn batches_per_chunklet() -> u64 {
    (CHUNKLET_USER_BYTES + SCRUB_BATCH_BYTES - 1) / SCRUB_BATCH_BYTES
}

fn batch_take(off: u64) -> usize {
    let remain = CHUNKLET_USER_BYTES.saturating_sub(off);
    std::cmp::min(SCRUB_BATCH_BYTES, remain) as usize
}

// Allow unused warning suppression for the LdRole import which isn't needed
// in this module but kept for parity with the rebuild module.
#[allow(dead_code)]
fn _unused_role(_r: LdRole) {}
