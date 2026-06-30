//! LD descriptor encoding (variable-length records embedded in
//! `SuperblockBody::ld_list_bytes`).
//!
//! # Why extent-run encoding (v2)
//!
//! v1 stored every chunklet placement explicitly (24 B/member). The superblock
//! manifest body is one 4028-byte slot, so an LD was capped at ~178 chunklets
//! (≈48 GiB RAID6) — far short of the 1 PB target (~1M chunklets). v2 keeps the
//! dense `members: Vec<LdMember>` IN MEMORY (so every consumer is unchanged) but
//! serialises it as **per-position run-length extents**: for each member slot
//! `p` in a stripe row (there are `P = set_size * row_size` of them, 1 for
//! Plain), the placement across rows `0..num_rows` is RLE'd into
//! `(pd, base_chunklet, row_span, stride, role, generation)` runs. A regular LD
//! whose allocator placed each position contiguously needs ONE run per position
//! → the descriptor is `O(P)`, INDEPENDENT of capacity. A 1 PB RAID6 6+2
//! (`set_size=8`, `row_size=3`, `num_rows≈58255`, `P=24`) is ~728 B, well inside
//! the slot. Fragmentation (post-rebuild) adds a few runs per position; the
//! `descriptor_size` u16 still bounds the whole thing.
//!
//! # Wire format (per descriptor, v2)
//!
//! ```text
//! [0..2]    descriptor_size u16 LE   (total bytes incl. header + all positions)
//! [2..3]    raid_level u8
//! [3..4]    set_size u8              (members per RAID set; 1 for Plain/Raid0)
//! [4..6]    row_size u16 LE          (sets per stripe row)
//! [6..8]    num_rows u16 LE          (rows; for Plain == chunklet count)
//! [8..9]    strip_size_log2 u8
//! [9..10]   ha_domain u8             (0 = Pd)
//! [10..11]  format_tag u8 = 2        (extent encoding)
//! [11..12]  reserved
//! [12..28]  ld_uuid (16 bytes)
//! [28..32]  position_count u32 LE    (= set_size * row_size)
//! [32..]    per position p in 0..P:
//!             [0..2]  run_count u16 LE
//!             [2..]   runs[run_count], 27 bytes each:
//!                       [0..16]  pd_id
//!                       [16..20] base_chunklet_index u32 LE  (idx at run's first row)
//!                       [20..24] row_span u32 LE             (rows covered)
//!                       [24]     stride i8                   (idx delta per row; +1 typical)
//!                       [25]     role u8                     (uniform within run)
//!                       [26]     generation u8               (uniform within run)
//! ```
//!
//! # List format (the bytes stored in `SuperblockBody::ld_list_bytes`)
//!
//! ```text
//! [0..4]     ld_count u32 LE
//! [4..]      [LdDescriptor; N]
//! ```

use std::convert::TryInto;

use crate::error::{ChunkletError, ChunkletResult};
use crate::types::{
    HaDomain, LdId, LdMember, LdRole, PdId, RaidLevel, CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE,
};

const DESC_HEADER_BYTES: usize = 32;
const RUN_BYTES: usize = 27;
const DESC_FORMAT_EXTENT: u8 = 2;
const CHUNKLET_USER_BYTES: u64 = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;

/// One RLE extent for a single member position, covering `row_span` consecutive
/// rows whose chunklet index is `base + k*stride` (k = 0..row_span) on `pd`,
/// all sharing `role` + `generation`.
#[derive(Clone, Copy, Debug)]
struct PositionRun {
    pd: PdId,
    base: u32,
    /// First row this run covers (within the position's 0..num_rows column).
    start_row: u32,
    row_span: u32,
    stride: i8,
    role: LdRole,
    generation: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LdDescriptor {
    pub id: LdId,
    pub raid_level: RaidLevel,
    pub set_size: u8,
    pub row_size: u16,
    pub num_rows: u16,
    pub strip_size_log2: u8,
    pub ha_domain: HaDomain,
    pub members: Vec<LdMember>,
}

impl LdDescriptor {
    /// Member-positions per stripe row: `set_size * row_size` (Plain/Raid0 have
    /// `set_size == 1`, so this is `row_size`; Plain has `row_size == 1` too).
    fn position_count(&self) -> usize {
        self.set_size as usize * self.row_size as usize
    }

    /// Flat index into `members` for member position `p` at row `row_n`. The
    /// canonical order is set-major within a row, rows outermost:
    /// `members[(row_n*row_size + set_in_row)*set_size + member_in_set]`.
    fn flat_index(&self, p: usize, row_n: usize) -> usize {
        let set_size = self.set_size as usize;
        let set_in_row = p / set_size;
        let member_in_set = p % set_size;
        (row_n * self.row_size as usize + set_in_row) * set_size + member_in_set
    }

    /// RLE-compress `members` into per-position run lists. Infallible: a stride
    /// that would not fit `i8`, or any field change, simply starts a new run.
    /// Returns one `Vec<PositionRun>` per position (length == `position_count`).
    /// Requires `members.len() == position_count * num_rows` (descriptor
    /// invariant); positions past the member set yield empty run lists.
    fn position_runs(&self) -> Vec<Vec<PositionRun>> {
        let p_count = self.position_count();
        let num_rows = self.num_rows as usize;
        let mut out: Vec<Vec<PositionRun>> = Vec::with_capacity(p_count);
        for p in 0..p_count {
            let mut runs: Vec<PositionRun> = Vec::new();
            for row_n in 0..num_rows {
                let flat = self.flat_index(p, row_n);
                let Some(m) = self.members.get(flat) else {
                    continue;
                };
                let idx = m.chunklet_index;
                if let Some(last) = runs.last_mut() {
                    if last.pd == m.pd && last.role == m.role && last.generation == m.generation {
                        let next_k = row_n as u32 - last.start_row;
                        // Extend if the index continues the run's stride.
                        let expected = (last.base as i64) + (next_k as i64) * (last.stride as i64);
                        if expected == idx as i64 {
                            last.row_span += 1;
                            continue;
                        }
                        // A 1-row run can still adopt any in-range stride for row 2.
                        if last.row_span == 1 {
                            let delta = idx as i64 - last.base as i64;
                            if (i8::MIN as i64..=i8::MAX as i64).contains(&delta) {
                                last.stride = delta as i8;
                                last.row_span = 2;
                                continue;
                            }
                        }
                    }
                }
                runs.push(PositionRun {
                    pd: m.pd,
                    base: idx,
                    start_row: row_n as u32,
                    row_span: 1,
                    stride: 1,
                    role: m.role,
                    generation: m.generation,
                });
            }
            out.push(runs);
        }
        out
    }

    pub fn encoded_len(&self) -> usize {
        let runs = self.position_runs();
        DESC_HEADER_BYTES
            + runs
                .iter()
                .map(|r| 2 + r.len() * RUN_BYTES)
                .sum::<usize>()
    }

    /// Logical user-addressable capacity (bytes), derived purely from the
    /// descriptor — no PD resolution required. Each LD impl computes this
    /// at `open_with_health` time, but the formula is determined entirely
    /// by `(raid_level, set_size, row_size, num_rows, strip_size_log2)`,
    /// so any caller holding a descriptor can ask without opening the LD.
    pub fn capacity_bytes(&self) -> ChunkletResult<u64> {
        let strip_bytes = super::compute_strip_bytes(self.strip_size_log2)?;
        if strip_bytes > CHUNKLET_USER_BYTES {
            return Err(ChunkletError::Invariant(format!(
                "strip_bytes {} > chunklet_user_size {}",
                strip_bytes, CHUNKLET_USER_BYTES
            )));
        }
        let usable_per_chunklet = (CHUNKLET_USER_BYTES / strip_bytes) * strip_bytes;
        let row_size = self.row_size as u64;
        let num_rows = self.num_rows as u64;
        Ok(match self.raid_level {
            RaidLevel::Plain => (self.members.len() as u64) * CHUNKLET_USER_BYTES,
            RaidLevel::Mirror | RaidLevel::Raid0 => row_size * num_rows * usable_per_chunklet,
            RaidLevel::Raid5 => {
                let data_per_set = (self.set_size - 1) as u64;
                row_size * num_rows * data_per_set * usable_per_chunklet
            }
            RaidLevel::Raid6 => {
                let data_per_set = (self.set_size - 2) as u64;
                row_size * num_rows * data_per_set * usable_per_chunklet
            }
        })
    }

    pub fn encode(&self) -> ChunkletResult<Vec<u8>> {
        let p_count = self.position_count();
        let expected = p_count.saturating_mul(self.num_rows as usize);
        if self.members.len() != expected {
            return Err(ChunkletError::Invariant(format!(
                "LD descriptor member count {} != position_count {} * num_rows {} = {}",
                self.members.len(),
                p_count,
                self.num_rows,
                expected
            )));
        }
        let runs = self.position_runs();
        let total = DESC_HEADER_BYTES
            + runs
                .iter()
                .map(|r| 2 + r.len() * RUN_BYTES)
                .sum::<usize>();
        if total > u16::MAX as usize {
            return Err(ChunkletError::Format(format!(
                "LD descriptor too large: {} bytes (fragmentation exceeds slot; \
                 out-of-line manifest spill not yet implemented)",
                total
            )));
        }
        let mut out = vec![0u8; total];
        out[0..2].copy_from_slice(&(total as u16).to_le_bytes());
        out[2] = self.raid_level as u8;
        out[3] = self.set_size;
        out[4..6].copy_from_slice(&self.row_size.to_le_bytes());
        out[6..8].copy_from_slice(&self.num_rows.to_le_bytes());
        out[8] = self.strip_size_log2;
        out[9] = self.ha_domain as u8;
        out[10] = DESC_FORMAT_EXTENT;
        // [11..12] reserved.
        out[12..28].copy_from_slice(&self.id.to_bytes());
        out[28..32].copy_from_slice(&(p_count as u32).to_le_bytes());

        let mut off = DESC_HEADER_BYTES;
        for pos_runs in &runs {
            out[off..off + 2].copy_from_slice(&(pos_runs.len() as u16).to_le_bytes());
            off += 2;
            for r in pos_runs {
                out[off..off + 16].copy_from_slice(&r.pd.to_bytes());
                out[off + 16..off + 20].copy_from_slice(&r.base.to_le_bytes());
                out[off + 20..off + 24].copy_from_slice(&r.row_span.to_le_bytes());
                out[off + 24] = r.stride as u8;
                out[off + 25] = r.role as u8;
                out[off + 26] = r.generation;
                off += RUN_BYTES;
            }
        }
        debug_assert_eq!(off, total);
        Ok(out)
    }

    /// Decode a single descriptor starting at `bytes[0..]`. Returns the
    /// descriptor (with `members` re-expanded from the extent runs into the
    /// canonical flat order) and the number of bytes consumed.
    pub fn decode_one(bytes: &[u8]) -> ChunkletResult<(Self, usize)> {
        if bytes.len() < DESC_HEADER_BYTES {
            return Err(ChunkletError::Format(format!(
                "ld descriptor truncated: {} bytes",
                bytes.len()
            )));
        }
        let total = u16::from_le_bytes(bytes[0..2].try_into().unwrap()) as usize;
        if total < DESC_HEADER_BYTES || total > bytes.len() {
            return Err(ChunkletError::Format(format!(
                "ld descriptor size {} out of range [{}, {}]",
                total,
                DESC_HEADER_BYTES,
                bytes.len()
            )));
        }
        let raid_level = RaidLevel::from_u8(bytes[2])?;
        let set_size = bytes[3];
        let row_size = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let num_rows = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        let strip_size_log2 = bytes[8];
        let ha_domain = HaDomain::from_u8(bytes[9])?;
        let format_tag = bytes[10];
        if format_tag != DESC_FORMAT_EXTENT {
            return Err(ChunkletError::Format(format!(
                "ld descriptor format_tag {} != {} (v2 extent); pool predates the \
                 extent manifest — recreate the pool",
                format_tag, DESC_FORMAT_EXTENT
            )));
        }
        let id = LdId::from_bytes(bytes[12..28].try_into().unwrap());
        let position_count = u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize;

        let p_count = set_size as usize * row_size as usize;
        if position_count != p_count {
            return Err(ChunkletError::Format(format!(
                "ld descriptor position_count {} != set_size {} * row_size {} = {}",
                position_count, set_size, row_size, p_count
            )));
        }

        // Build a partial descriptor for the flat_index geometry helper.
        let mut desc = Self {
            id,
            raid_level,
            set_size,
            row_size,
            num_rows,
            strip_size_log2,
            ha_domain,
            members: Vec::new(),
        };

        let num_rows_us = num_rows as usize;
        let mut members = vec![
            LdMember {
                pd: PdId::nil(),
                chunklet_index: 0,
                role: LdRole::Data,
                generation: 0,
            };
            p_count * num_rows_us
        ];

        let mut off = DESC_HEADER_BYTES;
        for p in 0..p_count {
            if off + 2 > total {
                return Err(ChunkletError::Format("ld descriptor truncated at run_count".into()));
            }
            let run_count = u16::from_le_bytes(bytes[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let mut row_n: usize = 0;
            for _ in 0..run_count {
                if off + RUN_BYTES > total {
                    return Err(ChunkletError::Format("ld descriptor truncated at run".into()));
                }
                let pd = PdId::from_bytes(bytes[off..off + 16].try_into().unwrap());
                let base = u32::from_le_bytes(bytes[off + 16..off + 20].try_into().unwrap());
                let row_span = u32::from_le_bytes(bytes[off + 20..off + 24].try_into().unwrap());
                let stride = bytes[off + 24] as i8;
                let role = LdRole::from_u8(bytes[off + 25])?;
                let generation = bytes[off + 26];
                off += RUN_BYTES;
                for k in 0..row_span as usize {
                    if row_n >= num_rows_us {
                        return Err(ChunkletError::Format(format!(
                            "ld descriptor position {} runs cover > num_rows {}",
                            p, num_rows
                        )));
                    }
                    let idx = base as i64 + (k as i64) * (stride as i64);
                    if !(0..=u32::MAX as i64).contains(&idx) {
                        return Err(ChunkletError::Format(format!(
                            "ld descriptor chunklet_index out of range: {}",
                            idx
                        )));
                    }
                    let flat = desc.flat_index(p, row_n);
                    members[flat] = LdMember {
                        pd,
                        chunklet_index: idx as u32,
                        role,
                        generation,
                    };
                    row_n += 1;
                }
            }
            if row_n != num_rows_us {
                return Err(ChunkletError::Format(format!(
                    "ld descriptor position {} runs cover {} rows != num_rows {}",
                    p, row_n, num_rows
                )));
            }
        }
        desc.members = members;
        Ok((desc, total))
    }
}

/// Top-level LD list, persisted in `SuperblockBody::ld_list_bytes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LdList {
    pub lds: Vec<LdDescriptor>,
}

impl LdList {
    pub fn encode(&self) -> ChunkletResult<Vec<u8>> {
        let mut out =
            Vec::with_capacity(4 + self.lds.iter().map(|d| d.encoded_len()).sum::<usize>());
        out.extend_from_slice(&(self.lds.len() as u32).to_le_bytes());
        for d in &self.lds {
            out.extend_from_slice(&d.encode()?);
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> ChunkletResult<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        if bytes.len() < 4 {
            return Err(ChunkletError::Format(format!(
                "ld_list bytes truncated: {}",
                bytes.len()
            )));
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut cursor = 4;
        let mut lds = Vec::with_capacity(count);
        for _ in 0..count {
            let (d, used) = LdDescriptor::decode_one(&bytes[cursor..])?;
            cursor += used;
            lds.push(d);
        }
        Ok(Self { lds })
    }

    pub fn find(&self, id: LdId) -> Option<&LdDescriptor> {
        self.lds.iter().find(|d| d.id == id)
    }

    pub fn upsert(&mut self, desc: LdDescriptor) {
        if let Some(slot) = self.lds.iter_mut().find(|d| d.id == desc.id) {
            *slot = desc;
        } else {
            self.lds.push(desc);
        }
    }

    pub fn remove(&mut self, id: LdId) -> Option<LdDescriptor> {
        let idx = self.lds.iter().position(|d| d.id == id)?;
        Some(self.lds.remove(idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PdId;

    fn sample(member_count: usize) -> LdDescriptor {
        LdDescriptor {
            id: LdId::new_v4(),
            raid_level: RaidLevel::Plain,
            set_size: 1,
            row_size: 1,
            num_rows: member_count as u16,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
            members: (0..member_count)
                .map(|i| LdMember {
                    pd: PdId::new_v4(),
                    chunklet_index: i as u32,
                    role: LdRole::Data,
                    generation: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn round_trip_single() {
        let d = sample(4);
        let bytes = d.encode().unwrap();
        let (decoded, used) = LdDescriptor::decode_one(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(decoded, d);
    }

    #[test]
    fn round_trip_list() {
        let lds = LdList {
            lds: vec![sample(2), sample(8), sample(1)],
        };
        let bytes = lds.encode().unwrap();
        let decoded = LdList::decode(&bytes).unwrap();
        assert_eq!(decoded, lds);
    }

    #[test]
    fn empty_list_round_trip() {
        let lds = LdList::default();
        let bytes = lds.encode().unwrap();
        let decoded = LdList::decode(&bytes).unwrap();
        assert_eq!(decoded, lds);
    }

    #[test]
    fn upsert_and_remove() {
        let mut lds = LdList::default();
        let d1 = sample(2);
        let id = d1.id;
        lds.upsert(d1);
        assert_eq!(lds.lds.len(), 1);
        // upsert with same id replaces.
        let mut d2 = sample(4);
        d2.id = id;
        lds.upsert(d2.clone());
        assert_eq!(lds.lds.len(), 1);
        assert_eq!(lds.lds[0], d2);
        assert!(lds.remove(id).is_some());
        assert!(lds.lds.is_empty());
    }

    #[test]
    fn rejects_truncated_descriptor() {
        let d = sample(3);
        let bytes = d.encode().unwrap();
        let truncated = &bytes[..bytes.len() - 1];
        assert!(LdDescriptor::decode_one(truncated).is_err());
    }

    // ── v2 extent-encoding coverage ──────────────────────────────────────

    /// Build a descriptor in canonical flat member order. `place(p, row_n)`
    /// returns the (pd, chunklet_index, generation) for member position `p`
    /// (0..set_size*row_size) at row `row_n`; role is derived from the RAID
    /// set pattern (last 1/2 members of a set are parity).
    fn build(
        raid: RaidLevel,
        set_size: u8,
        row_size: u16,
        num_rows: u16,
        place: impl Fn(usize, usize) -> (PdId, u32, u8),
    ) -> LdDescriptor {
        let ss = set_size as usize;
        let p_count = ss * row_size as usize;
        let role_for = |member_in_set: usize| -> LdRole {
            match raid {
                RaidLevel::Raid5 if member_in_set == ss - 1 => LdRole::ParityP,
                RaidLevel::Raid6 if member_in_set == ss - 1 => LdRole::ParityQ,
                RaidLevel::Raid6 if member_in_set == ss - 2 => LdRole::ParityP,
                _ => LdRole::Data,
            }
        };
        let mut members = vec![
            LdMember { pd: PdId::nil(), chunklet_index: 0, role: LdRole::Data, generation: 0 };
            p_count * num_rows as usize
        ];
        for row_n in 0..num_rows as usize {
            for p in 0..p_count {
                let set_in_row = p / ss;
                let member_in_set = p % ss;
                let flat = (row_n * row_size as usize + set_in_row) * ss + member_in_set;
                let (pd, idx, gen) = place(p, row_n);
                members[flat] = LdMember {
                    pd,
                    chunklet_index: idx,
                    role: role_for(member_in_set),
                    generation: gen,
                };
            }
        }
        LdDescriptor {
            id: LdId::new_v4(),
            raid_level: raid,
            set_size,
            row_size,
            num_rows,
            strip_size_log2: 0,
            ha_domain: HaDomain::Pd,
            members,
        }
    }

    fn rt(d: &LdDescriptor) -> LdDescriptor {
        let bytes = d.encode().unwrap();
        let (decoded, used) = LdDescriptor::decode_one(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        decoded
    }

    #[test]
    fn raid6_contiguous_one_run_per_position() {
        // 8-wide RAID6 (6+2), one set per row, 100 rows. Each position is a
        // contiguous chunklet range on its own PD → exactly 1 run/position.
        let pds: Vec<PdId> = (0..8).map(|_| PdId::new_v4()).collect();
        let d = build(RaidLevel::Raid6, 8, 1, 100, |p, row_n| {
            (pds[p], (p as u32) * 1000 + row_n as u32, 0)
        });
        let decoded = rt(&d);
        assert_eq!(decoded, d, "lossless round-trip");
        // 8 positions × 1 run each → tiny descriptor regardless of 100 rows.
        let runs: usize = d.position_runs().iter().map(|r| r.len()).sum();
        assert_eq!(runs, 8, "contiguous placement compresses to 1 run/position");
        assert!(d.encoded_len() < 300, "got {}", d.encoded_len());
    }

    #[test]
    fn mirror_raid10_round_trip() {
        let pds: Vec<PdId> = (0..4).map(|_| PdId::new_v4()).collect();
        // 2-way mirror, 2-wide stripe (P=4), 50 rows.
        let d = build(RaidLevel::Mirror, 2, 2, 50, |p, row_n| {
            (pds[p], (p as u32) * 500 + row_n as u32, 0)
        });
        assert_eq!(rt(&d), d);
    }

    #[test]
    fn fragmented_placement_multi_run_round_trip() {
        let pds: Vec<PdId> = (0..8).map(|_| PdId::new_v4()).collect();
        // Scatter the chunklet index non-monotonically per row so positions
        // break into several runs (post-fragmentation shape).
        let d = build(RaidLevel::Raid6, 8, 1, 20, |p, row_n| {
            let idx = (p as u32) * 1000 + (row_n as u32 * 7 % 13);
            (pds[p], idx, 0)
        });
        let decoded = rt(&d);
        assert_eq!(decoded, d);
        let runs: usize = d.position_runs().iter().map(|r| r.len()).sum();
        assert!(runs > 8, "fragmentation yields more than 1 run/position: {}", runs);
    }

    #[test]
    fn rebuild_generation_splits_a_position_run() {
        let pds: Vec<PdId> = (0..8).map(|_| PdId::new_v4()).collect();
        let mut d = build(RaidLevel::Raid6, 8, 1, 30, |p, row_n| {
            (pds[p], (p as u32) * 1000 + row_n as u32, 0)
        });
        // Simulate a rebuild that bumped generation on position 0, row 15.
        let flat = d.flat_index(0, 15);
        d.members[flat].generation = 1;
        let decoded = rt(&d);
        assert_eq!(decoded, d, "per-run generation preserved across a split");
    }

    #[test]
    fn pd_id_changes_split_run_round_trip() {
        let pds: Vec<PdId> = (0..8).map(|_| PdId::new_v4()).collect();
        let spare = PdId::new_v4();
        let mut d = build(RaidLevel::Raid6, 8, 1, 30, |p, row_n| {
            (pds[p], (p as u32) * 1000 + row_n as u32, 0)
        });
        // A rebuilt chunklet relocated to a spare PD at position 3, row 10.
        let flat = d.flat_index(3, 10);
        d.members[flat].pd = spare;
        d.members[flat].generation = 1;
        assert_eq!(rt(&d), d);
    }

    /// Codec capacity proof: a 1 PB RAID6 6+2 LD (set_size=8, row_size=3,
    /// num_rows=58255, P=24) with IDEALISED contiguous placement (1 run per
    /// position) encodes to <1 KB and round-trips 1.4M members losslessly —
    /// vs the v1 33 MB explicit member list. NOTE: real placement can't be
    /// 1 run/position at this scale because a PD only holds ~7000 chunklets,
    /// so a position's 58255-row column spans several PDs (more runs); true
    /// multi-PB therefore also needs the column-contiguous allocator (S2) to
    /// minimise runs AND an out-of-line superblock body (the pd_list for
    /// ~150 PDs alone exceeds the 4028-byte slot). This test pins the codec's
    /// best case + correctness, not the end-to-end 1 PB feasibility.
    #[test]
    fn one_petabyte_raid6_idealised_placement_fits_slot() {
        const MAX_BODY_BYTES: usize = 4028;
        let pds: Vec<PdId> = (0..24).map(|_| PdId::new_v4()).collect();
        let num_rows: u16 = 58255;
        let d = build(RaidLevel::Raid6, 8, 3, num_rows, |p, row_n| {
            (pds[p], (p as u32) * 100_000 + row_n as u32, 0)
        });
        // Sanity: capacity ≈ 1 PB (6 data * 3 * 58255 GiB-ish).
        let cap = d.capacity_bytes().unwrap();
        assert!(cap >= 1 << 50, "capacity {} should be ≥ 1 PiB", cap);
        // 24 positions × 1 run → well under the slot.
        let len = d.encoded_len();
        assert!(
            len < MAX_BODY_BYTES,
            "1 PB descriptor {} B must fit the {} B slot",
            len,
            MAX_BODY_BYTES
        );
        // And it must still round-trip losslessly (1.4M members reconstructed).
        assert_eq!(rt(&d), d);
    }
}
