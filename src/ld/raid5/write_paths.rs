//! RAID5 stripe write paths: full-stripe / data-only / partial RMW / RW.
//! Split out of the parent module for the file-size limit.
use super::*;

impl LdRaid5 {
    /// Write the segment `buf` that starts at `addr` and stays within a
    /// single set + set_stripe.
    ///
    /// The segment may cover anywhere from one byte of one data position up
    /// to the full stripe across all K data positions.
    pub(super) fn write_one_stripe_segment(&self, start: StripeAddr, buf: &[u8]) -> ChunkletResult<()> {
        // Compute failure pattern once and reuse for the F-budget check
        // and the path dispatch below.
        let f_data = self.failed_data_positions(start.set_idx).len();
        let p_failed = self.parity_failed(start.set_idx);
        let f_total = f_data + p_failed as usize;
        if f_total > 1 {
            return Err(ChunkletError::WriteRedundancyExceeded {
                raid: RaidLevel::Raid5,
                set_idx: start.set_idx,
                failed: f_total,
                budget: 1,
            });
        }

        // Runtime member-failure budget: R5 tolerates 1 total, `f_total` already
        // lost at open, so `1 - f_total` more may fail this write and reconstruct.
        let budget = 1u32 - f_total as u32;

        let strip = self.strip_bytes;
        let k = self.data_per_set;

        // Decompose the segment into (data_pos, in_strip_off, len) chunks.
        let mut positions: Vec<(usize, u64, std::ops::Range<usize>)> = Vec::with_capacity(k);
        let mut consumed = 0usize;
        let mut cur_pos = start.data_pos;
        let mut cur_off = start.in_strip_off;
        while consumed < buf.len() {
            let strip_remain = strip - cur_off;
            let take = std::cmp::min((buf.len() - consumed) as u64, strip_remain) as usize;
            positions.push((cur_pos, cur_off, consumed..consumed + take));
            consumed += take;
            cur_pos += 1;
            cur_off = 0;
        }
        debug_assert_eq!(consumed, buf.len());

        // Strip-aligned chunklet offset shared by every position + the parity
        // strip in this (set, set_stripe). Helpers below compute each
        // position's chunklet IO at `strip_base + off`. NEVER pass
        // `start.in_chunklet_off` (which embeds `start.in_strip_off`) directly
        // — that only matches pos[0] and corrupts pos[1+] / parity.
        let strip_base = start.in_chunklet_off - start.in_strip_off;
        let _stripe = self
            .stripe_locks
            .write_key(self.stripe_key(start.set_idx, strip_base));

        // Full-stripe = every data position covered, each spans the entire strip.
        let is_full_stripe = positions.len() == k
            && positions
                .iter()
                .all(|(_pos, off, range)| *off == 0 && (range.end - range.start) as u64 == strip);
        if is_full_stripe {
            return self.write_full_stripe(start.set_idx, strip_base, &positions, buf, budget);
        }

        if f_data == 0 && p_failed && !self.set_being_rebuilt(start.set_idx) {
            // Only parity is gone: skip parity entirely, write data direct.
            // A rebuilding parity target needs parity computed to write-forward,
            // so a rebuilding set falls through to RW instead of this fast path.
            return self.write_data_only(start.set_idx, strip_base, &positions, buf, budget);
        }

        // A rebuilding/rebalancing set MUST take a write-forwarding path: RMW
        // updates only the source data + parity and never write_forwards, so a
        // below-cursor foreground write would leave the shadow stale and Phase C
        // would swap onto stale data. RW materializes full new strips + parity
        // and write_forwards, so force it (mirrors the write_data_only guard above).
        if f_data == 0 && !p_failed && !self.set_being_rebuilt(start.set_idx) {
            // Healthy set — RMW vs RW based on M-vs-K threshold.
            // RW only beats RMW when modifications are full-strip; sub-strip
            // modifications add gap-fill reads to RW that wipe out the win.
            let m = positions.len();
            let all_full_strip = positions
                .iter()
                .all(|(_p, off, range)| *off == 0 && (range.end - range.start) as u64 == strip);
            if all_full_strip && (k - m) < (m + 1) {
                self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf, budget)
            } else {
                self.write_partial_stripe_rmw(start.set_idx, strip_base, &positions, buf, budget)
            }
        } else {
            // F=1 data-failed (parity healthy). RMW would need to read the
            // failed PD's old data for delta computation when that position
            // is in the modified set; RW handles it uniformly via reconstruct.
            self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf, budget)
        }
    }

    /// Fan out one write per surviving member, computed from the new data.
    /// Skips writes to failed PDs (degraded fast path; data parity must be
    /// rebuilt later for redundancy to come back).
    ///
    /// `strip_base` is the strip-aligned chunklet offset shared by every
    /// position and the parity strip. For full-stripe writes every position
    /// has `off == 0`, so each position's chunklet offset equals `strip_base`.
    fn write_full_stripe(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
        budget: u32,
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut parity = vec![0u8; strip];
        for (_pos, _off, range) in positions {
            xor_into(&mut parity, &buf[range.clone()]);
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 1);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            if let Some(pd) = &self.members[m] {
                ops.push(StripWrite {
                    pd: pd.clone(),
                    chunklet_index: self.desc.members[m].chunklet_index,
                    in_chunklet_off: strip_base + off,
                    data: &buf[range.clone()],
                });
            }
        }
        let pm = self.member_idx_parity(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &parity,
            });
        }
        self.submit_set_absorb(ops, budget)?;
        let strip = self.strip_bytes as usize;
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                buf[pos * strip..pos * strip + strip].to_vec()
            } else {
                parity.clone()
            }
        });
        Ok(())
    }

    /// F=1 parity-only fast path: skip parity entirely, write data only.
    /// Zero reads, ≤K writes. Parity is recovered later by rebuild.
    fn write_data_only(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
        budget: u32,
    ) -> ChunkletResult<()> {
        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len());
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            // Healthy data PDs only — F=1 with parity failed means all data
            // PDs are alive.
            let pd = self.members[m].as_ref().expect(
                "write_data_only invariant: data PDs must be healthy when only parity is failed",
            );
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: &buf[range.clone()],
            });
        }
        self.submit_set_absorb(ops, budget)
    }

    /// Read-modify-write: for M modified positions, read old slices, XOR
    /// into a delta, apply delta to old parity, write modified data + new
    /// parity. Healthy-set only — caller routes degraded sets to RW.
    ///
    /// Each position's old data is read at `strip_base + off` (the
    /// position-specific chunklet offset). Parity (full strip) lives at
    /// `strip_base`.
    fn write_partial_stripe_rmw(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
        budget: u32,
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut delta_full_strip = vec![0u8; strip];
        for (pos, off, range) in positions {
            let new_data = &buf[range.clone()];
            let len = new_data.len();
            let mut old_data = vec![0u8; len];
            match self.read_data_strip(set_idx, *pos, strip_base + off, &mut old_data) {
                Ok(()) => {}
                Err(e) if is_runtime_read_fault(&e) => {
                    // A modified position's OLD data faulted (dead-but-not-yet-
                    // isolated). The delta can't be applied, but RW recomputes
                    // parity from the NEW data + surviving strips WITHOUT reading
                    // old data/parity — so abandon RMW and recompute cleanly. RW
                    // runs under the SAME held stripe lock (no re-lock). The
                    // follow-on write to this still-is_some member is absorbed by
                    // submit_set_absorb within `budget`. Report a read suspect.
                    self.report_read_suspect(self.member_idx_data(set_idx, *pos));
                    return self.write_partial_stripe_rw(set_idx, strip_base, positions, buf, budget);
                }
                Err(e) => return Err(e),
            }
            let dst = &mut delta_full_strip[(*off as usize)..(*off as usize) + len];
            for i in 0..len {
                dst[i] ^= old_data[i] ^ new_data[i];
            }
        }
        let mut parity = vec![0u8; strip];
        match self.read_parity_strip(set_idx, strip_base, &mut parity) {
            Ok(()) => {}
            Err(e) if is_runtime_read_fault(&e) => {
                // Old parity faulted — RW recomputes parity from scratch and
                // never reads old parity, so abandon RMW.
                self.report_read_suspect(self.member_idx_parity(set_idx));
                return self.write_partial_stripe_rw(set_idx, strip_base, positions, buf, budget);
            }
            Err(e) => return Err(e),
        }
        xor_into(&mut parity, &delta_full_strip);

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 1);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            let pd = self.members[m]
                .as_ref()
                .expect("RMW path requires all data PDs healthy");
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: &buf[range.clone()],
            });
        }
        let pm = self.member_idx_parity(set_idx);
        let pd = self.members[pm]
            .as_ref()
            .expect("RMW path requires parity PD healthy");
        ops.push(StripWrite {
            pd: pd.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &parity,
        });
        self.submit_set_absorb(ops, budget)
    }

    /// Reconstruct-write: build full new strips for all K data positions
    /// (read unmodified, reconstruct unmodified-failed via parity, copy
    /// modified bytes into old/reconstructed strip for partial mods),
    /// recompute parity from scratch, write modified data + new parity.
    /// Skips writes to failed PDs.
    ///
    /// Precondition: parity PD is healthy. Caller routes parity-failed sets
    /// to `write_data_only` (no parity computation needed at all).
    ///
    /// All full-strip reads/reconstructs use `strip_base` (strip-aligned
    /// chunklet offset). Modified data writes use `strip_base + off` for
    /// each position; parity writes the full strip at `strip_base`.
    fn write_partial_stripe_rw(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
        budget: u32,
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let k = self.data_per_set;

        let modified_map: HashMap<usize, (u64, std::ops::Range<usize>)> = positions
            .iter()
            .map(|(p, off, r)| (*p, (*off, r.clone())))
            .collect();

        let mut new_strips: Vec<Vec<u8>> = (0..k).map(|_| vec![0u8; strip]).collect();
        for pos in 0..k {
            let pd_failed = self.data_position_failed(set_idx, pos);
            match modified_map.get(&pos) {
                Some((off, range)) => {
                    let new_data = &buf[range.clone()];
                    let off = *off as usize;
                    let len = new_data.len();
                    if off == 0 && len == strip {
                        new_strips[pos].copy_from_slice(new_data);
                    } else {
                        if pd_failed {
                            self.reconstruct_data(set_idx, pos, strip_base, &mut new_strips[pos])?;
                        } else {
                            match self.read_data_strip(set_idx, pos, strip_base, &mut new_strips[pos]) {
                                Ok(()) => {}
                                Err(e) if is_runtime_read_fault(&e) => {
                                    // Old strip on a still-is_some member that is faulting at runtime.
                                    // Reconstruct from parity + survivors (md "compute, don't read a
                                    // faulty device") within R5's budget of 1; a pre-existing is_none
                                    // loss makes this a 2nd loss → over budget → surface the error.
                                    if !self.failed_data_positions(set_idx).is_empty() {
                                        return Err(e);
                                    }
                                    self.reconstruct_data(set_idx, pos, strip_base, &mut new_strips[pos])?;
                                    self.report_read_suspect(self.member_idx_data(set_idx, pos));
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        new_strips[pos][off..off + len].copy_from_slice(new_data);
                    }
                }
                None => {
                    if pd_failed {
                        self.reconstruct_data(set_idx, pos, strip_base, &mut new_strips[pos])?;
                    } else {
                        match self.read_data_strip(set_idx, pos, strip_base, &mut new_strips[pos]) {
                            Ok(()) => {}
                            Err(e) if is_runtime_read_fault(&e) => {
                                if !self.failed_data_positions(set_idx).is_empty() {
                                    return Err(e);
                                }
                                self.reconstruct_data(set_idx, pos, strip_base, &mut new_strips[pos])?;
                                self.report_read_suspect(self.member_idx_data(set_idx, pos));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        }

        let mut parity = vec![0u8; strip];
        for s in &new_strips {
            xor_into(&mut parity, s);
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 1);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            if let Some(pd) = &self.members[m] {
                ops.push(StripWrite {
                    pd: pd.clone(),
                    chunklet_index: self.desc.members[m].chunklet_index,
                    in_chunklet_off: strip_base + off,
                    data: &buf[range.clone()],
                });
            }
        }
        // Parity may be the failed member during an online rebuild (routed here
        // instead of write_data_only so `parity` is computed for write-forward);
        // skip the live write when it is down, exactly like a failed data pos.
        let pm = self.member_idx_parity(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &parity,
            });
        }
        self.submit_set_absorb(ops, budget)?;
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                new_strips[pos].clone()
            } else {
                parity.clone()
            }
        });
        Ok(())
    }
}
