//! RAID6 stripe write paths: full-stripe / data-only / partial PDW / RW.
//! Split out of the parent module for the file-size limit.
use super::*;

fn parity_delta_read_cost(positions: &[(usize, u64, std::ops::Range<usize>)]) -> usize {
    positions.len() + 2
}

fn reconstruct_write_read_cost(
    data_per_set: usize,
    strip_bytes: u64,
    positions: &[(usize, u64, std::ops::Range<usize>)],
) -> usize {
    let modified_full_strips = positions
        .iter()
        .filter(|(_pos, off, range)| *off == 0 && (range.end - range.start) as u64 == strip_bytes)
        .count();
    let modified_partial_strips = positions.len() - modified_full_strips;
    data_per_set - modified_full_strips + modified_partial_strips
}

impl LdRaid6 {
    pub(super) fn write_one_stripe_segment(
        &self,
        start: StripeAddr,
        buf: &[u8],
    ) -> ChunkletResult<()> {
        // Compute failure pattern once and reuse for the F-budget check
        // and the path dispatch below.
        let f_data = self.failed_data_positions(start.set_idx).len();
        let p_failed = self.parity_p_failed(start.set_idx);
        let q_failed = self.parity_q_failed(start.set_idx);
        let f_total = f_data + p_failed as usize + q_failed as usize;
        if f_total > 2 {
            return Err(ChunkletError::WriteRedundancyExceeded {
                raid: RaidLevel::Raid6,
                set_idx: start.set_idx,
                failed: f_total,
                budget: 2,
            });
        }

        let strip = self.strip_bytes;
        let k = self.data_per_set;
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

        // Strip-aligned chunklet offset shared by every position + P + Q.
        // Helpers compute each position's chunklet IO at `strip_base + off`.
        let strip_base = start.in_chunklet_off - start.in_strip_off;
        let _stripe = self
            .stripe_locks
            .write_key(self.stripe_key(start.set_idx, strip_base));

        // Runtime member-failure budget for this set: R6 tolerates 2 total, and
        // `f_total` are already lost at open (excluded from `ops`), so up to
        // `2 - f_total` MORE members may fail this write and still reconstruct.
        let budget = 2u32 - f_total as u32;

        let is_full_stripe = positions.len() == k
            && positions
                .iter()
                .all(|(_p, off, range)| *off == 0 && (range.end - range.start) as u64 == strip);
        if is_full_stripe {
            return self.write_full_stripe(start.set_idx, strip_base, &positions, buf, budget);
        }

        if f_data == 0 && p_failed && q_failed && !self.set_being_rebuilt(start.set_idx) {
            // Both parities gone (F=2): just write data, no parity work.
            // An online rebuild targeting a parity needs P/Q computed so it can
            // write-forward to the shadow, so a rebuilding set falls through to
            // RW (which computes both parities) instead of this fast path.
            return self.write_data_only(start.set_idx, strip_base, &positions, buf, budget);
        }

        // A rebuilding/rebalancing set MUST take a write-forwarding path: PDW
        // updates only source data + delta P/Q and never write_forwards, so a
        // below-cursor foreground write would leave the shadow stale and Phase C
        // would swap onto stale data. RW materializes full new strips + P + Q and
        // write_forwards, so force it (mirrors the write_data_only guard above).
        let healthy =
            f_data == 0 && !p_failed && !q_failed && !self.set_being_rebuilt(start.set_idx);
        if healthy {
            // Pick between Ceph FastEC-style parity-delta write (PDW)
            // and reconstruct-write (RW) by the number of strip reads
            // each path needs. RW wins for narrow 3+2 single-strip writes;
            // PDW wins as K grows or partial gap-fill makes RW expensive.
            let pdw_reads = parity_delta_read_cost(&positions);
            let rw_reads = reconstruct_write_read_cost(k, strip, &positions);
            if rw_reads < pdw_reads {
                self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf, budget)
            } else {
                self.write_partial_stripe_pdw(start.set_idx, strip_base, &positions, buf, budget)
            }
        } else {
            // Any failure: unified RW handles all sub-cases (single
            // surviving parity, single missing data, two missing data).
            self.write_partial_stripe_rw(start.set_idx, strip_base, &positions, buf, budget)
        }
    }

    /// Fan out one write per surviving member, computed from the new data.
    /// Skips writes to failed PDs (degraded fast path).
    ///
    /// `strip_base` is the strip-aligned chunklet offset; for full-stripe
    /// writes every position has `off == 0` so each chunklet write lands at
    /// `strip_base`. P/Q always live at `strip_base`.
    fn write_full_stripe(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
        budget: u32,
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];
        for (pos, _off, range) in positions {
            let data = &buf[range.clone()];
            gf256::xor_into(&mut p, data);
            gf256::mul_xor_into(&mut q, data, gf256::g_pow(*pos));
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 2);
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
        let pm = self.member_idx_p(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &p,
            });
        }
        let qm = self.member_idx_q(set_idx);
        if let Some(pd) = &self.members[qm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[qm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &q,
            });
        }
        self.submit_set_absorb(ops, budget)?;
        // Mirror to online-rebuild shadow(s) below the cursor: full-stripe gives
        // each data pos its slice of `buf`; P/Q are the strips just computed.
        let strip = self.strip_bytes as usize;
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                buf[pos * strip..pos * strip + strip].to_vec()
            } else if pos == self.data_per_set {
                p.clone()
            } else {
                q.clone()
            }
        });
        Ok(())
    }

    /// F=2 P+Q failed fast path: skip both parities, write data only.
    /// Zero reads, ≤K writes. Both parities recovered later by rebuild.
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
            let pd = self.members[m].as_ref().expect(
                "write_data_only invariant: data PDs must be healthy when both parities are failed",
            );
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: &buf[range.clone()],
            });
        }
        // budget is 0 here (both parities already failed ⇒ f_total == 2), so ANY
        // runtime data-strip EIO surfaces as an error — correct, the set is at
        // its redundancy limit and a lost data strip is unrecoverable.
        self.submit_set_absorb(ops, budget)
    }

    /// Parity-delta write: per touched data position compute deltas for P
    /// and Q, apply to old P/Q, write modified data + new P + new Q.
    /// Healthy-set only (needs old data, P, Q all readable).
    ///
    /// Each position's old data is read at `strip_base + off`. P and Q are
    /// full-strip reads/writes at `strip_base`.
    fn write_partial_stripe_pdw(
        &self,
        set_idx: usize,
        strip_base: u64,
        positions: &[(usize, u64, std::ops::Range<usize>)],
        buf: &[u8],
        budget: u32,
    ) -> ChunkletResult<()> {
        let strip = self.strip_bytes as usize;
        let mut delta_p = vec![0u8; strip];
        let mut delta_q = vec![0u8; strip];

        let mut old_data: Vec<Vec<u8>> = positions
            .iter()
            .map(|(_pos, _off, range)| vec![0u8; range.end - range.start])
            .collect();
        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];

        let mut read_ops: Vec<StripRead> = Vec::with_capacity(positions.len() + 2);
        for ((pos, off, _range), old) in positions.iter().zip(old_data.iter_mut()) {
            let m = self.member_idx_data(set_idx, *pos);
            let pd = self.members[m]
                .as_ref()
                .expect("PDW path requires all data PDs healthy");
            read_ops.push(StripRead {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: old,
            });
        }
        let pm = self.member_idx_p(set_idx);
        let pd_p = self.members[pm]
            .as_ref()
            .expect("PDW path requires P healthy");
        read_ops.push(StripRead {
            pd: pd_p.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &mut p,
        });
        let qm = self.member_idx_q(set_idx);
        let pd_q = self.members[qm]
            .as_ref()
            .expect("PDW path requires Q healthy");
        read_ops.push(StripRead {
            pd: pd_q.clone(),
            chunklet_index: self.desc.members[qm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &mut q,
        });
        let read_result = parallel_strip_reads(&mut read_ops);
        drop(read_ops);
        if let Err(e) = read_result {
            if is_runtime_read_fault(&e) {
                // A modified position's old data, or old P/Q, faulted (dead-but-
                // not-yet-isolated). PDW's delta can't be applied, but RW
                // recomputes P and Q from scratch WITHOUT reading old parity, and
                // reconstructs any faulting base strip within R6's budget of 2.
                // RW runs under the SAME held stripe lock (no re-lock). A dead
                // member's follow-on write is absorbed + suspected by RW's
                // submit_set_absorb.
                return self.write_partial_stripe_rw(set_idx, strip_base, positions, buf, budget);
            }
            return Err(e);
        }

        // Same SIMD folding as the batched PDW arm: materialize `d = old ^ new`
        // once, then let `xor_into` / `mul_xor_into` do the vector work instead
        // of a byte-at-a-time `gf256::mul`.
        let mut d_scratch = vec![0u8; strip];
        for ((pos, off, range), old_data) in positions.iter().zip(old_data.iter()) {
            let new_data = &buf[range.clone()];
            let len = new_data.len();
            let g_i = gf256::g_pow(*pos);
            let d = &mut d_scratch[..len];
            d.copy_from_slice(new_data);
            gf256::xor_into(d, old_data);
            gf256::xor_into(&mut delta_p[(*off as usize)..(*off as usize) + len], d);
            gf256::mul_xor_into(&mut delta_q[(*off as usize)..(*off as usize) + len], d, g_i);
        }
        gf256::xor_into(&mut p, &delta_p);
        gf256::xor_into(&mut q, &delta_q);

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 2);
        for (pos, off, range) in positions {
            let m = self.member_idx_data(set_idx, *pos);
            let pd = self.members[m]
                .as_ref()
                .expect("PDW path requires all data PDs healthy");
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: &buf[range.clone()],
            });
        }
        ops.push(StripWrite {
            pd: pd_p.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &p,
        });
        ops.push(StripWrite {
            pd: pd_q.clone(),
            chunklet_index: self.desc.members[qm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &q,
        });
        // Healthy-set path (budget 2): the new P/Q reflect the new data, so a
        // ≤2-member EIO here still reconstructs the modified data on read.
        self.submit_set_absorb(ops, budget)
    }

    /// Reconstruct-write: build full new strips for every data position
    /// (read unmodified, reconstruct unmodified-failed via available
    /// parity, copy modified bytes), recompute P + Q from scratch, write
    /// modified data + parities. Skips writes to failed PDs.
    ///
    /// Handles every degraded sub-case the redundancy budget allows
    /// (F ≤ 2 with at least one parity surviving). Caller routes the
    /// double-parity-failed case to `write_data_only`.
    ///
    /// Full-strip reads/reconstructs use `strip_base`; modified data writes
    /// use `strip_base + off` per position; P and Q write the full strip
    /// at `strip_base`.
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
        let mut copy_after_reads: Vec<(usize, usize, std::ops::Range<usize>)> = Vec::new();
        let mut read_ops: Vec<StripRead> = Vec::with_capacity(k);
        // (set_idx, data_pos) aligned to `read_ops`, so a runtime read fault can
        // be reconstructed by position via `reconstruct_read_batch` instead of
        // surfacing the EIO (md "compute, don't read a faulty device").
        let mut read_ctxs: Vec<(usize, usize)> = Vec::with_capacity(k);
        for (pos, strip_buf) in new_strips.iter_mut().enumerate() {
            let pd_failed = self.members[self.member_idx_data(set_idx, pos)].is_none();
            match modified_map.get(&pos) {
                Some((off, range)) => {
                    let new_data = &buf[range.clone()];
                    let off = *off as usize;
                    let len = new_data.len();
                    if off == 0 && len == strip {
                        strip_buf.copy_from_slice(new_data);
                    } else {
                        if pd_failed {
                            self.reconstruct_unmodified_data(set_idx, pos, strip_base, strip_buf)?;
                            strip_buf[off..off + len].copy_from_slice(new_data);
                        } else {
                            let m = self.member_idx_data(set_idx, pos);
                            let pd = self.members[m]
                                .as_ref()
                                .expect("RW direct-read path requires data PD healthy");
                            read_ops.push(StripRead {
                                pd: pd.clone(),
                                chunklet_index: self.desc.members[m].chunklet_index,
                                in_chunklet_off: strip_base,
                                data: strip_buf,
                            });
                            read_ctxs.push((set_idx, pos));
                            copy_after_reads.push((pos, off, range.clone()));
                        }
                    }
                }
                None => {
                    if pd_failed {
                        self.reconstruct_unmodified_data(set_idx, pos, strip_base, strip_buf)?;
                    } else {
                        let m = self.member_idx_data(set_idx, pos);
                        let pd = self.members[m]
                            .as_ref()
                            .expect("RW direct-read path requires data PD healthy");
                        read_ops.push(StripRead {
                            pd: pd.clone(),
                            chunklet_index: self.desc.members[m].chunklet_index,
                            in_chunklet_off: strip_base,
                            data: strip_buf,
                        });
                        read_ctxs.push((set_idx, pos));
                    }
                }
            }
        }
        // A direct base-strip read hit a member that is faulting at runtime but
        // not yet isolated (still `is_some`). Reconstruct the faulting position(s)
        // from survivors within R6's budget of 2 — never surface the EIO for a
        // recoverable stripe. Same held stripe lock; reconstruct reads survivors
        // directly (no stripe lock) so there is no re-lock/deadlock.
        match parallel_strip_reads(&mut read_ops) {
            Ok(()) => {}
            Err(e) if is_runtime_read_fault(&e) => {
                self.reconstruct_read_batch(&mut read_ops, &read_ctxs)?
            }
            Err(e) => return Err(e),
        }
        drop(read_ops);
        for (pos, off, range) in copy_after_reads {
            let new_data = &buf[range];
            new_strips[pos][off..off + new_data.len()].copy_from_slice(new_data);
        }

        let mut p = vec![0u8; strip];
        let mut q = vec![0u8; strip];
        for (i, s) in new_strips.iter().enumerate() {
            gf256::xor_into(&mut p, s);
            gf256::mul_xor_into(&mut q, s, gf256::g_pow(i));
        }

        let mut ops: Vec<StripWrite> = Vec::with_capacity(positions.len() + 2);
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
        let pm = self.member_idx_p(set_idx);
        if let Some(pd) = &self.members[pm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[pm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &p,
            });
        }
        let qm = self.member_idx_q(set_idx);
        if let Some(pd) = &self.members[qm] {
            ops.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[qm].chunklet_index,
                in_chunklet_off: strip_base,
                data: &q,
            });
        }
        self.submit_set_absorb(ops, budget)?;
        // Mirror to online-rebuild shadow(s) below the cursor. RW built the full
        // new strip for every data position plus P and Q.
        self.write_forward(set_idx, strip_base, |pos| {
            if pos < self.data_per_set {
                new_strips[pos].clone()
            } else if pos == self.data_per_set {
                p.clone()
            } else {
                q.clone()
            }
        });
        Ok(())
    }
}
