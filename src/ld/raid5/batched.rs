//! RAID5 batched multi-op writer (the flusher hot path) + its planner types.
//! Split out of the parent module for the file-size limit.
use super::*;

/// Which healthy-set write path a batched segment takes. Degraded segments
/// bail the whole batch to the serial `write_at` path (which owns the
/// reconstruct plumbing) before reaching here.
#[derive(Clone, Copy)]
enum Kind5 {
    Full,
    Rmw,
    Rw,
}

/// One planned stripe segment of a batched `write_many_at`. Owns the scratch
/// buffers that span the read → compute → write phases so the whole batch
/// issues ONE `parallel_strip_reads` + ONE `parallel_strip_writes` instead of a
/// serial submit pair per segment (the flusher→RAID de-batching that turned µs
/// disk IO into ~200 ms wall-clock — see `LdRaid6::write_many_batched`).
struct Seg5<'a> {
    set_idx: usize,
    strip_base: u64,
    kind: Kind5,
    /// (data_pos, in_strip_off, new_data) per modified position, stripe order.
    mods: Vec<(usize, u64, &'a [u8])>,
    /// New parity strip. Full/Rw compute it from scratch; Rmw reads the old
    /// value here then applies the delta in place.
    parity: Vec<u8>,
    /// Rmw only: old data per modified position (each sized to its mod's len).
    old_data: Vec<Vec<u8>>,
    /// Rw only: K full new-data strips, parity recomputed from them.
    new_strips: Vec<Vec<u8>>,
}

impl LdRaid5 {
    /// Batched multi-op writer — the flusher's hot path. Groups every op's
    /// stripe segments BY physical stripe (merging colliding segments into one
    /// write), then collapses the batch into a single cross-PD read submit + a
    /// single write submit under one stripe-lock acquisition. Merging turns
    /// dense sub-stripe units into zero-RMW full-stripe writes. Bails to serial
    /// `write_at` only for a degraded set or a byte-overlapping stripe.
    pub(super) fn write_many_batched(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.ensure_aligned(*offset, buf.len())?;
        }
        let strip = self.strip_bytes as usize;
        let k = self.data_per_set;

        // Phase 0: decompose every op into per-position mods, GROUPED by
        // physical stripe. Segments colliding on one stripe are MERGED into a
        // single stripe write (their disjoint sub-ranges combine) instead of
        // bailing the whole batch to serial; a fully-covered stripe is promoted
        // to a zero-RMW full-stripe write. See LdRaid6::write_many_batched.
        struct RawStripe<'a> {
            set_idx: usize,
            strip_base: u64,
            mods: Vec<(usize, u64, &'a [u8])>,
        }
        let mut by_stripe: std::collections::BTreeMap<u64, RawStripe> =
            std::collections::BTreeMap::new();
        let mut serialize = false;
        'outer: for (offset, buf) in ops {
            let mut remaining = buf.len();
            let mut cursor = *offset;
            let mut buf_start = 0usize;
            while remaining > 0 {
                let addr = self.locate(cursor);
                let stripe_remain = self.full_stripe_bytes
                    - (addr.data_pos as u64 * self.strip_bytes + addr.in_strip_off);
                let take = std::cmp::min(remaining as u64, stripe_remain) as usize;
                let seg_buf = &buf[buf_start..buf_start + take];

                // Degraded set → the serial path owns reconstruct reads.
                // Rebuilding/rebalancing set → only the serial path write_forwards
                // to the shadow (the batched path never does), so bail there to
                // keep the shadow in sync below the cursor.
                if !self.failed_data_positions(addr.set_idx).is_empty()
                    || self.parity_failed(addr.set_idx)
                    || self.set_being_rebuilt(addr.set_idx)
                {
                    serialize = true;
                    break 'outer;
                }

                let strip_base = addr.in_chunklet_off - addr.in_strip_off;
                let key = self.stripe_key(addr.set_idx, strip_base);
                let entry = by_stripe.entry(key).or_insert_with(|| RawStripe {
                    set_idx: addr.set_idx,
                    strip_base,
                    mods: Vec::new(),
                });
                let mut consumed = 0usize;
                let mut cur_pos = addr.data_pos;
                let mut cur_off = addr.in_strip_off;
                while consumed < seg_buf.len() {
                    let strip_remain = self.strip_bytes - cur_off;
                    let t = std::cmp::min((seg_buf.len() - consumed) as u64, strip_remain) as usize;
                    entry
                        .mods
                        .push((cur_pos, cur_off, &seg_buf[consumed..consumed + t]));
                    consumed += t;
                    cur_pos += 1;
                    cur_off = 0;
                }

                buf_start += take;
                cursor += take as u64;
                remaining -= take;
            }
        }

        if serialize {
            for (offset, buf) in ops {
                self.write_at(*offset, buf)?;
            }
            return Ok(());
        }

        // Phase 0b: one merged Seg5 per stripe. Byte-disjointness check (overlap
        // → serial last-writer-wins). Promote a fully-covered stripe to Full
        // (zero RMW); never promote a partially-covered stripe (would clobber
        // unmodified positions).
        let mut segs: Vec<Seg5> = Vec::with_capacity(by_stripe.len());
        let mut stripe_keys: Vec<u64> = Vec::with_capacity(by_stripe.len());
        for (key, raw) in by_stripe {
            let mut spans: Vec<(u64, u64)> = raw
                .mods
                .iter()
                .map(|(p, off, nd)| {
                    let s = *p as u64 * self.strip_bytes + off;
                    (s, s + nd.len() as u64)
                })
                .collect();
            spans.sort_unstable_by_key(|(s, _)| *s);
            if spans.windows(2).any(|w| w[0].1 > w[1].0) {
                for (offset, buf) in ops {
                    self.write_at(*offset, buf)?;
                }
                return Ok(());
            }

            let mut clean_full = raw.mods.len() == k;
            if clean_full {
                let mut seen = vec![false; k];
                for (p, off, nd) in &raw.mods {
                    if *off != 0 || nd.len() != strip || *p >= k || seen[*p] {
                        clean_full = false;
                        break;
                    }
                    seen[*p] = true;
                }
            }
            let kind = if clean_full {
                Kind5::Full
            } else {
                // Same RMW-vs-RW threshold as the serial path, on merged mods:
                // RW only wins when every mod is full-strip and (K-M) < (M+1).
                let m = raw.mods.len();
                let all_full_strip = raw
                    .mods
                    .iter()
                    .all(|(_p, off, nd)| *off == 0 && nd.len() as u64 == self.strip_bytes);
                if all_full_strip && (k - m.min(k)) < (m + 1) {
                    Kind5::Rw
                } else {
                    Kind5::Rmw
                }
            };
            let old_data = match kind {
                Kind5::Rmw => raw
                    .mods
                    .iter()
                    .map(|(_p, _o, nd)| vec![0u8; nd.len()])
                    .collect(),
                _ => Vec::new(),
            };
            let new_strips = match kind {
                Kind5::Rw => (0..k).map(|_| vec![0u8; strip]).collect(),
                _ => Vec::new(),
            };
            stripe_keys.push(key);
            segs.push(Seg5 {
                set_idx: raw.set_idx,
                strip_base: raw.strip_base,
                kind,
                mods: raw.mods,
                parity: vec![0u8; strip],
                old_data,
                new_strips,
            });
        }

        let guards = self.stripe_locks.write_keys(&stripe_keys);

        // Phase 1: one batched read submit for every segment's RMW reads.
        let mut reads: Vec<StripRead> = Vec::new();
        for seg in segs.iter_mut() {
            self.r5_collect_reads(seg, &mut reads);
        }
        let read_result = parallel_strip_reads(&mut reads);
        drop(reads);
        if let Err(e) = read_result {
            // A member faulted on a Phase-1 old-data/parity read (dead-but-not-
            // yet-isolated). We hold the batch's stripe write locks, so we MUST
            // NOT re-enter write_at here (it re-locks the same buckets →
            // self-deadlock). Drop the guards first, then replay serially: each
            // stripe re-locks independently and write_one_stripe_segment handles
            // the fault via RMW→RW reconstruct. The flusher's spans are disjoint,
            // so dropping the batch guards loses no cross-stripe consistency.
            if is_runtime_read_fault(&e) {
                drop(guards);
                for (offset, buf) in ops {
                    self.write_at(*offset, buf)?;
                }
                return Ok(());
            }
            return Err(e);
        }

        // Phase 2: recompute parity per segment from the freshly-read state.
        for seg in segs.iter_mut() {
            self.r5_compute(seg);
        }

        // Phase 3: one batched write submit for all data + parity strips, with
        // inline-degrade. Every segment here is HEALTHY (degraded sets bailed to
        // the serial `write_at` above), so each is its own redundancy group with
        // a budget of 1 runtime member failure.
        let mut writes: Vec<StripWrite> = Vec::new();
        let mut group_of: Vec<u32> = Vec::new();
        for (gi, seg) in segs.iter().enumerate() {
            self.r5_collect_writes(seg, &mut writes);
            group_of.resize(writes.len(), gi as u32);
        }
        let max_fail = vec![1u32; segs.len()];
        let results = submit_strip_writes_detailed(&writes);
        let suspects = absorb_degraded(&writes, &results, &group_of, &max_fail)?;
        self.report_suspects(suspects);
        Ok(())
    }

    /// Append `seg`'s RMW reads (into its owned scratch) to the shared batch.
    fn r5_collect_reads<'s>(&self, seg: &'s mut Seg5<'_>, reads: &mut Vec<StripRead<'s>>) {
        let set_idx = seg.set_idx;
        let strip_base = seg.strip_base;
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind5::Full => {}
            Kind5::Rmw => {
                let Seg5 {
                    mods,
                    old_data,
                    parity,
                    ..
                } = &mut *seg;
                for ((pos, off, _nd), old) in mods.iter().zip(old_data.iter_mut()) {
                    let m = self.member_idx_data(set_idx, *pos);
                    let pd = self.members[m].as_ref().expect("healthy RMW data PD");
                    reads.push(StripRead {
                        pd: pd.clone(),
                        chunklet_index: self.desc.members[m].chunklet_index,
                        in_chunklet_off: strip_base + off,
                        data: old.as_mut_slice(),
                    });
                }
                let pm = self.member_idx_parity(set_idx);
                let pdp = self.members[pm].as_ref().expect("healthy RMW parity");
                reads.push(StripRead {
                    pd: pdp.clone(),
                    chunklet_index: self.desc.members[pm].chunklet_index,
                    in_chunklet_off: strip_base,
                    data: parity.as_mut_slice(),
                });
            }
            Kind5::Rw => {
                let Seg5 {
                    mods, new_strips, ..
                } = &mut *seg;
                for (pos, strip_buf) in new_strips.iter_mut().enumerate() {
                    let full_mod = mods
                        .iter()
                        .any(|(p, off, nd)| *p == pos && *off == 0 && nd.len() == strip);
                    if full_mod {
                        continue;
                    }
                    let m = self.member_idx_data(set_idx, pos);
                    let pd = self.members[m].as_ref().expect("healthy RW data PD");
                    reads.push(StripRead {
                        pd: pd.clone(),
                        chunklet_index: self.desc.members[m].chunklet_index,
                        in_chunklet_off: strip_base,
                        data: strip_buf.as_mut_slice(),
                    });
                }
            }
        }
    }

    /// Recompute the segment's parity from the read-phase results.
    fn r5_compute(&self, seg: &mut Seg5<'_>) {
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind5::Full => {
                seg.parity.iter_mut().for_each(|b| *b = 0);
                for &(_pos, _off, nd) in &seg.mods {
                    xor_into(&mut seg.parity, nd);
                }
            }
            Kind5::Rmw => {
                let mut delta = vec![0u8; strip];
                for ((_pos, off, nd), old) in seg.mods.iter().zip(seg.old_data.iter()) {
                    let off = *off as usize;
                    for i in 0..nd.len() {
                        delta[off + i] ^= old[i] ^ nd[i];
                    }
                }
                xor_into(&mut seg.parity, &delta);
            }
            Kind5::Rw => {
                for &(pos, off, nd) in &seg.mods {
                    let off = off as usize;
                    if off == 0 && nd.len() == strip {
                        seg.new_strips[pos].copy_from_slice(nd);
                    } else {
                        seg.new_strips[pos][off..off + nd.len()].copy_from_slice(nd);
                    }
                }
                seg.parity.iter_mut().for_each(|b| *b = 0);
                for s in seg.new_strips.iter() {
                    xor_into(&mut seg.parity, s);
                }
            }
        }
    }

    /// Append `seg`'s data + parity writes to the shared batch. Data payloads
    /// borrow the caller's buffers; parity borrows the computed scratch.
    fn r5_collect_writes<'s>(&self, seg: &'s Seg5<'s>, writes: &mut Vec<StripWrite<'s>>) {
        let set_idx = seg.set_idx;
        let strip_base = seg.strip_base;
        for &(pos, off, nd) in &seg.mods {
            let m = self.member_idx_data(set_idx, pos);
            let pd = self.members[m].as_ref().expect("healthy write data PD");
            writes.push(StripWrite {
                pd: pd.clone(),
                chunklet_index: self.desc.members[m].chunklet_index,
                in_chunklet_off: strip_base + off,
                data: nd,
            });
        }
        let pm = self.member_idx_parity(set_idx);
        let pdp = self.members[pm].as_ref().expect("healthy write parity");
        writes.push(StripWrite {
            pd: pdp.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &seg.parity,
        });
    }
}
