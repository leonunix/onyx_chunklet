//! RAID6 batched multi-op writer (the flusher hot path) + its planner types.
//! Split out of the parent module for the file-size limit.
use super::*;

/// Which healthy-set write path a batched segment takes. Degraded segments
/// never reach here — [`LdRaid6::write_many_batched`] bails the whole batch to
/// the serial `write_at` path, which owns the reconstruct plumbing.
#[derive(Clone, Copy)]
enum Kind6 {
    Full,
    Pdw,
    Rw,
}

/// One planned stripe segment of a batched `write_many_at`. Owns the scratch
/// buffers that span the read → compute → write phases so the whole batch
/// issues ONE `parallel_strip_reads` + ONE `parallel_strip_writes` instead of a
/// serial submit pair per segment. That per-segment serialization was the
/// flusher→RAID6 de-batching that turned µs disk IO into ~200 ms wall-clock:
/// onyx hands one deep-QD batch to `write_many_at`, and the trait-default
/// `for op { write_at(op) }` loop unrolled it into N serial read→compute→write
/// RMW chains, so the disk queue never went deep.
struct Seg6<'a> {
    set_idx: usize,
    strip_base: u64,
    kind: Kind6,
    /// (data_pos, in_strip_off, new_data) per modified position, stripe order.
    mods: Vec<(usize, u64, &'a [u8])>,
    /// New P / Q strip. Full/Rw compute it from scratch; Pdw reads the old
    /// value here then applies the delta in place.
    p: Vec<u8>,
    q: Vec<u8>,
    /// Pdw only: old data per modified position (each sized to its mod's len).
    old_data: Vec<Vec<u8>>,
    /// Rw only: K full new-data strips, parity recomputed from them.
    new_strips: Vec<Vec<u8>>,
}

impl LdRaid6 {
    /// Batched multi-op writer — the flusher's hot path. Groups every op's
    /// stripe segments BY physical stripe (merging segments that collide on one
    /// stripe instead of bailing), then collapses the whole batch into a single
    /// cross-PD read submit + a single write submit under one stripe-lock
    /// acquisition. Merging turns the flusher's dense sub-stripe units into
    /// zero-RMW full-stripe writes and eliminates the old duplicate-stripe
    /// serial fallback. Bails the whole batch to the serial `write_at` loop only
    /// for a degraded set (which needs reconstruct reads) or a byte-overlapping
    /// stripe (two ops writing the same bytes — last-writer-wins for safety).
    pub(super) fn write_many_batched(&self, ops: &[(u64, &[u8])]) -> ChunkletResult<()> {
        for (offset, buf) in ops {
            self.ensure_aligned(*offset, buf.len())?;
        }
        let strip = self.strip_bytes as usize;
        let k = self.data_per_set;

        // Phase 0: decompose every op into per-position mods, GROUPED by
        // physical stripe. Two segments landing on one stripe (dense-PBA
        // packing, ~13% of flusher batches) are MERGED into a single stripe
        // write instead of bailing the whole batch to serial: their disjoint
        // sub-ranges combine, and a stripe that ends up fully covered is
        // promoted to a zero-RMW full-stripe write (the dense sub-stripe units
        // the flusher emits become full stripes at 4 KiB strip). BTreeMap keyed
        // by stripe_key keeps segment/lock order deterministic.
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
                    || self.parity_p_failed(addr.set_idx)
                    || self.parity_q_failed(addr.set_idx)
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
                // Split this segment into per-position mods (same walk as
                // `write_one_stripe_segment`) and append to the stripe's list.
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

        // Phase 0b: one merged Seg6 per stripe. Verify the stripe's mods are
        // byte-disjoint (distinct sub-PBAs guarantee this; an overlap would mean
        // two ops writing the same bytes — bail to serial last-writer-wins for
        // safety). Promote a fully-covered stripe to Full (zero RMW); otherwise
        // classify PDW/RW on the merged mods with the same read-cost heuristic
        // as the serial path. Never promote a partially-covered stripe to Full
        // (that would clobber the unmodified positions — chunklet invariant).
        let mut segs: Vec<Seg6> = Vec::with_capacity(by_stripe.len());
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

            // Clean full stripe: exactly one full-strip mod per data position.
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
                Kind6::Full
            } else {
                // Same PDW-vs-RW read-cost heuristic as `write_one_stripe_segment`,
                // evaluated on the merged mods.
                let modified_full = raw
                    .mods
                    .iter()
                    .filter(|(_p, off, nd)| *off == 0 && nd.len() as u64 == self.strip_bytes)
                    .count();
                let modified_partial = raw.mods.len() - modified_full;
                let pdw_reads = raw.mods.len() + 2;
                let rw_reads = k - modified_full.min(k) + modified_partial;
                if rw_reads < pdw_reads {
                    Kind6::Rw
                } else {
                    Kind6::Pdw
                }
            };
            let old_data = match kind {
                Kind6::Pdw => raw
                    .mods
                    .iter()
                    .map(|(_p, _o, nd)| vec![0u8; nd.len()])
                    .collect(),
                _ => Vec::new(),
            };
            let new_strips = match kind {
                Kind6::Rw => (0..k).map(|_| vec![0u8; strip]).collect(),
                _ => Vec::new(),
            };
            stripe_keys.push(key);
            segs.push(Seg6 {
                set_idx: raw.set_idx,
                strip_base: raw.strip_base,
                kind,
                mods: raw.mods,
                p: vec![0u8; strip],
                q: vec![0u8; strip],
                old_data,
                new_strips,
            });
        }

        // Hold every touched stripe lock across read+compute+write, acquired in
        // one globally-sorted batch (matches `write_key`'s bucket order).
        let guards = self.stripe_locks.write_keys(&stripe_keys);

        // Phase 1: one batched read submit for every segment's RMW reads.
        let mut reads: Vec<StripRead> = Vec::new();
        for seg in segs.iter_mut() {
            self.r6_collect_reads(seg, &mut reads);
        }
        let read_result = parallel_strip_reads(&mut reads);
        drop(reads);
        if let Err(e) = read_result {
            // A member faulted on a Phase-1 old-data/P/Q read (dead-but-not-yet-
            // isolated). We hold the batch's stripe write locks; re-entering
            // write_at here would re-lock the same buckets (self-deadlock). Drop
            // the guards, then replay serially — each stripe re-locks on its own
            // and write_one_stripe_segment handles the fault via PDW→RW. The
            // flusher's spans are disjoint, so dropping the batch guards loses no
            // cross-stripe consistency.
            if is_runtime_read_fault(&e) {
                drop(guards);
                for (offset, buf) in ops {
                    self.write_at(*offset, buf)?;
                }
                return Ok(());
            }
            return Err(e);
        }

        // Phase 2: recompute P/Q per segment from the freshly-read state.
        for seg in segs.iter_mut() {
            self.r6_compute(seg);
        }

        // Phase 3: one batched write submit for all data + parity strips, with
        // inline-degrade. Every segment here is HEALTHY (degraded sets bailed to
        // the serial `write_at` above), so each is its own redundancy group with
        // a budget of 2 runtime member failures.
        let mut writes: Vec<StripWrite> = Vec::new();
        let mut group_of: Vec<u32> = Vec::new();
        for (gi, seg) in segs.iter().enumerate() {
            self.r6_collect_writes(seg, &mut writes);
            // Tag every op this seg just appended with the seg's group index.
            group_of.resize(writes.len(), gi as u32);
        }
        let max_fail = vec![2u32; segs.len()];
        let results = submit_strip_writes_detailed(&writes);
        let suspects = absorb_degraded(&writes, &results, &group_of, &max_fail)?;
        self.report_suspects(suspects);
        Ok(())
    }

    /// Append `seg`'s RMW reads (into its owned scratch) to the shared batch.
    fn r6_collect_reads<'s>(&self, seg: &'s mut Seg6<'_>, reads: &mut Vec<StripRead<'s>>) {
        let set_idx = seg.set_idx;
        let strip_base = seg.strip_base;
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind6::Full => {}
            Kind6::Pdw => {
                let Seg6 {
                    mods,
                    old_data,
                    p,
                    q,
                    ..
                } = &mut *seg;
                for ((pos, off, _nd), old) in mods.iter().zip(old_data.iter_mut()) {
                    let m = self.member_idx_data(set_idx, *pos);
                    let pd = self.members[m].as_ref().expect("healthy PDW data PD");
                    reads.push(StripRead {
                        pd: pd.clone(),
                        chunklet_index: self.desc.members[m].chunklet_index,
                        in_chunklet_off: strip_base + off,
                        data: old.as_mut_slice(),
                    });
                }
                let pm = self.member_idx_p(set_idx);
                let pdp = self.members[pm].as_ref().expect("healthy PDW P");
                reads.push(StripRead {
                    pd: pdp.clone(),
                    chunklet_index: self.desc.members[pm].chunklet_index,
                    in_chunklet_off: strip_base,
                    data: p.as_mut_slice(),
                });
                let qm = self.member_idx_q(set_idx);
                let pdq = self.members[qm].as_ref().expect("healthy PDW Q");
                reads.push(StripRead {
                    pd: pdq.clone(),
                    chunklet_index: self.desc.members[qm].chunklet_index,
                    in_chunklet_off: strip_base,
                    data: q.as_mut_slice(),
                });
            }
            Kind6::Rw => {
                let Seg6 {
                    mods, new_strips, ..
                } = &mut *seg;
                for (pos, strip_buf) in new_strips.iter_mut().enumerate() {
                    // Fully-modified positions are filled from new data in the
                    // compute phase; every other position reads its base strip.
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

    /// Recompute the segment's P/Q from the read-phase results.
    fn r6_compute(&self, seg: &mut Seg6<'_>) {
        let strip = self.strip_bytes as usize;
        match seg.kind {
            Kind6::Full => {
                seg.p.iter_mut().for_each(|b| *b = 0);
                seg.q.iter_mut().for_each(|b| *b = 0);
                for &(pos, _off, nd) in &seg.mods {
                    gf256::xor_into(&mut seg.p, nd);
                    gf256::mul_xor_into(&mut seg.q, nd, gf256::g_pow(pos));
                }
            }
            Kind6::Pdw => {
                let mut delta_p = vec![0u8; strip];
                let mut delta_q = vec![0u8; strip];
                // Materialize `d = old ^ new` once per position so both deltas
                // fold it through the same SIMD primitives Full/Rw already use.
                // The byte-at-a-time `gf256::mul` this replaces was the single
                // largest CPU item on the small-write RMW path (perf on
                // nvme-box: r6_compute 16.3% + gf256::mul 6.1% of all cycles at
                // 208 k ops/s), and it left `mul_avx*_calls` at exactly 0.
                // Scratch is hoisted so the arm still allocates only the two
                // delta strips per segment.
                let mut d = vec![0u8; strip];
                for ((pos, off, nd), old) in seg.mods.iter().zip(seg.old_data.iter()) {
                    let g_i = gf256::g_pow(*pos);
                    let off = *off as usize;
                    let len = nd.len();
                    let d = &mut d[..len];
                    d.copy_from_slice(nd);
                    gf256::xor_into(d, old);
                    gf256::xor_into(&mut delta_p[off..off + len], d);
                    gf256::mul_xor_into(&mut delta_q[off..off + len], d, g_i);
                }
                gf256::xor_into(&mut seg.p, &delta_p);
                gf256::xor_into(&mut seg.q, &delta_q);
            }
            Kind6::Rw => {
                for &(pos, off, nd) in &seg.mods {
                    let off = off as usize;
                    if off == 0 && nd.len() == strip {
                        seg.new_strips[pos].copy_from_slice(nd);
                    } else {
                        seg.new_strips[pos][off..off + nd.len()].copy_from_slice(nd);
                    }
                }
                seg.p.iter_mut().for_each(|b| *b = 0);
                seg.q.iter_mut().for_each(|b| *b = 0);
                for (i, s) in seg.new_strips.iter().enumerate() {
                    gf256::xor_into(&mut seg.p, s);
                    gf256::mul_xor_into(&mut seg.q, s, gf256::g_pow(i));
                }
            }
        }
    }

    /// Append `seg`'s data + P + Q writes to the shared batch. Data payloads
    /// borrow the caller's buffers; P/Q borrow the segment's computed scratch.
    fn r6_collect_writes<'s>(&self, seg: &'s Seg6<'s>, writes: &mut Vec<StripWrite<'s>>) {
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
        let pm = self.member_idx_p(set_idx);
        let pdp = self.members[pm].as_ref().expect("healthy write P");
        writes.push(StripWrite {
            pd: pdp.clone(),
            chunklet_index: self.desc.members[pm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &seg.p,
        });
        let qm = self.member_idx_q(set_idx);
        let pdq = self.members[qm].as_ref().expect("healthy write Q");
        writes.push(StripWrite {
            pd: pdq.clone(),
            chunklet_index: self.desc.members[qm].chunklet_index,
            in_chunklet_off: strip_base,
            data: &seg.q,
        });
    }
}
