//! RAID6 reconstruct primitives (Anvin P/Q): degraded reads + rebuild.
//! Split out of the parent module to keep it under the file-size limit;
//! these are `impl LdRaid6` methods and see the parent's private items.
use super::*;

impl LdRaid6 {
    pub fn encode_p_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        out.fill(0);
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::xor_into(out, &tmp);
        }
        Ok(())
    }

    /// Encode Q from all K data strips at `in_chunklet_off`.
    pub fn encode_q_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        out.fill(0);
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::mul_xor_into(out, &tmp, gf256::g_pow(pos));
        }
        Ok(())
    }

    pub fn stripes_per_chunklet(&self) -> u64 {
        CHUNKLET_USER_BYTES / self.strip_bytes
    }

    pub fn strip_bytes(&self) -> u64 {
        self.strip_bytes
    }

    pub fn data_per_set(&self) -> usize {
        self.data_per_set
    }

    /// Generic per-member rebuild helper: dispatches to the right reconstruct
    /// path based on member role + how many failures share this set.
    pub fn reconstruct_member_strip(
        &self,
        failed_member_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        let n = self.desc.set_size as usize;
        let set_idx = failed_member_idx / n;
        let pos = failed_member_idx % n;
        if pos == self.data_per_set {
            self.encode_p_strip(set_idx, in_chunklet_off, out)
        } else if pos == self.data_per_set + 1 {
            self.encode_q_strip(set_idx, in_chunklet_off, out)
        } else {
            // Data position. Check how many data positions in this set are
            // failed: 1 → use P; 2 → use PQ-solve and pick the right output.
            let failed = self.failed_data_positions(set_idx);
            if failed.len() == 1 && failed[0] == pos {
                self.reconstruct_one_data(set_idx, pos, in_chunklet_off, out)
            } else if failed.len() == 2 && failed.contains(&pos) {
                let (x, y) = (failed[0], failed[1]);
                let (dx, dy) =
                    self.reconstruct_two_data(set_idx, x, y, in_chunklet_off, out.len())?;
                if pos == x {
                    out.copy_from_slice(&dx);
                } else {
                    out.copy_from_slice(&dy);
                }
                Ok(())
            } else {
                Err(ChunkletError::Invariant(format!(
                    "Raid6 set {} has {} failed data positions (need <= 2 with this position included)",
                    set_idx,
                    failed.len()
                )))
            }
        }
    }

    /// Reconstruct one missing data position via P (XOR of surviving data
    /// strips with P). Identical to RAID-5's reconstruct.
    pub fn reconstruct_one_data(
        &self,
        set_idx: usize,
        missing_data_pos: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        if missing_data_pos >= self.data_per_set {
            return Err(ChunkletError::Invariant(format!(
                "missing_data_pos {} >= data_per_set {}",
                missing_data_pos, self.data_per_set
            )));
        }
        self.read_p(set_idx, in_chunklet_off, out)?;
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            if pos == missing_data_pos {
                continue;
            }
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::xor_into(out, &tmp);
        }
        Ok(())
    }

    /// Reconstruct one missing data position via Q (used when P is the
    /// missing parity).
    ///
    /// `Q = Σ g^i · D_i` ⇒  `g^x · D_x = Q ⊕ Σ_{i≠x} g^i · D_i`
    /// ⇒  `D_x = g^(-x) · (Q ⊕ Σ_{i≠x} g^i · D_i)`
    pub fn reconstruct_one_data_via_q(
        &self,
        set_idx: usize,
        missing_data_pos: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        if missing_data_pos >= self.data_per_set {
            return Err(ChunkletError::Invariant(format!(
                "missing_data_pos {} >= data_per_set {}",
                missing_data_pos, self.data_per_set
            )));
        }
        self.read_q(set_idx, in_chunklet_off, out)?;
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            if pos == missing_data_pos {
                continue;
            }
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            gf256::mul_xor_into(out, &tmp, gf256::g_pow(pos));
        }
        // out now = g^x · D_x; multiply by g^(-x) = inv(g^x).
        let g_neg_x = gf256::inv(gf256::g_pow(missing_data_pos));
        for byte in out.iter_mut() {
            *byte = gf256::mul(*byte, g_neg_x);
        }
        Ok(())
    }

    /// Reconstruct one unmodified-failed data position. Picks the cheapest
    /// surviving parity (P preferred over Q since XOR is cheaper than the
    /// Q-based formula). Used by the partial-stripe RW write path when
    /// computing new parity needs the old value of a failed PD's strip.
    pub(super) fn reconstruct_unmodified_data(
        &self,
        set_idx: usize,
        missing_data_pos: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        let failed = self.failed_data_positions(set_idx);
        let p_ok = !self.parity_p_failed(set_idx);
        let q_ok = !self.parity_q_failed(set_idx);

        if failed.len() == 2 {
            // Both data losses share this set — single-parity reconstruct
            // can't isolate them. Use the PQ system (needs both parities).
            if !(p_ok && q_ok) {
                return Err(ChunkletError::Invariant(format!(
                    "Raid6 set {}: 2 data losses + parity loss exceeds redundancy",
                    set_idx
                )));
            }
            let (x, y) = (failed[0], failed[1]);
            let (dx, dy) = self.reconstruct_two_data(set_idx, x, y, in_chunklet_off, out.len())?;
            if missing_data_pos == x {
                out.copy_from_slice(&dx);
            } else if missing_data_pos == y {
                out.copy_from_slice(&dy);
            } else {
                return Err(ChunkletError::Invariant(format!(
                    "reconstruct_unmodified_data pos {} not in failed set {:?}",
                    missing_data_pos, failed
                )));
            }
            Ok(())
        } else if p_ok {
            self.reconstruct_one_data(set_idx, missing_data_pos, in_chunklet_off, out)
        } else if q_ok {
            self.reconstruct_one_data_via_q(set_idx, missing_data_pos, in_chunklet_off, out)
        } else {
            Err(ChunkletError::Invariant(format!(
                "Raid6 set {}: cannot reconstruct (no surviving parity)",
                set_idx
            )))
        }
    }

    /// Reconstruct two missing data positions x < y via the PQ system.
    /// Outputs `(D_x, D_y)`.
    pub fn reconstruct_two_data(
        &self,
        set_idx: usize,
        x: usize,
        y: usize,
        in_chunklet_off: u64,
        strip_len: usize,
    ) -> ChunkletResult<(Vec<u8>, Vec<u8>)> {
        if !(x < y && y < self.data_per_set) {
            return Err(ChunkletError::Invariant(format!(
                "two-failure positions invalid: x={} y={} K={}",
                x, y, self.data_per_set
            )));
        }
        // Compute Pd = ⊕ surviving D_i ⊕ P,  Qd = ⊕ g^i·surviving D_i ⊕ Q.
        let mut pd = vec![0u8; strip_len];
        let mut qd = vec![0u8; strip_len];
        let mut tmp = vec![0u8; strip_len];

        self.read_p(set_idx, in_chunklet_off, &mut pd)?;
        self.read_q(set_idx, in_chunklet_off, &mut qd)?;
        for i in 0..self.data_per_set {
            if i == x || i == y {
                continue;
            }
            self.read_data_strip(set_idx, i, in_chunklet_off, &mut tmp)?;
            gf256::xor_into(&mut pd, &tmp);
            gf256::mul_xor_into(&mut qd, &tmp, gf256::g_pow(i));
        }

        // Solve:
        //   Pd = D_x ⊕ D_y
        //   Qd = g^x · D_x ⊕ g^y · D_y
        // ⇒ D_x = (g^(-y) · Qd ⊕ g^(x-y) · Pd) · 1 / (g^(x-y) ⊕ 1)  -- nope, let me redo
        //
        // From Pd: D_y = D_x ⊕ Pd
        // Substitute into Qd:
        //   Qd = g^x · D_x ⊕ g^y · (D_x ⊕ Pd)
        //      = (g^x ⊕ g^y) · D_x ⊕ g^y · Pd
        // ⇒ D_x = (Qd ⊕ g^y · Pd) · 1 / (g^x ⊕ g^y)
        let gx = gf256::g_pow(x);
        let gy = gf256::g_pow(y);
        let denom = gx ^ gy;
        let denom_inv = gf256::inv(denom);

        let mut dx = vec![0u8; strip_len];
        for i in 0..strip_len {
            let qbyte = qd[i] ^ gf256::mul(gy, pd[i]);
            dx[i] = gf256::mul(qbyte, denom_inv);
        }
        let mut dy = pd; // re-use
                         // dy currently = Pd; we want dy = D_x ⊕ Pd. So XOR dx into dy.
        gf256::xor_into(&mut dy, &dx);

        Ok((dx, dy))
    }
}
