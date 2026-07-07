//! RAID5 reconstruct primitives (XOR parity): degraded reads + rebuild.
//! Split out of the parent module for the file-size limit; `impl LdRaid5`
//! methods that see the parent's private items.
use super::*;

impl LdRaid5 {
    /// Encode the parity strip from all K data strips at `in_chunklet_off`.
    /// Used by rebuild when the parity chunklet itself is what was lost.
    pub fn encode_parity_strip(
        &self,
        set_idx: usize,
        in_chunklet_off: u64,
        out: &mut [u8],
    ) -> ChunkletResult<()> {
        out.fill(0);
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            xor_into(out, &tmp);
        }
        Ok(())
    }

    /// How many strips fit in one chunklet at the current strip size.
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
    /// path based on whether `failed_member_idx` lands on data or parity.
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
            // Parity slot.
            self.encode_parity_strip(set_idx, in_chunklet_off, out)
        } else {
            self.reconstruct_data(set_idx, pos, in_chunklet_off, out)
        }
    }

    /// Reconstruct one missing data position by XORing the surviving K-1
    /// data strips with the parity strip. Used by degraded read + rebuild.
    ///
    /// `out.len()` may be any block-aligned length up to `chunklet_user_size`;
    /// reads pull the same length from each surviving member at
    /// `in_chunklet_off`. Rebuild calls this with megabyte-sized buffers to
    /// minimize per-strip overhead.
    pub fn reconstruct_data(
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
        // Start with parity, XOR every surviving data strip in.
        self.read_parity_strip(set_idx, in_chunklet_off, out)?;
        let mut tmp = vec![0u8; out.len()];
        for pos in 0..self.data_per_set {
            if pos == missing_data_pos {
                continue;
            }
            self.read_data_strip(set_idx, pos, in_chunklet_off, &mut tmp)?;
            xor_into(out, &tmp);
        }
        Ok(())
    }

    /// Fallback for the fast batched read (`read_many_at`) when a member returns
    /// a runtime read fault: re-read each full-strip op, reconstructing any that
    /// fault from parity + surviving data (R5 budget 1). Good ops just re-read
    /// (bounded extra IO on the error path). A 2nd fault in a set — an
    /// open-failed data member, or a fault on parity/another strip during
    /// reconstruct — surfaces the error (over budget). Suspects are reported on
    /// both the Ok and Err paths so an over-budget read still drives isolation.
    pub(super) fn reconstruct_read_batch(
        &self,
        reads: &mut [StripRead<'_>],
        ctxs: &[(usize, usize)],
    ) -> ChunkletResult<()> {
        debug_assert_eq!(reads.len(), ctxs.len());
        let mut suspects: Vec<SuspectMember> = Vec::new();
        let mut result = Ok(());
        for (r, &(set_idx, data_pos)) in reads.iter_mut().zip(ctxs) {
            match self.read_data_strip(set_idx, data_pos, r.in_chunklet_off, r.data) {
                Ok(()) => {}
                Err(e) if is_runtime_read_fault(&e) => {
                    if !self.failed_data_positions(set_idx).is_empty() {
                        result = Err(e);
                        break;
                    }
                    if let Err(e2) =
                        self.reconstruct_data(set_idx, data_pos, r.in_chunklet_off, r.data)
                    {
                        result = Err(e2);
                        break;
                    }
                    let m = self.member_idx_data(set_idx, data_pos);
                    if let Some(pd) = self.members[m].as_ref() {
                        let pd_id = pd.pd_id();
                        if !suspects.iter().any(|s| s.pd_id == pd_id) {
                            suspects.push(SuspectMember {
                                pd_id,
                                chunklet_index: self.desc.members[m].chunklet_index,
                            });
                        }
                    }
                }
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        self.report_suspects(suspects);
        result
    }
}
