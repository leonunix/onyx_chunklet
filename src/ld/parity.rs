//! Multi-strip RAID6 syndrome generation in a single pass over the data.
//!
//! `gf256` owns the field arithmetic and the two-operand primitives
//! (`xor_into`, `mul_xor_into`). This module owns the *shape* of a syndrome
//! computation across K strips at once, which is a memory-traffic problem
//! rather than an arithmetic one.
//!
//! Accumulating P and Q with the two-operand primitives costs one
//! read-modify-write of the parity strip per data position, per syndrome. For
//! K=8 and a 128 KiB strip that is ~6.25 MiB of traffic to produce 1 MiB of
//! payload — data read twice (once for P, once for Q), P and Q each read and
//! written eight times, plus two full-strip zeroing passes. Measured on
//! nvme-box that left the AVX512 kernels running at ~1.06 GB/s per core: the
//! cores were waiting on memory, not on the multiplier.
//!
//! Walking columns instead — hold the P and Q accumulators in vector registers
//! across the K loop, store each once — costs ~1.25 MiB for the same 1 MiB of
//! payload, and needs no zeroing because the first store *writes* rather than
//! accumulates.
//!
//! Concurrency: pure functions over caller-owned slices, no shared state
//! beyond the relaxed counters.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::gf256;

/// Fused single-pass syndrome generation. `false` falls back to accumulating
/// with the two-operand `gf256` primitives — the pre-fusion behaviour — so the
/// two shapes can be A/B'd **inside one process against one pool**. That matters
/// here: on nvme-box a fresh pool carries an uncontrolled burst phase, so
/// restarting per arm measures the burst rather than the knob.
static FUSED: AtomicBool = AtomicBool::new(true);

/// Applies process-wide to every subsequent syndrome computation.
pub fn set_fused(enabled: bool) {
    FUSED.store(enabled, Ordering::Relaxed);
}

/// Diagnostic override, same shape as `CHUNKLET_WRITEV_COALESCE`: lets the whole
/// suite be re-run against the legacy path (`CHUNKLET_FUSED_PARITY=0 cargo test
/// --release`). Overrides the runtime setter, so production must not set it.
fn fused_env_override() -> Option<bool> {
    static OVERRIDE: std::sync::OnceLock<Option<bool>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("CHUNKLET_FUSED_PARITY")
            .ok()
            .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "no" | "off"))
    })
}

pub fn fused_enabled() -> bool {
    fused_env_override().unwrap_or_else(|| FUSED.load(Ordering::Relaxed))
}

/// Positions per set that the vector paths keep table state for. RAID6 sets are
/// far smaller than this in practice; a wider set falls back to the two-operand
/// primitives, which are correct but pay the extra traffic.
const MAX_VECTOR_STRIPS: usize = 32;

static ENCODE_CALLS_AVX512: AtomicU64 = AtomicU64::new(0);
static ENCODE_CALLS_AVX2: AtomicU64 = AtomicU64::new(0);
static ENCODE_CALLS_SCALAR: AtomicU64 = AtomicU64::new(0);
static ENCODE_CALLS_LEGACY: AtomicU64 = AtomicU64::new(0);
static ENCODE_SRC_BYTES: AtomicU64 = AtomicU64::new(0);
static DELTA_CALLS_AVX512: AtomicU64 = AtomicU64::new(0);
static DELTA_CALLS_AVX2: AtomicU64 = AtomicU64::new(0);
static DELTA_CALLS_SCALAR: AtomicU64 = AtomicU64::new(0);
static DELTA_CALLS_LEGACY: AtomicU64 = AtomicU64::new(0);
static DELTA_SRC_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default)]
pub struct ParityStats {
    pub encode_calls_avx512: u64,
    pub encode_calls_avx2: u64,
    pub encode_calls_scalar: u64,
    pub encode_calls_legacy: u64,
    pub encode_src_bytes: u64,
    pub delta_calls_avx512: u64,
    pub delta_calls_avx2: u64,
    pub delta_calls_scalar: u64,
    pub delta_calls_legacy: u64,
    pub delta_src_bytes: u64,
}

pub fn stats_snapshot() -> ParityStats {
    ParityStats {
        encode_calls_avx512: ENCODE_CALLS_AVX512.load(Ordering::Relaxed),
        encode_calls_avx2: ENCODE_CALLS_AVX2.load(Ordering::Relaxed),
        encode_calls_scalar: ENCODE_CALLS_SCALAR.load(Ordering::Relaxed),
        encode_calls_legacy: ENCODE_CALLS_LEGACY.load(Ordering::Relaxed),
        encode_src_bytes: ENCODE_SRC_BYTES.load(Ordering::Relaxed),
        delta_calls_avx512: DELTA_CALLS_AVX512.load(Ordering::Relaxed),
        delta_calls_avx2: DELTA_CALLS_AVX2.load(Ordering::Relaxed),
        delta_calls_scalar: DELTA_CALLS_SCALAR.load(Ordering::Relaxed),
        delta_calls_legacy: DELTA_CALLS_LEGACY.load(Ordering::Relaxed),
        delta_src_bytes: DELTA_SRC_BYTES.load(Ordering::Relaxed),
    }
}

/// `P = ⊕ Dᵢ` and `Q = ⊕ gᵢ·Dᵢ`, computed in one pass over `data`.
///
/// `data` is `(strip, g^pos)` in any order; every strip must be `p.len()` long.
/// `p` and `q` are **written**, not accumulated, so the caller must not
/// pre-zero them. An empty `data` yields all-zero syndromes.
pub fn encode_pq(p: &mut [u8], q: &mut [u8], data: &[(&[u8], u8)]) {
    debug_assert_eq!(p.len(), q.len());
    debug_assert!(data.iter().all(|(s, _)| s.len() == p.len()));
    if data.is_empty() {
        p.fill(0);
        q.fill(0);
        return;
    }
    ENCODE_SRC_BYTES.fetch_add((data.len() as u64) * (p.len() as u64), Ordering::Relaxed);
    if !fused_enabled() {
        ENCODE_CALLS_LEGACY.fetch_add(1, Ordering::Relaxed);
        encode_pq_two_operand(p, q, data);
        return;
    }
    if data.len() <= MAX_VECTOR_STRIPS {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if std::arch::is_x86_feature_detected!("avx512f")
                && std::arch::is_x86_feature_detected!("avx512bw")
            {
                ENCODE_CALLS_AVX512.fetch_add(1, Ordering::Relaxed);
                unsafe { x86::encode_pq_avx512(p, q, data) };
                return;
            }
            if std::arch::is_x86_feature_detected!("avx2") {
                ENCODE_CALLS_AVX2.fetch_add(1, Ordering::Relaxed);
                unsafe { x86::encode_pq_avx2(p, q, data) };
                return;
            }
        }
    }
    ENCODE_CALLS_SCALAR.fetch_add(1, Ordering::Relaxed);
    encode_pq_scalar_from(p, q, data, 0);
}

/// Fold one modified position's parity delta straight into the old syndromes:
/// `p ^= new ⊕ old` and `q ^= g·(new ⊕ old)`, in one pass.
///
/// All four slices are the same length — the caller passes the sub-slices for
/// this position's byte range, so a partial-strip write folds without touching
/// the untouched bytes. `d = new ⊕ old` never reaches memory.
pub fn accumulate_delta_pq(p: &mut [u8], q: &mut [u8], new: &[u8], old: &[u8], g: u8) {
    debug_assert_eq!(p.len(), q.len());
    debug_assert_eq!(p.len(), new.len());
    debug_assert_eq!(p.len(), old.len());
    if p.is_empty() {
        return;
    }
    DELTA_SRC_BYTES.fetch_add(2 * p.len() as u64, Ordering::Relaxed);
    if !fused_enabled() {
        DELTA_CALLS_LEGACY.fetch_add(1, Ordering::Relaxed);
        accumulate_delta_pq_two_operand(p, q, new, old, g);
        return;
    }
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
        {
            DELTA_CALLS_AVX512.fetch_add(1, Ordering::Relaxed);
            unsafe { x86::accumulate_delta_pq_avx512(p, q, new, old, g) };
            return;
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            DELTA_CALLS_AVX2.fetch_add(1, Ordering::Relaxed);
            unsafe { x86::accumulate_delta_pq_avx2(p, q, new, old, g) };
            return;
        }
    }
    DELTA_CALLS_SCALAR.fetch_add(1, Ordering::Relaxed);
    accumulate_delta_pq_scalar_from(p, q, new, old, g, 0);
}

/// Pre-fusion shape: one read-modify-write of the parity strip per data
/// position, per syndrome, plus the zeroing pass those require. Retained as the
/// A/B arm and as the correctness oracle — it is what produced every `Q`
/// already on disk, so the fused kernels must match it byte for byte.
fn encode_pq_two_operand(p: &mut [u8], q: &mut [u8], data: &[(&[u8], u8)]) {
    p.fill(0);
    q.fill(0);
    for &(src, g) in data {
        gf256::xor_into(p, src);
        gf256::mul_xor_into(q, src, g);
    }
}

/// Pre-fusion shape for the delta path. `d = new ^ old` is materialised to
/// memory so both syndromes can fold it through the two-operand primitives.
fn accumulate_delta_pq_two_operand(p: &mut [u8], q: &mut [u8], new: &[u8], old: &[u8], g: u8) {
    let mut d = new.to_vec();
    gf256::xor_into(&mut d, old);
    gf256::xor_into(p, &d);
    gf256::mul_xor_into(q, &d, g);
}

/// Reference implementation and vector tail. Processes `base..p.len()`.
fn encode_pq_scalar_from(p: &mut [u8], q: &mut [u8], data: &[(&[u8], u8)], base: usize) {
    for i in base..p.len() {
        let mut acc_p = 0u8;
        let mut acc_q = 0u8;
        for &(src, g) in data {
            let b = src[i];
            acc_p ^= b;
            acc_q ^= gf256::mul(g, b);
        }
        p[i] = acc_p;
        q[i] = acc_q;
    }
}

fn accumulate_delta_pq_scalar_from(
    p: &mut [u8],
    q: &mut [u8],
    new: &[u8],
    old: &[u8],
    g: u8,
    base: usize,
) {
    for i in base..p.len() {
        let d = new[i] ^ old[i];
        p[i] ^= d;
        q[i] ^= gf256::mul(g, d);
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod x86 {
    use super::{encode_pq_scalar_from, MAX_VECTOR_STRIPS};
    use crate::ld::gf256::repeated_nibble_mul_table;

    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    /// Per-position nibble tables are built once here, outside the column loop.
    /// Rebuilding them per column would reintroduce exactly the per-position
    /// overhead this module exists to remove.
    #[target_feature(enable = "avx512f,avx512bw")]
    pub(super) unsafe fn encode_pq_avx512(p: &mut [u8], q: &mut [u8], data: &[(&[u8], u8)]) {
        let mut lo_tbl = [_mm512_setzero_si512(); MAX_VECTOR_STRIPS];
        let mut hi_tbl = [_mm512_setzero_si512(); MAX_VECTOR_STRIPS];
        for (k, &(_, g)) in data.iter().enumerate() {
            let lo = repeated_nibble_mul_table::<64>(g, 0);
            let hi = repeated_nibble_mul_table::<64>(g, 4);
            lo_tbl[k] = _mm512_loadu_si512(lo.as_ptr() as *const __m512i);
            hi_tbl[k] = _mm512_loadu_si512(hi.as_ptr() as *const __m512i);
        }
        let mask = _mm512_set1_epi8(0x0f);
        let len = p.len();
        let mut i = 0usize;
        while i + 64 <= len {
            let mut acc_p = _mm512_setzero_si512();
            let mut acc_q = _mm512_setzero_si512();
            for (k, &(src, _)) in data.iter().enumerate() {
                let s = _mm512_loadu_si512(src.as_ptr().add(i) as *const __m512i);
                acc_p = _mm512_xor_si512(acc_p, s);
                let s_lo = _mm512_and_si512(s, mask);
                let s_hi = _mm512_and_si512(_mm512_srli_epi16(s, 4), mask);
                let prod = _mm512_xor_si512(
                    _mm512_shuffle_epi8(lo_tbl[k], s_lo),
                    _mm512_shuffle_epi8(hi_tbl[k], s_hi),
                );
                acc_q = _mm512_xor_si512(acc_q, prod);
            }
            _mm512_storeu_si512(p.as_mut_ptr().add(i) as *mut __m512i, acc_p);
            _mm512_storeu_si512(q.as_mut_ptr().add(i) as *mut __m512i, acc_q);
            i += 64;
        }
        if i < len {
            encode_pq_scalar_from(p, q, data, i);
        }
    }

    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn encode_pq_avx2(p: &mut [u8], q: &mut [u8], data: &[(&[u8], u8)]) {
        let mut lo_tbl = [_mm256_setzero_si256(); MAX_VECTOR_STRIPS];
        let mut hi_tbl = [_mm256_setzero_si256(); MAX_VECTOR_STRIPS];
        for (k, &(_, g)) in data.iter().enumerate() {
            let lo = repeated_nibble_mul_table::<32>(g, 0);
            let hi = repeated_nibble_mul_table::<32>(g, 4);
            lo_tbl[k] = _mm256_loadu_si256(lo.as_ptr() as *const __m256i);
            hi_tbl[k] = _mm256_loadu_si256(hi.as_ptr() as *const __m256i);
        }
        let mask = _mm256_set1_epi8(0x0f);
        let len = p.len();
        let mut i = 0usize;
        while i + 32 <= len {
            let mut acc_p = _mm256_setzero_si256();
            let mut acc_q = _mm256_setzero_si256();
            for (k, &(src, _)) in data.iter().enumerate() {
                let s = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
                acc_p = _mm256_xor_si256(acc_p, s);
                let s_lo = _mm256_and_si256(s, mask);
                let s_hi = _mm256_and_si256(_mm256_srli_epi16(s, 4), mask);
                let prod = _mm256_xor_si256(
                    _mm256_shuffle_epi8(lo_tbl[k], s_lo),
                    _mm256_shuffle_epi8(hi_tbl[k], s_hi),
                );
                acc_q = _mm256_xor_si256(acc_q, prod);
            }
            _mm256_storeu_si256(p.as_mut_ptr().add(i) as *mut __m256i, acc_p);
            _mm256_storeu_si256(q.as_mut_ptr().add(i) as *mut __m256i, acc_q);
            i += 32;
        }
        if i < len {
            encode_pq_scalar_from(p, q, data, i);
        }
    }

    #[target_feature(enable = "avx512f,avx512bw")]
    pub(super) unsafe fn accumulate_delta_pq_avx512(
        p: &mut [u8],
        q: &mut [u8],
        new: &[u8],
        old: &[u8],
        g: u8,
    ) {
        let lo = repeated_nibble_mul_table::<64>(g, 0);
        let hi = repeated_nibble_mul_table::<64>(g, 4);
        let lo_tbl = _mm512_loadu_si512(lo.as_ptr() as *const __m512i);
        let hi_tbl = _mm512_loadu_si512(hi.as_ptr() as *const __m512i);
        let mask = _mm512_set1_epi8(0x0f);
        let len = p.len();
        let mut i = 0usize;
        while i + 64 <= len {
            let n = _mm512_loadu_si512(new.as_ptr().add(i) as *const __m512i);
            let o = _mm512_loadu_si512(old.as_ptr().add(i) as *const __m512i);
            let d = _mm512_xor_si512(n, o);
            let pv = _mm512_loadu_si512(p.as_ptr().add(i) as *const __m512i);
            _mm512_storeu_si512(
                p.as_mut_ptr().add(i) as *mut __m512i,
                _mm512_xor_si512(pv, d),
            );
            let d_lo = _mm512_and_si512(d, mask);
            let d_hi = _mm512_and_si512(_mm512_srli_epi16(d, 4), mask);
            let prod = _mm512_xor_si512(
                _mm512_shuffle_epi8(lo_tbl, d_lo),
                _mm512_shuffle_epi8(hi_tbl, d_hi),
            );
            let qv = _mm512_loadu_si512(q.as_ptr().add(i) as *const __m512i);
            _mm512_storeu_si512(
                q.as_mut_ptr().add(i) as *mut __m512i,
                _mm512_xor_si512(qv, prod),
            );
            i += 64;
        }
        if i < len {
            super::accumulate_delta_pq_scalar_from(p, q, new, old, g, i);
        }
    }

    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn accumulate_delta_pq_avx2(
        p: &mut [u8],
        q: &mut [u8],
        new: &[u8],
        old: &[u8],
        g: u8,
    ) {
        let lo = repeated_nibble_mul_table::<32>(g, 0);
        let hi = repeated_nibble_mul_table::<32>(g, 4);
        let lo_tbl = _mm256_loadu_si256(lo.as_ptr() as *const __m256i);
        let hi_tbl = _mm256_loadu_si256(hi.as_ptr() as *const __m256i);
        let mask = _mm256_set1_epi8(0x0f);
        let len = p.len();
        let mut i = 0usize;
        while i + 32 <= len {
            let n = _mm256_loadu_si256(new.as_ptr().add(i) as *const __m256i);
            let o = _mm256_loadu_si256(old.as_ptr().add(i) as *const __m256i);
            let d = _mm256_xor_si256(n, o);
            let pv = _mm256_loadu_si256(p.as_ptr().add(i) as *const __m256i);
            _mm256_storeu_si256(
                p.as_mut_ptr().add(i) as *mut __m256i,
                _mm256_xor_si256(pv, d),
            );
            let d_lo = _mm256_and_si256(d, mask);
            let d_hi = _mm256_and_si256(_mm256_srli_epi16(d, 4), mask);
            let prod = _mm256_xor_si256(
                _mm256_shuffle_epi8(lo_tbl, d_lo),
                _mm256_shuffle_epi8(hi_tbl, d_hi),
            );
            let qv = _mm256_loadu_si256(q.as_ptr().add(i) as *const __m256i);
            _mm256_storeu_si256(
                q.as_mut_ptr().add(i) as *mut __m256i,
                _mm256_xor_si256(qv, prod),
            );
            i += 32;
        }
        if i < len {
            super::accumulate_delta_pq_scalar_from(p, q, new, old, g, i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The oracles ARE the shipped legacy arm, not a copy of it — a second
    // transcription could drift from the path the A/B actually selects.
    use super::accumulate_delta_pq_two_operand as delta_oracle;
    use super::encode_pq_two_operand as encode_pq_oracle;

    /// Deterministic, dependency-free byte stream — xorshift64* so the test does
    /// not need the `rand` dev-dependency to be seeded identically.
    fn stream(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x >> 12;
                x ^= x << 25;
                x ^= x >> 27;
                (x.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 56) as u8
            })
            .collect()
    }

    #[test]
    fn encode_matches_two_operand_oracle() {
        // Lengths straddle the 64 B and 32 B vector widths so the scalar tail is
        // exercised, and K sweeps past a single-strip set up to a wide one.
        for len in [1usize, 31, 32, 33, 63, 64, 65, 127, 128, 191, 4096, 4160] {
            for k in 1..=16usize {
                let strips: Vec<Vec<u8>> = (0..k)
                    .map(|i| stream(0x9e37 + (i as u64) * 7919 + len as u64, len))
                    .collect();
                let data: Vec<(&[u8], u8)> = strips
                    .iter()
                    .enumerate()
                    .map(|(i, s)| (s.as_slice(), gf256::g_pow(i)))
                    .collect();
                let mut p = vec![0xAAu8; len];
                let mut q = vec![0x55u8; len];
                let mut want_p = vec![0u8; len];
                let mut want_q = vec![0u8; len];
                encode_pq(&mut p, &mut q, &data);
                encode_pq_oracle(&mut want_p, &mut want_q, &data);
                assert_eq!(p, want_p, "P mismatch len={len} k={k}");
                assert_eq!(q, want_q, "Q mismatch len={len} k={k}");
            }
        }
    }

    #[test]
    fn encode_writes_without_pre_zeroing() {
        // The callers stopped zeroing; prove a dirty destination is fully
        // overwritten rather than accumulated into.
        let len = 256;
        let s = stream(1, len);
        let data = [(s.as_slice(), gf256::g_pow(0))];
        let mut p = vec![0xFFu8; len];
        let mut q = vec![0xFFu8; len];
        encode_pq(&mut p, &mut q, &data);
        assert_eq!(p, s);
        let mut want_q = vec![0u8; len];
        gf256::mul_xor_into(&mut want_q, &s, gf256::g_pow(0));
        assert_eq!(q, want_q);
    }

    #[test]
    fn encode_of_nothing_is_zero() {
        let mut p = vec![0xFFu8; 96];
        let mut q = vec![0xFFu8; 96];
        encode_pq(&mut p, &mut q, &[]);
        assert!(p.iter().all(|&b| b == 0));
        assert!(q.iter().all(|&b| b == 0));
    }

    #[test]
    fn delta_matches_two_operand_oracle() {
        for len in [1usize, 31, 32, 33, 63, 64, 65, 127, 4096] {
            for pos in 0..4usize {
                let new = stream(0x1234 + len as u64, len);
                let old = stream(0x5678 + len as u64 + pos as u64, len);
                let base_p = stream(0x11, len);
                let base_q = stream(0x22, len);
                let g = gf256::g_pow(pos);
                let mut p = base_p.clone();
                let mut q = base_q.clone();
                let mut want_p = base_p.clone();
                let mut want_q = base_q.clone();
                accumulate_delta_pq(&mut p, &mut q, &new, &old, g);
                delta_oracle(&mut want_p, &mut want_q, &new, &old, g);
                assert_eq!(p, want_p, "delta P mismatch len={len} pos={pos}");
                assert_eq!(q, want_q, "delta Q mismatch len={len} pos={pos}");
            }
        }
    }

    /// A partial-strip write must leave the bytes outside its range untouched —
    /// this is the invariant that makes folding straight into the old syndrome
    /// (instead of into a full-strip delta buffer) equivalent.
    #[test]
    fn delta_on_a_sub_range_leaves_neighbours_alone() {
        let strip = 512usize;
        let off = 64usize;
        let len = 128usize;
        let new = stream(7, len);
        let old = stream(9, len);
        let g = gf256::g_pow(3);
        let mut p = stream(11, strip);
        let mut q = stream(13, strip);
        let before_p = p.clone();
        let before_q = q.clone();
        accumulate_delta_pq(
            &mut p[off..off + len],
            &mut q[off..off + len],
            &new,
            &old,
            g,
        );
        assert_eq!(p[..off], before_p[..off]);
        assert_eq!(p[off + len..], before_p[off + len..]);
        assert_eq!(q[..off], before_q[..off]);
        assert_eq!(q[off + len..], before_q[off + len..]);
        let mut want_p = before_p[off..off + len].to_vec();
        let mut want_q = before_q[off..off + len].to_vec();
        delta_oracle(&mut want_p, &mut want_q, &new, &old, g);
        assert_eq!(&p[off..off + len], want_p.as_slice());
        assert_eq!(&q[off..off + len], want_q.as_slice());
    }

    /// The rollback arm has to produce the same bytes as the fused one, or the
    /// knob is not a knob. Serialised through the process-global flag, so this
    /// test sets it back before returning.
    #[test]
    fn legacy_arm_and_fused_arm_agree() {
        assert!(
            fused_env_override().is_none(),
            "unset CHUNKLET_FUSED_PARITY to run this test"
        );
        let len = 320usize;
        let strips: Vec<Vec<u8>> = (0..6).map(|i| stream(200 + i, len)).collect();
        let data: Vec<(&[u8], u8)> = strips
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_slice(), gf256::g_pow(i)))
            .collect();
        let mut fused_p = vec![0u8; len];
        let mut fused_q = vec![0u8; len];
        set_fused(true);
        encode_pq(&mut fused_p, &mut fused_q, &data);
        let mut legacy_p = vec![0u8; len];
        let mut legacy_q = vec![0u8; len];
        set_fused(false);
        encode_pq(&mut legacy_p, &mut legacy_q, &data);
        set_fused(true);
        assert_eq!(fused_p, legacy_p);
        assert_eq!(fused_q, legacy_q);
    }

    /// The scalar path is the oracle for the vector paths, so it has to be
    /// checked against the primitives on its own too.
    #[test]
    fn scalar_path_matches_oracle() {
        let len = 200usize;
        let strips: Vec<Vec<u8>> = (0..5).map(|i| stream(100 + i, len)).collect();
        let data: Vec<(&[u8], u8)> = strips
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_slice(), gf256::g_pow(i)))
            .collect();
        let mut p = vec![0u8; len];
        let mut q = vec![0u8; len];
        encode_pq_scalar_from(&mut p, &mut q, &data, 0);
        let mut want_p = vec![0u8; len];
        let mut want_q = vec![0u8; len];
        encode_pq_oracle(&mut want_p, &mut want_q, &data);
        assert_eq!(p, want_p);
        assert_eq!(q, want_q);
    }
}
