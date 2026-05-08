//! GF(2^8) arithmetic for RAID-6 parity (Anvin convention).
//!
//! Field: GF(2^8) with primitive polynomial `x^8 + x^4 + x^3 + x^2 + 1`
//! (`0x11d`), generator `g = 2`. Used by `LdRaid6` to compute `Q` parity
//! and to reconstruct missing strips.
//!
//! Operations:
//! - `mul_by_g(byte)` — multiply by g (i.e. shift-left, conditionally XOR with 0x1d)
//! - `mul(a, b)` — full multiplication via `log + antilog` tables
//! - `inv(a)` — multiplicative inverse via `log` (a != 0)
//! - `g_pow(i)` — `g^i` (precomputed)
//!
//! All tables are computed at startup via `lazy_static`-equivalent
//! `OnceLock` so we don't pay LUT init at every encode.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

const POLY: u16 = 0x11d;

static XOR_AVX512_CALLS: AtomicU64 = AtomicU64::new(0);
static XOR_AVX512_BYTES: AtomicU64 = AtomicU64::new(0);
static XOR_AVX2_CALLS: AtomicU64 = AtomicU64::new(0);
static XOR_AVX2_BYTES: AtomicU64 = AtomicU64::new(0);
static XOR_SCALAR_CALLS: AtomicU64 = AtomicU64::new(0);
static XOR_SCALAR_BYTES: AtomicU64 = AtomicU64::new(0);
static MUL_AVX512_CALLS: AtomicU64 = AtomicU64::new(0);
static MUL_AVX512_BYTES: AtomicU64 = AtomicU64::new(0);
static MUL_AVX2_CALLS: AtomicU64 = AtomicU64::new(0);
static MUL_AVX2_BYTES: AtomicU64 = AtomicU64::new(0);
static MUL_SCALAR_CALLS: AtomicU64 = AtomicU64::new(0);
static MUL_SCALAR_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default)]
pub struct Gf256Stats {
    pub xor_avx512_calls: u64,
    pub xor_avx512_bytes: u64,
    pub xor_avx2_calls: u64,
    pub xor_avx2_bytes: u64,
    pub xor_scalar_calls: u64,
    pub xor_scalar_bytes: u64,
    pub mul_avx512_calls: u64,
    pub mul_avx512_bytes: u64,
    pub mul_avx2_calls: u64,
    pub mul_avx2_bytes: u64,
    pub mul_scalar_calls: u64,
    pub mul_scalar_bytes: u64,
}

pub fn stats_snapshot() -> Gf256Stats {
    Gf256Stats {
        xor_avx512_calls: XOR_AVX512_CALLS.load(Ordering::Relaxed),
        xor_avx512_bytes: XOR_AVX512_BYTES.load(Ordering::Relaxed),
        xor_avx2_calls: XOR_AVX2_CALLS.load(Ordering::Relaxed),
        xor_avx2_bytes: XOR_AVX2_BYTES.load(Ordering::Relaxed),
        xor_scalar_calls: XOR_SCALAR_CALLS.load(Ordering::Relaxed),
        xor_scalar_bytes: XOR_SCALAR_BYTES.load(Ordering::Relaxed),
        mul_avx512_calls: MUL_AVX512_CALLS.load(Ordering::Relaxed),
        mul_avx512_bytes: MUL_AVX512_BYTES.load(Ordering::Relaxed),
        mul_avx2_calls: MUL_AVX2_CALLS.load(Ordering::Relaxed),
        mul_avx2_bytes: MUL_AVX2_BYTES.load(Ordering::Relaxed),
        mul_scalar_calls: MUL_SCALAR_CALLS.load(Ordering::Relaxed),
        mul_scalar_bytes: MUL_SCALAR_BYTES.load(Ordering::Relaxed),
    }
}

/// Multiply x by g (i.e. by 2) in GF(2^8) with primitive poly 0x11d.
#[inline]
pub fn mul_by_g(x: u8) -> u8 {
    let high_bit = (x & 0x80) != 0;
    let mut r = (x << 1) as u16;
    if high_bit {
        r ^= POLY;
    }
    r as u8
}

/// Tables: log[a] gives the discrete log base g, antilog[i] = g^i.
struct Tables {
    log: [u8; 256],
    antilog: [u8; 512], // duplicated to avoid mod 255 in hot path
}

fn init_tables() -> Tables {
    let mut log = [0u8; 256];
    let mut antilog = [0u8; 512];
    let mut x: u16 = 1;
    for i in 0..255u16 {
        antilog[i as usize] = x as u8;
        log[x as usize] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= POLY;
        }
    }
    // Duplicate so a + b in [0, 510] indexes safely.
    for i in 0..255 {
        antilog[i + 255] = antilog[i];
    }
    // log[0] is undefined; leave as 0. Callers must not call mul/inv with 0
    // expecting log lookup to make sense.
    Tables { log, antilog }
}

fn tables() -> &'static Tables {
    static T: OnceLock<Tables> = OnceLock::new();
    T.get_or_init(init_tables)
}

/// GF(2^8) multiplication via log/antilog tables. Returns 0 if either operand
/// is 0.
#[inline]
pub fn mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    let t = tables();
    let i = t.log[a as usize] as usize + t.log[b as usize] as usize;
    t.antilog[i]
}

/// Multiplicative inverse in GF(2^8). Panics if `a == 0` (the operation is
/// undefined in the field). Release builds panic too — silently returning a
/// bogus value would let an upstream invariant violation produce wrong
/// reconstruct math without any signal.
#[inline]
pub fn inv(a: u8) -> u8 {
    if a == 0 {
        panic!("gf256::inv(0) is undefined");
    }
    let t = tables();
    t.antilog[(255 - t.log[a as usize] as usize) % 255]
}

/// `g^i` for i in 0..255.
#[inline]
pub fn g_pow(i: usize) -> u8 {
    let t = tables();
    t.antilog[i % 255]
}

/// XOR `src` into `dst` byte-wise.
#[inline]
pub fn xor_into(dst: &mut [u8], src: &[u8]) {
    debug_assert_eq!(dst.len(), src.len());
    if try_xor_into_avx512(dst, src) {
        return;
    }
    if try_xor_into_avx2(dst, src) {
        return;
    }
    xor_into_scalar(dst, src);
}

#[inline]
fn xor_into_scalar(dst: &mut [u8], src: &[u8]) {
    XOR_SCALAR_CALLS.fetch_add(1, Ordering::Relaxed);
    XOR_SCALAR_BYTES.fetch_add(dst.len() as u64, Ordering::Relaxed);
    for i in 0..dst.len() {
        dst[i] ^= src[i];
    }
}

/// Multiply every byte of `src` by `c` and XOR into `dst`. Hot path for Q
/// updates; could be SIMD-accelerated later.
#[inline]
pub fn mul_xor_into(dst: &mut [u8], src: &[u8], c: u8) {
    debug_assert_eq!(dst.len(), src.len());
    if c == 0 {
        return;
    }
    if c == 1 {
        xor_into(dst, src);
        return;
    }
    if try_mul_xor_into_avx512(dst, src, c) {
        return;
    }
    if try_mul_xor_into_avx2(dst, src, c) {
        return;
    }
    mul_xor_into_scalar(dst, src, c);
}

#[inline]
fn mul_xor_into_scalar(dst: &mut [u8], src: &[u8], c: u8) {
    MUL_SCALAR_CALLS.fetch_add(1, Ordering::Relaxed);
    MUL_SCALAR_BYTES.fetch_add(dst.len() as u64, Ordering::Relaxed);
    let t = tables();
    let log_c = t.log[c as usize] as usize;
    for i in 0..src.len() {
        let s = src[i];
        if s != 0 {
            let log_s = t.log[s as usize] as usize;
            dst[i] ^= t.antilog[log_c + log_s];
        }
    }
}

#[inline]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn try_xor_into_avx512(dst: &mut [u8], src: &[u8]) -> bool {
    if dst.len() < 128
        || !std::is_x86_feature_detected!("avx512f")
        || !std::is_x86_feature_detected!("avx512bw")
    {
        return false;
    }
    unsafe { xor_into_avx512(dst, src) };
    true
}

#[inline]
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn try_xor_into_avx512(_dst: &mut [u8], _src: &[u8]) -> bool {
    false
}

#[inline]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn try_xor_into_avx2(dst: &mut [u8], src: &[u8]) -> bool {
    if dst.len() < 64 || !std::is_x86_feature_detected!("avx2") {
        return false;
    }
    unsafe { xor_into_avx2(dst, src) };
    true
}

#[inline]
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn try_xor_into_avx2(_dst: &mut [u8], _src: &[u8]) -> bool {
    false
}

#[inline]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn try_mul_xor_into_avx512(dst: &mut [u8], src: &[u8], c: u8) -> bool {
    if dst.len() < 128
        || !std::is_x86_feature_detected!("avx512f")
        || !std::is_x86_feature_detected!("avx512bw")
    {
        return false;
    }
    unsafe { mul_xor_into_avx512(dst, src, c) };
    true
}

#[inline]
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn try_mul_xor_into_avx512(_dst: &mut [u8], _src: &[u8], _c: u8) -> bool {
    false
}

#[inline]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn try_mul_xor_into_avx2(dst: &mut [u8], src: &[u8], c: u8) -> bool {
    if dst.len() < 64 || !std::is_x86_feature_detected!("avx2") {
        return false;
    }
    unsafe { mul_xor_into_avx2(dst, src, c) };
    true
}

#[inline]
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn try_mul_xor_into_avx2(_dst: &mut [u8], _src: &[u8], _c: u8) -> bool {
    false
}

#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[target_feature(enable = "avx512f,avx512bw")]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
unsafe fn xor_into_avx512(dst: &mut [u8], src: &[u8]) {
    XOR_AVX512_CALLS.fetch_add(1, Ordering::Relaxed);
    XOR_AVX512_BYTES.fetch_add(dst.len() as u64, Ordering::Relaxed);
    let mut i = 0usize;
    let len = dst.len();
    while i + 64 <= len {
        let d = _mm512_loadu_si512(dst.as_ptr().add(i) as *const __m512i);
        let s = _mm512_loadu_si512(src.as_ptr().add(i) as *const __m512i);
        let r = _mm512_xor_si512(d, s);
        _mm512_storeu_si512(dst.as_mut_ptr().add(i) as *mut __m512i, r);
        i += 64;
    }
    if i < len {
        xor_into_avx2(&mut dst[i..], &src[i..]);
    }
}

#[target_feature(enable = "avx2")]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
unsafe fn xor_into_avx2(dst: &mut [u8], src: &[u8]) {
    XOR_AVX2_CALLS.fetch_add(1, Ordering::Relaxed);
    XOR_AVX2_BYTES.fetch_add(dst.len() as u64, Ordering::Relaxed);
    let mut i = 0usize;
    let len = dst.len();
    while i + 32 <= len {
        let d = _mm256_loadu_si256(dst.as_ptr().add(i) as *const __m256i);
        let s = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
        let r = _mm256_xor_si256(d, s);
        _mm256_storeu_si256(dst.as_mut_ptr().add(i) as *mut __m256i, r);
        i += 32;
    }
    xor_into_scalar(&mut dst[i..], &src[i..]);
}

#[target_feature(enable = "avx512f,avx512bw")]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
unsafe fn mul_xor_into_avx512(dst: &mut [u8], src: &[u8], c: u8) {
    MUL_AVX512_CALLS.fetch_add(1, Ordering::Relaxed);
    MUL_AVX512_BYTES.fetch_add(dst.len() as u64, Ordering::Relaxed);
    let lo = repeated_nibble_mul_table::<64>(c, 0);
    let hi = repeated_nibble_mul_table::<64>(c, 4);
    let lo_tbl = _mm512_loadu_si512(lo.as_ptr() as *const __m512i);
    let hi_tbl = _mm512_loadu_si512(hi.as_ptr() as *const __m512i);
    let mask = _mm512_set1_epi8(0x0f);

    let mut i = 0usize;
    let len = dst.len();
    while i + 64 <= len {
        let s = _mm512_loadu_si512(src.as_ptr().add(i) as *const __m512i);
        let d = _mm512_loadu_si512(dst.as_ptr().add(i) as *const __m512i);
        let s_lo = _mm512_and_si512(s, mask);
        let s_hi = _mm512_and_si512(_mm512_srli_epi16(s, 4), mask);
        let p_lo = _mm512_shuffle_epi8(lo_tbl, s_lo);
        let p_hi = _mm512_shuffle_epi8(hi_tbl, s_hi);
        let product = _mm512_xor_si512(p_lo, p_hi);
        let out = _mm512_xor_si512(d, product);
        _mm512_storeu_si512(dst.as_mut_ptr().add(i) as *mut __m512i, out);
        i += 64;
    }
    if i < len {
        mul_xor_into_avx2(&mut dst[i..], &src[i..], c);
    }
}

#[target_feature(enable = "avx2")]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
unsafe fn mul_xor_into_avx2(dst: &mut [u8], src: &[u8], c: u8) {
    MUL_AVX2_CALLS.fetch_add(1, Ordering::Relaxed);
    MUL_AVX2_BYTES.fetch_add(dst.len() as u64, Ordering::Relaxed);
    let lo = nibble_mul_table(c, 0);
    let hi = nibble_mul_table(c, 4);
    let lo_tbl = _mm256_setr_epi8(
        lo[0] as i8,
        lo[1] as i8,
        lo[2] as i8,
        lo[3] as i8,
        lo[4] as i8,
        lo[5] as i8,
        lo[6] as i8,
        lo[7] as i8,
        lo[8] as i8,
        lo[9] as i8,
        lo[10] as i8,
        lo[11] as i8,
        lo[12] as i8,
        lo[13] as i8,
        lo[14] as i8,
        lo[15] as i8,
        lo[0] as i8,
        lo[1] as i8,
        lo[2] as i8,
        lo[3] as i8,
        lo[4] as i8,
        lo[5] as i8,
        lo[6] as i8,
        lo[7] as i8,
        lo[8] as i8,
        lo[9] as i8,
        lo[10] as i8,
        lo[11] as i8,
        lo[12] as i8,
        lo[13] as i8,
        lo[14] as i8,
        lo[15] as i8,
    );
    let hi_tbl = _mm256_setr_epi8(
        hi[0] as i8,
        hi[1] as i8,
        hi[2] as i8,
        hi[3] as i8,
        hi[4] as i8,
        hi[5] as i8,
        hi[6] as i8,
        hi[7] as i8,
        hi[8] as i8,
        hi[9] as i8,
        hi[10] as i8,
        hi[11] as i8,
        hi[12] as i8,
        hi[13] as i8,
        hi[14] as i8,
        hi[15] as i8,
        hi[0] as i8,
        hi[1] as i8,
        hi[2] as i8,
        hi[3] as i8,
        hi[4] as i8,
        hi[5] as i8,
        hi[6] as i8,
        hi[7] as i8,
        hi[8] as i8,
        hi[9] as i8,
        hi[10] as i8,
        hi[11] as i8,
        hi[12] as i8,
        hi[13] as i8,
        hi[14] as i8,
        hi[15] as i8,
    );
    let mask = _mm256_set1_epi8(0x0f);

    let mut i = 0usize;
    let len = dst.len();
    while i + 32 <= len {
        let s = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
        let d = _mm256_loadu_si256(dst.as_ptr().add(i) as *const __m256i);
        let s_lo = _mm256_and_si256(s, mask);
        let s_hi = _mm256_and_si256(_mm256_srli_epi16(s, 4), mask);
        let p_lo = _mm256_shuffle_epi8(lo_tbl, s_lo);
        let p_hi = _mm256_shuffle_epi8(hi_tbl, s_hi);
        let product = _mm256_xor_si256(p_lo, p_hi);
        let out = _mm256_xor_si256(d, product);
        _mm256_storeu_si256(dst.as_mut_ptr().add(i) as *mut __m256i, out);
        i += 32;
    }
    mul_xor_into_scalar(&mut dst[i..], &src[i..], c);
}

#[inline]
fn nibble_mul_table(c: u8, shift: u8) -> [u8; 16] {
    let mut out = [0u8; 16];
    for n in 0..16u8 {
        out[n as usize] = mul(c, n << shift);
    }
    out
}

#[inline]
fn repeated_nibble_mul_table<const N: usize>(c: u8, shift: u8) -> [u8; N] {
    debug_assert_eq!(N % 16, 0);
    let base = nibble_mul_table(c, shift);
    let mut out = [0u8; N];
    for i in 0..N {
        out[i] = base[i & 0x0f];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mul_by_g_lowbits() {
        assert_eq!(mul_by_g(1), 2);
        assert_eq!(mul_by_g(2), 4);
        assert_eq!(mul_by_g(0x40), 0x80);
        // 0x80 << 1 = 0x100 -> ^= 0x1d -> 0x1d
        assert_eq!(mul_by_g(0x80), 0x1d);
    }

    #[test]
    fn mul_identity_and_zero() {
        assert_eq!(mul(0, 5), 0);
        assert_eq!(mul(5, 0), 0);
        assert_eq!(mul(1, 5), 5);
        assert_eq!(mul(5, 1), 5);
    }

    #[test]
    fn mul_commutative_and_associative_sample() {
        for &a in &[1u8, 2, 17, 200, 255] {
            for &b in &[1u8, 3, 50, 199] {
                assert_eq!(mul(a, b), mul(b, a));
                for &c in &[1u8, 5, 100] {
                    assert_eq!(mul(mul(a, b), c), mul(a, mul(b, c)));
                }
            }
        }
    }

    #[test]
    fn inv_yields_one_under_mul() {
        for a in 1u8..=255 {
            assert_eq!(mul(a, inv(a)), 1, "inv({}) failed", a);
        }
    }

    #[test]
    fn g_pow_matches_iterated_mul_by_g() {
        let mut acc = 1u8;
        for i in 0..255 {
            assert_eq!(g_pow(i), acc, "g^{} mismatch", i);
            acc = mul_by_g(acc);
        }
        // g^255 wraps to g^0 = 1
        assert_eq!(g_pow(255), 1);
    }

    #[test]
    #[should_panic(expected = "gf256::inv(0)")]
    fn inv_zero_panics() {
        // Defensive: release-mode panic too. inv(0) is undefined and any
        // hit means an upstream invariant violation that mustn't be
        // silently papered over.
        let _ = inv(0);
    }

    #[test]
    fn mul_xor_into_matches_naive() {
        let src = vec![1u8, 2, 3, 4, 5, 0xff];
        let mut dst = vec![0xa5u8; src.len()];
        let c = 7u8;
        let mut expected = dst.clone();
        for i in 0..src.len() {
            expected[i] ^= mul(src[i], c);
        }
        mul_xor_into(&mut dst, &src, c);
        assert_eq!(dst, expected);
    }

    #[test]
    fn xor_into_large_matches_naive() {
        let src: Vec<u8> = (0..4096).map(|i| (i * 29 % 251) as u8).collect();
        let mut dst: Vec<u8> = (0..4096).map(|i| (i * 13 % 253) as u8).collect();
        let mut expected = dst.clone();

        xor_into(&mut dst, &src);
        for i in 0..src.len() {
            expected[i] ^= src[i];
        }

        assert_eq!(dst, expected);
    }

    #[test]
    fn mul_xor_into_large_all_constants_match_naive() {
        let src: Vec<u8> = (0..4096).map(|i| (i * 37 % 251) as u8).collect();
        for c in 1u8..=255 {
            let mut dst: Vec<u8> = (0..4096).map(|i| (i * 17 % 257) as u8).collect();
            let mut expected = dst.clone();

            mul_xor_into(&mut dst, &src, c);
            for i in 0..src.len() {
                expected[i] ^= mul(src[i], c);
            }

            assert_eq!(dst, expected, "c={}", c);
        }
    }
}
