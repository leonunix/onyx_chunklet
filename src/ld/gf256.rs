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

use std::sync::OnceLock;

const POLY: u16 = 0x11d;

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

/// Multiplicative inverse in GF(2^8). Caller must ensure `a != 0`.
#[inline]
pub fn inv(a: u8) -> u8 {
    debug_assert!(a != 0);
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
}
