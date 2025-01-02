/**
 * Claude wrote this from the scylladb rust package impl
 */
use std::num::Wrapping;

pub fn murmur3_128(bytes: &[u8]) -> (i64, i64) {
    const C1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
    const C2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

    let mut h1 = Wrapping(0i64);
    let mut h2 = Wrapping(0i64);
    let total_len = bytes.len();
    let mut remaining = bytes;

    // Process 16 bytes at a time
    while remaining.len() >= 16 {
        let k1 = Wrapping(i64::from_le_bytes(remaining[0..8].try_into().unwrap()));
        let k2 = Wrapping(i64::from_le_bytes(remaining[8..16].try_into().unwrap()));
        remaining = &remaining[16..];

        // Mixing function for h1
        let mut k1 = k1 * C1;
        k1 = rotl64(k1, 31);
        k1 *= C2;
        h1 ^= k1;
        h1 = rotl64(h1, 27);
        h1 += h2;
        h1 = h1 * Wrapping(5) + Wrapping(0x52dce729);

        // Mixing function for h2
        let mut k2 = k2 * C2;
        k2 = rotl64(k2, 33);
        k2 *= C1;
        h2 ^= k2;
        h2 = rotl64(h2, 31);
        h2 += h1;
        h2 = h2 * Wrapping(5) + Wrapping(0x38495ab5);
    }

    // Process remaining bytes
    let mut k1 = Wrapping(0i64);
    let mut k2 = Wrapping(0i64);
    let remaining_len = remaining.len();

    if remaining_len > 8 {
        for i in (8..remaining_len).rev() {
            k2 ^= Wrapping(remaining[i] as i8 as i64) << ((i - 8) * 8);
        }
        k2 *= C2;
        k2 = rotl64(k2, 33);
        k2 *= C1;
        h2 ^= k2;
    }

    if remaining_len > 0 {
        for i in (0..remaining_len.min(8)).rev() {
            k1 ^= Wrapping(remaining[i] as i8 as i64) << (i * 8);
        }
        k1 *= C1;
        k1 = rotl64(k1, 31);
        k1 *= C2;
        h1 ^= k1;
    }

    // Finalization
    h1 ^= Wrapping(total_len as i64);
    h2 ^= Wrapping(total_len as i64);

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

    h1 += h2;
    h2 += h1;

    (h1.0, h2.0)
}

#[inline]
fn rotl64(v: Wrapping<i64>, n: u32) -> Wrapping<i64> {
    Wrapping((v.0 << n) | (v.0 as u64 >> (64 - n)) as i64)
}

#[inline]
fn fmix(mut k: Wrapping<i64>) -> Wrapping<i64> {
    k ^= Wrapping((k.0 as u64 >> 33) as i64);
    k *= Wrapping(0xff51afd7ed558ccd_u64 as i64);
    k ^= Wrapping((k.0 as u64 >> 33) as i64);
    k *= Wrapping(0xc4ceb9fe1a85ec53_u64 as i64);
    k ^= Wrapping((k.0 as u64 >> 33) as i64);
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur3_known_values() {
        // These test values should be updated with known 128-bit hash values
        let (h1, _) = murmur3_128("test".as_bytes());
        assert_eq!(h1, -6017608668500074083);

        let (h1, _) = murmur3_128("xd".as_bytes());
        assert_eq!(h1, 4507812186440344727);

        let (h1, _) = murmur3_128("primary_key".as_bytes());
        assert_eq!(h1, -1632642444691073360);
    }
}
