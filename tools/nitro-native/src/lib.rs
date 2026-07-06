//! Native (Rust) hash-join probe kernels for Nitro, called via the Java FFM downcall in
//! org.weakref.nitro.operator.NativeProbe. Each is a byte-identical port of the corresponding
//! Java Swiss-tag probe (LongPairJoinIndex / LongTripleJoinIndex) with optional software prefetch
//! (prefetch_distance == 0 disables it). The caller passes Nitro's on-heap tags/entries arrays by
//! reference via FFM Linker.Option.critical(true); this code only reads them.

mod grouper;
use std::arch::x86_64::{
    __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_prefetch, _mm_set1_epi8,
    _mm_setzero_si128, _MM_HINT_T0,
};

const NO_MATCH: i64 = -1;
const GROUP: usize = 16; // ByteVector.SPECIES_128

#[inline(always)]
fn mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xFF51AFD7ED558CCD);
    h ^= h >> 33;
    h = h.wrapping_mul(0xC4CEB9FE1A85EC53);
    h ^= h >> 33;
    h
}

#[inline(always)]
fn hash2(first: i64, second: i64) -> u64 {
    mix((first as u64)
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add((second as u64).wrapping_mul(0xC4CEB9FE1A85EC53)))
}

#[inline(always)]
fn hash3(first: i64, second: i64, third: i64) -> u64 {
    mix((first as u64)
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add((second as u64).wrapping_mul(0xC4CEB9FE1A85EC53))
        .wrapping_add((third as u64).wrapping_mul(0x94D049BB133111EB)))
}

/// Scan a 16-slot tag group at `group`; returns (match_bitmask, has_empty).
#[inline(always)]
unsafe fn scan_group(tags: *const u8, group: usize, wanted: __m128i, empty: __m128i) -> (u32, bool) {
    let tv = _mm_loadu_si128(tags.add(group) as *const __m128i);
    let mbits = (_mm_movemask_epi8(_mm_cmpeq_epi8(tv, wanted)) as u32) & 0xFFFF;
    let has_empty = (_mm_movemask_epi8(_mm_cmpeq_epi8(tv, empty)) as u32) & 0xFFFF != 0;
    (mbits, has_empty)
}

#[inline(always)]
unsafe fn single_ref2(tags: *const u8, mask: usize, entries: *const i64, first: i64, second: i64) -> i64 {
    let h = hash2(first, second);
    let wanted = _mm_set1_epi8(((h >> 56) | 0x80) as i8);
    let empty = _mm_setzero_si128();
    let mut group = (h as usize) & mask & !(GROUP - 1);
    loop {
        let (mut mbits, has_empty) = scan_group(tags, group, wanted, empty);
        while mbits != 0 {
            let base = (group + mbits.trailing_zeros() as usize) * 3;
            if *entries.add(base) == first && *entries.add(base + 1) == second {
                return *entries.add(base + 2);
            }
            mbits &= mbits - 1;
        }
        if has_empty {
            return NO_MATCH;
        }
        group = (group + GROUP) & mask;
    }
}

#[inline(always)]
unsafe fn single_ref3(tags: *const u8, mask: usize, entries: *const i64, a: i64, b: i64, c: i64) -> i64 {
    let h = hash3(a, b, c);
    let wanted = _mm_set1_epi8(((h >> 56) | 0x80) as i8);
    let empty = _mm_setzero_si128();
    let mut group = (h as usize) & mask & !(GROUP - 1);
    loop {
        let (mut mbits, has_empty) = scan_group(tags, group, wanted, empty);
        while mbits != 0 {
            let base = (group + mbits.trailing_zeros() as usize) * 4;
            if *entries.add(base) == a && *entries.add(base + 1) == b && *entries.add(base + 2) == c {
                return *entries.add(base + 3);
            }
            mbits &= mbits - 1;
        }
        if has_empty {
            return NO_MATCH;
        }
        group = (group + GROUP) & mask;
    }
}

/// Two-key single-match batch probe (LongPairJoinIndex).
#[no_mangle]
pub unsafe extern "C" fn probe_pairs(
    tags: *const u8, capacity: u64, entries: *const i64,
    first: *const i64, second: *const i64, n: usize, out: *mut i64, prefetch_distance: usize,
) {
    let mask = (capacity - 1) as usize;
    for i in 0..n {
        if prefetch_distance != 0 {
            let a = i + prefetch_distance;
            if a < n {
                let g = (hash2(*first.add(a), *second.add(a)) as usize) & mask & !(GROUP - 1);
                _mm_prefetch(tags.add(g) as *const i8, _MM_HINT_T0);
                _mm_prefetch(entries.add(g * 3) as *const i8, _MM_HINT_T0);
            }
        }
        *out.add(i) = single_ref2(tags, mask, entries, *first.add(i), *second.add(i));
    }
}

/// Three-key single-match batch probe (LongTripleJoinIndex).
#[no_mangle]
pub unsafe extern "C" fn probe_triples(
    tags: *const u8, capacity: u64, entries: *const i64,
    first: *const i64, second: *const i64, third: *const i64, n: usize, out: *mut i64, prefetch_distance: usize,
) {
    let mask = (capacity - 1) as usize;
    for i in 0..n {
        if prefetch_distance != 0 {
            let a = i + prefetch_distance;
            if a < n {
                let g = (hash3(*first.add(a), *second.add(a), *third.add(a)) as usize) & mask & !(GROUP - 1);
                _mm_prefetch(tags.add(g) as *const i8, _MM_HINT_T0);
                _mm_prefetch(entries.add(g * 4) as *const i8, _MM_HINT_T0);
            }
        }
        *out.add(i) = single_ref3(tags, mask, entries, *first.add(i), *second.add(i), *third.add(i));
    }
}
