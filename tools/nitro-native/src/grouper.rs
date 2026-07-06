//! Native grouping-insert kernel for q24's key shape (2 long keys + 3 string keys). find-or-insert with
//! scan-order group-id assignment (byte-identical to Java's FlatGroupingTable output: ids are first-seen order,
//! equality is exact, and the hash only picks the slot — so any hash is fine). Strings are copied into an
//! owned arena on first insert. This is the de-risking core: insert + group count, no materialization yet.
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

#[inline(always)]
fn mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xFF51AFD7ED558CCD);
    h ^= h >> 33;
    h = h.wrapping_mul(0xC4CEB9FE1A85EC53);
    h ^= h >> 33;
    h
}

/// SWAR word-at-a-time bytes hash (matches nothing in Java by value — hash is free — just needs to be good).
#[inline(always)]
unsafe fn hash_bytes(mut p: *const u8, len: usize, seed: u64) -> u64 {
    let mut h = seed ^ (len as u64);
    let mut n = len;
    while n >= 8 {
        h ^= mix((p as *const u64).read_unaligned());
        h = h.rotate_left(27).wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(0x52DCE729);
        p = p.add(8);
        n -= 8;
    }
    if n > 0 {
        let mut tail = 0u64;
        for i in 0..n {
            tail |= (*p.add(i) as u64) << (i * 8);
        }
        h ^= mix(tail);
    }
    mix(h)
}

pub struct Grouper {
    control: Vec<u8>, // 0 = empty, else (tag|0x80)
    slot_group: Vec<i32>,
    mask: usize,
    max_fill: usize,
    count: usize,
    // per-group key storage, indexed by group id
    k0: Vec<i64>,
    k1: Vec<i64>,
    ghash: Vec<u64>,
    arena: Vec<u8>,
    s_off: [Vec<usize>; 3],
    s_len: [Vec<u32>; 3],
}

impl Grouper {
    fn new(expected: usize) -> Box<Grouper> {
        let mut cap = 16usize;
        while (cap as f64) * 0.9375 < expected as f64 {
            cap <<= 1;
        }
        Box::new(Grouper {
            control: vec![0u8; cap],
            slot_group: vec![-1i32; cap],
            mask: cap - 1,
            max_fill: (cap as f64 * 0.9375) as usize,
            count: 0,
            k0: Vec::with_capacity(expected),
            k1: Vec::with_capacity(expected),
            ghash: Vec::with_capacity(expected),
            arena: Vec::with_capacity(expected * 24),
            s_off: [Vec::new(), Vec::new(), Vec::new()],
            s_len: [Vec::new(), Vec::new(), Vec::new()],
        })
    }

    #[inline(always)]
    unsafe fn eq_group(&self, g: usize, k0: i64, k1: i64, s: &[(*const u8, usize); 3]) -> bool {
        if self.k0[g] != k0 || self.k1[g] != k1 {
            return false;
        }
        for c in 0..3 {
            let len = self.s_len[c][g] as usize;
            if len != s[c].1 {
                return false;
            }
            let stored = self.arena.as_ptr().add(self.s_off[c][g]);
            if libc_memcmp(stored, s[c].0, len) != 0 {
                return false;
            }
        }
        true
    }

    unsafe fn insert(&mut self, k0: i64, k1: i64, s: &[(*const u8, usize); 3]) -> i32 {
        let mut h = (k0 as u64).wrapping_mul(0x9E3779B97F4A7C15)
            ^ (k1 as u64).wrapping_mul(0xC4CEB9FE1A85EC53);
        for c in 0..3 {
            h ^= hash_bytes(s[c].0, s[c].1, 0x94D049BB133111EB).rotate_left((c * 7) as u32);
        }
        h = mix(h);
        let tag = ((h >> 56) | 0x80) as u8;
        let mut slot = (h as usize) & self.mask;
        loop {
            let c = self.control[slot];
            if c == 0 {
                let g = self.count as i32;
                self.control[slot] = tag;
                self.slot_group[slot] = g;
                self.k0.push(k0);
                self.k1.push(k1);
                self.ghash.push(h);
                for ci in 0..3 {
                    let off = self.arena.len();
                    self.arena.extend_from_slice(std::slice::from_raw_parts(s[ci].0, s[ci].1));
                    self.s_off[ci].push(off);
                    self.s_len[ci].push(s[ci].1 as u32);
                }
                self.count += 1;
                if self.count >= self.max_fill {
                    self.rehash();
                }
                return g;
            }
            if c == tag {
                let g = self.slot_group[slot] as usize;
                if self.ghash[g] == h && self.eq_group(g, k0, k1, s) {
                    return g as i32;
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }

    fn rehash(&mut self) {
        let cap = self.control.len() * 2;
        self.mask = cap - 1;
        self.max_fill = (cap as f64 * 0.9375) as usize;
        self.control = vec![0u8; cap];
        self.slot_group = vec![-1i32; cap];
        for g in 0..self.count {
            let h = self.ghash[g];
            let tag = ((h >> 56) | 0x80) as u8;
            let mut slot = (h as usize) & self.mask;
            while self.control[slot] != 0 {
                slot = (slot + 1) & self.mask;
            }
            self.control[slot] = tag;
            self.slot_group[slot] = g as i32;
        }
    }
}

#[inline(always)]
unsafe fn libc_memcmp(a: *const u8, b: *const u8, n: usize) -> i32 {
    // small-string fast path: word compares then tail, avoids a libc call
    let mut i = 0;
    while i + 8 <= n {
        if (a.add(i) as *const u64).read_unaligned() != (b.add(i) as *const u64).read_unaligned() {
            return 1;
        }
        i += 8;
    }
    while i < n {
        if *a.add(i) != *b.add(i) {
            return 1;
        }
        i += 1;
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn grouper_new(expected: u64) -> u64 {
    Box::into_raw(Grouper::new(expected as usize)) as u64
}

#[no_mangle]
pub unsafe extern "C" fn grouper_free(g: u64) {
    drop(Box::from_raw(g as *mut Grouper));
}

#[no_mangle]
pub unsafe extern "C" fn grouper_count(g: u64) -> u64 {
    (*(g as *const Grouper)).count as u64
}

/// Insert a batch of q24-shape rows. Strings: s{c}_data is the (shared, e.g. dictionary) byte buffer,
/// s{c}_off[i]/s{c}_len[i] locate row i's bytes within it. out[i] receives the group id. prefetch>0 enables
/// prefetching the control line for the k0/k1 slot of row i+prefetch (approximate — string hash not looked ahead).
#[no_mangle]
pub unsafe extern "C" fn grouper_insert(
    g: u64,
    k0: *const i64, k1: *const i64,
    s0_data: *const u8, s0_off: *const u64, s0_len: *const u32,
    s1_data: *const u8, s1_off: *const u64, s1_len: *const u32,
    s2_data: *const u8, s2_off: *const u64, s2_len: *const u32,
    n: usize, out: *mut i32, prefetch: usize,
) {
    let gr = &mut *(g as *mut Grouper);
    for i in 0..n {
        if prefetch != 0 && i + prefetch < n {
            let a = i + prefetch;
            let h = (*k0.add(a) as u64).wrapping_mul(0x9E3779B97F4A7C15)
                ^ (*k1.add(a) as u64).wrapping_mul(0xC4CEB9FE1A85EC53);
            _mm_prefetch(gr.control.as_ptr().add((h as usize) & gr.mask) as *const i8, _MM_HINT_T0);
        }
        let s: [(*const u8, usize); 3] = [
            (s0_data.add(*s0_off.add(i) as usize), *s0_len.add(i) as usize),
            (s1_data.add(*s1_off.add(i) as usize), *s1_len.add(i) as usize),
            (s2_data.add(*s2_off.add(i) as usize), *s2_len.add(i) as usize),
        ];
        *out.add(i) = gr.insert(*k0.add(i), *k1.add(i), &s);
    }
}

// ---- GENERIC schema-interpreted grouper: same algorithm, but the record layout + per-row hash/eq/store are
// driven by a runtime schema (Vec<ColType>) with a per-column type dispatch — NO hardcoded shape, NO codegen.
// Records are a flat byte buffer [hash:8][field bytes...]; Long field = 8 bytes inline, Binary field = [off:u64][len:u32].
// This exists only to measure the genericity tax vs the hardcoded grouper above (same ABI, same data, same q24 schema).
#[derive(Clone, Copy, PartialEq)]
enum ColType { Long, Binary }

pub struct GenGrouper {
    control: Vec<u8>,
    slot_group: Vec<i32>,
    mask: usize,
    max_fill: usize,
    count: usize,
    schema: Vec<ColType>,
    sub: Vec<usize>,      // per schema col: index among its type's columns
    field_off: Vec<usize>,
    rec_size: usize,      // fixed bytes after the 8-byte hash
    records: Vec<u8>,
    arena: Vec<u8>,
    ghash: Vec<u64>,
}

impl GenGrouper {
    fn new(schema: Vec<ColType>, expected: usize) -> Box<GenGrouper> {
        let mut cap = 16usize;
        while (cap as f64) * 0.9375 < expected as f64 {
            cap <<= 1;
        }
        let mut sub = vec![0usize; schema.len()];
        let mut field_off = vec![0usize; schema.len()];
        let (mut nl, mut nb, mut off) = (0usize, 0usize, 0usize);
        for (i, &ct) in schema.iter().enumerate() {
            field_off[i] = off;
            match ct {
                ColType::Long => { sub[i] = nl; nl += 1; off += 8; }
                ColType::Binary => { sub[i] = nb; nb += 1; off += 12; }
            }
        }
        Box::new(GenGrouper {
            control: vec![0u8; cap], slot_group: vec![-1i32; cap], mask: cap - 1,
            max_fill: (cap as f64 * 0.9375) as usize, count: 0,
            schema, sub, field_off, rec_size: off,
            records: Vec::with_capacity(expected * (8 + off)),
            arena: Vec::with_capacity(expected * 24),
            ghash: Vec::with_capacity(expected),
        })
    }

    #[inline(always)]
    unsafe fn hash_row(&self, longs: &[*const i64], bins: &[(*const u8, *const u64, *const u32)], i: usize) -> u64 {
        let mut h = 0u64;
        for (ci, &ct) in self.schema.iter().enumerate() {
            let s = self.sub[ci];
            match ct {
                ColType::Long => {
                    h ^= mix(*longs[s].add(i) as u64);
                }
                ColType::Binary => {
                    let (d, o, l) = bins[s];
                    h ^= hash_bytes(d.add(*o.add(i) as usize), *l.add(i) as usize, 0x94D049BB133111EB);
                }
            }
            h = h.rotate_left(27).wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(0x52DCE729);
        }
        mix(h)
    }

    #[inline(always)]
    unsafe fn eq_row(&self, g: usize, longs: &[*const i64], bins: &[(*const u8, *const u64, *const u32)], i: usize) -> bool {
        let rec = self.records.as_ptr().add(g * (8 + self.rec_size) + 8);
        for (ci, &ct) in self.schema.iter().enumerate() {
            let fp = rec.add(self.field_off[ci]);
            let s = self.sub[ci];
            match ct {
                ColType::Long => {
                    if (fp as *const i64).read_unaligned() != *longs[s].add(i) {
                        return false;
                    }
                }
                ColType::Binary => {
                    let aoff = (fp as *const u64).read_unaligned() as usize;
                    let alen = (fp.add(8) as *const u32).read_unaligned() as usize;
                    let (d, o, l) = bins[s];
                    let ilen = *l.add(i) as usize;
                    if alen != ilen || libc_memcmp(self.arena.as_ptr().add(aoff), d.add(*o.add(i) as usize), ilen) != 0 {
                        return false;
                    }
                }
            }
        }
        true
    }

    unsafe fn insert(&mut self, longs: &[*const i64], bins: &[(*const u8, *const u64, *const u32)], i: usize) -> i32 {
        let h = self.hash_row(longs, bins, i);
        let tag = ((h >> 56) | 0x80) as u8;
        let mut slot = (h as usize) & self.mask;
        loop {
            let c = self.control[slot];
            if c == 0 {
                let g = self.count as i32;
                self.control[slot] = tag;
                self.slot_group[slot] = g;
                self.ghash.push(h);
                let base = self.records.len();
                self.records.resize(base + 8 + self.rec_size, 0);
                let recp = self.records.as_mut_ptr().add(base);
                (recp as *mut u64).write_unaligned(h);
                for (ci, &ct) in self.schema.iter().enumerate() {
                    let fp = recp.add(8 + self.field_off[ci]);
                    let s = self.sub[ci];
                    match ct {
                        ColType::Long => {
                            (fp as *mut i64).write_unaligned(*longs[s].add(i));
                        }
                        ColType::Binary => {
                            let (d, o, l) = bins[s];
                            let ilen = *l.add(i) as usize;
                            let aoff = self.arena.len();
                            self.arena.extend_from_slice(std::slice::from_raw_parts(d.add(*o.add(i) as usize), ilen));
                            (fp as *mut u64).write_unaligned(aoff as u64);
                            (fp.add(8) as *mut u32).write_unaligned(ilen as u32);
                        }
                    }
                }
                self.count += 1;
                if self.count >= self.max_fill {
                    self.rehash();
                }
                return g;
            }
            if c == tag {
                let g = self.slot_group[slot] as usize;
                if self.ghash[g] == h && self.eq_row(g, longs, bins, i) {
                    return g as i32;
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }

    fn rehash(&mut self) {
        let cap = self.control.len() * 2;
        self.mask = cap - 1;
        self.max_fill = (cap as f64 * 0.9375) as usize;
        self.control = vec![0u8; cap];
        self.slot_group = vec![-1i32; cap];
        for g in 0..self.count {
            let h = self.ghash[g];
            let mut slot = (h as usize) & self.mask;
            while self.control[slot] != 0 {
                slot = (slot + 1) & self.mask;
            }
            self.control[slot] = ((h >> 56) | 0x80) as u8;
            self.slot_group[slot] = g as i32;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn grouper_g_new(expected: u64) -> u64 {
    // q24 schema for the bench: 2 long keys + 3 binary keys (the CODE is generic; the schema is just data).
    let schema = vec![ColType::Long, ColType::Long, ColType::Binary, ColType::Binary, ColType::Binary];
    Box::into_raw(GenGrouper::new(schema, expected as usize)) as u64
}

#[no_mangle]
pub unsafe extern "C" fn grouper_g_free(g: u64) {
    drop(Box::from_raw(g as *mut GenGrouper));
}

#[no_mangle]
pub unsafe extern "C" fn grouper_g_count(g: u64) -> u64 {
    (*(g as *const GenGrouper)).count as u64
}

#[no_mangle]
pub unsafe extern "C" fn grouper_g_insert(
    g: u64,
    k0: *const i64, k1: *const i64,
    s0_data: *const u8, s0_off: *const u64, s0_len: *const u32,
    s1_data: *const u8, s1_off: *const u64, s1_len: *const u32,
    s2_data: *const u8, s2_off: *const u64, s2_len: *const u32,
    n: usize, out: *mut i32, _prefetch: usize,
) {
    let gr = &mut *(g as *mut GenGrouper);
    let longs = [k0, k1];
    let bins = [(s0_data, s0_off, s0_len), (s1_data, s1_off, s1_len), (s2_data, s2_off, s2_len)];
    for i in 0..n {
        *out.add(i) = gr.insert(&longs, &bins, i);
    }
}
