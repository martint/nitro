//! Native port of ColumnReader.readSelectedLongs' skip-decode for a dict LONG column, called via the Java FFM
//! downcall in org.weakref.nitro.parquet.NativeSkipDecode. Given a page's definition-level RLE stream (bit width 1),
//! its dictionary-id RLE/bit-packed stream (one id per non-null), the dictionary, and the page-relative survivor
//! positions, it gathers dict[id] (or 0/null) at each survivor in one native pass -- no per-run JVM method dispatch,
//! bounds checks, or scalar mid-byte reader. This is the hypothesis test for "q20's scan gap is the JVM-scalar-reader
//! vs native-columnar-reader constant factor".

#[inline(always)]
unsafe fn read_uleb128(src: *const u8, pos: u64) -> (u64, u64) {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut consumed: u64 = 0;
    loop {
        let b = *src.add((pos + consumed) as usize);
        consumed += 1;
        value |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return (value, consumed);
        }
        shift += 7;
    }
}

#[inline(always)]
unsafe fn read_le(src: *const u8, pos: u64, byte_width: u64) -> u64 {
    let mut v: u64 = 0;
    let mut i = 0u64;
    while i < byte_width {
        v |= (*src.add((pos + i) as usize) as u64) << (i * 8);
        i += 1;
    }
    v
}

#[inline(always)]
unsafe fn load_u64_le(src: *const u8, byte_off: u64) -> u64 {
    (src.add(byte_off as usize) as *const u64).read_unaligned().to_le()
}

/// A cursor over a Parquet RLE / bit-packed hybrid stream (mirrors the Java RleReader for read/skip/count).
struct RleWalk {
    base: *const u8,
    bit_width: u32,
    byte_width: u64,
    pos: u64,
    rle_remaining: u64,
    rle_value: u64,
    bp_remaining: u64,
    bit_cursor: u64,
}

impl RleWalk {
    #[inline(always)]
    unsafe fn new(base: *const u8, offset: u64, bit_width: u32) -> RleWalk {
        RleWalk {
            base,
            bit_width,
            byte_width: ((bit_width + 7) / 8) as u64,
            pos: offset,
            rle_remaining: 0,
            rle_value: 0,
            bp_remaining: 0,
            bit_cursor: 0,
        }
    }

    #[inline(always)]
    unsafe fn load_next_run(&mut self) {
        let (header, hlen) = read_uleb128(self.base, self.pos);
        self.pos += hlen;
        if header & 1 == 0 {
            self.rle_remaining = header >> 1;
            self.rle_value = read_le(self.base, self.pos, self.byte_width);
            self.pos += self.byte_width;
        } else {
            self.bp_remaining = (header >> 1) * 8;
            self.bit_cursor = self.pos * 8;
        }
    }

    /// Read one value, advancing one position.
    #[inline(always)]
    unsafe fn next(&mut self) -> u64 {
        if self.rle_remaining == 0 && self.bp_remaining == 0 {
            self.load_next_run();
        }
        if self.rle_remaining > 0 {
            self.rle_remaining -= 1;
            self.rle_value
        } else {
            let mask = if self.bit_width >= 64 { u64::MAX } else { (1u64 << self.bit_width) - 1 };
            let word = load_u64_le(self.base, self.bit_cursor >> 3);
            let v = (word >> (self.bit_cursor & 7)) & mask;
            self.bit_cursor += self.bit_width as u64;
            self.bp_remaining -= 1;
            if self.bp_remaining == 0 {
                self.pos = (self.bit_cursor + 7) >> 3;
            }
            v
        }
    }

    /// Skip `n` values.
    #[inline(always)]
    unsafe fn skip(&mut self, mut n: u64) {
        while n > 0 {
            if self.rle_remaining == 0 && self.bp_remaining == 0 {
                self.load_next_run();
            }
            if self.rle_remaining > 0 {
                let t = n.min(self.rle_remaining);
                self.rle_remaining -= t;
                n -= t;
            } else {
                let t = n.min(self.bp_remaining);
                self.bit_cursor += t * self.bit_width as u64;
                self.bp_remaining -= t;
                n -= t;
                if self.bp_remaining == 0 {
                    self.pos = (self.bit_cursor + 7) >> 3;
                }
            }
        }
    }

    /// Advance `n` bit-1 def levels, returning the number of ones (non-nulls). RLE runs are O(1); bit-packed runs are
    /// popcounted word-at-a-time.
    #[inline(always)]
    unsafe fn skip_counting_ones(&mut self, mut n: u64) -> u64 {
        let mut ones: u64 = 0;
        while n > 0 {
            if self.rle_remaining == 0 && self.bp_remaining == 0 {
                self.load_next_run();
            }
            if self.rle_remaining > 0 {
                let t = n.min(self.rle_remaining);
                if self.rle_value == 1 {
                    ones += t;
                }
                self.rle_remaining -= t;
                n -= t;
            } else {
                let t = n.min(self.bp_remaining);
                let mut produced = 0u64;
                let mut cursor = self.bit_cursor;
                while produced < t {
                    let word = load_u64_le(self.base, cursor >> 3);
                    let shift = (cursor & 7) as u32;
                    let take = (64 - shift as u64).min(t - produced);
                    let m = if take == 64 { u64::MAX } else { (1u64 << take) - 1 };
                    ones += ((word >> shift) & m).count_ones() as u64;
                    cursor += take;
                    produced += take;
                }
                self.bit_cursor = cursor;
                self.bp_remaining -= t;
                n -= t;
                if self.bp_remaining == 0 {
                    self.pos = self.bit_cursor >> 3;
                }
            }
        }
        ones
    }

    /// Read the next `n` bit-1 def levels into `out`, returning the number of ones.
    #[inline(always)]
    unsafe fn read_def(&mut self, out: *mut u8, n: u64) -> u64 {
        let mut ones: u64 = 0;
        let mut i = 0u64;
        while i < n {
            let d = self.next();
            *out.offset(i as isize) = d as u8;
            ones += d;
            i += 1;
        }
        ones
    }
}

/// Skip-decode a dict LONG page. `def_offset == u64::MAX` => null-free (value index == position). Otherwise walk the
/// def stream at `def_offset` (bit width 1) for null positions and the running non-null (value) index. `survivors`
/// are sorted, page-relative positions; results go to out[out_offset + i], nulls (if `nulls_out` non-null) to 1/0.
#[no_mangle]
pub unsafe extern "C" fn skip_decode_dict_longs(
    body: *const u8,
    value_offset: u64,
    value_bit_width: u32,
    def_offset: u64,
    survivors: *const i32,
    survivor_count: u64,
    dict: *const i64,
    out: *mut i64,
    out_offset: u64,
    nulls_out: *mut u8,
) {
    let mut values = RleWalk::new(body, value_offset, value_bit_width);
    let has_nulls = def_offset != u64::MAX;

    if !has_nulls {
        let mut cursor: u64 = 0;
        let mut i = 0u64;
        while i < survivor_count {
            let p = *survivors.offset(i as isize) as u64;
            if p > cursor {
                values.skip(p - cursor);
            }
            let id = values.next();
            cursor = p + 1;
            *out.offset((out_offset + i) as isize) = *dict.offset(id as isize);
            if !nulls_out.is_null() {
                *nulls_out.offset(i as isize) = 0;
            }
            i += 1;
        }
        return;
    }

    let mut defs = RleWalk::new(body, def_offset, 1);
    let mut def_pos: u64 = 0; // positions consumed from the def stream
    let mut i = 0u64;
    while i < survivor_count {
        let p = *survivors.offset(i as isize) as u64;
        // Skip the value stream by the number of non-nulls strictly between the last def position and p.
        if def_pos < p {
            let gap_nonnull = defs.skip_counting_ones(p - def_pos);
            def_pos = p;
            if gap_nonnull > 0 {
                values.skip(gap_nonnull);
            }
        }
        let present = defs.next();
        def_pos += 1;
        if present != 0 {
            let id = values.next();
            *out.offset((out_offset + i) as isize) = *dict.offset(id as isize);
            if !nulls_out.is_null() {
                *nulls_out.offset(i as isize) = 0;
            }
        } else {
            *out.offset((out_offset + i) as isize) = 0;
            if !nulls_out.is_null() {
                *nulls_out.offset(i as isize) = 1;
            }
        }
        i += 1;
    }
}
