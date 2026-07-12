/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.parquet;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.weakref.nitro.parquet.ParquetFile.LE_LONG;

/**
 * A reusable decoder for Parquet's RLE / bit-packing hybrid encoding (used for dictionary ids and
 * definition levels). {@link #init} resets it over a region of a {@link MemorySegment}; {@link #read}
 * pulls the next {@code count} values, transparently crossing RLE and bit-packed runs. Holds no
 * per-call allocation — the caller owns the output array.
 *
 * <p>Bit-packed runs are unpacked with a branchless per-value kernel: one unaligned little-endian
 * 64-bit load at the value's byte offset, shifted by its in-byte bit offset and masked. This avoids the
 * per-byte refill loop and is the same access shape a Vector-API gather would use. It reads up to 7 bytes
 * past a value's start, so the source segment must carry {@code >= 7} bytes of trailing slack past the
 * last bit-packed run (the decompression buffer is over-allocated for this; the mmap has the footer after).
 */
final class RleReader
{
    private static final boolean UNROLLED_ULEB128 =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.unrolledUleb128", "true"));
    private static final boolean SWAR_ULEB128 =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.swarUleb128", "true"));
    private static final boolean SCAN_ALL_ONE_DEFINITION_RUNS =
            Boolean.parseBoolean(System.getProperty("nitro.parquet.scanAllOneDefinitionRuns", "true"));
    private MemorySegment segment;
    private long segmentLimit;
    private long position;
    private int bitWidth;
    private int byteWidth;

    // current run state
    private int rleValue;
    private int rleRemaining;
    private int bitPackedRemaining;
    private long bitCursor; // absolute bit offset into the segment, within the active bit-packed run

    void init(MemorySegment segment, long offset, int bitWidth)
    {
        this.segment = segment;
        this.segmentLimit = segment.byteSize();
        this.position = offset;
        this.bitWidth = bitWidth;
        this.byteWidth = (bitWidth + 7) / 8;
        this.rleRemaining = 0;
        this.bitPackedRemaining = 0;
    }

    void read(int[] out, int outOffset, int count)
    {
        // Kept deliberately small: the bit-packed unpack lives in its own method so this hot loop (the RLE fill is
        // the common case — short runs) stays low on register pressure and the JIT doesn't spill the fill's
        // array/index to the stack each iteration.
        int produced = 0;
        while (produced < count) {
            if (rleRemaining == 0 && bitPackedRemaining == 0) {
                loadNextRun();
            }
            if (rleRemaining > 0) {
                int n = Math.min(rleRemaining, count - produced);
                int base = outOffset + produced;
                java.util.Arrays.fill(out, base, base + n, rleValue);
                rleRemaining -= n;
                produced += n;
            }
            else {
                int n = Math.min(bitPackedRemaining, count - produced);
                readBitPacked(out, outOffset + produced, n);
                bitPackedRemaining -= n;
                produced += n;
                if (bitPackedRemaining == 0) {
                    position = bitCursor >>> 3; // bit-packed runs end byte-aligned
                }
            }
        }
    }

    /**
     * Run-oriented cursor for the predicate-over-dictionary filter. Returns a positive {@code k} when the next
     * {@code k} values (capped at {@code max}) are a single RLE run — all equal to {@link #currentRleValue()} — and
     * consumes them; the caller tests the predicate once and emits/skips the whole run. Returns a negative {@code -k}
     * when the next {@code k} values are bit-packed (heterogeneous): they are left in place for {@link #read} to
     * decode per value. This turns a rejected homogeneous run from {@code O(run)} per-row tests into {@code O(1)},
     * which dominates on run-length-encoded key columns (a date column can be one run of tens of millions).
     */
    int nextRun(int max)
    {
        if (rleRemaining == 0 && bitPackedRemaining == 0) {
            loadNextRun();
        }
        if (rleRemaining > 0) {
            int n = Math.min(rleRemaining, max);
            rleRemaining -= n;
            return n;
        }
        return -Math.min(bitPackedRemaining, max);
    }

    /** The value of the RLE run most recently returned by {@link #nextRun}. Valid only immediately after a positive return. */
    int currentRleValue()
    {
        return rleValue;
    }

    private void readBitPacked(int[] out, int base, int n)
    {
        int width = bitWidth;
        long mask = (1L << width) - 1;
        long cursor = bitCursor;
        MemorySegment data = segment;
        int i = 0;
        // SIMD the byte-aligned groups of 8, then scalar tail. unpack8 assumes each group starts on a byte
        // boundary; that holds when the run starts byte-aligned (full decode), but NOT after a skip(gap) leaves the
        // cursor mid-byte (skip-decode) -- then the scalar branchless reader runs. cursor advances by width*8 bits
        // = whole bytes per group, so alignment is invariant across groups.
        if ((cursor & 7) == 0) {
            VectorBitUnpacker.Tables tables = VectorBitUnpacker.tablesFor(width);
            int groups = n & ~7;
            long limit = segmentLimit - 32;
            while (i < groups) {
                long byteOffset = cursor >>> 3;
                if (byteOffset > limit) {
                    break;
                }
                VectorBitUnpacker.unpack8(data, byteOffset, out, base + i, tables);
                cursor += (long) width * 8;
                i += 8;
            }
        }
        for (; i < n; i++) {
            long word = data.get(LE_LONG, cursor >>> 3);
            out[base + i] = (int) ((word >>> ((int) cursor & 7)) & mask);
            cursor += width;
        }
        bitCursor = cursor;
    }

    /**
     * Fast path for bitWidth-1 definition levels: if the next {@code count} levels are all {@code 1} (i.e. the page
     * is null-free, by far the common case), consume them and return {@code true} without materializing each level.
     * The stream may contain multiple RLE and bit-packed runs; parquet writers commonly choose bit packing even for
     * an all-one page. Returns {@code false} otherwise, leaving the reader in an undefined position — the caller must
     * {@link #init} again before a full {@link #read}.
     */
    boolean consumeIfAllOnes(int count)
    {
        if (rleRemaining == 0 && bitPackedRemaining == 0) {
            loadNextRun();
        }
        if (!SCAN_ALL_ONE_DEFINITION_RUNS) {
            if (rleRemaining < count || rleValue != 1) {
                return false;
            }
            rleRemaining -= count;
            return true;
        }

        int remaining = count;
        while (remaining > 0) {
            if (rleRemaining == 0 && bitPackedRemaining == 0) {
                loadNextRun();
            }
            if (rleRemaining > 0) {
                int n = Math.min(rleRemaining, remaining);
                if (rleValue != 1) {
                    return false;
                }
                rleRemaining -= n;
                remaining -= n;
                continue;
            }

            int n = Math.min(bitPackedRemaining, remaining);
            if (!consumeBitPackedOnes(n)) {
                return false;
            }
            bitPackedRemaining -= n;
            remaining -= n;
            if (bitPackedRemaining == 0) {
                position = bitCursor >>> 3;
            }
        }
        return true;
    }

    private boolean consumeBitPackedOnes(int count)
    {
        if (bitWidth != 1) {
            throw new IllegalStateException("all-one scan requires bit width 1");
        }
        int remaining = count;
        long cursor = bitCursor;
        while (remaining > 0) {
            int shift = (int) cursor & 7;
            int take = Math.min(Long.SIZE - shift, remaining);
            long word = segment.get(LE_LONG, cursor >>> 3);
            long mask = take == Long.SIZE ? -1L : (1L << take) - 1;
            if (((word >>> shift) & mask) != mask) {
                bitCursor = cursor;
                return false;
            }
            cursor += take;
            remaining -= take;
        }
        bitCursor = cursor;
        return true;
    }

    /**
     * Decode {@code count} bitWidth-1 definition levels into a running non-null prefix: {@code prefixOut[i]} is the
     * number of non-nulls (def level {@code 1}) in positions {@code [0, i)}, and {@code prefixOut[count]} is the
     * total (also returned). This fuses the three passes the skip path otherwise needs — decode levels, sum them,
     * prefix-sum them — into one, and avoids materializing the per-position level array entirely (a survivor's
     * nullness is then {@code prefixOut[p + 1] == prefixOut[p]}). Only valid for a {@code bitWidth == 1} stream.
     */
    int readNonNullPrefix(int[] prefixOut, int count)
    {
        int produced = 0;
        int running = 0;
        while (produced < count) {
            if (rleRemaining == 0 && bitPackedRemaining == 0) {
                loadNextRun();
            }
            if (rleRemaining > 0) {
                int n = Math.min(rleRemaining, count - produced);
                int end = produced + n;
                if (rleValue == 0) {
                    java.util.Arrays.fill(prefixOut, produced, end, running);
                }
                else {
                    int r = running;
                    for (int p = produced; p < end; p++) {
                        prefixOut[p] = r++;
                    }
                    running = r;
                }
                rleRemaining -= n;
                produced += n;
            }
            else {
                int n = Math.min(bitPackedRemaining, count - produced);
                long cursor = bitCursor;
                MemorySegment data = segment;
                int p = produced;
                int end = produced + n;
                while (p < end) {
                    long word = data.get(LE_LONG, cursor >>> 3);
                    int shift = (int) cursor & 7;
                    int take = Math.min(64 - shift, end - p);
                    for (int k = 0; k < take; k++) {
                        prefixOut[p++] = running;
                        running += (int) ((word >>> (shift + k)) & 1);
                    }
                    cursor += take;
                }
                bitCursor = cursor;
                bitPackedRemaining -= n;
                produced += n;
                if (bitPackedRemaining == 0) {
                    position = bitCursor >>> 3;
                }
            }
        }
        prefixOut[count] = running;
        return running;
    }

    /**
     * Skip {@code n} bitWidth-1 definition levels, returning the number of non-nulls (level {@code 1}) among them.
     * An RLE run contributes {@code O(1)} (its value times the consumed length); a bit-packed run is popcounted.
     * This is the streaming counterpart of {@link #skip} for the skip-decode path, letting the value decoder advance
     * by exactly the non-null count without materializing the levels.
     */
    int skipCountingOnes(int n)
    {
        int remaining = n;
        int ones = 0;
        while (remaining > 0) {
            if (rleRemaining == 0 && bitPackedRemaining == 0) {
                loadNextRun();
            }
            if (rleRemaining > 0) {
                int s = Math.min(rleRemaining, remaining);
                if (rleValue == 1) {
                    ones += s;
                }
                rleRemaining -= s;
                remaining -= s;
            }
            else {
                int s = Math.min(bitPackedRemaining, remaining);
                long cursor = bitCursor;
                int end = s;
                int produced = 0;
                while (produced < end) {
                    long word = segment.get(LE_LONG, cursor >>> 3);
                    int shift = (int) cursor & 7;
                    int take = Math.min(64 - shift, end - produced);
                    long bits = (word >>> shift) & ((take == 64) ? -1L : ((1L << take) - 1));
                    ones += Long.bitCount(bits);
                    cursor += take;
                    produced += take;
                }
                bitCursor = cursor;
                bitPackedRemaining -= s;
                remaining -= s;
                if (bitPackedRemaining == 0) {
                    position = bitCursor >>> 3;
                }
            }
        }
        return ones;
    }

    /**
     * Decode the next {@code n} bitWidth-1 definition levels for one survivor run and return the non-null count.
     * <p>Fast path: when the run lies entirely within a single RLE run it is homogeneous (all non-null or all null),
     * so the count is {@code n} or {@code 0} in {@code O(1)} and {@code defOut} is left untouched — this is the
     * common dense case and the reason streaming does not regress high-selectivity scans. Only a run that straddles
     * an RLE/bit-packed boundary (i.e. actually mixes nulls and non-nulls) falls back to materializing {@code defOut}
     * with the per-position 0/1 levels. The caller therefore inspects {@code defOut} only when {@code 0 < count < n}.
     */
    int readRunCountingOnes(int[] defOut, int n)
    {
        if (rleRemaining == 0 && bitPackedRemaining == 0) {
            loadNextRun();
        }
        if (bitPackedRemaining == 0 && rleRemaining >= n) {
            int value = rleValue;
            rleRemaining -= n;
            return value == 1 ? n : 0;
        }
        read(defOut, 0, n);
        int ones = 0;
        for (int i = 0; i < n; i++) {
            ones += defOut[i];
        }
        return ones;
    }

    /** Advance past {@code n} values without decoding them. */
    void skip(int n)
    {
        int remaining = n;
        while (remaining > 0) {
            if (rleRemaining == 0 && bitPackedRemaining == 0) {
                loadNextRun();
            }
            if (rleRemaining > 0) {
                int s = Math.min(rleRemaining, remaining);
                rleRemaining -= s;
                remaining -= s;
            }
            else {
                int s = Math.min(bitPackedRemaining, remaining);
                bitCursor += (long) s * bitWidth;
                bitPackedRemaining -= s;
                remaining -= s;
                if (bitPackedRemaining == 0) {
                    position = bitCursor >>> 3;
                }
            }
        }
    }

    private void loadNextRun()
    {
        int header = readUleb128();
        if ((header & 1) == 0) {
            rleRemaining = header >>> 1;
            rleValue = readLittleEndian(byteWidth);
        }
        else {
            bitPackedRemaining = (header >>> 1) * 8;
            bitCursor = position << 3;
        }
    }

    private int readLittleEndian(int bytes)
    {
        // The RLE run value is read once per RLE run (tens of millions of times per query); a single masked
        // little-endian load beats a per-byte refill. byteWidth is 1..4; mask to the requested width.
        if (position + 8 <= segmentLimit) {
            long word = segment.get(LE_LONG, position);
            position += bytes;
            return (int) (word & ((1L << (bytes << 3)) - 1));
        }
        int value = 0;
        for (int i = 0; i < bytes; i++) {
            value |= (segment.get(JAVA_BYTE, position++) & 0xFF) << (8 * i);
        }
        return value;
    }

    private int readUleb128()
    {
        // Run-length headers are tiny varints (<= 5 bytes for any int). One unaligned 64-bit load covers the whole
        // header in a single access, then the bytes are extracted from the register. The page body carries trailing
        // slack so the 8-byte read stays in bounds, with a byte-by-byte fallback at the very end of the segment.
        if (position + 8 <= segmentLimit) {
            long word = segment.get(LE_LONG, position);
            // Wide dictionary IDs produce varied multi-byte run headers where branch removal wins.
            // Narrow level/ID streams have highly predictable headers and retain the unrolled path.
            if (SWAR_ULEB128 && bitWidth >= 16) {
                // Locate the first byte whose continuation bit is clear, then remove the one-bit gap
                // between each seven-bit payload. Parquet integer run headers are limited to five bytes.
                long stops = ~word & 0x0000_0080_8080_8080L;
                int bytes = (Long.numberOfTrailingZeros(stops) >>> 3) + 1;
                long packed = (word & 0x7FL) |
                        ((word >>> 1) & 0x3F80L) |
                        ((word >>> 2) & 0x1FC000L) |
                        ((word >>> 3) & 0x0FE00000L) |
                        ((word >>> 4) & 0xF0000000L);
                position += bytes;
                int payloadBits = Math.min(bytes * 7, Integer.SIZE);
                return (int) (packed & (-1L >>> (Long.SIZE - payloadBits)));
            }
            if (UNROLLED_ULEB128) {
                int first = (int) word & 0xFF;
                if ((first & 0x80) == 0) {
                    position++;
                    return first;
                }
                int second = (int) (word >>> 8) & 0xFF;
                int value = (first & 0x7F) | ((second & 0x7F) << 7);
                if ((second & 0x80) == 0) {
                    position += 2;
                    return value;
                }
                int third = (int) (word >>> 16) & 0xFF;
                value |= (third & 0x7F) << 14;
                if ((third & 0x80) == 0) {
                    position += 3;
                    return value;
                }
                int fourth = (int) (word >>> 24) & 0xFF;
                value |= (fourth & 0x7F) << 21;
                if ((fourth & 0x80) == 0) {
                    position += 4;
                    return value;
                }
                int fifth = (int) (word >>> 32) & 0x0F;
                position += 5;
                return value | (fifth << 28);
            }
            int value = 0;
            int shift = 0;
            int consumed = 0;
            while (true) {
                int b = (int) (word >>> (consumed << 3)) & 0xFF;
                consumed++;
                value |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    position += consumed;
                    return value;
                }
                shift += 7;
            }
        }
        int value = 0;
        int shift = 0;
        while (true) {
            int b = segment.get(JAVA_BYTE, position++) & 0xFF;
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }
}
