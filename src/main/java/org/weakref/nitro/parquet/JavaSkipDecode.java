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
 * Pure-Java, fully-inlined per-page skip-decode for a dict LONG column — the same one-pass algorithm as the native
 * (Rust) kernel, but with both RLE/bit-packed cursors held in local variables so the JIT keeps them in registers and
 * emits no per-value method call. This is what {@code ColumnReader.readSelectedLongs}'s layered per-run pipeline
 * (separate {@code RleReader} calls + {@code idBuffer}/{@code runDef} materialization) costs relative to; folding it
 * into one loop removes the dispatch + buffer round-trips (the JVM-recoverable part of the native win).
 */
final class JavaSkipDecode
{
    private JavaSkipDecode() {}

    /**
     * Little-endian 64-bit word at {@code byteOffset}, with a byte-by-byte fallback when the read would run past
     * {@code segmentLimit} (the last page of a file is sliced without the usual trailing SLACK).
     */
    private static long readWord(MemorySegment body, long byteOffset, long segmentLimit)
    {
        if (byteOffset + 8 <= segmentLimit) {
            return body.get(LE_LONG, byteOffset);
        }
        long word = 0;
        long end = Math.min(byteOffset + 8, segmentLimit);
        for (long i = byteOffset; i < end; i++) {
            word |= (body.get(JAVA_BYTE, i) & 0xFFL) << ((i - byteOffset) * 8);
        }
        return word;
    }

    /**
     * Gather {@code dict[id]} (or 0/null) at each of the sorted, page-relative {@code survivors[survStart..survEnd)}
     * (offset by {@code posBase}) into {@code out[produced..]}. {@code defOffset < 0} => null-free page (value index
     * == position); otherwise walk the bit-1 definition-level stream at {@code defOffset} for nulls + the value index.
     * Returns the advanced output cursor.
     */
    static int skipDecodeDictLongs(MemorySegment body, long valueOffset, int valueBitWidth, long defOffset,
            int[] survivors, int survStart, int survEnd, int posBase,
            long[] dict, long[] out, int produced, boolean[] nullsOut)
    {
        // The last uncompressed page of a file is sliced without the usual trailing SLACK (only `available` bytes
        // remain), so an 8-byte word load near the tail can run past the segment. Guard the bit reads with a
        // byte-by-byte fallback in that window, exactly as RleReader does.
        long segmentLimit = body.byteSize();
        // Value-stream cursor state (locals => registers).
        int vWidth = valueBitWidth;
        long vByteWidth = (valueBitWidth + 7) >>> 3;
        long vMask = vWidth >= 64 ? -1L : (1L << vWidth) - 1;
        long vPos = valueOffset;      // byte cursor to the next run header
        int vRleRem = 0;
        long vRleVal = 0;
        long vBpRem = 0;
        long vBitCur = 0;             // absolute bit offset within the active bit-packed run

        if (defOffset < 0) {
            long cursor = 0;          // next unread value position
            for (int s = survStart; s < survEnd; s++) {
                long p = posBase + survivors[s];
                long toSkip = p - cursor;
                // --- value.skip(toSkip) ---
                while (toSkip > 0) {
                    if (vRleRem == 0 && vBpRem == 0) {
                        // loadNextRun
                        int header = 0;
                        int shift = 0;
                        int consumed = 0;
                        while (true) {
                            int b = body.get(JAVA_BYTE, vPos + consumed) & 0xFF;
                            consumed++;
                            header |= (b & 0x7F) << shift;
                            if ((b & 0x80) == 0) {
                                break;
                            }
                            shift += 7;
                        }
                        vPos += consumed;
                        if ((header & 1) == 0) {
                            vRleRem = header >>> 1;
                            long v = 0;
                            for (int i = 0; i < vByteWidth; i++) {
                                v |= (body.get(JAVA_BYTE, vPos + i) & 0xFFL) << (i * 8);
                            }
                            vRleVal = v;
                            vPos += vByteWidth;
                        }
                        else {
                            vBpRem = (long) (header >>> 1) * 8;
                            vBitCur = vPos << 3;
                        }
                    }
                    if (vRleRem > 0) {
                        long t = Math.min(vRleRem, toSkip);
                        vRleRem -= (int) t;
                        toSkip -= t;
                    }
                    else {
                        long t = Math.min(vBpRem, toSkip);
                        vBitCur += t * vWidth;
                        vBpRem -= t;
                        toSkip -= t;
                        if (vBpRem == 0) {
                            vPos = (vBitCur + 7) >>> 3;
                        }
                    }
                }
                // --- value.next() ---
                if (vRleRem == 0 && vBpRem == 0) {
                    int header = 0;
                    int shift = 0;
                    int consumed = 0;
                    while (true) {
                        int b = body.get(JAVA_BYTE, vPos + consumed) & 0xFF;
                        consumed++;
                        header |= (b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            break;
                        }
                        shift += 7;
                    }
                    vPos += consumed;
                    if ((header & 1) == 0) {
                        vRleRem = header >>> 1;
                        long v = 0;
                        for (int i = 0; i < vByteWidth; i++) {
                            v |= (body.get(JAVA_BYTE, vPos + i) & 0xFFL) << (i * 8);
                        }
                        vRleVal = v;
                        vPos += vByteWidth;
                    }
                    else {
                        vBpRem = (long) (header >>> 1) * 8;
                        vBitCur = vPos << 3;
                    }
                }
                long id;
                if (vRleRem > 0) {
                    id = vRleVal;
                    vRleRem--;
                }
                else {
                    long word = readWord(body, vBitCur >>> 3, segmentLimit);
                    id = (word >>> ((int) vBitCur & 7)) & vMask;
                    vBitCur += vWidth;
                    vBpRem--;
                    if (vBpRem == 0) {
                        vPos = (vBitCur + 7) >>> 3;
                    }
                }
                cursor = p + 1;
                out[produced] = dict[(int) id];
                if (nullsOut != null) {
                    nullsOut[produced] = false;
                }
                produced++;
            }
            return produced;
        }

        // Nullable: def stream cursor (bit width 1).
        long dPos = defOffset;
        int dRleRem = 0;
        long dRleVal = 0;
        long dBpRem = 0;
        long dBitCur = 0;
        long defPos = 0;

        for (int s = survStart; s < survEnd; s++) {
            long p = posBase + survivors[s];
            long gapNonNull = 0;
            long need = p - defPos;
            // --- def.skipCountingOnes(need) ---
            while (need > 0) {
                if (dRleRem == 0 && dBpRem == 0) {
                    int header = 0;
                    int shift = 0;
                    int consumed = 0;
                    while (true) {
                        int b = body.get(JAVA_BYTE, dPos + consumed) & 0xFF;
                        consumed++;
                        header |= (b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            break;
                        }
                        shift += 7;
                    }
                    dPos += consumed;
                    if ((header & 1) == 0) {
                        dRleRem = header >>> 1;
                        dRleVal = body.get(JAVA_BYTE, dPos) & 0xFFL;
                        dPos += 1;
                    }
                    else {
                        dBpRem = (long) (header >>> 1) * 8;
                        dBitCur = dPos << 3;
                    }
                }
                if (dRleRem > 0) {
                    long t = Math.min(dRleRem, need);
                    if (dRleVal == 1) {
                        gapNonNull += t;
                    }
                    dRleRem -= (int) t;
                    need -= t;
                }
                else {
                    long t = Math.min(dBpRem, need);
                    long produced2 = 0;
                    long c = dBitCur;
                    while (produced2 < t) {
                        long word = readWord(body, c >>> 3, segmentLimit);
                        int sh = (int) (c & 7);
                        long take = Math.min(64 - sh, t - produced2);
                        long bits = (word >>> sh) & (take == 64 ? -1L : (1L << take) - 1);
                        gapNonNull += Long.bitCount(bits);
                        c += take;
                        produced2 += take;
                    }
                    dBitCur = c;
                    dBpRem -= t;
                    need -= t;
                    if (dBpRem == 0) {
                        dPos = dBitCur >>> 3;
                    }
                }
            }
            defPos = p;
            // --- value.skip(gapNonNull) ---
            while (gapNonNull > 0) {
                if (vRleRem == 0 && vBpRem == 0) {
                    int header = 0;
                    int shift = 0;
                    int consumed = 0;
                    while (true) {
                        int b = body.get(JAVA_BYTE, vPos + consumed) & 0xFF;
                        consumed++;
                        header |= (b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            break;
                        }
                        shift += 7;
                    }
                    vPos += consumed;
                    if ((header & 1) == 0) {
                        vRleRem = header >>> 1;
                        long v = 0;
                        for (int i = 0; i < vByteWidth; i++) {
                            v |= (body.get(JAVA_BYTE, vPos + i) & 0xFFL) << (i * 8);
                        }
                        vRleVal = v;
                        vPos += vByteWidth;
                    }
                    else {
                        vBpRem = (long) (header >>> 1) * 8;
                        vBitCur = vPos << 3;
                    }
                }
                if (vRleRem > 0) {
                    long t = Math.min(vRleRem, gapNonNull);
                    vRleRem -= (int) t;
                    gapNonNull -= t;
                }
                else {
                    long t = Math.min(vBpRem, gapNonNull);
                    vBitCur += t * vWidth;
                    vBpRem -= t;
                    gapNonNull -= t;
                    if (vBpRem == 0) {
                        vPos = (vBitCur + 7) >>> 3;
                    }
                }
            }
            // --- def.next() (the level at p) ---
            if (dRleRem == 0 && dBpRem == 0) {
                int header = 0;
                int shift = 0;
                int consumed = 0;
                while (true) {
                    int b = body.get(JAVA_BYTE, dPos + consumed) & 0xFF;
                    consumed++;
                    header |= (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) {
                        break;
                    }
                    shift += 7;
                }
                dPos += consumed;
                if ((header & 1) == 0) {
                    dRleRem = header >>> 1;
                    dRleVal = body.get(JAVA_BYTE, dPos) & 0xFFL;
                    dPos += 1;
                }
                else {
                    dBpRem = (long) (header >>> 1) * 8;
                    dBitCur = dPos << 3;
                }
            }
            long present;
            if (dRleRem > 0) {
                present = dRleVal;
                dRleRem--;
            }
            else {
                long word = readWord(body, dBitCur >>> 3, segmentLimit);
                present = (word >>> ((int) dBitCur & 7)) & 1;
                dBitCur += 1;
                dBpRem--;
                if (dBpRem == 0) {
                    dPos = (dBitCur + 7) >>> 3;
                }
            }
            defPos = p + 1;
            if (present != 0) {
                // --- value.next() --- (load a new run first if the prior one was exactly exhausted, e.g. a
                // preceding value.skip(0) at a run boundary left no live run; mirrors the Rust RleWalk.next).
                if (vRleRem == 0 && vBpRem == 0) {
                    int header = 0;
                    int shift = 0;
                    int consumed = 0;
                    while (true) {
                        int b = body.get(JAVA_BYTE, vPos + consumed) & 0xFF;
                        consumed++;
                        header |= (b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            break;
                        }
                        shift += 7;
                    }
                    vPos += consumed;
                    if ((header & 1) == 0) {
                        vRleRem = header >>> 1;
                        long v = 0;
                        for (int i = 0; i < vByteWidth; i++) {
                            v |= (body.get(JAVA_BYTE, vPos + i) & 0xFFL) << (i * 8);
                        }
                        vRleVal = v;
                        vPos += vByteWidth;
                    }
                    else {
                        vBpRem = (long) (header >>> 1) * 8;
                        vBitCur = vPos << 3;
                    }
                }
                long id;
                if (vRleRem > 0) {
                    id = vRleVal;
                    vRleRem--;
                }
                else {
                    long word = readWord(body, vBitCur >>> 3, segmentLimit);
                    id = (word >>> ((int) vBitCur & 7)) & vMask;
                    vBitCur += vWidth;
                    vBpRem--;
                    if (vBpRem == 0) {
                        vPos = (vBitCur + 7) >>> 3;
                    }
                }
                out[produced] = dict[(int) id];
                if (nullsOut != null) {
                    nullsOut[produced] = false;
                }
            }
            else {
                out[produced] = 0L;
                if (nullsOut != null) {
                    nullsOut[produced] = true;
                }
            }
            produced++;
        }
        return produced;
    }
}
