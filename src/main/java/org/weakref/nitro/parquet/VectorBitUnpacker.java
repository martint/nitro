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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * SIMD unpacker for one byte-aligned group of 8 bit-packed values, generalized over the bit width into a
 * single parametric kernel (one {@link Tables} per width, cached). Mirrors the technique in Trino's
 * hand-written per-width unpackers — load 8 little-endian ints (256 bits), and for each output lane gather
 * the int holding the value's low bits (shift right + mask) OR the int holding any bits that spilled into
 * the next int (mask + shift left) — but derives the shuffles/shifts/masks arithmetically from the width
 * rather than enumerating 20 classes. Loads straight from the native {@link MemorySegment} page buffer.
 *
 * <p>{@link #unpack8} reads 32 bytes from {@code byteOffset}; callers guarantee that much slack.
 */
final class VectorBitUnpacker
{
    private static final Tables[] CACHE = new Tables[33];

    private VectorBitUnpacker() {}

    record Tables(
            VectorShuffle<Integer> shuffleLow,
            VectorShuffle<Integer> shuffleHigh,
            IntVector shiftRight,
            IntVector shiftLeft,
            IntVector maskBeforeShiftLeft,
            IntVector mask) {}

    static Tables tablesFor(int width)
    {
        Tables tables = CACHE[width];
        if (tables == null) {
            tables = build(width);
            CACHE[width] = tables;
        }
        return tables;
    }

    private static Tables build(int width)
    {
        int[] shuffleLow = new int[8];
        int[] shuffleHigh = new int[8];
        int[] shiftRight = new int[8];
        int[] shiftLeft = new int[8];
        int[] maskBeforeShiftLeft = new int[8];
        for (int lane = 0; lane < 8; lane++) {
            int startBit = lane * width;
            int intIndex = startBit >>> 5;
            int bitOffset = startBit & 31;
            shuffleLow[lane] = intIndex;
            shiftRight[lane] = bitOffset;
            if (bitOffset + width > 32) {
                int bitsFromNext = bitOffset + width - 32;
                shuffleHigh[lane] = intIndex + 1;
                maskBeforeShiftLeft[lane] = bitsFromNext == 32 ? -1 : (1 << bitsFromNext) - 1;
                shiftLeft[lane] = 32 - bitOffset;
            }
            // else: shuffleHigh/maskBeforeShiftLeft/shiftLeft stay 0 -> the high term contributes 0.
        }
        int maskValue = width == 32 ? -1 : (1 << width) - 1;
        return new Tables(
                VectorShuffle.fromArray(IntVector.SPECIES_256, shuffleLow, 0),
                VectorShuffle.fromArray(IntVector.SPECIES_256, shuffleHigh, 0),
                IntVector.fromArray(IntVector.SPECIES_256, shiftRight, 0),
                IntVector.fromArray(IntVector.SPECIES_256, shiftLeft, 0),
                IntVector.fromArray(IntVector.SPECIES_256, maskBeforeShiftLeft, 0),
                IntVector.broadcast(IntVector.SPECIES_256, maskValue));
    }

    static void unpack8(MemorySegment segment, long byteOffset, int[] output, int outputOffset, Tables tables)
    {
        IntVector window = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, segment, byteOffset, ByteOrder.LITTLE_ENDIAN)
                .reinterpretAsInts();
        IntVector low = window.rearrange(tables.shuffleLow())
                .lanewise(VectorOperators.LSHR, tables.shiftRight())
                .and(tables.mask());
        IntVector high = window.rearrange(tables.shuffleHigh())
                .and(tables.maskBeforeShiftLeft())
                .lanewise(VectorOperators.LSHL, tables.shiftLeft());
        low.or(high).intoArray(output, outputOffset);
    }
}
