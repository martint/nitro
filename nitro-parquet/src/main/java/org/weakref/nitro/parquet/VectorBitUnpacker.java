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
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * SIMD unpacker for byte-aligned groups of 8 bit-packed values. Narrow widths use a byte- or short-lane kernel;
 * wider irregular widths use one parametric integer kernel with reader-owned derived tables. The latter loads 8
 * little-endian ints (256 bits), and for each output lane gathers the int holding the value's low bits (shift right
 * + mask) and the int holding any bits that spilled into the next int (mask + shift left). All kernels load straight
 * from the native {@link MemorySegment} page buffer.
 *
 * <p>{@link #unpack8} reads 32 bytes from {@code byteOffset}; callers guarantee that much slack.
 */
final class VectorBitUnpacker
{
    private VectorBitUnpacker() {}

    static int unpackGroups(
            MemorySegment segment,
            long byteOffset,
            long endOffset,
            int[] output,
            int outputOffset,
            int valueCount,
            int width,
            Tables tables)
    {
        return switch (width) {
            case 1 -> unpackWidth1(segment, byteOffset, endOffset, output, outputOffset, valueCount, tables);
            case 2, 3, 4, 5 -> unpackBytes(segment, byteOffset, endOffset, output, outputOffset, valueCount, width, tables);
            case 6, 7, 9, 10, 11, 12, 13 -> unpackShorts(segment, byteOffset, endOffset, output, outputOffset, valueCount, width, tables);
            case 8 -> unpackWidth8(segment, byteOffset, endOffset, output, outputOffset, valueCount, tables);
            case 16 -> unpackWidth16(segment, byteOffset, endOffset, output, outputOffset, valueCount, tables);
            default -> unpackGeneral(segment, byteOffset, endOffset, output, outputOffset, valueCount, width, tables);
        };
    }

    sealed interface Tables
            permits ByteTables, GeneralTables, ShortTables, WideningTables {}

    record GeneralTables(
            VectorShuffle<Integer> shuffleLow,
            VectorShuffle<Integer> shuffleHigh,
            IntVector shiftRight,
            IntVector shiftLeft,
            IntVector maskBeforeShiftLeft,
            IntVector mask)
            implements Tables {}

    record ByteTables(
            VectorShuffle<Byte> shuffleLow,
            VectorShuffle<Byte> shuffleHigh,
            ByteVector shiftRight,
            ByteVector shiftLeft,
            ByteVector maskBeforeShiftLeft,
            ByteVector mask)
            implements Tables {}

    record ShortTables(
            VectorShuffle<Short> shuffleLow,
            VectorShuffle<Short> shuffleHigh,
            ShortVector shiftRight,
            ShortVector shiftLeft,
            ShortVector maskBeforeShiftLeft,
            ShortVector mask)
            implements Tables {}

    record WideningTables(IntVector mask)
            implements Tables {}

    static Tables buildTables(int width)
    {
        return switch (width) {
            case 1, 2, 3, 4, 5 -> buildByteTables(width);
            case 6, 7, 9, 10, 11, 12, 13 -> buildShortTables(width);
            case 8 -> new WideningTables(IntVector.broadcast(IntVector.SPECIES_256, 255));
            case 16 -> new WideningTables(IntVector.broadcast(IntVector.SPECIES_256, 65_535));
            default -> buildGeneralTables(width);
        };
    }

    static GeneralTables buildGeneralTables(int width)
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
        return new GeneralTables(
                VectorShuffle.fromArray(IntVector.SPECIES_256, shuffleLow, 0),
                VectorShuffle.fromArray(IntVector.SPECIES_256, shuffleHigh, 0),
                IntVector.fromArray(IntVector.SPECIES_256, shiftRight, 0),
                IntVector.fromArray(IntVector.SPECIES_256, shiftLeft, 0),
                IntVector.fromArray(IntVector.SPECIES_256, maskBeforeShiftLeft, 0),
                IntVector.broadcast(IntVector.SPECIES_256, maskValue));
    }

    private static ByteTables buildByteTables(int width)
    {
        int[] shuffleLow = new int[8];
        int[] shuffleHigh = new int[8];
        byte[] shiftRight = new byte[8];
        byte[] shiftLeft = new byte[8];
        byte[] maskBeforeShiftLeft = new byte[8];
        for (int lane = 0; lane < 8; lane++) {
            int startBit = lane * width;
            int byteIndex = startBit >>> 3;
            int bitOffset = startBit & 7;
            shuffleLow[lane] = byteIndex;
            shiftRight[lane] = (byte) bitOffset;
            if (bitOffset + width > Byte.SIZE) {
                int bitsFromNext = bitOffset + width - Byte.SIZE;
                shuffleHigh[lane] = byteIndex + 1;
                maskBeforeShiftLeft[lane] = (byte) ((1 << bitsFromNext) - 1);
                shiftLeft[lane] = (byte) (Byte.SIZE - bitOffset);
            }
        }
        return new ByteTables(
                VectorShuffle.fromArray(ByteVector.SPECIES_64, shuffleLow, 0),
                VectorShuffle.fromArray(ByteVector.SPECIES_64, shuffleHigh, 0),
                ByteVector.fromArray(ByteVector.SPECIES_64, shiftRight, 0),
                ByteVector.fromArray(ByteVector.SPECIES_64, shiftLeft, 0),
                ByteVector.fromArray(ByteVector.SPECIES_64, maskBeforeShiftLeft, 0),
                ByteVector.broadcast(ByteVector.SPECIES_64, (1 << width) - 1));
    }

    private static ShortTables buildShortTables(int width)
    {
        int[] shuffleLow = new int[8];
        int[] shuffleHigh = new int[8];
        short[] shiftRight = new short[8];
        short[] shiftLeft = new short[8];
        short[] maskBeforeShiftLeft = new short[8];
        for (int lane = 0; lane < 8; lane++) {
            int startBit = lane * width;
            int shortIndex = startBit >>> 4;
            int bitOffset = startBit & 15;
            shuffleLow[lane] = shortIndex;
            shiftRight[lane] = (short) bitOffset;
            if (bitOffset + width > Short.SIZE) {
                int bitsFromNext = bitOffset + width - Short.SIZE;
                shuffleHigh[lane] = shortIndex + 1;
                maskBeforeShiftLeft[lane] = (short) ((1 << bitsFromNext) - 1);
                shiftLeft[lane] = (short) (Short.SIZE - bitOffset);
            }
        }
        return new ShortTables(
                VectorShuffle.fromArray(ShortVector.SPECIES_128, shuffleLow, 0),
                VectorShuffle.fromArray(ShortVector.SPECIES_128, shuffleHigh, 0),
                ShortVector.fromArray(ShortVector.SPECIES_128, shiftRight, 0),
                ShortVector.fromArray(ShortVector.SPECIES_128, shiftLeft, 0),
                ShortVector.fromArray(ShortVector.SPECIES_128, maskBeforeShiftLeft, 0),
                ShortVector.broadcast(ShortVector.SPECIES_128, (1 << width) - 1));
    }

    static void unpack8(MemorySegment segment, long byteOffset, int[] output, int outputOffset, GeneralTables tables)
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

    private static int unpackWidth1(MemorySegment segment, long byteOffset, long endOffset, int[] output, int outputOffset, int valueCount, Tables tables)
    {
        ByteTables bytes = (ByteTables) tables;
        int processed = 0;
        while (processed < valueCount && byteOffset < endOffset) {
            ByteVector.broadcast(ByteVector.SPECIES_64, segment.get(JAVA_BYTE, byteOffset))
                    .lanewise(VectorOperators.LSHR, bytes.shiftRight())
                    .and(bytes.mask())
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset + processed);
            processed += 8;
            byteOffset++;
        }
        return processed;
    }

    private static int unpackBytes(
            MemorySegment segment,
            long byteOffset,
            long endOffset,
            int[] output,
            int outputOffset,
            int valueCount,
            int width,
            Tables tables)
    {
        ByteTables bytes = (ByteTables) tables;
        int processed = 0;
        while (processed < valueCount && byteOffset + ByteVector.SPECIES_64.vectorByteSize() <= endOffset) {
            ByteVector input = ByteVector.fromMemorySegment(ByteVector.SPECIES_64, segment, byteOffset, ByteOrder.LITTLE_ENDIAN);
            ByteVector low = input.rearrange(bytes.shuffleLow())
                    .lanewise(VectorOperators.LSHR, bytes.shiftRight())
                    .and(bytes.mask());
            ByteVector high = input.rearrange(bytes.shuffleHigh())
                    .and(bytes.maskBeforeShiftLeft())
                    .lanewise(VectorOperators.LSHL, bytes.shiftLeft());
            low.or(high).castShape(IntVector.SPECIES_256, 0).reinterpretAsInts().intoArray(output, outputOffset + processed);
            processed += 8;
            byteOffset += width;
        }
        return processed;
    }

    private static int unpackShorts(
            MemorySegment segment,
            long byteOffset,
            long endOffset,
            int[] output,
            int outputOffset,
            int valueCount,
            int width,
            Tables tables)
    {
        ShortTables shorts = (ShortTables) tables;
        int processed = 0;
        while (processed < valueCount && byteOffset + ByteVector.SPECIES_128.vectorByteSize() <= endOffset) {
            ShortVector input = ByteVector.fromMemorySegment(ByteVector.SPECIES_128, segment, byteOffset, ByteOrder.LITTLE_ENDIAN)
                    .reinterpretAsShorts();
            ShortVector low = input.rearrange(shorts.shuffleLow())
                    .lanewise(VectorOperators.LSHR, shorts.shiftRight())
                    .and(shorts.mask());
            ShortVector high = input.rearrange(shorts.shuffleHigh())
                    .and(shorts.maskBeforeShiftLeft())
                    .lanewise(VectorOperators.LSHL, shorts.shiftLeft());
            low.or(high).castShape(IntVector.SPECIES_256, 0).reinterpretAsInts().intoArray(output, outputOffset + processed);
            processed += 8;
            byteOffset += width;
        }
        return processed;
    }

    private static int unpackWidth8(MemorySegment segment, long byteOffset, long endOffset, int[] output, int outputOffset, int valueCount, Tables tables)
    {
        WideningTables widening = (WideningTables) tables;
        int processed = 0;
        while (processed < valueCount && byteOffset + 8 <= endOffset) {
            ByteVector.fromMemorySegment(ByteVector.SPECIES_64, segment, byteOffset, ByteOrder.LITTLE_ENDIAN)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .and(widening.mask())
                    .intoArray(output, outputOffset + processed);
            processed += 8;
            byteOffset += 8;
        }
        return processed;
    }

    private static int unpackWidth16(MemorySegment segment, long byteOffset, long endOffset, int[] output, int outputOffset, int valueCount, Tables tables)
    {
        WideningTables widening = (WideningTables) tables;
        int processed = 0;
        while (processed < valueCount && byteOffset + 16 <= endOffset) {
            ByteVector.fromMemorySegment(ByteVector.SPECIES_128, segment, byteOffset, ByteOrder.LITTLE_ENDIAN)
                    .reinterpretAsShorts()
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .and(widening.mask())
                    .intoArray(output, outputOffset + processed);
            processed += 8;
            byteOffset += 16;
        }
        return processed;
    }

    static int unpackGeneral(
            MemorySegment segment,
            long byteOffset,
            long endOffset,
            int[] output,
            int outputOffset,
            int valueCount,
            int width,
            Tables tables)
    {
        GeneralTables general = (GeneralTables) tables;
        int processed = 0;
        while (processed < valueCount && byteOffset + 32 <= endOffset) {
            unpack8(segment, byteOffset, output, outputOffset + processed, general);
            processed += 8;
            byteOffset += width;
        }
        return processed;
    }
}
