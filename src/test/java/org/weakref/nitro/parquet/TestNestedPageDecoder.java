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

import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class TestNestedPageDecoder
{
    @Test
    void testSkipsSharedRepetitionLevelMaterialization()
    {
        NestedPageDecoder decoder = new NestedPageDecoder(
                1,
                2,
                RleReaderPolicy.defaults(),
                new PrimitiveArrayPool(0, 0),
                false);
        LongPhysicalValueDecoder values = new LongPhysicalValueDecoder(Type.INT64, new PrimitiveArrayPool(0, 0));
        values.decodeDictionary(longs(10, 20), 2, Encoding.PLAIN);
        decodeDictionaryPage(
                decoder,
                values,
                new int[] {0, 1, 0, 1},
                1,
                new int[] {2, 2, 2, 2},
                2,
                new int[] {0, 1, 1, 0},
                1);

        assertThat(decoder.hasRepetitionLevels()).isFalse();
        assertThat(decoder.definitionLevel(3)).isEqualTo(2);
        assertThat(longValue(decoder, values, 2)).isEqualTo(20);
    }

    @Test
    void testDictionaryLongPagePreservesMapEvents()
    {
        NestedPageDecoder decoder = new NestedPageDecoder(
                1,
                2,
                RleReaderPolicy.defaults(),
                new PrimitiveArrayPool(0, 0));
        LongPhysicalValueDecoder values = new LongPhysicalValueDecoder(Type.INT64, new PrimitiveArrayPool(0, 0));
        values.decodeDictionary(longs(10, 20), 2, Encoding.PLAIN);
        decodeDictionaryPage(
                decoder,
                values,
                new int[] {0, 1, 0, 1},
                1,
                new int[] {2, 2, 2, 2},
                2,
                new int[] {0, 1, 1, 0},
                1);

        assertThat(decoder.eventCount()).isEqualTo(4);
        assertThat(decoder.repetitionLevel(0)).isZero();
        assertThat(decoder.repetitionLevel(1)).isOne();
        assertThat(decoder.definitionLevel(3)).isEqualTo(2);
        assertThat(longValue(decoder, values, 0)).isEqualTo(10);
        assertThat(longValue(decoder, values, 1)).isEqualTo(20);
        assertThat(longValue(decoder, values, 2)).isEqualTo(20);
        assertThat(longValue(decoder, values, 3)).isEqualTo(10);
    }

    @Test
    void testDictionaryBinaryPageDoesNotConsumeValueForNullEntry()
    {
        NestedPageDecoder decoder = new NestedPageDecoder(
                1,
                3,
                RleReaderPolicy.defaults(),
                new PrimitiveArrayPool(0, 0));
        BinaryPhysicalValueDecoder values = new BinaryPhysicalValueDecoder(new PrimitiveArrayPool(0, 0));
        values.decodeDictionary(binary("one", "two"), 2, Encoding.PLAIN);
        decodeDictionaryPage(
                decoder,
                values,
                new int[] {0, 1, 0},
                1,
                new int[] {3, 2, 3},
                2,
                new int[] {0, 1},
                1);

        assertThat(decoder.hasValue(0)).isTrue();
        assertThat(decoder.hasValue(1)).isFalse();
        assertThat(decoder.hasValue(2)).isTrue();
        assertThat(binary(decoder, values, 0)).isEqualTo("one");
        assertThat(binary(decoder, values, 2)).isEqualTo("two");
    }

    private static void decodeDictionaryPage(
            NestedPageDecoder decoder,
            PhysicalValueDecoder values,
            int[] repetitions,
            int repetitionWidth,
            int[] definitions,
            int definitionWidth,
            int[] ids,
            int idWidth)
    {
        MemorySegment page = dictionaryPage(repetitions, repetitionWidth, definitions, definitionWidth, ids, idWidth);
        decoder.decodeDataPageV1(page, repetitions.length, Encoding.RLE_DICTIONARY, values.dictionarySize());
    }

    private static MemorySegment dictionaryPage(
            int[] repetitions,
            int repetitionWidth,
            int[] definitions,
            int definitionWidth,
            int[] ids,
            int idWidth)
    {
        byte[] repetitionData = bitPackedRun(repetitionWidth, repetitions);
        byte[] definitionData = bitPackedRun(definitionWidth, definitions);
        byte[] idData = bitPackedRun(idWidth, ids);
        ByteBuffer output = ByteBuffer.allocate(
                        Integer.BYTES + repetitionData.length + Integer.BYTES + definitionData.length + 1 + idData.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        output.putInt(repetitionData.length).put(repetitionData);
        output.putInt(definitionData.length).put(definitionData);
        output.put((byte) idWidth).put(idData);
        return MemorySegment.ofArray(output.array());
    }

    private static MemorySegment longs(long... values)
    {
        ByteBuffer output = ByteBuffer.allocate(values.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (long value : values) {
            output.putLong(value);
        }
        return MemorySegment.ofArray(output.array());
    }

    private static MemorySegment binary(String... values)
    {
        int size = Arrays.stream(values).mapToInt(value -> Integer.BYTES + value.length()).sum();
        ByteBuffer output = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        for (String value : values) {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            output.putInt(bytes.length).put(bytes);
        }
        return MemorySegment.ofArray(output.array());
    }

    private static long longValue(NestedPageDecoder decoder, LongPhysicalValueDecoder values, int event)
    {
        int ordinal = decoder.valueOrdinal(event);
        return values.value(ordinal, decoder.dictionaryId(ordinal));
    }

    private static String binary(NestedPageDecoder decoder, BinaryPhysicalValueDecoder values, int event)
    {
        int ordinal = decoder.valueOrdinal(event);
        int dictionaryId = decoder.dictionaryId(ordinal);
        byte[] value = new byte[values.length(ordinal, dictionaryId)];
        values.copy(ordinal, dictionaryId, value, 0);
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] bitPackedRun(int bitWidth, int[] values)
    {
        int groups = (values.length + 7) / 8;
        byte[] output = new byte[5 + groups * bitWidth];
        int offset = writeUleb128(output, (groups << 1) | 1);
        long bitCursor = (long) offset * Byte.SIZE;
        for (int value : values) {
            for (int bit = 0; bit < bitWidth; bit++) {
                if (((value >>> bit) & 1) != 0) {
                    output[(int) (bitCursor >>> 3)] |= (byte) (1 << ((int) bitCursor & 7));
                }
                bitCursor++;
            }
        }
        return Arrays.copyOf(output, offset + groups * bitWidth);
    }

    private static int writeUleb128(byte[] output, int value)
    {
        int offset = 0;
        do {
            int next = value & 0x7F;
            value >>>= 7;
            output[offset++] = (byte) (value == 0 ? next : next | 0x80);
        }
        while (value != 0);
        return offset;
    }
}
