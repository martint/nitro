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
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TestBinaryPhysicalValueDecoder
{
    @Test
    void testDecodesFixedWidthValues()
    {
        try (BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(3, new PrimitiveArrayPool(0, 0))) {
            decoder.decodeDictionary(MemorySegment.ofArray(new byte[] {1, 2, 3, 4, 5, 6}), 2, Encoding.PLAIN);
            decoder.decodePlain(MemorySegment.ofArray(new byte[] {9, 8, 7, 6, 5, 4, 3}), 1, 2);

            assertThat(bytes(decoder, 0, -1)).containsExactly(8, 7, 6);
            assertThat(bytes(decoder, 1, -1)).containsExactly(5, 4, 3);
            assertThat(bytes(decoder, 0, 1)).containsExactly(4, 5, 6);
        }
    }

    @Test
    void testDeltaLengthConstantLengths()
    {
        // block size 128, four mini-blocks, three values, first length 1;
        // all following deltas are zero and therefore have zero bit width.
        byte[] page = {
                (byte) 0x80, 0x01, 0x04, 0x03, 0x02,
                0x00, 0x00, 0x00, 0x00, 0x00,
                'a', 'b', 'c'};

        try (BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(new PrimitiveArrayPool(0, 0))) {
            decoder.decodeData(MemorySegment.ofArray(page), 0, 3, Encoding.DELTA_LENGTH_BYTE_ARRAY);

            assertThat(value(decoder, 0)).isEqualTo("a");
            assertThat(value(decoder, 1)).isEqualTo("b");
            assertThat(value(decoder, 2)).isEqualTo("c");
        }
    }

    @Test
    void testDeltaLengthMixedLengths()
    {
        // Lengths [1, 2, 0]. The minimum delta is -2; adjusted deltas [3, 0]
        // use a two-bit mini-block (the remaining 30 entries are padding).
        byte[] page = {
                (byte) 0x80, 0x01, 0x04, 0x03, 0x02,
                0x03, 0x02, 0x00, 0x00, 0x00,
                0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                'a', 'b', 'c'};

        try (BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(new PrimitiveArrayPool(0, 0))) {
            decoder.decodeData(MemorySegment.ofArray(page), 0, 3, Encoding.DELTA_LENGTH_BYTE_ARRAY);

            assertThat(value(decoder, 0)).isEqualTo("a");
            assertThat(value(decoder, 1)).isEqualTo("bc");
            assertThat(value(decoder, 2)).isEmpty();
        }
    }

    private static String value(BinaryPhysicalValueDecoder decoder, int ordinal)
    {
        byte[] output = new byte[decoder.length(ordinal, -1)];
        decoder.copy(ordinal, -1, output, 0);
        return new String(output, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(BinaryPhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        byte[] output = new byte[decoder.length(ordinal, dictionaryId)];
        decoder.copy(ordinal, dictionaryId, output, 0);
        return output;
    }
}
