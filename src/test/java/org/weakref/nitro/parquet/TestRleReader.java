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

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.assertj.core.api.Assertions.assertThat;

class TestRleReader
{
    private static final RleReaderPolicy DEFAULT_POLICY = RleReaderPolicy.defaults();

    @Test
    void testRleHeadersAcrossEveryUlebWidth()
    {
        for (int header : new int[] {2, 128, 16_384, 2_097_152, 268_435_456}) {
            byte[] encoded = new byte[16];
            int offset = writeUleb128(encoded, header);
            encoded[offset] = 73;

            RleReader reader = new RleReader(DEFAULT_POLICY);
            reader.init(MemorySegment.ofArray(encoded), 0, 16);
            assertThat(reader.nextRun(1)).isEqualTo(1);
            assertThat(reader.currentRleValue()).isEqualTo(73);
        }
    }

    @Test
    void testConsumeAllOnesAcrossRleAndBitPackedRuns()
    {
        byte[] encoded = new byte[24];
        int offset = writeUleb128(encoded, 8); // four-value RLE run
        encoded[offset++] = 1;
        encoded[offset++] = 3; // one eight-value bit-packed group
        encoded[offset] = (byte) 0xFF;

        RleReader reader = new RleReader(DEFAULT_POLICY);
        reader.init(MemorySegment.ofArray(encoded), 0, 1);

        assertThat(reader.consumeIfAllOnes(12)).isTrue();
    }

    @Test
    void testConsumeAllOnesIgnoresBitPackedPadding()
    {
        byte[] encoded = new byte[16];
        int offset = writeUleb128(encoded, 3);
        encoded[offset] = 0x1F; // five live ones followed by three zero padding bits

        RleReader reader = new RleReader(DEFAULT_POLICY);
        reader.init(MemorySegment.ofArray(encoded), 0, 1);

        assertThat(reader.consumeIfAllOnes(5)).isTrue();
    }

    @Test
    void testConsumeAllOnesRejectsAZero()
    {
        byte[] encoded = new byte[16];
        int offset = writeUleb128(encoded, 3);
        encoded[offset] = (byte) 0xEF;

        RleReader reader = new RleReader(DEFAULT_POLICY);
        reader.init(MemorySegment.ofArray(encoded), 0, 1);

        assertThat(reader.consumeIfAllOnes(8)).isFalse();
    }

    @Test
    void testFilterNullableDictionaryLongsPreservesBothCursorsAcrossWindows()
    {
        int[] definitionLevels = {1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1};
        int[] dictionaryIds = {0, 1, 2, 3, 4, 5, 6, 7, 1, 4, 6, 2};

        RleReader definitions = new RleReader(DEFAULT_POLICY);
        definitions.init(MemorySegment.ofArray(bitPackedRun(1, definitionLevels)), 0, 1);
        RleReader ids = new RleReader(DEFAULT_POLICY);
        ids.init(MemorySegment.ofArray(bitPackedRun(3, dictionaryIds)), 0, 3);

        boolean[] accepted = new boolean[8];
        accepted[1] = true;
        accepted[4] = true;
        accepted[6] = true;
        long[] dictionary = {100, 101, 102, 103, 104, 105, 106, 107};
        int[] survivors = new int[16];
        long[] values = new long[16];

        int output = definitions.filterNullableDictionaryLongs(
                ids, accepted, dictionary, 7, 0, survivors, values, 0);
        output = definitions.filterNullableDictionaryLongs(
                ids, accepted, dictionary, 9, 7, survivors, values, output);

        assertThat(output).isEqualTo(6);
        assertThat(survivors).startsWith(1, 6, 8, 11, 13, 14);
        assertThat(values).startsWith(101, 104, 106, 101, 104, 106);
    }

    @Test
    void testDisabledPolicyUsesGenericDecodingPaths()
    {
        RleReaderPolicy policy = new RleReaderPolicy(false, false, false, false);

        byte[] header = new byte[16];
        int offset = writeUleb128(header, 16_384);
        header[offset] = 73;
        RleReader headerReader = new RleReader(policy);
        headerReader.init(MemorySegment.ofArray(header), 0, 16);
        assertThat(headerReader.nextRun(1)).isEqualTo(1);
        assertThat(headerReader.currentRleValue()).isEqualTo(73);

        byte[] definitionsData = new byte[24];
        offset = writeUleb128(definitionsData, 8);
        definitionsData[offset++] = 1;
        definitionsData[offset++] = 3;
        definitionsData[offset] = (byte) 0xFF;
        RleReader singleRunDefinitions = new RleReader(policy);
        singleRunDefinitions.init(MemorySegment.ofArray(definitionsData), 0, 1);
        assertThat(singleRunDefinitions.consumeIfAllOnes(12)).isFalse();

        int[] definitionLevels = {1, 1, 0, 1, 1, 0, 1, 1};
        int[] dictionaryIds = {0, 1, 2, 3, 1, 2};
        RleReader definitions = new RleReader(policy);
        definitions.init(MemorySegment.ofArray(bitPackedRun(1, definitionLevels)), 0, 1);
        RleReader ids = new RleReader(policy);
        ids.init(MemorySegment.ofArray(bitPackedRun(2, dictionaryIds)), 0, 2);

        boolean[] accepted = {false, true, true, false};
        int[] survivors = new int[8];
        long[] values = new long[8];
        int output = definitions.filterNullableDictionaryLongs(
                ids, accepted, new long[] {100, 101, 102, 103}, 8, 0, survivors, values, 0);

        assertThat(output).isEqualTo(4);
        assertThat(survivors).startsWith(1, 3, 6, 7);
        assertThat(values).startsWith(101, 102, 101, 102);
    }

    private static byte[] bitPackedRun(int bitWidth, int[] values)
    {
        int groups = (values.length + 7) / 8;
        byte[] encoded = new byte[32 + groups * bitWidth];
        int offset = writeUleb128(encoded, (groups << 1) | 1);
        long bitCursor = (long) offset * Byte.SIZE;
        for (int value : values) {
            for (int bit = 0; bit < bitWidth; bit++) {
                if (((value >>> bit) & 1) != 0) {
                    int byteIndex = (int) (bitCursor >>> 3);
                    encoded[byteIndex] |= (byte) (1 << ((int) bitCursor & 7));
                }
                bitCursor++;
            }
        }
        return encoded;
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
