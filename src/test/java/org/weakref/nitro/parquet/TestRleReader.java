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
import java.util.Arrays;

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
    void testConsumeSingleRleValue()
    {
        byte[] encoded = new byte[16];
        int offset = writeUleb128(encoded, 16); // eight-value RLE run
        encoded[offset] = 3;

        RleReader reader = new RleReader(DEFAULT_POLICY);
        reader.init(MemorySegment.ofArray(encoded), 0, 2);

        assertThat(reader.consumeSingleRleValue(8)).isEqualTo(3);
    }

    @Test
    void testConsumeSingleRleValueRejectsNonConstantWindowWithoutConsumingValues()
    {
        int[] expected = {0, 1, 2, 3, 0, 1, 2, 3};
        byte[] encoded = bitPackedRun(2, expected);

        RleReader reader = new RleReader(DEFAULT_POLICY);
        reader.init(MemorySegment.ofArray(encoded), 0, 2);

        assertThat(reader.consumeSingleRleValue(expected.length)).isEqualTo(-1);
        int[] actual = new int[expected.length];
        reader.read(actual, 0, actual.length);
        assertThat(actual).containsExactly(expected);
    }

    @Test
    void testConsumeSingleRleValueRejectsShortRun()
    {
        byte[] encoded = new byte[16];
        int offset = writeUleb128(encoded, 8); // four-value RLE run
        encoded[offset] = 1;

        RleReader reader = new RleReader(DEFAULT_POLICY);
        reader.init(MemorySegment.ofArray(encoded), 0, 1);

        assertThat(reader.consumeSingleRleValue(5)).isEqualTo(-1);
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
    void testReaderLocalVectorTablesFollowChangingBitWidths()
    {
        RleReader reader = new RleReader(DEFAULT_POLICY);

        for (int width : new int[] {3, 7, 3}) {
            int[] expected = {0, 1, 2, 3, 4, 5, 6, (1 << width) - 1};
            reader.init(MemorySegment.ofArray(bitPackedRun(width, expected)), 0, width);
            int[] actual = new int[expected.length];
            reader.read(actual, 0, actual.length);
            assertThat(actual).containsExactly(expected);
        }
    }

    @Test
    void testVectorUnpackAcrossPhysicalWidthsAndReadBoundaries()
    {
        for (RleReaderPolicy policy : new RleReaderPolicy[] {
                DEFAULT_POLICY,
                new RleReaderPolicy(true, true, true, false, false)}) {
            RleReader reader = new RleReader(policy);
            for (int width = 1; width <= 20; width++) {
                int[] expected = new int[40];
                int mask = (1 << width) - 1;
                for (int index = 0; index < expected.length; index++) {
                    expected[index] = (index * 17 + 3) & mask;
                }
                reader.init(MemorySegment.ofArray(bitPackedRun(width, expected)), 0, width);
                int[] actual = new int[expected.length];
                reader.read(actual, 0, 13);
                reader.read(actual, 13, expected.length - 13);
                assertThat(actual).as("width %s with policy %s", width, policy).containsExactly(expected);
            }
        }
    }

    @Test
    void testBitPackedTailDoesNotRequireTrailingBytes()
    {
        int[] expected = {0, 1, 2, 3, 4, 5, 6, 7};
        byte[] encoded = Arrays.copyOf(bitPackedRun(3, expected), 4);
        RleReader reader = new RleReader(DEFAULT_POLICY);
        reader.init(MemorySegment.ofArray(encoded), 0, 3);

        int[] actual = new int[expected.length];
        reader.read(actual, 0, actual.length);

        assertThat(actual).containsExactly(expected);

        byte[] definitions = {3, (byte) 0xFF};
        RleReader definitionReader = new RleReader(DEFAULT_POLICY);
        definitionReader.init(MemorySegment.ofArray(definitions), 0, 1);
        assertThat(definitionReader.consumeIfAllOnes(8)).isTrue();

        for (int width : new int[] {1, 2, 4, 8, 16}) {
            int mask = (1 << width) - 1;
            int[] widthExpected = {0, 1 & mask, 2 & mask, 3 & mask, 4 & mask, 5 & mask, 6 & mask, mask};
            byte[] exactEncoded = Arrays.copyOf(bitPackedRun(width, widthExpected), 1 + width);
            RleReader widthReader = new RleReader(DEFAULT_POLICY);
            widthReader.init(MemorySegment.ofArray(exactEncoded), 0, width);
            int[] widthActual = new int[widthExpected.length];
            widthReader.read(widthActual, 0, widthActual.length);
            assertThat(widthActual).containsExactly(widthExpected);
        }
    }

    @Test
    void testDisabledPolicyUsesGenericDecodingPaths()
    {
        RleReaderPolicy policy = new RleReaderPolicy(false, false, false, false, false);

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
