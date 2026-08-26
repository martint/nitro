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

import static org.assertj.core.api.Assertions.assertThat;

class TestFixedDecimalPhysicalValueDecoder
{
    @Test
    void testDecodesSignedBigEndianValues()
    {
        FixedDecimalPhysicalValueDecoder decoder = new FixedDecimalPhysicalValueDecoder(6, new PrimitiveArrayPool(0, 0));
        decoder.decodePlain(MemorySegment.ofArray(new byte[] {
                0, 0, 0, 0, 0x30, 0x39,
                -1, -1, -1, -1, -1, -2}), 0, 2);

        assertThat(decoder.value(0, -1)).isEqualTo(12_345);
        assertThat(decoder.value(1, -1)).isEqualTo(-2);
        long[] output = new long[4];
        decoder.copyPlain(0, output, 1, 2);
        assertThat(output).containsExactly(0, 12_345, -2, 0);
    }

    @Test
    void testDecodesAllocatorOwnedDictionary()
    {
        FixedDecimalPhysicalValueDecoder decoder = new FixedDecimalPhysicalValueDecoder(2, new PrimitiveArrayPool(0, 0));
        decoder.decodeDictionary(MemorySegment.ofArray(new byte[] {0, 100, -1, -100}), 2, Encoding.PLAIN);

        assertThat(decoder.value(0, 0)).isEqualTo(100);
        assertThat(decoder.value(0, 1)).isEqualTo(-100);
        assertThat(decoder.dictionarySize()).isEqualTo(2);
    }
}
