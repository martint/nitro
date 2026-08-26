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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestBooleanPhysicalValueDecoder
{
    @Test
    void testDecodesPlainValuesAcrossByteBoundary()
    {
        BooleanPhysicalValueDecoder decoder = new BooleanPhysicalValueDecoder(new PrimitiveArrayPool(0, 0));
        decoder.decodePlain(MemorySegment.ofArray(new byte[] {(byte) 0b0101_0011, 0b0000_0011}), 0, 10);

        assertThat(values(decoder, 10))
                .containsExactly(true, true, false, false, true, false, true, false, true, true);
    }

    @Test
    void testRejectsTruncatedAndDictionaryInputs()
    {
        BooleanPhysicalValueDecoder decoder = new BooleanPhysicalValueDecoder(new PrimitiveArrayPool(0, 0));

        assertThatThrownBy(() -> decoder.decodePlain(MemorySegment.ofArray(new byte[1]), 0, 9))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Truncated");
        assertThatThrownBy(() -> decoder.decodeDictionary(MemorySegment.NULL, 1, Encoding.PLAIN))
                .isInstanceOf(UnsupportedParquetFeatureException.class)
                .hasMessageContaining("cannot use dictionary");
    }

    private static boolean[] values(BooleanPhysicalValueDecoder decoder, int count)
    {
        boolean[] values = new boolean[count];
        for (int index = 0; index < count; index++) {
            values[index] = decoder.value(index);
        }
        return values;
    }
}
