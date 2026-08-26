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

import static org.assertj.core.api.Assertions.assertThat;

class TestLongPhysicalValueDecoder
{
    @Test
    void testCopiesDictionaryRun()
    {
        try (LongPhysicalValueDecoder decoder = new LongPhysicalValueDecoder(Type.INT64, new PrimitiveArrayPool(0, 0))) {
            decoder.decodeDictionary(longs(11, 22, 33), 3, Encoding.PLAIN);
            int[] ids = {2, 0, 1, 1};
            long[] output = new long[6];

            decoder.copyDictionary(ids, 0, output, 1, ids.length);

            assertThat(output).containsExactly(0, 33, 11, 22, 22, 0);
        }
    }

    private static MemorySegment longs(long... values)
    {
        ByteBuffer output = ByteBuffer.allocate(values.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (long value : values) {
            output.putLong(value);
        }
        return MemorySegment.ofArray(output.array());
    }
}
