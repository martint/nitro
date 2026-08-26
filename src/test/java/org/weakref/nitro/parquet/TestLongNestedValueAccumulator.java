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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

class TestLongNestedValueAccumulator
{
    @Test
    void testPreservesBeneficialDictionaryEncoding()
    {
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                LongPhysicalValueDecoder decoder = new LongPhysicalValueDecoder(Type.INT64, arrays);
                LongNestedValueAccumulator accumulator = new LongNestedValueAccumulator(false, true)) {
            decoder.decodeDictionary(longs(11, 22), 2, Encoding.PLAIN);
            accumulator.reset(allocator);
            int[] ids = new int[1_000];
            for (int position = 0; position < ids.length; position++) {
                ids[position] = position & 1;
            }
            accumulator.appendDictionaryRun(decoder, ids, 0, ids.length);
            accumulator.appendNull();

            Streams streams = accumulator.materialize(allocator, new Allocator.Context("test"));
            assertThat(streams.values()).isInstanceOf(DictionaryVector.class);
            VectorAccess.LongValues values = VectorAccess.longValues(streams.values());
            assertThat(values.value(0)).isEqualTo(11);
            assertThat(values.value(1)).isEqualTo(22);
            assertThat(values.value(999)).isEqualTo(22);
            assertThat(VectorAccess.booleanValues(streams.get(Stream.NULLS)).value(999)).isFalse();
            assertThat(VectorAccess.booleanValues(streams.get(Stream.NULLS)).value(1_000)).isTrue();
        }
    }

    @Test
    void testFallsBackToFlatAcrossDictionaryGenerations()
    {
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                LongPhysicalValueDecoder decoder = new LongPhysicalValueDecoder(Type.INT64, arrays);
                LongNestedValueAccumulator accumulator = new LongNestedValueAccumulator(false, false)) {
            accumulator.reset(allocator);
            decoder.decodeDictionary(longs(11), 1, Encoding.PLAIN);
            accumulator.append(decoder, 0, 0);
            decoder.decodeDictionary(longs(22), 1, Encoding.PLAIN);
            accumulator.append(decoder, 0, 0);

            VectorAccess.LongValues values = VectorAccess.longValues(
                    accumulator.materialize(allocator, new Allocator.Context("test")).values());
            assertThat(values.value(0)).isEqualTo(11);
            assertThat(values.value(1)).isEqualTo(22);
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
