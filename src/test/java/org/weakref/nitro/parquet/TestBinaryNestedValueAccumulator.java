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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class TestBinaryNestedValueAccumulator
{
    @Test
    void testAllNonNullNullableDictionaryOmitsSyntheticNullValue()
    {
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
        try (BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(3, arrays);
                Allocator allocator = new Allocator(EngineResources.createDefault())) {
            decoder.decodeDictionary(MemorySegment.ofArray(new byte[] {1, 2, 3, 4, 5, 6}), 2, Encoding.PLAIN);
            BinaryNestedValueAccumulator accumulator = new BinaryNestedValueAccumulator(false, true);
            accumulator.reset(allocator);
            for (int position = 0; position < 1_000; position++) {
                accumulator.append(decoder, 0, position & 1);
            }

            Streams streams = accumulator.materialize(allocator, new Allocator.Context("test"));
            assertThat(streams.values()).isInstanceOf(DictionaryVector.class);
            assertThat(((DictionaryVector) streams.values()).values().length()).isEqualTo(2);
            assertThat(streams.get(Stream.NULLS).length()).isEqualTo(1_000);
            assertThat(VectorAccess.booleanValues(streams.get(Stream.NULLS)).value(0)).isFalse();
        }
    }

    @Test
    void testPreservesContiguousDictionaryRun()
    {
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(arrays);
                BinaryNestedValueAccumulator accumulator = new BinaryNestedValueAccumulator(true, false)) {
            decoder.decodeDictionary(binary("zero", "one"), 2, Encoding.PLAIN);
            accumulator.reset(allocator);
            int[] ordinals = new int[1_000];
            int[] ids = new int[1_000];
            for (int position = 0; position < ordinals.length; position++) {
                ordinals[position] = position;
                ids[position] = position & 1;
            }
            accumulator.appendEvents(decoder, ordinals, ids, 0, ordinals.length);

            Streams streams = accumulator.materialize(allocator, new Allocator.Context("test"));
            assertThat(streams.values()).isInstanceOf(DictionaryVector.class);
            VectorAccess.BinaryValues values = VectorAccess.binaryValues(streams.values());
            assertThat(utf8(values, 0)).isEqualTo("zero");
            assertThat(utf8(values, 1)).isEqualTo("one");
            assertThat(utf8(values, 999)).isEqualTo("one");
        }
    }

    @Test
    void testPreservesBeneficialDictionaryEncoding()
    {
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(arrays);
                BinaryNestedValueAccumulator accumulator = new BinaryNestedValueAccumulator(true, true)) {
            decoder.decodeDictionary(binary("frequently-repeated-value", "another-repeated-value"), 2, Encoding.PLAIN);
            accumulator.reset(allocator);
            int[] ordinals = new int[1_000];
            int[] ids = new int[1_000];
            for (int position = 0; position < 1_000; position++) {
                if (position % 11 == 0) {
                    ordinals[position] = -1;
                }
                else {
                    ordinals[position] = position;
                    ids[position] = position & 1;
                }
            }
            accumulator.appendEvents(decoder, ordinals, ids, 0, ordinals.length);

            Streams streams = accumulator.materialize(allocator, new Allocator.Context("test"));
            assertThat(streams.values()).isInstanceOf(DictionaryVector.class);
            VectorAccess.BinaryValues values = VectorAccess.binaryValues(streams.values());
            assertThat(utf8(values, 1)).isEqualTo("another-repeated-value");
            assertThat(utf8(values, 2)).isEqualTo("frequently-repeated-value");
            assertThat(VectorAccess.booleanValues(streams.get(Stream.NULLS)).value(0)).isTrue();
            assertThat(VectorAccess.booleanValues(streams.get(Stream.NULLS)).value(1)).isFalse();
        }
    }

    @Test
    void testFallsBackToFlatAcrossDictionaryGenerations()
    {
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(arrays);
                BinaryNestedValueAccumulator accumulator = new BinaryNestedValueAccumulator(true, false)) {
            accumulator.reset(allocator);
            decoder.decodeDictionary(binary("first"), 1, Encoding.PLAIN);
            accumulator.append(decoder, 0, 0);
            decoder.decodeDictionary(binary("second"), 1, Encoding.PLAIN);
            accumulator.append(decoder, 0, 0);

            Streams streams = accumulator.materialize(allocator, new Allocator.Context("test"));
            assertThat(streams.values()).isInstanceOf(BinaryVector.class);
            VectorAccess.BinaryValues values = VectorAccess.binaryValues(streams.values());
            assertThat(utf8(values, 0)).isEqualTo("first");
            assertThat(utf8(values, 1)).isEqualTo("second");
        }
    }

    @Test
    void testRecoversRepeatedDomainFromPlainValues()
    {
        PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                BinaryPhysicalValueDecoder decoder = new BinaryPhysicalValueDecoder(arrays);
                BinaryNestedValueAccumulator accumulator = new BinaryNestedValueAccumulator(true, true)) {
            String[] input = new String[1_024];
            for (int position = 0; position < input.length; position++) {
                input[position] = "repeated-value-" + (position & 3);
            }
            decoder.decodePlain(binary(input), 0, input.length);
            accumulator.reset(allocator);
            accumulator.appendPlainRun(decoder, 0, input.length);

            Streams streams = accumulator.materialize(allocator, new Allocator.Context("test"));
            assertThat(streams.values()).isInstanceOf(DictionaryVector.class);
            DictionaryVector dictionary = (DictionaryVector) streams.values();
            assertThat(dictionary.values().length()).isEqualTo(4);
            assertThat(dictionary.hasDomainFrequencies()).isTrue();
            assertThat(dictionary.domainFrequency(0)).isEqualTo(256);
            assertThat(utf8(VectorAccess.binaryValues(dictionary), 1_023)).isEqualTo("repeated-value-3");
            assertThat(VectorAccess.isAllFalseNulls(streams.get(Stream.NULLS))).isTrue();
        }
    }

    private static String utf8(VectorAccess.BinaryValues values, int position)
    {
        VectorAccess.BinarySlice value = values.value(position);
        return new String(value.data(), value.offset(), value.length(), StandardCharsets.UTF_8);
    }

    private static MemorySegment binary(String... values)
    {
        int bytes = 0;
        for (String value : values) {
            bytes += Integer.BYTES + value.getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer output = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (String value : values) {
            byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            output.putInt(encoded.length).put(encoded);
        }
        return MemorySegment.ofArray(output.array());
    }
}
