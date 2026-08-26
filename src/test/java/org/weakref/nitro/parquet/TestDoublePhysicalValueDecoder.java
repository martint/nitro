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
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestDoublePhysicalValueDecoder
{
    @Test
    void testDecodesPlainAndDictionaryValues()
    {
        DoublePhysicalValueDecoder decoder = new DoublePhysicalValueDecoder(new PrimitiveArrayPool(0, 0));
        decoder.decodeDictionary(doubles(1.5, -2.25), 2, Encoding.PLAIN);
        decoder.decodePlain(doubles(7.75, 9.5), 0, 2);

        assertThat(decoder.value(0, -1)).isEqualTo(7.75);
        assertThat(decoder.value(1, -1)).isEqualTo(9.5);
        assertThat(decoder.value(0, 1)).isEqualTo(-2.25);
        assertThat(decoder.dictionarySize()).isEqualTo(2);

        double[] output = new double[4];
        decoder.copyPlain(0, output, 1, 2);
        assertThat(output).containsExactly(0, 7.75, 9.5, 0);
        decoder.close();
    }

    @Test
    void testMaterializesNullableNestedValues()
    {
        ParquetSchema.Primitive leaf = new ParquetSchema.Primitive(
                "score", FieldRepetitionType.OPTIONAL, Type.DOUBLE, null, null, 0, 0, 0, 0,
                List.of("person", "score"), 2, 0);
        DoublePhysicalValueDecoder decoder = new DoublePhysicalValueDecoder(new PrimitiveArrayPool(0, 0));
        decoder.decodePlain(doubles(1.5, 2.5), 0, 2);
        NestedValueAccumulator values = NestedValueAccumulators.create(leaf, true);

        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources);
                values) {
            values.reset(allocator);
            values.append(decoder, 0, -1);
            values.appendNull();
            values.append(decoder, 1, -1);
            Streams streams = values.materialize(allocator, new Allocator.Context("test"));
            assertThat(((F64Vector) streams.values()).values()).containsExactly(1.5, 0, 2.5);
            assertThat(((BooleanVector) streams.get(Stream.NULLS)).values()).containsExactly(false, true, false);
        }
    }

    @Test
    void testPreservesOneDictionaryGenerationAndFlattensAcrossGenerations()
    {
        ParquetSchema.Primitive leaf = new ParquetSchema.Primitive(
                "score", FieldRepetitionType.OPTIONAL, Type.DOUBLE, null, null, 0, 0, 0, 0,
                List.of("person", "score"), 2, 0);
        DoublePhysicalValueDecoder decoder = new DoublePhysicalValueDecoder(new PrimitiveArrayPool(0, 0));
        NestedValueAccumulator values = NestedValueAccumulators.create(leaf, true);

        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources);
                values) {
            Allocator.Context context = new Allocator.Context("test");
            decoder.decodeDictionary(doubles(1.5, -2.25), 2, Encoding.PLAIN);
            values.reset(allocator, context, 3);
            values.appendEvents(decoder, new int[] {-1, 0, 1}, new int[] {1, 0}, 0, 3);
            Streams streams = values.materialize(allocator, context);
            DictionaryVector dictionary = (DictionaryVector) streams.values();
            assertThat(dictionary.ids()).containsExactly(2, 1, 0);
            assertThat(dictionary.domainFrequency(0)).isEqualTo(1);
            assertThat(dictionary.domainFrequency(1)).isEqualTo(1);
            assertThat(dictionary.domainFrequency(2)).isEqualTo(1);
            assertThat(((F64Vector) dictionary.values()).values()).containsExactly(1.5, -2.25, 0);
            DictionaryVector dictionaryNulls = (DictionaryVector) streams.get(Stream.NULLS);
            assertThat(dictionary.hasSameRowMapping(dictionaryNulls)).isTrue();
            assertThat(((BooleanVector) dictionaryNulls.values()).values()).containsExactly(false, false, true);

            values.reset(allocator, context, 2);
            values.append(decoder, 0, 0);
            decoder.decodeDictionary(doubles(7.5), 1, Encoding.PLAIN);
            values.append(decoder, 0, 0);
            streams = values.materialize(allocator, context);
            assertThat(((F64Vector) streams.values()).values()).containsExactly(1.5, 7.5);
        }
        decoder.close();
    }

    private static MemorySegment doubles(double... values)
    {
        ByteBuffer bytes = ByteBuffer.allocate(values.length * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (double value : values) {
            bytes.putDouble(value);
        }
        return MemorySegment.ofArray(bytes.array());
    }
}
