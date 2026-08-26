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
import org.weakref.nitro.data.F64Vector;
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
        DoublePhysicalValueDecoder decoder = new DoublePhysicalValueDecoder();
        decoder.decodeDictionary(doubles(1.5, -2.25), 2, Encoding.PLAIN);
        decoder.decodePlain(doubles(7.75, 9.5), 0, 2);

        assertThat(decoder.value(0, -1)).isEqualTo(7.75);
        assertThat(decoder.value(1, -1)).isEqualTo(9.5);
        assertThat(decoder.value(0, 1)).isEqualTo(-2.25);
        assertThat(decoder.dictionarySize()).isEqualTo(2);
    }

    @Test
    void testMaterializesNullableNestedValues()
    {
        ParquetSchema.Primitive leaf = new ParquetSchema.Primitive(
                "score", FieldRepetitionType.OPTIONAL, Type.DOUBLE, null, null, 0, 0, 0, 0,
                List.of("person", "score"), 2, 0);
        DoublePhysicalValueDecoder decoder = new DoublePhysicalValueDecoder();
        decoder.decodePlain(doubles(1.5, 2.5), 0, 2);
        NestedValueAccumulator values = NestedValueAccumulators.create(leaf, true);
        values.append(decoder, 0, -1);
        values.appendNull();
        values.append(decoder, 1, -1);

        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Streams streams = values.materialize(allocator, new Allocator.Context("test"));
            assertThat(((F64Vector) streams.values()).values()).containsExactly(1.5, 0, 2.5);
            assertThat(((BooleanVector) streams.get(Stream.NULLS)).values()).containsExactly(false, true, false);
        }
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
