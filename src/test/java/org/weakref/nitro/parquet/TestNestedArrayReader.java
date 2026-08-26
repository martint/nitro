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

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestNestedArrayReader
{
    @Test
    void testReconstructsNullEmptyAndPopulatedLists()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = reader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 4, Mask.all(4));
            ArrayVector arrays = (ArrayVector) streams.values();
            BooleanVector listNulls = (BooleanVector) streams.get(Stream.NULLS);
            I64Vector elements = (I64Vector) arrays.elementValues();
            BooleanVector elementNulls = arrays.elementNulls();

            assertThat(arrays.offsets()).containsExactly(0, 3, 3, 3, 4);
            assertThat(listNulls.values()).containsExactly(false, true, false, false);
            assertThat(elements.values()).containsExactly(1, 0, 3, 4);
            assertThat(elementNulls.values()).containsExactly(false, true, false, false);
        }
    }

    @Test
    void testMaterializesOnlySelectedListElements()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = reader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 4, Mask.sparse(new int[] {3}, 4));
            ArrayVector arrays = (ArrayVector) streams.values();

            assertThat(arrays.offsets()).containsExactly(0, 0, 0, 0, 1);
            assertThat(((I64Vector) arrays.elementValues()).values()).containsExactly(4);
        }
    }

    @Test
    void testSkipAdvancesWithoutMaterializingElements()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = reader()) {
            reader.skip(3);
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 1, Mask.all(1));
            ArrayVector arrays = (ArrayVector) streams.values();

            assertThat(arrays.offsets()).containsExactly(0, 1);
            assertThat(((I64Vector) arrays.elementValues()).values()).containsExactly(4);
        }
    }

    private static NestedArrayReader reader()
    {
        ParquetSchema.Primitive element = new ParquetSchema.Primitive(
                "element", FieldRepetitionType.OPTIONAL, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("items", "list", "element"), 3, 1);
        ParquetSchema.Group repeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(element), 2, 1);
        ParquetSchema.Group list = new ParquetSchema.Group(
                "items", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(repeatedValues), 1, 0);

        LongPhysicalValueDecoder values = new LongPhysicalValueDecoder(Type.INT64);
        values.decodePlain(longs(1, 3, 4), 0, 3);
        return new NestedArrayReader(
                list,
                RleReaderPolicy.defaults(),
                new TestingCursor(
                        values,
                        new int[] {0, 1, 1, 0, 0, 0},
                        new int[] {3, 2, 3, 0, 1, 3},
                        new int[] {0, -1, 1, -1, -1, 2}));
    }

    private static MemorySegment longs(long... values)
    {
        ByteBuffer output = ByteBuffer.allocate(values.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        Arrays.stream(values).forEach(output::putLong);
        return MemorySegment.ofArray(output.array());
    }

    private static final class TestingCursor
            implements NestedLeafCursor
    {
        private final PhysicalValueDecoder decoder;
        private final int[] repetitions;
        private final int[] definitions;
        private final int[] ordinals;
        private int event = -1;

        private TestingCursor(PhysicalValueDecoder decoder, int[] repetitions, int[] definitions, int[] ordinals)
        {
            this.decoder = decoder;
            this.repetitions = repetitions;
            this.definitions = definitions;
            this.ordinals = ordinals;
        }

        @Override
        public boolean next()
        {
            return ++event < repetitions.length;
        }

        @Override
        public int repetitionLevel()
        {
            return repetitions[event];
        }

        @Override
        public int definitionLevel()
        {
            return definitions[event];
        }

        @Override
        public boolean hasValue()
        {
            return ordinals[event] >= 0;
        }

        @Override
        public PhysicalValueDecoder valueDecoder()
        {
            return decoder;
        }

        @Override
        public int valueOrdinal()
        {
            return ordinals[event];
        }

        @Override
        public int dictionaryId()
        {
            return -1;
        }

        @Override
        public void close() {}
    }
}
