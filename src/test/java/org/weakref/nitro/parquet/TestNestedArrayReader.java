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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.execution.EngineResources;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

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
    void testReconstructsDenseEventWindow()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = denseReader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 2, Mask.all(2));
            ArrayVector arrays = (ArrayVector) streams.values();

            assertThat(arrays.offsets()).containsExactly(0, 2, 5);
            assertThat(((I64Vector) arrays.elementValues()).values()).containsExactly(1, 2, 3, 4, 5);
            assertThat(((BooleanVector) streams.get(Stream.NULLS)).values()).containsExactly(false, false);
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

    @Test
    void testEmptyReadDoesNotAdvanceEventWindow()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = reader()) {
            ArrayVector empty = (ArrayVector) reader.read(
                    allocator,
                    new Allocator.Context("empty"),
                    0,
                    Mask.all(0)).values();
            assertThat(empty.offsets()).containsExactly(0);

            ArrayVector first = (ArrayVector) reader.read(
                    allocator,
                    new Allocator.Context("first"),
                    1,
                    Mask.all(1)).values();
            assertThat(first.offsets()).containsExactly(0, 3);
            assertThat(((I64Vector) first.elementValues()).values()).containsExactly(1, 0, 3);
        }
    }

    @Test
    void testReconstructsStructElementsWithoutRereadingAnAnchorLeaf()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = structReader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 4, Mask.all(4));
            ArrayVector arrays = (ArrayVector) streams.values();
            StructVector rows = (StructVector) arrays.elementValues();
            BooleanVector rowNulls = arrays.elementNulls();
            I64Vector ids = (I64Vector) rows.fieldValues("id");
            BooleanVector idNulls = (BooleanVector) rows.field("id").get(Stream.NULLS);
            BinaryVector labels = (BinaryVector) rows.fieldValues("label");
            BooleanVector labelNulls = (BooleanVector) rows.field("label").get(Stream.NULLS);

            assertThat(arrays.offsets()).containsExactly(0, 2, 2, 3, 3);
            assertThat(((BooleanVector) streams.get(Stream.NULLS)).values()).containsExactly(false, false, false, true);
            assertThat(rowNulls.values()).containsExactly(false, true, false);
            assertThat(ids.values()).containsExactly(10, 0, 30);
            assertThat(idNulls.values()).containsExactly(false, true, false);
            assertThat(value(labels, 0)).isEqualTo("ten");
            assertThat(labelNulls.values()).containsExactly(false, true, true);
        }
    }

    @Test
    void testReconstructsRecursivelyNestedLists()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = nestedReader(Integer.MAX_VALUE, true)) {
            assertNestedLists(
                    reader.read(allocator, new Allocator.Context("test"), 4, Mask.all(4)),
                    new int[] {0, 4, 4, 4, 5},
                    new boolean[] {false, true, false, false},
                    new int[] {0, 2, 2, 2, 3, 3},
                    new boolean[] {false, true, false, false, false},
                    new long[] {1, 0, 4},
                    new boolean[] {false, true, false});
        }
    }

    @Test
    void testSelectsRecursivelyNestedLists()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = nestedReader(Integer.MAX_VALUE, true)) {
            assertNestedLists(
                    reader.read(allocator, new Allocator.Context("test"), 4, Mask.sparse(new int[] {0, 3}, 4)),
                    new int[] {0, 4, 4, 4, 5},
                    new boolean[] {false, false, false, false},
                    new int[] {0, 2, 2, 2, 3, 3},
                    new boolean[] {false, true, false, false, false},
                    new long[] {1, 0, 4},
                    new boolean[] {false, true, false});
        }
    }

    @Test
    void testMaterializesEmptyNestedDomain()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = nestedReader(Integer.MAX_VALUE, true)) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 4, Mask.sparse(new int[] {1}, 4));
            ArrayVector outer = (ArrayVector) streams.values();
            ArrayVector inner = (ArrayVector) outer.elementValues();

            assertThat(outer.offsets()).containsExactly(0, 0, 0, 0, 0);
            assertThat(inner.offsets()).containsExactly(0);
            assertThat(outer.elementNulls().values()).isEmpty();
            assertThat(((I64Vector) inner.elementValues()).values()).isEmpty();
            assertThat(inner.elementNulls().values()).isEmpty();
        }
    }

    @Test
    void testSkipsRecursivelyNestedLists()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = nestedReader(Integer.MAX_VALUE, true)) {
            reader.skip(3);
            assertNestedLists(
                    reader.read(allocator, new Allocator.Context("test"), 1, Mask.all(1)),
                    new int[] {0, 1},
                    new boolean[] {false},
                    new int[] {0, 0},
                    new boolean[] {false},
                    new long[] {},
                    new boolean[] {});
        }
    }

    @Test
    void testReconstructsRecursivelyNestedListsAcrossEventWindows()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedArrayReader reader = nestedReader(2, false)) {
            assertNestedLists(
                    reader.read(allocator, new Allocator.Context("test"), 4, Mask.all(4)),
                    new int[] {0, 4, 4, 4, 5},
                    new boolean[] {false, true, false, false},
                    new int[] {0, 2, 2, 2, 3, 3},
                    new boolean[] {false, true, false, false, false},
                    new long[] {1, 0, 4},
                    new boolean[] {false, true, false});
        }
    }

    @Test
    void testReconstructsDictionaryNestedListsAfterPoolReuse()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            try (NestedArrayReader reader = dictionaryNestedReader()) {
                assertNestedLists(
                        reader.read(allocator, new Allocator.Context("first"), 2, Mask.all(2)),
                        new int[] {0, 1, 2},
                        new boolean[] {false, false},
                        new int[] {0, 2, 4},
                        new boolean[] {false, false},
                        new long[] {10, 20, 20, 10},
                        new boolean[] {false, false, false, false});
            }
            try (NestedArrayReader reader = dictionaryNestedReader()) {
                assertNestedLists(
                        reader.read(allocator, new Allocator.Context("second"), 2, Mask.all(2)),
                        new int[] {0, 1, 2},
                        new boolean[] {false, false},
                        new int[] {0, 2, 4},
                        new boolean[] {false, false},
                        new long[] {10, 20, 20, 10},
                        new boolean[] {false, false, false, false});
            }
        }
    }

    @Test
    void testRejectsNestedMapElementsExplicitly()
    {
        ParquetSchema.Group map = new ParquetSchema.Group(
                "element", FieldRepetitionType.OPTIONAL, ConvertedType.MAP, null, List.of(), 3, 1);
        ParquetSchema.Group repeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(map), 2, 1);
        ParquetSchema.Group list = new ParquetSchema.Group(
                "outer", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(repeatedValues), 1, 0);

        assertThat(catchThrowable(() -> new NestedArrayReader(
                list,
                RleReaderPolicy.defaults(),
                new NestedLeafCursor[0])))
                .isInstanceOf(UnsupportedParquetFeatureException.class)
                .hasMessageContaining("nested MAP elements are not implemented yet");
    }

    private static void assertNestedLists(
            Streams streams,
            int[] outerOffsets,
            boolean[] outerNulls,
            int[] innerOffsets,
            boolean[] innerNulls,
            long[] values,
            boolean[] valueNulls)
    {
        ArrayVector outer = (ArrayVector) streams.values();
        ArrayVector inner = (ArrayVector) outer.elementValues();

        assertThat(outer.offsets()).containsExactly(outerOffsets);
        assertThat(((BooleanVector) streams.get(Stream.NULLS)).values()).containsExactly(outerNulls);
        assertThat(inner.offsets()).containsExactly(innerOffsets);
        assertThat(outer.elementNulls().values()).containsExactly(innerNulls);
        assertThat(((I64Vector) inner.elementValues()).values()).containsExactly(values);
        assertThat(inner.elementNulls().values()).containsExactly(valueNulls);
    }

    private static NestedArrayReader reader()
    {
        return reader(
                new long[] {1, 3, 4},
                new int[] {0, 1, 1, 0, 0, 0},
                new int[] {3, 2, 3, 0, 1, 3},
                new int[] {0, -1, 1, -1, -1, 2});
    }

    private static NestedArrayReader denseReader()
    {
        return reader(
                new long[] {1, 2, 3, 4, 5},
                new int[] {0, 1, 0, 1, 1},
                new int[] {3, 3, 3, 3, 3},
                new int[] {0, 1, 2, 3, 4});
    }

    private static NestedArrayReader reader(long[] physicalValues, int[] repetitions, int[] definitions, int[] ordinals)
    {
        ParquetSchema.Primitive element = new ParquetSchema.Primitive(
                "element", FieldRepetitionType.OPTIONAL, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("items", "list", "element"), 3, 1);
        ParquetSchema.Group repeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(element), 2, 1);
        ParquetSchema.Group list = new ParquetSchema.Group(
                "items", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(repeatedValues), 1, 0);

        LongPhysicalValueDecoder values = new LongPhysicalValueDecoder(Type.INT64, new PrimitiveArrayPool(0, 0));
        values.decodePlain(longs(physicalValues), 0, physicalValues.length);
        return new NestedArrayReader(
                list,
                RleReaderPolicy.defaults(),
                new TestingCursor(values, repetitions, definitions, ordinals));
    }

    private static NestedArrayReader structReader()
    {
        ParquetSchema.Primitive id = new ParquetSchema.Primitive(
                "id", FieldRepetitionType.OPTIONAL, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("records", "list", "element", "id"), 4, 1);
        ParquetSchema.Primitive label = new ParquetSchema.Primitive(
                "label", FieldRepetitionType.OPTIONAL, Type.BYTE_ARRAY, ConvertedType.UTF8, null, 0, 0, 0, 1,
                List.of("records", "list", "element", "label"), 4, 1);
        ParquetSchema.Group element = new ParquetSchema.Group(
                "element", FieldRepetitionType.OPTIONAL, null, null, List.of(id, label), 3, 1);
        ParquetSchema.Group repeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(element), 2, 1);
        ParquetSchema.Group list = new ParquetSchema.Group(
                "records", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(repeatedValues), 1, 0);

        PrimitiveArrayPool pool = new PrimitiveArrayPool(0, 0);
        LongPhysicalValueDecoder ids = new LongPhysicalValueDecoder(Type.INT64, pool);
        ids.decodePlain(longs(10, 30), 0, 2);
        BinaryPhysicalValueDecoder labels = new BinaryPhysicalValueDecoder(pool);
        labels.decodePlain(binary("ten"), 0, 1);
        int[] repetitions = {0, 1, 0, 0, 0};
        return new NestedArrayReader(
                list,
                RleReaderPolicy.defaults(),
                new NestedLeafCursor[] {
                    new TestingCursor(ids, repetitions, new int[] {4, 2, 1, 4, 0}, new int[] {0, -1, -1, 1, -1}, 2, false),
                    new TestingCursor(labels, repetitions, new int[] {4, 2, 1, 3, 0}, new int[] {0, -1, -1, -1, -1}, 3, false)});
    }

    private static NestedArrayReader nestedReader(int maximumWindowLength, boolean scalarAdvanceSupported)
    {
        ParquetSchema.Primitive value = new ParquetSchema.Primitive(
                "element", FieldRepetitionType.OPTIONAL, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("outer", "list", "element", "list", "element"), 5, 2);
        ParquetSchema.Group innerRepeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(value), 4, 2);
        ParquetSchema.Group inner = new ParquetSchema.Group(
                "element", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(innerRepeatedValues), 3, 1);
        ParquetSchema.Group outerRepeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(inner), 2, 1);
        ParquetSchema.Group outer = new ParquetSchema.Group(
                "outer", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(outerRepeatedValues), 1, 0);

        PrimitiveArrayPool pool = new PrimitiveArrayPool(0, 0);
        LongPhysicalValueDecoder values = new LongPhysicalValueDecoder(Type.INT64, pool);
        values.decodePlain(longs(1, 4), 0, 2);
        return new NestedArrayReader(
                outer,
                RleReaderPolicy.defaults(),
                new TestingCursor(
                        values,
                        new int[] {0, 2, 1, 1, 1, 0, 0, 0},
                        new int[] {5, 4, 2, 3, 5, 0, 1, 3},
                        new int[] {0, -1, -1, -1, 1, -1, -1, -1},
                        maximumWindowLength,
                        scalarAdvanceSupported));
    }

    private static NestedArrayReader dictionaryNestedReader()
    {
        ParquetSchema.Primitive value = new ParquetSchema.Primitive(
                "element", FieldRepetitionType.OPTIONAL, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("outer", "list", "element", "list", "element"), 5, 2);
        ParquetSchema.Group innerRepeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(value), 4, 2);
        ParquetSchema.Group inner = new ParquetSchema.Group(
                "element", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(innerRepeatedValues), 3, 1);
        ParquetSchema.Group outerRepeatedValues = new ParquetSchema.Group(
                "list", FieldRepetitionType.REPEATED, null, null, List.of(inner), 2, 1);
        ParquetSchema.Group outer = new ParquetSchema.Group(
                "outer", FieldRepetitionType.OPTIONAL, ConvertedType.LIST, null, List.of(outerRepeatedValues), 1, 0);

        PrimitiveArrayPool pool = new PrimitiveArrayPool(0, 0);
        LongPhysicalValueDecoder values = new LongPhysicalValueDecoder(Type.INT64, pool);
        values.decodeDictionary(longs(10, 20), 2, org.apache.parquet.format.Encoding.PLAIN);
        return new NestedArrayReader(
                outer,
                RleReaderPolicy.defaults(),
                new TestingCursor(
                        values,
                        new int[] {0, 2, 0, 2},
                        new int[] {5, 5, 5, 5},
                        new int[] {0, 1, 1, 0},
                        new int[] {0, 1, 1, 0}));
    }

    private static String value(BinaryVector vector, int position)
    {
        return new String(
                vector.data(),
                vector.offsets()[position],
                vector.offsets()[position + 1] - vector.offsets()[position],
                StandardCharsets.UTF_8);
    }

    private static MemorySegment longs(long... values)
    {
        ByteBuffer output = ByteBuffer.allocate(values.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        Arrays.stream(values).forEach(output::putLong);
        return MemorySegment.ofArray(output.array());
    }

    private static MemorySegment binary(String... values)
    {
        int bytes = Arrays.stream(values).mapToInt(value -> Integer.BYTES + value.length()).sum();
        ByteBuffer output = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (String value : values) {
            byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            output.putInt(encoded.length).put(encoded);
        }
        return MemorySegment.ofArray(output.array());
    }

    private static final class TestingCursor
            implements NestedLeafEventSource
    {
        private final PhysicalValueDecoder decoder;
        private final int[] repetitions;
        private final int[] definitions;
        private final int[] ordinals;
        private final int[] dictionaryIds;
        private final int maximumWindowLength;
        private final boolean scalarAdvanceSupported;
        private final NestedEventWindow window = new NestedEventWindow();
        private int event = -1;

        private TestingCursor(PhysicalValueDecoder decoder, int[] repetitions, int[] definitions, int[] ordinals)
        {
            this(decoder, repetitions, definitions, ordinals, Integer.MAX_VALUE, true);
        }

        private TestingCursor(
                PhysicalValueDecoder decoder,
                int[] repetitions,
                int[] definitions,
                int[] ordinals,
                int maximumWindowLength,
                boolean scalarAdvanceSupported)
        {
            this.decoder = decoder;
            this.repetitions = repetitions;
            this.definitions = definitions;
            this.ordinals = ordinals;
            this.dictionaryIds = null;
            this.maximumWindowLength = maximumWindowLength;
            this.scalarAdvanceSupported = scalarAdvanceSupported;
        }

        private TestingCursor(
                PhysicalValueDecoder decoder,
                int[] repetitions,
                int[] definitions,
                int[] ordinals,
                int[] dictionaryIds)
        {
            this.decoder = decoder;
            this.repetitions = repetitions;
            this.definitions = definitions;
            this.ordinals = ordinals;
            this.dictionaryIds = dictionaryIds;
            this.maximumWindowLength = Integer.MAX_VALUE;
            this.scalarAdvanceSupported = false;
        }

        @Override
        public boolean next()
        {
            if (!scalarAdvanceSupported) {
                throw new AssertionError("scalar event advance is not supported");
            }
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
            return dictionaryIds == null ? -1 : dictionaryIds[event];
        }

        @Override
        public NestedEventWindow eventWindow()
        {
            int offset = event + 1;
            if (offset == repetitions.length) {
                return null;
            }
            window.reset(
                    decoder,
                    repetitions,
                    definitions,
                    ordinals,
                    dictionaryIds,
                    offset,
                    Math.min(maximumWindowLength, repetitions.length - offset));
            return window;
        }

        @Override
        public void advanceEvents(int count)
        {
            event += count;
        }

        @Override
        public void close() {}
    }
}
