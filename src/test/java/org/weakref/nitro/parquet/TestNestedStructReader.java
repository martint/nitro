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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
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

class TestNestedStructReader
{
    @Test
    void testReconstructsNullableStructAndFields()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedStructReader reader = reader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 3, Mask.all(3));
            StructVector rows = (StructVector) streams.values();
            BooleanVector rowNulls = (BooleanVector) streams.get(Stream.NULLS);
            I64Vector ids = (I64Vector) rows.fieldValues("id");
            BooleanVector idNulls = (BooleanVector) rows.field("id").get(Stream.NULLS);
            BinaryVector names = (BinaryVector) rows.fieldValues("name");
            BooleanVector nameNulls = (BooleanVector) rows.field("name").get(Stream.NULLS);
            BooleanVector active = (BooleanVector) rows.fieldValues("active");
            BooleanVector activeNulls = (BooleanVector) rows.field("active").get(Stream.NULLS);

            assertThat(rowNulls.values()).containsExactly(false, true, false);
            assertThat(ids.values()).containsExactly(1, 0, 2);
            assertThat(idNulls.values()).containsExactly(false, true, false);
            assertThat(value(names, 0)).isEqualTo("alice");
            assertThat(nameNulls.values()).containsExactly(false, true, true);
            assertThat(active.values()).containsExactly(true, false, false);
            assertThat(activeNulls.values()).containsExactly(false, true, false);
        }
    }

    @Test
    void testMaterializesOnlySelectedStructFields()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedStructReader reader = reader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 3, Mask.sparse(new int[] {2}, 3));
            StructVector rows = (StructVector) streams.values();

            assertThat(((I64Vector) rows.fieldValues("id")).values()).containsExactly(0, 0, 2);
            assertThat(((BooleanVector) rows.field("id").get(Stream.NULLS)).values()).containsExactly(true, true, false);
        }
    }

    @Test
    void testSkipAdvancesAllLeavesTogether()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedStructReader reader = reader()) {
            reader.skip(2);
            StructVector rows = (StructVector) reader.read(allocator, new Allocator.Context("test"), 1, Mask.all(1)).values();

            assertThat(((I64Vector) rows.fieldValues("id")).values()).containsExactly(2);
            assertThat(((BooleanVector) rows.field("name").get(Stream.NULLS)).values()).containsExactly(true);
        }
    }

    @Test
    void testCoalescesFieldsWithEqualDictionaryMappings()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            I64Vector firstDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            firstDomain.values()[0] = 10;
            firstDomain.values()[1] = 20;
            I64Vector secondDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            secondDomain.values()[0] = 100;
            secondDomain.values()[1] = 200;
            int[] ids = {0, 1, 0, 1};
            DictionaryVector first = allocator.allocateDictionary(context, ids, firstDomain);
            DictionaryVector second = allocator.allocateDictionary(context, Arrays.copyOf(ids, ids.length), secondDomain);
            BooleanVector noNulls = allocator.allocate(context, BooleanVector.class, ids.length, BooleanVector::new);

            DictionaryVector rows = NestedStructReader.coalesceDictionaryStruct(
                    allocator,
                    context,
                    ids.length,
                    List.of(requiredLong("first", 0), requiredLong("second", 1)),
                    new Streams[] {Streams.ofValues(first), Streams.ofValuesAndNulls(second, noNulls)});

            assertThat(rows).isNotNull();
            assertThat(rows.ids()).containsExactly(ids);
            StructVector domain = (StructVector) rows.values();
            assertThat(((I64Vector) domain.fieldValues("first")).values()).containsExactly(10, 20);
            assertThat(((I64Vector) domain.fieldValues("second")).values()).containsExactly(100, 200);
            assertThat(((BooleanVector) domain.field("second").get(Stream.NULLS)).values()).containsExactly(false, false);
        }
    }

    @Test
    void testCoalescesCorrelatedDictionaryMappings()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            I64Vector firstDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            I64Vector secondDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            firstDomain.values()[0] = 10;
            firstDomain.values()[1] = 20;
            secondDomain.values()[0] = 100;
            secondDomain.values()[1] = 200;
            DictionaryVector first = allocator.allocateDictionary(context, new int[] {0, 1, 0, 1}, firstDomain);
            DictionaryVector second = allocator.allocateDictionary(context, new int[] {1, 0, 1, 0}, secondDomain);

            DictionaryVector rows = NestedStructReader.coalesceDictionaryStruct(
                    allocator,
                    context,
                    4,
                    List.of(requiredLong("first", 0), requiredLong("second", 1)),
                    new Streams[] {Streams.ofValues(first), Streams.ofValues(second)});

            assertThat(rows).isNotNull();
            StructVector domain = (StructVector) rows.values();
            DictionaryVector correlated = (DictionaryVector) domain.fieldValues("second");
            assertThat(correlated.ids()).containsExactly(1, 0);
        }
    }

    @Test
    void testChoosesDiscriminatingMappingIndependentlyOfFieldOrder()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            I64Vector firstDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            I64Vector secondDomain = allocator.allocate(context, I64Vector.class, 4, I64Vector::new);
            DictionaryVector first = allocator.allocateDictionary(context, new int[] {0, 0, 1, 1}, firstDomain);
            DictionaryVector second = allocator.allocateDictionary(context, new int[] {0, 1, 2, 3}, secondDomain);

            DictionaryVector rows = NestedStructReader.coalesceDictionaryStruct(
                    allocator,
                    context,
                    4,
                    List.of(requiredLong("first", 0), requiredLong("second", 1)),
                    new Streams[] {Streams.ofValues(first), Streams.ofValues(second)});

            assertThat(rows).isNotNull();
            assertThat(rows.ids()).containsExactly(0, 1, 2, 3);
            StructVector domain = (StructVector) rows.values();
            DictionaryVector correlated = (DictionaryVector) domain.fieldValues("first");
            assertThat(correlated.ids()).containsExactly(0, 0, 1, 1);
        }
    }

    @Test
    void testDoesNotCoalesceNonFunctionalDictionaryMappings()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            I64Vector firstDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            I64Vector secondDomain = allocator.allocate(context, I64Vector.class, 2, I64Vector::new);
            DictionaryVector first = allocator.allocateDictionary(context, new int[] {0, 1, 0, 1}, firstDomain);
            DictionaryVector second = allocator.allocateDictionary(context, new int[] {1, 0, 0, 1}, secondDomain);

            assertThat(NestedStructReader.coalesceDictionaryStruct(
                    allocator,
                    context,
                    4,
                    List.of(requiredLong("first", 0), requiredLong("second", 1)),
                    new Streams[] {Streams.ofValues(first), Streams.ofValues(second)}))
                    .isNull();
        }
    }

    private static ParquetSchema.Primitive requiredLong(String name, int leafIndex)
    {
        return new ParquetSchema.Primitive(
                name, FieldRepetitionType.REQUIRED, Type.INT64, null, null, 0, 0, 0, leafIndex,
                List.of("person", name), 1, 0);
    }

    private static NestedStructReader reader()
    {
        ParquetSchema.Primitive id = new ParquetSchema.Primitive(
                "id", FieldRepetitionType.REQUIRED, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("person", "id"), 1, 0);
        ParquetSchema.Primitive name = new ParquetSchema.Primitive(
                "name", FieldRepetitionType.OPTIONAL, Type.BYTE_ARRAY, ConvertedType.UTF8, null, 0, 0, 0, 1,
                List.of("person", "name"), 2, 0);
        ParquetSchema.Primitive active = new ParquetSchema.Primitive(
                "active", FieldRepetitionType.OPTIONAL, Type.BOOLEAN, null, null, 0, 0, 0, 2,
                List.of("person", "active"), 2, 0);
        ParquetSchema.Group person = new ParquetSchema.Group(
                "person", FieldRepetitionType.OPTIONAL, null, null, List.of(id, name, active), 1, 0);

        LongPhysicalValueDecoder ids = new LongPhysicalValueDecoder(Type.INT64, new PrimitiveArrayPool(0, 0));
        ids.decodePlain(longs(1, 2), 0, 2);
        BinaryPhysicalValueDecoder names = new BinaryPhysicalValueDecoder(new PrimitiveArrayPool(0, 0));
        names.decodePlain(binary("alice"), 0, 1);
        BooleanPhysicalValueDecoder activeValues = new BooleanPhysicalValueDecoder(new PrimitiveArrayPool(0, 0));
        activeValues.decodePlain(MemorySegment.ofArray(new byte[] {0b0000_0001}), 0, 2);
        return new NestedStructReader(
                person,
                RleReaderPolicy.defaults(),
                new NestedLeafCursor[] {
                    new TestingCursor(ids, new int[] {1, 0, 1}, new int[] {0, -1, 1}),
                    new TestingCursor(names, new int[] {2, 0, 1}, new int[] {0, -1, -1}),
                    new TestingCursor(activeValues, new int[] {2, 0, 2}, new int[] {0, -1, 1})});
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
        private final int[] definitions;
        private final int[] ordinals;
        private final int[] repetitions;
        private final NestedEventWindow window = new NestedEventWindow();
        private int position;
        private int event = -1;

        private TestingCursor(PhysicalValueDecoder decoder, int[] definitions, int[] ordinals)
        {
            this.decoder = decoder;
            this.definitions = definitions;
            this.ordinals = ordinals;
            this.repetitions = new int[definitions.length];
        }

        @Override
        public boolean next()
        {
            if (position >= definitions.length) {
                event = -1;
                return false;
            }
            event = position++;
            return true;
        }

        @Override
        public int repetitionLevel()
        {
            return 0;
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
        public NestedEventWindow eventWindow()
        {
            if (position >= definitions.length) {
                return null;
            }
            window.reset(decoder, repetitions, definitions, ordinals, null, position, definitions.length - position);
            event = -1;
            return window;
        }

        @Override
        public void advanceEvents(int count)
        {
            position += count;
            event = -1;
        }

        @Override
        public void close() {}
    }
}
