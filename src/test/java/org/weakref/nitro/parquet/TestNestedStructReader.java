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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
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

            assertThat(rowNulls.values()).containsExactly(false, true, false);
            assertThat(ids.values()).containsExactly(1, 0, 2);
            assertThat(idNulls.values()).containsExactly(false, true, false);
            assertThat(value(names, 0)).isEqualTo("alice");
            assertThat(nameNulls.values()).containsExactly(false, true, true);
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

    private static NestedStructReader reader()
    {
        ParquetSchema.Primitive id = new ParquetSchema.Primitive(
                "id", FieldRepetitionType.REQUIRED, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("person", "id"), 1, 0);
        ParquetSchema.Primitive name = new ParquetSchema.Primitive(
                "name", FieldRepetitionType.OPTIONAL, Type.BYTE_ARRAY, ConvertedType.UTF8, null, 0, 0, 0, 1,
                List.of("person", "name"), 2, 0);
        ParquetSchema.Group person = new ParquetSchema.Group(
                "person", FieldRepetitionType.OPTIONAL, null, null, List.of(id, name), 1, 0);

        LongPhysicalValueDecoder ids = new LongPhysicalValueDecoder(Type.INT64);
        ids.decodePlain(longs(1, 2), 0, 2);
        BinaryPhysicalValueDecoder names = new BinaryPhysicalValueDecoder();
        names.decodePlain(binary("alice"), 0, 1);
        return new NestedStructReader(
                person,
                RleReaderPolicy.defaults(),
                new NestedLeafCursor[] {
                    new TestingCursor(ids, new int[] {1, 0, 1}, new int[] {0, -1, 1}),
                    new TestingCursor(names, new int[] {2, 0, 1}, new int[] {0, -1, -1})});
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
            implements NestedLeafCursor
    {
        private final PhysicalValueDecoder decoder;
        private final int[] definitions;
        private final int[] ordinals;
        private int event = -1;

        private TestingCursor(PhysicalValueDecoder decoder, int[] definitions, int[] ordinals)
        {
            this.decoder = decoder;
            this.definitions = definitions;
            this.ordinals = ordinals;
        }

        @Override
        public boolean next()
        {
            return ++event < definitions.length;
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
        public void close() {}
    }
}
