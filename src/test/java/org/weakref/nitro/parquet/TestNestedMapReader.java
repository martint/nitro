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
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestNestedMapReader
{
    @Test
    void testReconstructsNullEmptyAndPopulatedMaps()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedMapReader reader = reader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 4, Mask.all(4));
            MapVector maps = (MapVector) streams.values();
            BooleanVector mapNulls = (BooleanVector) streams.get(Stream.NULLS);
            I64Vector keys = (I64Vector) maps.keyValues();
            BinaryVector values = (BinaryVector) maps.valueValues();
            BooleanVector valueNulls = (BooleanVector) maps.valueStreamOrNull(Stream.NULLS);

            assertThat(maps.offsets()).containsExactly(0, 2, 2, 2, 3);
            assertThat(mapNulls.values()).containsExactly(false, true, false, false);
            assertThat(keys.values()).containsExactly(1, 2, 3);
            assertThat(value(values, 0)).isEqualTo("one");
            assertThat(value(values, 1)).isEmpty();
            assertThat(value(values, 2)).isEqualTo("three");
            assertThat(valueNulls.values()).containsExactly(false, true, false);
        }
    }

    @Test
    void testMaterializesOnlySelectedMapEntries()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedMapReader reader = reader()) {
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 4, Mask.sparse(new int[] {3}, 4));
            MapVector maps = (MapVector) streams.values();
            I64Vector keys = (I64Vector) maps.keyValues();
            BinaryVector values = (BinaryVector) maps.valueValues();

            assertThat(maps.offsets()).containsExactly(0, 0, 0, 0, 1);
            assertThat(keys.values()).containsExactly(3);
            assertThat(value(values, 0)).isEqualTo("three");
        }
    }

    @Test
    void testSkipAdvancesWithoutMaterializingEntries()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                NestedMapReader reader = reader()) {
            reader.skip(3);
            Streams streams = reader.read(allocator, new Allocator.Context("test"), 1, Mask.all(1));
            MapVector maps = (MapVector) streams.values();

            assertThat(maps.offsets()).containsExactly(0, 1);
            assertThat(((I64Vector) maps.keyValues()).values()).containsExactly(3);
            assertThat(value((BinaryVector) maps.valueValues(), 0)).isEqualTo("three");
        }
    }

    private static NestedMapReader reader()
    {
        ParquetSchema.Primitive key = new ParquetSchema.Primitive(
                "key", FieldRepetitionType.REQUIRED, Type.INT64, null, null, 0, 0, 0, 0,
                List.of("attributes", "key_value", "key"), 2, 1);
        ParquetSchema.Primitive value = new ParquetSchema.Primitive(
                "value", FieldRepetitionType.OPTIONAL, Type.BYTE_ARRAY, ConvertedType.UTF8, null, 0, 0, 0, 1,
                List.of("attributes", "key_value", "value"), 3, 1);
        ParquetSchema.Group entries = new ParquetSchema.Group(
                "key_value", FieldRepetitionType.REPEATED, null, null, List.of(key, value), 2, 1);
        ParquetSchema.Group map = new ParquetSchema.Group(
                "attributes", FieldRepetitionType.OPTIONAL, ConvertedType.MAP, null, List.of(entries), 1, 0);

        LongPhysicalValueDecoder keys = new LongPhysicalValueDecoder(Type.INT64);
        keys.decodePlain(longs(1, 2, 3), 0, 3);
        BinaryPhysicalValueDecoder values = new BinaryPhysicalValueDecoder();
        values.decodePlain(binary("one", "three"), 0, 2);

        return new NestedMapReader(
                map,
                RleReaderPolicy.defaults(),
                new TestingCursor(keys, new int[] {0, 1, 0, 0, 0}, new int[] {2, 2, 0, 1, 2}, new int[] {0, 1, -1, -1, 2}),
                new TestingCursor(values, new int[] {0, 1, 0, 0, 0}, new int[] {3, 2, 0, 1, 3}, new int[] {0, -1, -1, -1, 1}));
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
