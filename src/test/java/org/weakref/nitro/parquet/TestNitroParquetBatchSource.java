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

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.operator.DynamicFilter;

import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestNitroParquetBatchSource
{
    @Test
    void testNestedSchemaFailsAtNativeReaderAdmission()
    {
        List<SchemaElement> schema = List.of(
                new SchemaElement("root").setNum_children(2),
                new SchemaElement("code").setType(Type.INT64).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL),
                new SchemaElement("attributes").setNum_children(1).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL),
                new SchemaElement("key_value").setNum_children(2).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.REPEATED),
                new SchemaElement("key").setType(Type.INT64).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.REQUIRED),
                new SchemaElement("value").setType(Type.BYTE_ARRAY).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL));

        UnsupportedParquetFeatureException failure = assertThrows(
                UnsupportedParquetFeatureException.class,
                () -> ParquetFile.parseFlatSchema(schema));

        assertEquals("Native Nitro Parquet reader does not yet support nested field 'attributes'", failure.getMessage());
    }

    @Test
    void testNestedSchemaTracksPhysicalLeavesAndLevels()
    {
        ParquetSchema schema = ParquetSchema.parse(List.of(
                new SchemaElement("root").setNum_children(2),
                new SchemaElement("code").setType(Type.INT64).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL),
                new SchemaElement("attributes")
                        .setNum_children(1)
                        .setLogicalType(LogicalType.MAP(new MapType()))
                        .setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL),
                new SchemaElement("key_value").setNum_children(2).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.REPEATED),
                new SchemaElement("key").setType(Type.INT64).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.REQUIRED),
                new SchemaElement("value")
                        .setType(Type.BYTE_ARRAY)
                        .setLogicalType(LogicalType.STRING(new StringType()))
                        .setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL)));

        assertEquals(3, schema.leaves().size());
        assertEquals(List.of("attributes", "key_value", "key"), schema.leaves().get(1).path());
        assertEquals(1, schema.leaves().get(1).maximumRepetitionLevel());
        assertEquals(2, schema.leaves().get(1).maximumDefinitionLevel());
        assertEquals(3, schema.leaves().get(2).maximumDefinitionLevel());
        assertTrue(((ParquetSchema.Group) schema.field("attributes")).isMap());
        assertTrue(schema.leaves().get(2).string());
    }

    @Test
    void testFlatSchemaKeepsPhysicalLeafOrdinals()
    {
        ParquetFile.SchemaColumns schema = ParquetFile.parseFlatSchema(List.of(
                new SchemaElement("root").setNum_children(3),
                new SchemaElement("first").setType(Type.INT64).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.REQUIRED),
                new SchemaElement("second").setType(Type.BYTE_ARRAY).setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL),
                new SchemaElement("third")
                        .setType(Type.BYTE_ARRAY)
                        .setLogicalType(LogicalType.STRING(new StringType()))
                        .setRepetition_type(org.apache.parquet.format.FieldRepetitionType.REQUIRED)));

        assertEquals(0, schema.columns().get(0).leafIndex());
        assertEquals(1, schema.columns().get(1).leafIndex());
        assertFalse(schema.columns().get(1).string());
        assertEquals(2, schema.columns().get(2).leafIndex());
        assertTrue(schema.columns().get(2).string());
        assertEquals(1, schema.columnIndexByName().get("second"));
    }

    @Test
    void testFragmentedNumericSkipAdmissionBoundaries()
    {
        assertTrue(admits(600, 10_000, 128, 2));
        assertTrue(admits(600, 10_000, 128, 128));

        assertFalse(admits(601, 10_000, 128, 2));
        assertFalse(admits(600, 10_000, 129, 2));
        assertFalse(admits(600, 10_000, 128, 1));
        assertFalse(admits(600, 10_000, 128, 129));
        assertFalse(admits(0, 0, 128, 2));
        assertFalse(new ParquetLateMaterializationPolicy.FragmentedNumeric(false, 6, 6, 2, 8)
                .admits(600, 10_000, 6, 2));
    }

    @Test
    void testSplitOwnsRowGroupByFirstPhysicalColumnStart()
    {
        RowGroup plain = rowGroup(100, 0);
        assertTrue(ParquetFile.splitContainsRowGroup(plain, 100, 200));
        assertFalse(ParquetFile.splitContainsRowGroup(plain, 0, 100));
        assertFalse(ParquetFile.splitContainsRowGroup(plain, 101, 200));

        RowGroup dictionary = rowGroup(150, 90);
        assertTrue(ParquetFile.splitContainsRowGroup(dictionary, 0, 100));
        assertFalse(ParquetFile.splitContainsRowGroup(dictionary, 100, 200));
    }

    @Test
    void testLateRuntimeFilterPreservesEstablishedOrder()
    {
        int[] establishedOrder = {2, 0};
        assertArrayEquals(new int[] {2, 0, 1}, NitroParquetBatchSource.appendFilterColumn(establishedOrder, 1));
        assertArrayEquals(new int[] {2, 0}, establishedOrder);
    }

    @Test
    void testWideFixedDecimalRemainsPhysicalBinary()
    {
        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
                ColumnReader shortDecimal = new ColumnReader(Type.FIXED_LEN_BYTE_ARRAY, false, 8, true, null, arrays, ParquetReaderPolicy.defaults());
                ColumnReader longDecimal = new ColumnReader(Type.FIXED_LEN_BYTE_ARRAY, false, 16, true, null, arrays, ParquetReaderPolicy.defaults())) {
            assertEquals(ColumnReader.Kind.LONG, shortDecimal.kind());
            assertEquals(ColumnReader.Kind.BINARY, longDecimal.kind());
        }
    }

    @Test
    void testNumericChunkStatisticsRejectDisjointDomain()
    {
        ColumnMetaData metadata = new ColumnMetaData(
                Type.INT32,
                List.of(Encoding.PLAIN),
                List.of("value"),
                CompressionCodec.UNCOMPRESSED,
                100,
                400,
                400,
                0);
        metadata.setStatistics(new Statistics()
                .setMin_value(littleEndianInt(10))
                .setMax_value(littleEndianInt(19)));

        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
                ColumnReader reader = new ColumnReader(Type.INT32, false, 0, false, null, arrays, ParquetReaderPolicy.defaults())) {
            reader.addChunk(MemorySegment.NULL, metadata, 100);

            assertFalse(reader.chunkMayMatch(0, DynamicFilter.fromRange(0, 20, 30)));
            assertTrue(reader.chunkMayMatch(0, DynamicFilter.fromRange(0, 19, 30)));
            assertTrue(reader.chunkMayMatch(0, DynamicFilter.fromRange(0, 0, 10)));
        }
    }

    @Test
    void testWholeChunkSkipReleasesRangeBeforeReaderClose()
    {
        ColumnMetaData metadata = new ColumnMetaData(
                Type.INT32,
                List.of(Encoding.PLAIN),
                List.of("value"),
                CompressionCodec.UNCOMPRESSED,
                100,
                400,
                400,
                0);
        AtomicBoolean firstReleased = new AtomicBoolean();
        AtomicBoolean secondReleased = new AtomicBoolean();

        try (PrimitiveArrayPool arrays = new PrimitiveArrayPool(0, 0);
                ColumnReader reader = new ColumnReader(Type.INT32, false, 0, false, null, arrays, ParquetReaderPolicy.defaults())) {
            reader.addChunk(new ParquetInputRange(MemorySegment.ofArray(new byte[400]), () -> firstReleased.set(true)), metadata, 100);
            reader.addChunk(new ParquetInputRange(MemorySegment.ofArray(new byte[400]), () -> secondReleased.set(true)), metadata, 100);

            reader.skip(100);

            assertTrue(firstReleased.get());
            assertFalse(secondReleased.get());
        }
        assertTrue(secondReleased.get());
    }

    @Test
    void testInputIsClosedWhenSourceConstructionFails()
    {
        AtomicBoolean closed = new AtomicBoolean();
        ParquetInput input = new ParquetInput()
        {
            @Override
            public String id()
            {
                return "test://invalid.parquet";
            }

            @Override
            public long size()
            {
                return 8;
            }

            @Override
            public ParquetInputRange readRange(long offset, int length)
            {
                return ParquetInputRange.retained(MemorySegment.ofArray(new byte[length]));
            }

            @Override
            public void close()
            {
                closed.set(true);
            }
        };

        try (AllocationResources allocationResources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(allocationResources)) {
            assertThrows(UncheckedIOException.class, () -> NitroParquetBatchSource.forInputs(
                    NitroParquetScanResources.createDefault(),
                    allocator,
                    List.of(new NitroParquetBatchSource.InputSplit(input, 0, 8)),
                    Schema.unspecified(List.of("value"))));
        }
        assertTrue(closed.get());
    }

    private static boolean admits(int selected, int total, int scanColumns, int payloadColumns)
    {
        return ParquetLateMaterializationPolicy.defaults()
                .skipDecode()
                .fragmentedNumeric()
                .admits(selected, total, scanColumns, payloadColumns);
    }

    private static RowGroup rowGroup(long dataOffset, long dictionaryOffset)
    {
        ColumnMetaData metadata = new ColumnMetaData(
                Type.INT64,
                List.of(Encoding.PLAIN),
                List.of("value"),
                CompressionCodec.UNCOMPRESSED,
                1,
                8,
                8,
                dataOffset);
        metadata.setDictionary_page_offset(dictionaryOffset);
        ColumnChunk column = new ColumnChunk();
        column.setMeta_data(metadata);
        return new RowGroup(List.of(column), 8, 1);
    }

    private static byte[] littleEndianInt(int value)
    {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
