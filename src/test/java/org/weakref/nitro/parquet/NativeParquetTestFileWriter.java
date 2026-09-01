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

import io.airlift.compress.v3.snappy.SnappyCompressor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** Writes deliberately small, deterministic Parquet fixtures without a Hadoop or parquet-mr reader/writer stack. */
public final class NativeParquetTestFileWriter
{
    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private NativeParquetTestFileWriter() {}

    public record LongStringStruct(long id, String name) {}

    public record Column(String name, Type type, boolean optional, boolean utf8, LogicalType logicalType, List<?> values)
    {
        public Column
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            values = Collections.unmodifiableList(new ArrayList<>(values));
            if (!optional && values.stream().anyMatch(value -> value == null)) {
                throw new IllegalArgumentException("Required column contains null: " + name);
            }
        }

        public static Column required(String name, Type type, List<?> values)
        {
            return new Column(name, type, false, false, null, values);
        }

        public static Column optional(String name, Type type, List<?> values)
        {
            return new Column(name, type, true, false, null, values);
        }

        public static Column requiredInt32(String name, List<?> values)
        {
            return required(name, Type.INT32, values);
        }

        public static Column optionalInt32(String name, List<?> values)
        {
            return optional(name, Type.INT32, values);
        }

        public static Column requiredInt64(String name, List<?> values)
        {
            return required(name, Type.INT64, values);
        }

        public static Column optionalInt64(String name, List<?> values)
        {
            return optional(name, Type.INT64, values);
        }

        public static Column requiredBoolean(String name, List<?> values)
        {
            return required(name, Type.BOOLEAN, values);
        }

        public static Column requiredBinary(String name, List<?> values)
        {
            return required(name, Type.BYTE_ARRAY, values);
        }

        public static Column optionalBinary(String name, List<?> values)
        {
            return optional(name, Type.BYTE_ARRAY, values);
        }

        public Column asUtf8()
        {
            if (type != Type.BYTE_ARRAY) {
                throw new IllegalStateException("UTF-8 annotation requires BYTE_ARRAY");
            }
            return new Column(name, type, optional, true, logicalType, values);
        }

        public Column asLogicalType(LogicalType logicalType)
        {
            return new Column(name, type, optional, utf8, requireNonNull(logicalType, "logicalType is null"), values);
        }
    }

    public static void write(Path path, String schemaName, List<Column> columns, boolean dictionaryEnabled)
            throws IOException
    {
        write(path, schemaName, columns, dictionaryEnabled, CompressionCodec.UNCOMPRESSED);
    }

    public static void write(Path path, String schemaName, List<Column> columns, boolean dictionaryEnabled, CompressionCodec codec)
            throws IOException
    {
        requireNonNull(path, "path is null");
        requireNonNull(schemaName, "schemaName is null");
        columns = List.copyOf(columns);
        requireNonNull(codec, "codec is null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns is empty");
        }
        int rowCount = columns.getFirst().values().size();
        if (columns.stream().anyMatch(column -> column.values().size() != rowCount)) {
            throw new IllegalArgumentException("columns have different row counts");
        }

        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.write(MAGIC);
        List<ColumnChunk> chunks = new ArrayList<>(columns.size());
        long totalUncompressedSize = 0;
        long totalCompressedSize = 0;
        for (Column column : columns) {
            WrittenChunk chunk = writeColumn(file, column, dictionaryEnabled && column.type() != Type.BOOLEAN, codec);
            chunks.add(chunk.metadata());
            totalUncompressedSize += chunk.uncompressedSize();
            totalCompressedSize += chunk.compressedSize();
        }

        RowGroup rowGroup = new RowGroup(chunks, totalUncompressedSize, rowCount)
                .setFile_offset(chunks.getFirst().file_offset)
                .setTotal_compressed_size(totalCompressedSize);
        List<SchemaElement> schema = new ArrayList<>(columns.size() + 1);
        schema.add(new SchemaElement(schemaName).setNum_children(columns.size()));
        for (Column column : columns) {
            SchemaElement field = new SchemaElement(column.name())
                    .setType(column.type())
                    .setRepetition_type(column.optional() ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED);
            if (column.utf8()) {
                field.setConverted_type(ConvertedType.UTF8);
            }
            if (column.logicalType() != null) {
                field.setLogicalType(column.logicalType());
            }
            schema.add(field);
        }
        FileMetaData footer = new FileMetaData(1, schema, rowCount, List.of(rowGroup))
                .setCreated_by("nitro-native-test-writer");
        ByteArrayOutputStream footerBytes = new ByteArrayOutputStream();
        Util.writeFileMetaData(footer, footerBytes);
        file.write(footerBytes.toByteArray());
        writeLittleEndianInt(file, footerBytes.size());
        file.write(MAGIC);
        Files.write(path, file.toByteArray());
    }

    public static void writeRepeatedOptionalInt64(Path path, String schemaName, String columnName, List<List<Long>> rows)
            throws IOException
    {
        List<Event> events = new ArrayList<>();
        for (List<Long> row : rows) {
            if (row.isEmpty()) {
                events.add(new Event(0, 0, null));
                continue;
            }
            for (int index = 0; index < row.size(); index++) {
                Long value = row.get(index);
                events.add(new Event(index == 0 ? 0 : 1, value == null ? 1 : 2, value));
            }
        }
        List<SchemaElement> schema = List.of(
                new SchemaElement(schemaName).setNum_children(1),
                new SchemaElement(columnName).setNum_children(1).setRepetition_type(FieldRepetitionType.REPEATED),
                new SchemaElement("element").setType(Type.INT64).setRepetition_type(FieldRepetitionType.OPTIONAL));
        writeNested(path, schema, rows.size(), List.of(new NestedColumn(List.of(columnName, "element"), Type.INT64, 1, 2, events)), false);
    }

    public static void writeOptionalUtf8LongMap(Path path, String schemaName, String columnName, List<Map<String, Long>> rows)
            throws IOException
    {
        List<Event> keyEvents = new ArrayList<>();
        List<Event> valueEvents = new ArrayList<>();
        addMapEvents(rows, keyEvents, valueEvents);
        List<SchemaElement> schema = List.of(
                new SchemaElement(schemaName).setNum_children(1),
                new SchemaElement(columnName)
                        .setNum_children(1)
                        .setRepetition_type(FieldRepetitionType.OPTIONAL)
                        .setConverted_type(ConvertedType.MAP),
                new SchemaElement("key_value").setNum_children(2).setRepetition_type(FieldRepetitionType.REPEATED),
                new SchemaElement("key")
                        .setType(Type.BYTE_ARRAY)
                        .setRepetition_type(FieldRepetitionType.REQUIRED)
                        .setConverted_type(ConvertedType.UTF8),
                new SchemaElement("value").setType(Type.INT64).setRepetition_type(FieldRepetitionType.OPTIONAL));
        writeNested(path, schema, rows.size(), List.of(
                new NestedColumn(List.of(columnName, "key_value", "key"), Type.BYTE_ARRAY, 1, 2, keyEvents),
                new NestedColumn(List.of(columnName, "key_value", "value"), Type.INT64, 1, 3, valueEvents)), true);
    }

    public static void writeRequiredLongAndOptionalUtf8LongMap(
            Path path,
            String schemaName,
            String longColumnName,
            List<Long> longValues,
            String mapColumnName,
            List<Map<String, Long>> mapValues)
            throws IOException
    {
        if (longValues.size() != mapValues.size()) {
            throw new IllegalArgumentException("columns have different row counts");
        }
        List<Event> longEvents = longValues.stream()
                .map(value -> new Event(0, 0, requireNonNull(value, "required long value is null")))
                .toList();
        List<Event> keyEvents = new ArrayList<>();
        List<Event> valueEvents = new ArrayList<>();
        addMapEvents(mapValues, keyEvents, valueEvents);
        List<SchemaElement> schema = List.of(
                new SchemaElement(schemaName).setNum_children(2),
                new SchemaElement(longColumnName).setType(Type.INT64).setRepetition_type(FieldRepetitionType.REQUIRED),
                new SchemaElement(mapColumnName)
                        .setNum_children(1)
                        .setRepetition_type(FieldRepetitionType.OPTIONAL)
                        .setConverted_type(ConvertedType.MAP),
                new SchemaElement("key_value").setNum_children(2).setRepetition_type(FieldRepetitionType.REPEATED),
                new SchemaElement("key")
                        .setType(Type.BYTE_ARRAY)
                        .setRepetition_type(FieldRepetitionType.REQUIRED)
                        .setConverted_type(ConvertedType.UTF8),
                new SchemaElement("value").setType(Type.INT64).setRepetition_type(FieldRepetitionType.OPTIONAL));
        writeNested(path, schema, longValues.size(), List.of(
                new NestedColumn(List.of(longColumnName), Type.INT64, 0, 0, longEvents),
                new NestedColumn(List.of(mapColumnName, "key_value", "key"), Type.BYTE_ARRAY, 1, 2, keyEvents),
                new NestedColumn(List.of(mapColumnName, "key_value", "value"), Type.INT64, 1, 3, valueEvents)), true);
    }

    private static void addMapEvents(
            List<Map<String, Long>> rows,
            List<Event> keyEvents,
            List<Event> valueEvents)
    {
        for (Map<String, Long> row : rows) {
            if (row == null) {
                keyEvents.add(new Event(0, 0, null));
                valueEvents.add(new Event(0, 0, null));
                continue;
            }
            if (row.isEmpty()) {
                keyEvents.add(new Event(0, 1, null));
                valueEvents.add(new Event(0, 1, null));
                continue;
            }
            int entryIndex = 0;
            for (Map.Entry<String, Long> entry : row.entrySet()) {
                int repetitionLevel = entryIndex++ == 0 ? 0 : 1;
                keyEvents.add(new Event(repetitionLevel, 2, entry.getKey()));
                Long value = entry.getValue();
                valueEvents.add(new Event(repetitionLevel, value == null ? 2 : 3, value));
            }
        }
    }

    public static void writeOptionalLongUtf8Struct(Path path, String schemaName, String columnName, List<LongStringStruct> rows)
            throws IOException
    {
        List<Event> idEvents = new ArrayList<>();
        List<Event> nameEvents = new ArrayList<>();
        for (LongStringStruct row : rows) {
            if (row == null) {
                idEvents.add(new Event(0, 0, null));
                nameEvents.add(new Event(0, 0, null));
                continue;
            }
            idEvents.add(new Event(0, 1, row.id()));
            nameEvents.add(new Event(0, row.name() == null ? 1 : 2, row.name()));
        }
        List<SchemaElement> schema = List.of(
                new SchemaElement(schemaName).setNum_children(1),
                new SchemaElement(columnName).setNum_children(2).setRepetition_type(FieldRepetitionType.OPTIONAL),
                new SchemaElement("id").setType(Type.INT64).setRepetition_type(FieldRepetitionType.REQUIRED),
                new SchemaElement("name")
                        .setType(Type.BYTE_ARRAY)
                        .setRepetition_type(FieldRepetitionType.OPTIONAL)
                        .setConverted_type(ConvertedType.UTF8));
        writeNested(path, schema, rows.size(), List.of(
                new NestedColumn(List.of(columnName, "id"), Type.INT64, 0, 1, idEvents),
                new NestedColumn(List.of(columnName, "name"), Type.BYTE_ARRAY, 0, 2, nameEvents)), true);
    }

    private static void writeNested(
            Path path,
            List<SchemaElement> schema,
            int rowCount,
            List<NestedColumn> columns,
            boolean dictionaryEnabled)
            throws IOException
    {
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.write(MAGIC);
        List<ColumnChunk> chunks = new ArrayList<>(columns.size());
        long totalUncompressedSize = 0;
        long totalCompressedSize = 0;
        for (NestedColumn column : columns) {
            WrittenChunk chunk = writeNestedColumn(file, column, dictionaryEnabled);
            chunks.add(chunk.metadata());
            totalUncompressedSize += chunk.uncompressedSize();
            totalCompressedSize += chunk.compressedSize();
        }
        RowGroup rowGroup = new RowGroup(chunks, totalUncompressedSize, rowCount)
                .setFile_offset(chunks.getFirst().file_offset)
                .setTotal_compressed_size(totalCompressedSize);
        FileMetaData footer = new FileMetaData(1, schema, rowCount, List.of(rowGroup))
                .setCreated_by("nitro-native-test-writer");
        ByteArrayOutputStream footerBytes = new ByteArrayOutputStream();
        Util.writeFileMetaData(footer, footerBytes);
        file.write(footerBytes.toByteArray());
        writeLittleEndianInt(file, footerBytes.size());
        file.write(MAGIC);
        Files.write(path, file.toByteArray());
    }

    private static WrittenChunk writeNestedColumn(ByteArrayOutputStream file, NestedColumn column, boolean dictionaryEnabled)
            throws IOException
    {
        long chunkOffset = file.size();
        List<?> physicalValues = column.events().stream()
                .filter(event -> event.value() != null)
                .map(Event::value)
                .toList();
        long dictionaryOffset = 0;
        int dictionaryUncompressedSize = 0;
        List<Encoding> encodings = new ArrayList<>();
        byte[] values;
        Encoding dataEncoding;
        if (dictionaryEnabled) {
            Dictionary dictionary = dictionary(physicalValues);
            dictionaryOffset = file.size();
            byte[] dictionaryBody = encodePlain(column.type(), dictionary.values());
            dictionaryUncompressedSize = writePage(file, PageType.DICTIONARY_PAGE, dictionaryBody, CompressionCodec.UNCOMPRESSED,
                    new DictionaryPageHeader(dictionary.values().size(), Encoding.PLAIN), null);
            values = encodeDictionaryIds(dictionary.ids(), dictionary.values().size());
            dataEncoding = Encoding.PLAIN_DICTIONARY;
            encodings.add(Encoding.PLAIN_DICTIONARY);
        }
        else {
            values = encodePlain(column.type(), physicalValues);
            dataEncoding = Encoding.PLAIN;
            encodings.add(Encoding.PLAIN);
        }

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (column.maximumRepetitionLevel() > 0) {
            byte[] repetitions = encodeRuns(column.events().stream().map(Event::repetitionLevel).toList(), bitWidth(column.maximumRepetitionLevel()));
            writeLittleEndianInt(body, repetitions.length);
            body.write(repetitions);
        }
        if (column.maximumDefinitionLevel() > 0) {
            byte[] definitions = encodeRuns(column.events().stream().map(Event::definitionLevel).toList(), bitWidth(column.maximumDefinitionLevel()));
            writeLittleEndianInt(body, definitions.length);
            body.write(definitions);
        }
        body.write(values);
        encodings.add(Encoding.RLE);
        long dataOffset = file.size();
        int dataUncompressedSize = writePage(
                file,
                PageType.DATA_PAGE,
                body.toByteArray(),
                CompressionCodec.UNCOMPRESSED,
                null,
                new DataPageHeader(column.events().size(), dataEncoding, Encoding.RLE, Encoding.RLE));
        long compressedSize = file.size() - chunkOffset;
        long uncompressedSize = dictionaryUncompressedSize + dataUncompressedSize;
        ColumnMetaData metadata = new ColumnMetaData(
                column.type(),
                List.copyOf(encodings),
                column.path(),
                CompressionCodec.UNCOMPRESSED,
                column.events().size(),
                uncompressedSize,
                compressedSize,
                dataOffset)
                .setStatistics(statistics(
                        column.type(),
                        column.events().stream().map(Event::value).toList(),
                        column.events().stream().filter(event -> event.value() == null).count()));
        if (dictionaryEnabled) {
            metadata.setDictionary_page_offset(dictionaryOffset);
        }
        return new WrittenChunk(new ColumnChunk(chunkOffset).setMeta_data(metadata), uncompressedSize, compressedSize);
    }

    private static WrittenChunk writeColumn(ByteArrayOutputStream file, Column column, boolean dictionaryEnabled, CompressionCodec codec)
            throws IOException
    {
        long chunkOffset = file.size();
        long dictionaryOffset = 0;
        int dictionaryUncompressedSize = 0;
        List<Encoding> encodings = new ArrayList<>();
        byte[] values;
        Encoding dataEncoding;
        Dictionary dictionary = null;
        if (dictionaryEnabled) {
            dictionary = dictionary(column);
            dictionaryOffset = file.size();
            byte[] dictionaryBody = encodePlain(column.type(), dictionary.values());
            dictionaryUncompressedSize = writePage(file, PageType.DICTIONARY_PAGE, dictionaryBody, codec,
                    new DictionaryPageHeader(dictionary.values().size(), Encoding.PLAIN), null);
            values = encodeDictionaryIds(dictionary.ids(), dictionary.values().size());
            dataEncoding = Encoding.PLAIN_DICTIONARY;
            encodings.add(Encoding.PLAIN_DICTIONARY);
        }
        else {
            values = encodePlain(column.type(), column.values().stream().filter(value -> value != null).toList());
            dataEncoding = Encoding.PLAIN;
            encodings.add(Encoding.PLAIN);
        }

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (column.optional()) {
            byte[] definitions = encodeRuns(column.values().stream().map(value -> value == null ? 0 : 1).toList(), 1);
            writeLittleEndianInt(body, definitions.length);
            body.write(definitions);
            encodings.add(Encoding.RLE);
        }
        body.write(values);
        long dataOffset = file.size();
        int dataUncompressedSize = writePage(
                file,
                PageType.DATA_PAGE,
                body.toByteArray(),
                codec,
                null,
                new DataPageHeader(column.values().size(), dataEncoding, Encoding.RLE, Encoding.RLE));
        long chunkEnd = file.size();
        long compressedSize = chunkEnd - chunkOffset;
        long uncompressedSize = dictionaryUncompressedSize + dataUncompressedSize;

        long nullCount = column.values().stream().filter(value -> value == null).count();
        ColumnMetaData metadata = new ColumnMetaData(
                column.type(),
                List.copyOf(encodings),
                List.of(column.name()),
                codec,
                column.values().size(),
                uncompressedSize,
                compressedSize,
                dataOffset)
                .setStatistics(statistics(column, nullCount));
        if (dictionary != null) {
            metadata.setDictionary_page_offset(dictionaryOffset);
        }
        return new WrittenChunk(new ColumnChunk(chunkOffset).setMeta_data(metadata), uncompressedSize, compressedSize);
    }

    private static int writePage(
            ByteArrayOutputStream output,
            PageType pageType,
            byte[] body,
            CompressionCodec codec,
            DictionaryPageHeader dictionaryHeader,
            DataPageHeader dataHeader)
            throws IOException
    {
        byte[] compressed = compress(body, codec);
        PageHeader header = new PageHeader(pageType, body.length, compressed.length);
        if (dictionaryHeader != null) {
            header.setDictionary_page_header(dictionaryHeader);
        }
        if (dataHeader != null) {
            header.setData_page_header(dataHeader);
        }
        Util.writePageHeader(header, output);
        output.write(compressed);
        return serializedHeaderSize(header) + body.length;
    }

    private static int serializedHeaderSize(PageHeader header)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Util.writePageHeader(header, output);
        return output.size();
    }

    private static byte[] compress(byte[] input, CompressionCodec codec)
    {
        return switch (codec) {
            case UNCOMPRESSED -> input;
            case SNAPPY -> {
                SnappyCompressor compressor = SnappyCompressor.create();
                byte[] output = new byte[compressor.maxCompressedLength(input.length)];
                int size = compressor.compress(input, 0, input.length, output, 0, output.length);
                yield Arrays.copyOf(output, size);
            }
            default -> throw new IllegalArgumentException("Unsupported test compression codec: " + codec);
        };
    }

    private static Dictionary dictionary(Column column)
    {
        return dictionary(column.values().stream().filter(value -> value != null).toList());
    }

    private static Dictionary dictionary(List<?> inputValues)
    {
        Map<ValueKey, Integer> idsByValue = new LinkedHashMap<>();
        List<Object> values = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        for (Object value : inputValues) {
            ValueKey key = new ValueKey(value);
            Integer id = idsByValue.get(key);
            if (id == null) {
                id = values.size();
                idsByValue.put(key, id);
                values.add(value);
            }
            ids.add(id);
        }
        return new Dictionary(List.copyOf(values), List.copyOf(ids));
    }

    private static int bitWidth(int maximumValue)
    {
        return maximumValue == 0 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(maximumValue);
    }

    private static byte[] encodeDictionaryIds(List<Integer> ids, int dictionarySize)
            throws IOException
    {
        int bitWidth = dictionarySize <= 1 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(dictionarySize - 1);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(bitWidth);
        output.write(encodeRuns(ids, bitWidth));
        return output.toByteArray();
    }

    private static byte[] encodeRuns(List<Integer> values, int bitWidth)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int byteWidth = (bitWidth + 7) / 8;
        int position = 0;
        while (position < values.size()) {
            int value = values.get(position);
            int end = position + 1;
            while (end < values.size() && values.get(end) == value) {
                end++;
            }
            writeUnsignedLeb128(output, (end - position) << 1);
            for (int byteIndex = 0; byteIndex < byteWidth; byteIndex++) {
                output.write(value >>> (byteIndex * Byte.SIZE));
            }
            position = end;
        }
        return output.toByteArray();
    }

    private static byte[] encodePlain(Type type, List<?> values)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        if (type == Type.BOOLEAN) {
            int bits = 0;
            int bit = 0;
            for (Object value : values) {
                if ((Boolean) value) {
                    bits |= 1 << bit;
                }
                bit++;
                if (bit == Byte.SIZE) {
                    output.write(bits);
                    bits = 0;
                    bit = 0;
                }
            }
            if (bit != 0) {
                output.write(bits);
            }
            return output.toByteArray();
        }
        for (Object value : values) {
            switch (type) {
                case INT32 -> writeLittleEndianInt(output, ((Number) value).intValue());
                case FLOAT -> writeLittleEndianInt(output, Float.floatToRawIntBits(((Number) value).floatValue()));
                case INT64 -> writeLittleEndianLong(output, ((Number) value).longValue());
                case INT96 -> {
                    byte[] bytes = (byte[]) value;
                    if (bytes.length != 12) {
                        throw new IllegalArgumentException("INT96 test value must contain 12 bytes");
                    }
                    output.write(bytes);
                }
                case DOUBLE -> writeLittleEndianLong(output, Double.doubleToRawLongBits(((Number) value).doubleValue()));
                case BYTE_ARRAY -> {
                    byte[] bytes = value instanceof byte[] array ? array : value.toString().getBytes(StandardCharsets.UTF_8);
                    writeLittleEndianInt(output, bytes.length);
                    output.write(bytes);
                }
                default -> throw new IllegalArgumentException("Unsupported test physical type: " + type);
            }
        }
        return output.toByteArray();
    }

    private static Statistics statistics(Column column, long nullCount)
    {
        return statistics(column.type(), column.values(), nullCount);
    }

    private static Statistics statistics(Type type, List<?> allValues, long nullCount)
    {
        Statistics statistics = new Statistics().setNull_count(nullCount);
        List<?> values = allValues.stream().filter(value -> value != null).toList();
        if (values.isEmpty() || type == Type.BOOLEAN || type == Type.INT96) {
            return statistics;
        }
        Object minimum = values.getFirst();
        Object maximum = minimum;
        for (Object value : values.subList(1, values.size())) {
            if (compare(type, value, minimum) < 0) {
                minimum = value;
            }
            if (compare(type, value, maximum) > 0) {
                maximum = value;
            }
        }
        try {
            byte[] minimumBytes = encodePlain(type, List.of(minimum));
            byte[] maximumBytes = encodePlain(type, List.of(maximum));
            return statistics
                    .setMin_value(minimumBytes)
                    .setMax_value(maximumBytes)
                    .setIs_min_value_exact(true)
                    .setIs_max_value_exact(true);
        }
        catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    @SuppressWarnings("unchecked")
    private static int compare(Type type, Object left, Object right)
    {
        if (type == Type.BYTE_ARRAY) {
            byte[] leftBytes = left instanceof byte[] array ? array : left.toString().getBytes(StandardCharsets.UTF_8);
            byte[] rightBytes = right instanceof byte[] array ? array : right.toString().getBytes(StandardCharsets.UTF_8);
            return Arrays.compareUnsigned(leftBytes, rightBytes);
        }
        return ((Comparable<Object>) left).compareTo(right);
    }

    private static void writeUnsignedLeb128(ByteArrayOutputStream output, int value)
    {
        do {
            int next = value & 0x7F;
            value >>>= 7;
            output.write(value == 0 ? next : next | 0x80);
        }
        while (value != 0);
    }

    private static void writeLittleEndianInt(ByteArrayOutputStream output, int value)
    {
        output.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
    }

    private static void writeLittleEndianLong(ByteArrayOutputStream output, long value)
    {
        output.writeBytes(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
    }

    private record Dictionary(List<Object> values, List<Integer> ids) {}

    private record Event(int repetitionLevel, int definitionLevel, Object value) {}

    private record NestedColumn(
            List<String> path,
            Type type,
            int maximumRepetitionLevel,
            int maximumDefinitionLevel,
            List<Event> events)
    {
        private NestedColumn
        {
            path = List.copyOf(path);
            events = List.copyOf(events);
        }
    }

    private record WrittenChunk(ColumnChunk metadata, long uncompressedSize, long compressedSize) {}

    private record ValueKey(Object value)
    {
        @Override
        public boolean equals(Object object)
        {
            return object instanceof ValueKey other &&
                    (value instanceof byte[] left && other.value instanceof byte[] right
                            ? Arrays.equals(left, right)
                            : value.equals(other.value));
        }

        @Override
        public int hashCode()
        {
            return value instanceof byte[] bytes ? Arrays.hashCode(bytes) : value.hashCode();
        }
    }
}
