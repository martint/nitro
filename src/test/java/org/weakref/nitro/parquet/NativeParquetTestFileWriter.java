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

    public record Column(String name, Type type, boolean optional, boolean utf8, List<?> values)
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
            return new Column(name, type, false, false, values);
        }

        public static Column optional(String name, Type type, List<?> values)
        {
            return new Column(name, type, true, false, values);
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
            return new Column(name, type, optional, true, values);
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
        Map<ValueKey, Integer> idsByValue = new LinkedHashMap<>();
        List<Object> values = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        for (Object value : column.values()) {
            if (value == null) {
                continue;
            }
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
                case INT64 -> writeLittleEndianLong(output, ((Number) value).longValue());
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
        Statistics statistics = new Statistics().setNull_count(nullCount);
        List<?> values = column.values().stream().filter(value -> value != null).toList();
        if (values.isEmpty() || column.type() == Type.BOOLEAN) {
            return statistics;
        }
        Object minimum = values.getFirst();
        Object maximum = minimum;
        for (Object value : values.subList(1, values.size())) {
            if (compare(column.type(), value, minimum) < 0) {
                minimum = value;
            }
            if (compare(column.type(), value, maximum) > 0) {
                maximum = value;
            }
        }
        try {
            byte[] minimumBytes = encodePlain(column.type(), List.of(minimum));
            byte[] maximumBytes = encodePlain(column.type(), List.of(maximum));
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
