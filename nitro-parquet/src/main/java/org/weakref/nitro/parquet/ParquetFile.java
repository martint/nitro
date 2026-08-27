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
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.Math.addExact;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

/**
 * Metadata and bounded column-chunk access over a connector-supplied {@link ParquetInput}. Local callers may use a
 * zero-copy mapping; object-store connectors can provide the same contract over their ordinary range-I/O layer.
 */
public final class ParquetFile
        implements AutoCloseable
{
    static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);
    static final ValueLayout.OfDouble LE_DOUBLE = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(LITTLE_ENDIAN);
    // Big-endian unaligned long: short decimals (FIXED_LEN_BYTE_ARRAY) are stored big-endian, so a value's bytes
    // are the most significant bytes of a word load and an arithmetic shift recovers the sign-extended unscaled long.
    static final ValueLayout.OfLong BE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(BIG_ENDIAN);

    private static final int MAGIC = 0x31524150; // "PAR1" little-endian

    private final ParquetInput input;
    private final FileMetaData footer;
    private final ParquetSchema schema;
    private final List<Column> columns;
    private final Map<String, Integer> columnIndexByName;

    /**
     * A flat top-level column: its physical type, nullability, leaf index (matching the row group's
     * column-chunk order), the fixed-length-byte-array length ({@code 0} unless FLBA), and whether it carries
     * DECIMAL or STRING logical semantics. Physical BYTE_ARRAY alone is not sufficient to distinguish VARCHAR
     * from VARBINARY.
     */
    public record Column(
            String name,
            Type type,
            boolean optional,
            int leafIndex,
            int typeLength,
            boolean decimal,
            boolean string,
            ParquetPrimitiveDescriptor descriptor) {}

    public record PrimitiveField(
            String name,
            Type type,
            boolean optional,
            boolean string,
            boolean date,
            boolean decimal,
            int precision,
            int scale,
            boolean integer,
            int integerBitWidth,
            boolean integerSigned) {}

    record Metadata(FileMetaData footer, ParquetSchema schema, List<Column> columns, Map<String, Integer> columnIndexByName)
    {
        Metadata
        {
            footer = requireNonNull(footer, "footer is null");
            schema = requireNonNull(schema, "schema is null");
            columns = List.copyOf(columns);
            columnIndexByName = Map.copyOf(columnIndexByName);
        }
    }

    public static ParquetFile open(Path path)
    {
        return open(path, ParquetArenaPolicy.confined());
    }

    static ParquetFile open(Path path, ParquetArenaPolicy arenaPolicy)
    {
        return open(path, arenaPolicy.createArena(), true);
    }

    static ParquetFile open(Path path, ParquetArenaPolicy arenaPolicy, ParquetMetadataCache metadataCache)
    {
        return open(path, arenaPolicy.createArena(), true, requireNonNull(metadataCache, "metadataCache is null"));
    }

    static ParquetFile open(Path path, Arena arena)
    {
        return open(path, arena, false, null);
    }

    static ParquetFile open(Path path, Arena arena, ParquetMetadataCache metadataCache)
    {
        return open(path, arena, false, requireNonNull(metadataCache, "metadataCache is null"));
    }

    private static ParquetFile open(Path path, Arena arena, boolean ownsArena)
    {
        return open(path, arena, ownsArena, null);
    }

    private static ParquetFile open(Path path, Arena arena, boolean ownsArena, ParquetMetadataCache metadataCache)
    {
        try {
            BasicFileAttributes attributes = java.nio.file.Files.readAttributes(path, BasicFileAttributes.class);
            ParquetInput input = MappedParquetInput.open(path, arena, ownsArena);
            long size = input.size();
            if (size != attributes.size()) {
                input.close();
                throw new IOException("Parquet file size changed while opening: " + path);
            }
            Metadata metadata = metadataCache == null
                    ? readMetadata(input)
                    : metadataCache.get(path, size, attributes.lastModifiedTime(), () -> readMetadata(input));
            return new ParquetFile(input, metadata);
        }
        catch (IOException e) {
            if (ownsArena && arena.scope().isAlive()) {
                arena.close();
            }
            throw new UncheckedIOException("Unable to open Parquet file: " + path, e);
        }
        catch (RuntimeException e) {
            if (ownsArena && arena.scope().isAlive()) {
                arena.close();
            }
            throw e;
        }
    }

    public static ParquetFile open(ParquetInput input)
    {
        input = requireNonNull(input, "input is null");
        String inputId = requireNonNull(input.id(), "input id is null");
        try {
            return new ParquetFile(input, readMetadata(input));
        }
        catch (IOException e) {
            closeAfterFailure(input, e);
            throw new UncheckedIOException("Unable to open Parquet input: " + inputId, e);
        }
        catch (RuntimeException e) {
            closeAfterFailure(input, e);
            throw e;
        }
    }

    private static void closeAfterFailure(ParquetInput input, Throwable failure)
    {
        try {
            input.close();
        }
        catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static Metadata readMetadata(ParquetInput input)
            throws IOException
    {
        long size = input.size();
        if (size < 8) {
            throw new IOException("Not a Parquet file (bad magic)");
        }
        int footerLength;
        try (ParquetInputRange tailRange = input.readRange(size - 8, 8)) {
            MemorySegment tail = tailRange.data();
            if (tail.get(LE_INT, 4) != MAGIC) {
                throw new IOException("Not a Parquet file (bad magic)");
            }
            footerLength = tail.get(LE_INT, 0);
        }
        long footerStart = size - 8 - footerLength;
        if (footerLength < 0 || footerStart < 0) {
            throw new IOException("Invalid Parquet footer length: " + footerLength);
        }
        try (ParquetInputRange footerRange = input.readRange(footerStart, footerLength);
                InputStream in = new SegmentInputStream(footerRange.data(), 0, footerLength)) {
            FileMetaData footer = Util.readFileMetaData(in);

            ParquetSchema parquetSchema = ParquetSchema.parse(footer.schema);
            SchemaColumns flatSchema = primitiveColumns(parquetSchema, false);
            return new Metadata(footer, parquetSchema, flatSchema.columns(), flatSchema.columnIndexByName());
        }
    }

    record SchemaColumns(List<Column> columns, Map<String, Integer> columnIndexByName) {}

    /**
     * Parses the flat schema currently admitted by the native reader.
     *
     * <p>A group must never be mistaken for a physical leaf. Doing so shifts every later leaf ordinal and can route
     * the wrong column chunk into a decoder. Nested support is added by extending the native schema/level model; until
     * then the file is rejected here with the exact unsupported field instead of falling through to another reader.
     */
    static SchemaColumns parseFlatSchema(List<SchemaElement> schema)
    {
        return flatColumns(ParquetSchema.parse(schema));
    }

    private static SchemaColumns flatColumns(ParquetSchema schema)
    {
        return primitiveColumns(schema, true);
    }

    private static SchemaColumns primitiveColumns(ParquetSchema schema, boolean rejectNested)
    {
        List<Column> columns = new ArrayList<>(schema.fields().size());
        Map<String, Integer> columnIndexByName = new HashMap<>();
        for (ParquetSchema.Node field : schema.fields()) {
            if (!(field instanceof ParquetSchema.Primitive primitive)) {
                if (rejectNested) {
                    throw new UnsupportedParquetFeatureException(
                            "Native Nitro Parquet reader does not yet support nested field '" + field.name() + "'");
                }
                continue;
            }
            if (primitive.repetition() == FieldRepetitionType.REPEATED) {
                throw new UnsupportedParquetFeatureException(
                        "Native Nitro Parquet reader does not yet support repeated field '" + primitive.name() + "'");
            }
            boolean optional = primitive.repetition() == FieldRepetitionType.OPTIONAL;
            boolean decimal = primitive.decimal();
            Integer previous = columnIndexByName.put(primitive.name(), columns.size());
            if (previous != null) {
                throw new UnsupportedParquetFeatureException("Duplicate top-level Parquet field '" + primitive.name() + "'");
            }
            columns.add(new Column(
                    primitive.name(),
                    primitive.type(),
                    optional,
                    primitive.leafIndex(),
                    primitive.typeLength(),
                    decimal,
                    primitive.string(),
                    primitive.descriptor()));
        }
        return new SchemaColumns(List.copyOf(columns), Map.copyOf(columnIndexByName));
    }

    private ParquetFile(ParquetInput input, Metadata metadata)
    {
        this.input = requireNonNull(input, "input is null");
        this.footer = metadata.footer();
        this.schema = metadata.schema();
        this.columns = metadata.columns();
        this.columnIndexByName = metadata.columnIndexByName();
    }

    public long numRows()
    {
        return footer.num_rows;
    }

    ParquetSchema schema()
    {
        return schema;
    }

    public List<String> fieldNames()
    {
        return schema.fields().stream().map(ParquetSchema.Node::name).toList();
    }

    public PrimitiveField primitiveField(String name)
    {
        ParquetSchema.Node node = schema.field(name);
        if (!(node instanceof ParquetSchema.Primitive primitive)) {
            throw new UnsupportedParquetFeatureException("Parquet field '" + name + "' is nested");
        }
        boolean date = primitive.convertedType() == ConvertedType.DATE ||
                (primitive.logicalType() != null && primitive.logicalType().isSetDATE());
        boolean integer = primitive.logicalType() != null && primitive.logicalType().isSetINTEGER();
        int integerBitWidth = integer ? primitive.logicalType().getINTEGER().bitWidth : convertedIntegerBitWidth(primitive.convertedType());
        boolean integerSigned = integer ? primitive.logicalType().getINTEGER().isSigned : convertedIntegerSigned(primitive.convertedType());
        return new PrimitiveField(
                primitive.name(),
                primitive.type(),
                primitive.repetition() == FieldRepetitionType.OPTIONAL,
                primitive.string(),
                date,
                primitive.decimal(),
                primitive.precision(),
                primitive.scale(),
                integer || integerBitWidth != 0,
                integerBitWidth,
                integerSigned);
    }

    private static int convertedIntegerBitWidth(ConvertedType type)
    {
        if (type == null) {
            return 0;
        }
        return switch (type) {
            case INT_8, UINT_8 -> 8;
            case INT_16, UINT_16 -> 16;
            case INT_32, UINT_32 -> 32;
            case INT_64, UINT_64 -> 64;
            default -> 0;
        };
    }

    private static boolean convertedIntegerSigned(ConvertedType type)
    {
        return type == ConvertedType.INT_8 || type == ConvertedType.INT_16 || type == ConvertedType.INT_32 || type == ConvertedType.INT_64;
    }

    public List<RowGroup> rowGroups()
    {
        return footer.row_groups;
    }

    /**
     * Returns the row groups owned by a byte-range split.
     *
     * <p>Parquet splits own a row group when the first physical column starts within the half-open
     * split range. Dictionary data, when present before the first data page, is the physical start
     * of that column. This is the same ownership rule used by Trino's Parquet metadata reader and
     * guarantees that adjacent splits neither duplicate nor omit a row group.
     */
    public List<RowGroup> rowGroups(long splitStart, long splitLength)
    {
        if (splitStart < 0) {
            throw new IllegalArgumentException("splitStart is negative");
        }
        if (splitLength < 0) {
            throw new IllegalArgumentException("splitLength is negative");
        }
        long splitEnd = addExact(splitStart, splitLength);
        return footer.row_groups.stream()
                .filter(rowGroup -> splitContainsRowGroup(rowGroup, splitStart, splitEnd))
                .toList();
    }

    static boolean splitContainsRowGroup(RowGroup rowGroup, long splitStart, long splitEnd)
    {
        ColumnChunk firstColumn = rowGroup.columns.getFirst();
        long dataOffset = firstColumn.meta_data.data_page_offset;
        long dictionaryOffset = firstColumn.meta_data.dictionary_page_offset;
        long rowGroupStart = dictionaryOffset > 0 && dictionaryOffset < dataOffset
                ? dictionaryOffset
                : dataOffset;
        return splitStart <= rowGroupStart && rowGroupStart < splitEnd;
    }

    public Column column(String name)
    {
        return column(name, ParquetColumnNameMatching.EXACT);
    }

    public Column column(int ordinal)
    {
        ParquetSchema.Node field = schema.fields().get(ordinal);
        if (!(field instanceof ParquetSchema.Primitive primitive)) {
            throw new UnsupportedParquetFeatureException(
                    "Native Nitro Parquet primitive reader cannot decode nested field '" + field.name() + "'");
        }
        return primitiveColumn(primitive);
    }

    public Column column(String name, ParquetColumnNameMatching matching)
    {
        Integer index = columnIndexByName.get(name);
        if (index == null && matching == ParquetColumnNameMatching.CASE_INSENSITIVE) {
            String normalizedName = name.toLowerCase(Locale.ROOT);
            ParquetSchema.Node matched = null;
            for (int fieldIndex = 0; fieldIndex < schema.fields().size(); fieldIndex++) {
                ParquetSchema.Node field = schema.fields().get(fieldIndex);
                if (!field.name().toLowerCase(Locale.ROOT).equals(normalizedName)) {
                    continue;
                }
                if (matched != null) {
                    throw new IllegalArgumentException("Ambiguous case-insensitive column: " + name);
                }
                matched = field;
            }
            if (matched != null) {
                if (!(matched instanceof ParquetSchema.Primitive primitive)) {
                    throw new UnsupportedParquetFeatureException(
                            "Native Nitro Parquet primitive reader cannot decode nested field '" + matched.name() + "'");
                }
                return primitiveColumn(primitive);
            }
        }
        if (index == null) {
            ParquetSchema.Node field;
            try {
                field = schema.field(name);
            }
            catch (IllegalArgumentException _) {
                throw new IllegalArgumentException("No such column: " + name + " (have " + schema.fields().stream().map(ParquetSchema.Node::name).toList() + ")");
            }
            if (!(field instanceof ParquetSchema.Primitive primitive)) {
                throw new UnsupportedParquetFeatureException(
                        "Native Nitro Parquet primitive reader cannot decode nested field '" + field.name() + "'");
            }
            return primitiveColumn(primitive);
        }
        return columns.get(index);
    }

    private static Column primitiveColumn(ParquetSchema.Primitive primitive)
    {
        if (primitive.repetition() == FieldRepetitionType.REPEATED) {
            throw new UnsupportedParquetFeatureException(
                    "Native Nitro Parquet primitive reader does not support repeated field '" + primitive.name() + "'");
        }
        return new Column(
                primitive.name(),
                primitive.type(),
                primitive.repetition() == FieldRepetitionType.OPTIONAL,
                primitive.leafIndex(),
                primitive.typeLength(),
                primitive.decimal(),
                primitive.string(),
                primitive.descriptor());
    }

    /** The column chunk for {@code column} within {@code rowGroup}. */
    public ColumnChunk columnChunk(RowGroup rowGroup, Column column)
    {
        return rowGroup.columns.get(column.leafIndex());
    }

    ColumnChunk columnChunk(RowGroup rowGroup, ParquetSchema.Primitive leaf)
    {
        return rowGroup.columns.get(leaf.leafIndex());
    }

    public ParquetInputRange readRange(long offset, long length)
    {
        if (offset < 0) {
            throw new IllegalArgumentException("Parquet range offset is negative: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("Parquet range length is negative: " + length);
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Parquet range is too large: " + length);
        }
        long size = input.size();
        if (offset > size || length > size - offset) {
            throw new IllegalArgumentException("Parquet range is outside the input: offset=" + offset + ", length=" + length + ", size=" + size);
        }
        try {
            return input.readRange(offset, (int) length);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to read Parquet range from " + input.id(), e);
        }
    }

    public long size()
    {
        return input.size();
    }

    @Override
    public void close()
    {
        String inputId = input.id();
        try {
            input.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to close Parquet input: " + inputId, e);
        }
    }

    /** A minimal {@link InputStream} over a region of a {@link MemorySegment}, for the Thrift footer/page-header reads. */
    static final class SegmentInputStream
            extends InputStream
    {
        private final MemorySegment segment;
        private long position;
        private final long end;

        SegmentInputStream(MemorySegment segment, long offset, long length)
        {
            this.segment = segment;
            this.position = offset;
            this.end = offset + length;
        }

        long position()
        {
            return position;
        }

        @Override
        public int read()
        {
            if (position >= end) {
                return -1;
            }
            return segment.get(JAVA_BYTE, position++) & 0xFF;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
        {
            if (position >= end) {
                return -1;
            }
            int count = (int) Math.min(length, end - position);
            MemorySegment.copy(segment, JAVA_BYTE, position, buffer, offset, count);
            position += count;
            return count;
        }
    }
}
