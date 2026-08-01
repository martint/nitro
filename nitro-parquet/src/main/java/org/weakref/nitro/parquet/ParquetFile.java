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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
 * A memory-mapped Parquet file. The whole file is mapped into a single off-heap {@link MemorySegment}
 * (zero I/O copy); the footer is parsed once via the parquet-format Thrift reader. Column-chunk page
 * bytes are then read as zero-copy slices of the mapping. Designed as the foundation of a Nitro-native
 * decoder: no per-page heap I/O buffers, no {@code Slice} allocation machinery.
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

    private final Arena arena;
    private final boolean ownsArena;
    private final MemorySegment data;
    private final FileMetaData footer;
    private final List<Column> columns;
    private final Map<String, Integer> columnIndexByName;

    /**
     * A flat top-level column: its physical type, nullability, leaf index (matching the row group's
     * column-chunk order), the fixed-length-byte-array length ({@code 0} unless FLBA), and whether it carries
     * a DECIMAL logical type (short decimals decode to an unscaled long).
     */
    public record Column(String name, Type type, boolean optional, int leafIndex, int typeLength, boolean decimal) {}

    record Metadata(FileMetaData footer, List<Column> columns, Map<String, Integer> columnIndexByName)
    {
        Metadata
        {
            footer = requireNonNull(footer, "footer is null");
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
            // The caller selects confined or shared lifetime according to the execution boundary.
            MemorySegment data;
            long size;
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                size = channel.size();
                data = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            }
            if (size != attributes.size()) {
                throw new IOException("Parquet file size changed while opening: " + path);
            }
            Metadata metadata = metadataCache == null
                    ? readMetadata(data, size)
                    : metadataCache.get(path, size, attributes.lastModifiedTime(), () -> readMetadata(data, size));
            return new ParquetFile(arena, ownsArena, data, size, metadata);
        }
        catch (IOException e) {
            if (ownsArena) {
                arena.close();
            }
            throw new UncheckedIOException("Unable to open Parquet file: " + path, e);
        }
    }

    private static Metadata readMetadata(MemorySegment data, long size)
            throws IOException
    {
        if (size < 8 || data.get(LE_INT, size - 4) != MAGIC) {
            throw new IOException("Not a Parquet file (bad magic)");
        }
        int footerLength = data.get(LE_INT, size - 8);
        long footerStart = size - 8 - footerLength;
        try (InputStream in = new SegmentInputStream(data, footerStart, footerLength)) {
            FileMetaData footer = Util.readFileMetaData(in);

            // Flat schema: schema[0] is the root; schema[1..] are leaf columns in column-chunk order.
            List<SchemaElement> schema = footer.schema;
            List<Column> columns = new ArrayList<>();
            Map<String, Integer> columnIndexByName = new HashMap<>();
            for (int i = 1; i < schema.size(); i++) {
                SchemaElement element = schema.get(i);
                boolean optional = element.repetition_type == FieldRepetitionType.OPTIONAL;
                int leafIndex = i - 1;
                boolean decimal = element.converted_type == ConvertedType.DECIMAL
                        || (element.logicalType != null && element.logicalType.isSetDECIMAL());
                columnIndexByName.put(element.name, columns.size());
                columns.add(new Column(element.name, element.type, optional, leafIndex, element.type_length, decimal));
            }
            return new Metadata(footer, columns, columnIndexByName);
        }
    }

    private ParquetFile(Arena arena, boolean ownsArena, MemorySegment data, long size, Metadata metadata)
            throws IOException
    {
        this.arena = arena;
        this.ownsArena = ownsArena;
        this.data = data;
        if (size < 8 || data.get(LE_INT, size - 4) != MAGIC) {
            throw new IOException("Not a Parquet file (bad magic)");
        }
        this.footer = metadata.footer();
        this.columns = metadata.columns();
        this.columnIndexByName = metadata.columnIndexByName();
    }

    public long numRows()
    {
        return footer.num_rows;
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

    public Column column(String name, ParquetColumnNameMatching matching)
    {
        Integer index = columnIndexByName.get(name);
        if (index == null && matching == ParquetColumnNameMatching.CASE_INSENSITIVE) {
            String normalizedName = name.toLowerCase(Locale.ROOT);
            for (Map.Entry<String, Integer> entry : columnIndexByName.entrySet()) {
                if (!entry.getKey().toLowerCase(Locale.ROOT).equals(normalizedName)) {
                    continue;
                }
                if (index != null) {
                    throw new IllegalArgumentException("Ambiguous case-insensitive column: " + name);
                }
                index = entry.getValue();
            }
        }
        if (index == null) {
            throw new IllegalArgumentException("No such column: " + name + " (have " + columnIndexByName.keySet() + ")");
        }
        return columns.get(index);
    }

    /** The column chunk for {@code column} within {@code rowGroup}. */
    public ColumnChunk columnChunk(RowGroup rowGroup, Column column)
    {
        return rowGroup.columns.get(column.leafIndex());
    }

    public MemorySegment data()
    {
        return data;
    }

    @Override
    public void close()
    {
        if (ownsArena) {
            arena.close();
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
