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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.addExact;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

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

    public static ParquetFile open(Path path)
    {
        return open(path, ParquetArenaPolicy.confined());
    }

    static ParquetFile open(Path path, ParquetArenaPolicy arenaPolicy)
    {
        try {
            // Confined to the opening thread: single-threaded scan, and confined sessions skip the atomic
            // liveness checks a shared session pays on every native (snappy) downcall.
            Arena arena = arenaPolicy.createArena();
            MemorySegment data;
            long size;
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                size = channel.size();
                data = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            }
            return new ParquetFile(arena, data, size);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to open Parquet file: " + path, e);
        }
    }

    private ParquetFile(Arena arena, MemorySegment data, long size)
            throws IOException
    {
        this.arena = arena;
        this.data = data;
        if (size < 8 || data.get(LE_INT, size - 4) != MAGIC) {
            throw new IOException("Not a Parquet file (bad magic)");
        }
        int footerLength = data.get(LE_INT, size - 8);
        long footerStart = size - 8 - footerLength;
        try (InputStream in = new SegmentInputStream(data, footerStart, footerLength)) {
            this.footer = Util.readFileMetaData(in);
        }

        // Flat schema: schema[0] is the root; schema[1..] are leaf columns in column-chunk order.
        List<SchemaElement> schema = footer.schema;
        this.columns = new ArrayList<>();
        this.columnIndexByName = new HashMap<>();
        for (int i = 1; i < schema.size(); i++) {
            SchemaElement element = schema.get(i);
            boolean optional = element.repetition_type == FieldRepetitionType.OPTIONAL;
            int leafIndex = i - 1;
            boolean decimal = element.converted_type == ConvertedType.DECIMAL
                    || (element.logicalType != null && element.logicalType.isSetDECIMAL());
            columnIndexByName.put(element.name, columns.size());
            columns.add(new Column(element.name, element.type, optional, leafIndex, element.type_length, decimal));
        }
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
        Integer index = columnIndexByName.get(name);
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
        arena.close();
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
