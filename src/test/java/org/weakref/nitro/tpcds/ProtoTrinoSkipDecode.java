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
/*
 * Prototype (dormant unless -Dnitro.protoSkip=true): drive a single Trino FlatColumnReader over a real lineitem
 * INT64 column, comparing FULL decode vs SKIP-decode where a runtime scattered survivor mask is injected as
 * FilteredRowRanges via the public setPageReader API (plus the RowRangesShim). Confirms Trino's existing read loop
 * skip-decodes a value-level mask on uniform data, and measures the speedup vs selectivity end-to-end.
 */
package org.weakref.nitro.tpcds;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.ColumnChunk;
import io.trino.parquet.reader.ColumnReader;
import io.trino.parquet.reader.FilteredRowRanges;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.PageReader;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.internal.filter2.columnindex.RowRangesShim;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.spi.type.BigintType.BIGINT;

public class ProtoTrinoSkipDecode
{
    @Test
    public void proto()
            throws Exception
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.protoSkip"), "set -Dnitro.protoSkip=true");
        // Trino's row-range skip path needs an OffsetIndex (per-page firstRowIndex); the TPC-H/DS dump lacks page
        // indexes, so write a small indexed file (Trino's writer emits column+offset indexes by default).
        Path file = ensureIndexedFile(8_000_000);
        ParquetReaderOptions options = ParquetReaderOptions.builder().build();
        FileDataSource ds = new FileDataSource(file.toFile(), options);
        ParquetMetadata md = MetadataReader.readFooter(ds, Optional.empty());
        MessageType schema = md.getFileMetaData().getSchema();
        String column = "v";
        System.out.println("column = " + column);

        MessageType requested = new MessageType(schema.getName(), schema.getType(column));
        Map<List<String>, ColumnDescriptor> descriptors = getDescriptors(schema, requested);
        ColumnDescriptor descriptor = descriptors.values().iterator().next();
        MessageColumnIO columnIO = getColumnIO(schema, requested);
        ColumnIO leaf = lookupColumnByName(columnIO, column);
        PrimitiveField field = (PrimitiveField) constructField((Type) BIGINT, leaf).orElseThrow();

        BlockMetadata block = md.getBlocks().get(0);
        long rowCount = block.rowCount();
        PrunedBlockMetadata pruned = PrunedBlockMetadata.createPrunedColumnsMetadata(block, ds.getId(), descriptors);
        ColumnChunkMetadata chunkMeta = pruned.getColumnChunkMetaData(descriptor);
        System.out.printf("row group 0: %,d rows%n", rowCount);

        io.trino.spi.predicate.TupleDomain<ColumnDescriptor> domain = io.trino.spi.predicate.TupleDomain.withColumnDomains(
                com.google.common.collect.ImmutableMap.of(descriptor, io.trino.spi.predicate.Domain.notNull(BIGINT)));
        Optional<org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore> store =
                io.trino.parquet.reader.TrinoColumnIndexStore.getColumnIndexStore(ds, block, descriptors, domain, options, Optional.empty());
        org.apache.parquet.hadoop.metadata.ColumnPath columnPath = org.apache.parquet.hadoop.metadata.ColumnPath.get(descriptor.getPath());
        org.apache.parquet.internal.column.columnindex.OffsetIndex offsetIndex =
                store.isPresent() ? store.orElseThrow().getOffsetIndex(columnPath) : null;
        System.out.println("store present = " + store.isPresent()
                + ", offsetIndexRef = " + (chunkMeta.getOffsetIndexReference() != null)
                + ", offsetIndex = " + (offsetIndex != null));

        // full decode baseline
        long tFull = bench(() -> readColumn(ds, options, field, descriptor, chunkMeta, rowCount, offsetIndex, Optional.empty()));
        System.out.printf("%nFULL decode (all %,d rows):            %6.1f ms/op%n", rowCount, tFull / 1e6);

        for (double sel : new double[] {0.25, 0.05, 0.01}) {
            long[] survivors = scattered(rowCount, sel);
            FilteredRowRanges ranges = new FilteredRowRanges(RowRangesShim.fromSortedPositions(survivors, survivors.length));
            long t = bench(() -> readColumn(ds, options, field, descriptor, chunkMeta, rowCount, offsetIndex, Optional.of(ranges)));
            System.out.printf("SKIP decode sel=%5.0f%% (%,9d rows):  %6.2f ms/op   speedup = %.2fx%n",
                    sel * 100, survivors.length, t / 1e6, (double) tFull / t);
        }
    }

    private static long readColumn(FileDataSource dsTemplate, ParquetReaderOptions options, PrimitiveField field,
            ColumnDescriptor descriptor, ColumnChunkMetadata chunkMeta, long rowCount,
            org.apache.parquet.internal.column.columnindex.OffsetIndex offsetIndex, Optional<FilteredRowRanges> ranges)
    {
        try {
            FileDataSource ds = new FileDataSource(dsTemplate.file, options);
            ListMultimap<Integer, DiskRange> diskRanges = ArrayListMultimap.create();
            diskRanges.put(0, new DiskRange(chunkMeta.getStartingPos(), chunkMeta.getTotalSize()));
            Map<Integer, ChunkedInputStream> chunks = ds.planRead(diskRanges, AggregatedMemoryContext.newSimpleAggregatedMemoryContext());
            PageReader pageReader = PageReader.createPageReader(ds.getId(), chunks.get(0), chunkMeta, descriptor,
                    offsetIndex, Optional.empty(), Optional.empty(), 8 * 1024 * 1024);
            ColumnReader reader = io.trino.parquet.reader.flat.SkipFlatColumnReader.createForLong(
                    field, options.isVectorizedDecodingEnabled(),
                    AggregatedMemoryContext.newSimpleAggregatedMemoryContext().newLocalMemoryContext("proto"));
            reader.setPageReader(pageReader, ranges);
            long checksum = 0;
            long remaining = ranges.isPresent() ? ranges.orElseThrow().getRowCount() : rowCount;
            while (remaining > 0) {
                int batch = (int) Math.min(8192, remaining);
                reader.prepareNextRead(batch);
                ColumnChunk chunk = reader.readPrimitive();
                Block b = chunk.getBlock();
                for (int i = 0; i < b.getPositionCount(); i++) {
                    if (!b.isNull(i)) {
                        checksum += BIGINT.getLong(b, i);
                    }
                }
                remaining -= batch;
            }
            ds.close();
            return checksum;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static long[] scattered(long rowCount, double sel)
    {
        int m = (int) (rowCount * sel);
        long[] s = new long[m];
        double step = (double) rowCount / m;
        java.util.Random r = new java.util.Random(7);
        long prev = -1;
        for (int i = 0; i < m; i++) {
            long p = Math.min(rowCount - 1, (long) (i * step + r.nextInt(Math.max(1, (int) step))));
            if (p <= prev) {
                p = prev + 1;
            }
            s[i] = p;
            prev = p;
        }
        return s;
    }

    private static long bench(LongSupplierThrows op)
    {
        for (int i = 0; i < 3; i++) {
            op.get();
        }
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 6; i++) {
            long s = System.nanoTime();
            op.get();
            best = Math.min(best, System.nanoTime() - s);
        }
        return best;
    }

    private interface LongSupplierThrows
    {
        long get();
    }

    private static Path ensureIndexedFile(int rows)
            throws IOException
    {
        Path path = Path.of("/root/data/proto-indexed.parquet");
        if (Files.exists(path)) {
            return path;
        }
        org.apache.parquet.schema.MessageType messageType = org.apache.parquet.schema.Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("v")
                .named("proto");
        Map<List<String>, io.trino.spi.type.Type> primitiveTypes = com.google.common.collect.ImmutableMap.of(List.of("v"), (io.trino.spi.type.Type) BIGINT);
        try (java.io.OutputStream out = Files.newOutputStream(path)) {
            io.trino.parquet.writer.ParquetWriter writer = new io.trino.parquet.writer.ParquetWriter(
                    out, messageType, primitiveTypes,
                    io.trino.parquet.writer.ParquetWriterOptions.builder().build(),
                    org.apache.parquet.format.CompressionCodec.SNAPPY, "proto",
                    Optional.of(org.joda.time.DateTimeZone.UTC), Optional.empty());
            java.util.Random r = new java.util.Random(1);
            int written = 0;
            while (written < rows) {
                int n = Math.min(100_000, rows - written);
                long[] values = new long[n];
                for (int i = 0; i < n; i++) {
                    values[i] = r.nextInt(4096);   // low-card => dictionary/bit-packed, like a real dimension key
                }
                writer.write(new io.trino.spi.Page(n, new io.trino.spi.block.LongArrayBlock(n, Optional.empty(), values)));
                written += n;
            }
            writer.close();
        }
        return path;
    }

    private static final class FileDataSource
            extends io.trino.parquet.AbstractParquetDataSource
    {
        private final File file;
        private final RandomAccessFile input;

        private FileDataSource(File file, ParquetReaderOptions options)
                throws java.io.FileNotFoundException
        {
            super(new io.trino.parquet.ParquetDataSourceId(file.toString()), file.length(), options);
            this.file = file;
            this.input = new RandomAccessFile(file, "r");
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }

        @Override
        public void close()
                throws IOException
        {
            input.close();
        }
    }
}
