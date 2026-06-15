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
 * Prototype (dormant unless -Dnitro.protoSkip=true; run with -Dlicense.skip=true): validates and benchmarks the
 * overhead-free selection-driven skip-decode path SkipFlatColumnReader.readSelected against (a) full decode + gather
 * for byte-exact correctness, and (b) the older FilteredRowRanges/RowRangesIterator skip path, on a REAL nullable
 * store_sales column. Confirms readSelected removes the per-range overhead so it no longer loses to bulk decode at
 * moderate selectivity.
 */
package org.weakref.nitro.tpcds;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.FilteredRowRanges;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.flat.SkipFlatColumnReader;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.internal.filter2.columnindex.RowRangesShim;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
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

public class ProtoSkipReader
{
    private static final int BATCH = 8192;

    @Test
    public void proto()
            throws Exception
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.protoSkip"), "set -Dnitro.protoSkip=true");
        Path file = firstFile("/root/data/tpcds-parquet-sf10/sf10/store_sales");
        // ss_sold_time_sk is nullable (null path); ss_item_sk is non-null (the readSelectedNonNull fast path).
        String column = System.getProperty("nitro.proto.column", "ss_sold_time_sk");
        ParquetReaderOptions options = ParquetReaderOptions.builder().build();
        FileDataSource ds = new FileDataSource(file.toFile(), options);
        ParquetMetadata md = MetadataReader.readFooter(ds, Optional.empty());
        MessageType schema = md.getFileMetaData().getSchema();
        BlockMetadata block = md.getBlocks().get(0);
        int rowCount = (int) block.rowCount();
        System.out.printf("store_sales row group 0: %,d rows; column = %s%n", rowCount, column);

        // Ground truth: full decode of all rows.
        long[] fullValues = new long[rowCount];
        boolean[] fullNulls = new boolean[rowCount];
        readFull(ds, options, schema, block, column, rowCount, fullValues, fullNulls);

        for (double sel : new double[] {1.0, 0.50, 0.25, 0.05, 0.01}) {
            int[] survivors = scattered(rowCount, sel);
            // Correctness: selection-skip values must equal the ground truth at survivor positions.
            long[] gotValues = new long[survivors.length];
            boolean[] gotNulls = new boolean[survivors.length];
            readSelected(ds, options, schema, block, column, rowCount, survivors, gotValues, gotNulls);
            int mismatches = 0;
            for (int j = 0; j < survivors.length; j++) {
                int p = survivors[j];
                if (gotNulls[j] != fullNulls[p] || (!gotNulls[j] && gotValues[j] != fullValues[p])) {
                    mismatches++;
                }
            }
            // Timing.
            long tFull = time(() -> readFull(ds, options, schema, block, column, rowCount, null, null));
            long tSel = time(() -> readSelected(ds, options, schema, block, column, rowCount, survivors, null, null));
            long tRanges = time(() -> readRowRanges(ds, options, schema, block, column, rowCount, survivors));
            System.out.printf(
                    "sel=%5.1f%% (%,9d) | mismatches=%d | full %5.1fms  selSkip %5.1fms (%.2fx)  rowRanges %5.1fms (%.2fx)%n",
                    sel * 100, survivors.length, mismatches,
                    tFull / 1e6, tSel / 1e6, (double) tFull / tSel, tRanges / 1e6, (double) tFull / tRanges);
        }
        ds.close();
    }

    /** Full decode in 8192-row batches via readPrimitive; optionally collect values/nulls. */
    private static void readFull(FileDataSource template, ParquetReaderOptions options, MessageType schema,
            BlockMetadata block, String column, int rowCount, long[] values, boolean[] nulls)
    {
        try (Reader reader = new Reader(template, options, schema, block, column)) {
            reader.reader.setPageReader(reader.pageReader, Optional.empty());
            int out = 0;
            int remaining = rowCount;
            while (remaining > 0) {
                int batch = Math.min(BATCH, remaining);
                reader.reader.prepareNextRead(batch);
                Block b = reader.reader.readPrimitive().getBlock();
                for (int i = 0; i < b.getPositionCount(); i++) {
                    boolean isNull = b.isNull(i);
                    if (values != null) {
                        nulls[out] = isNull;
                        values[out] = isNull ? 0 : BIGINT.getLong(b, i);
                    }
                    out++;
                }
                remaining -= batch;
            }
        }
    }

    /** Selection-driven skip-decode in 8192-row batches via readSelected; optionally collect gathered values/nulls. */
    private static void readSelected(FileDataSource template, ParquetReaderOptions options, MessageType schema,
            BlockMetadata block, String column, int rowCount, int[] survivors, long[] values, boolean[] nulls)
    {
        try (Reader reader = new Reader(template, options, schema, block, column)) {
            reader.reader.setPageReader(reader.pageReader, Optional.empty());
            int out = 0;
            int survivorIndex = 0;
            int[] batchSelection = new int[BATCH];
            for (int base = 0; base < rowCount; base += BATCH) {
                int batch = Math.min(BATCH, rowCount - base);
                int batchEnd = base + batch;
                int k = 0;
                while (survivorIndex < survivors.length && survivors[survivorIndex] < batchEnd) {
                    batchSelection[k++] = survivors[survivorIndex++] - base;
                }
                reader.reader.prepareNextRead(batch);
                if (k == 0) {
                    // Nothing selected in this batch: still must consume it. Use an empty selection.
                    reader.reader.readSelected(batchSelection, 0);
                    continue;
                }
                Block b = reader.reader.readSelected(batchSelection, k).getBlock();
                for (int i = 0; i < b.getPositionCount(); i++) {
                    boolean isNull = b.isNull(i);
                    if (values != null) {
                        nulls[out] = isNull;
                        values[out] = isNull ? 0 : BIGINT.getLong(b, i);
                    }
                    out++;
                }
            }
        }
    }

    /** The older FilteredRowRanges/RowRangesIterator skip path (for overhead comparison). */
    private static void readRowRanges(FileDataSource template, ParquetReaderOptions options, MessageType schema,
            BlockMetadata block, String column, int rowCount, int[] survivors)
    {
        try (Reader reader = new Reader(template, options, schema, block, column)) {
            long[] positions = new long[survivors.length];
            for (int i = 0; i < survivors.length; i++) {
                positions[i] = survivors[i];
            }
            FilteredRowRanges ranges = new FilteredRowRanges(RowRangesShim.fromSortedPositions(positions, positions.length));
            reader.reader.setPageReader(reader.pageReader, Optional.of(ranges));
            long remaining = ranges.getRowCount();
            while (remaining > 0) {
                int batch = (int) Math.min(BATCH, remaining);
                reader.reader.prepareNextRead(batch);
                reader.reader.readPrimitive();
                remaining -= batch;
            }
        }
    }

    /** Holds a fresh data source + page reader + SkipFlatColumnReader for one column. */
    private static final class Reader
            implements AutoCloseable
    {
        private final FileDataSource ds;
        private final PageReader pageReader;
        private final SkipFlatColumnReader<long[]> reader;

        private Reader(FileDataSource template, ParquetReaderOptions options, MessageType schema,
                BlockMetadata block, String column)
        {
            try {
                this.ds = new FileDataSource(template.file, options);
                MessageType requested = new MessageType(schema.getName(), schema.getType(column));
                Map<List<String>, ColumnDescriptor> descriptors = getDescriptors(schema, requested);
                ColumnDescriptor descriptor = descriptors.values().iterator().next();
                MessageColumnIO columnIO = getColumnIO(schema, requested);
                PrimitiveField field = (PrimitiveField) constructField((Type) BIGINT, lookupColumnByName(columnIO, column)).orElseThrow();
                PrunedBlockMetadata pruned = PrunedBlockMetadata.createPrunedColumnsMetadata(block, ds.getId(), descriptors);
                ColumnChunkMetadata chunkMeta = pruned.getColumnChunkMetaData(descriptor);
                ListMultimap<Integer, DiskRange> diskRanges = ArrayListMultimap.create();
                diskRanges.put(0, new DiskRange(chunkMeta.getStartingPos(), chunkMeta.getTotalSize()));
                Map<Integer, ChunkedInputStream> chunks = ds.planRead(diskRanges, AggregatedMemoryContext.newSimpleAggregatedMemoryContext());
                this.pageReader = PageReader.createPageReader(ds.getId(), chunks.get(0), chunkMeta, descriptor,
                        null, Optional.empty(), Optional.empty(), 8 * 1024 * 1024);
                this.reader = SkipFlatColumnReader.createForLong(field, options.isVectorizedDecodingEnabled(),
                        AggregatedMemoryContext.newSimpleAggregatedMemoryContext().newLocalMemoryContext("proto"));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close()
        {
            try {
                ds.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static long time(Runnable op)
    {
        for (int i = 0; i < 2; i++) {
            op.run();
        }
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            long s = System.nanoTime();
            op.run();
            best = Math.min(best, System.nanoTime() - s);
        }
        return best;
    }

    private static int[] scattered(int rowCount, double sel)
    {
        int m = (int) (rowCount * sel);
        if (m >= rowCount) {
            int[] all = new int[rowCount];
            for (int i = 0; i < rowCount; i++) {
                all[i] = i;
            }
            return all;
        }
        int[] s = new int[m];
        double step = (double) rowCount / m;
        java.util.Random r = new java.util.Random(7);
        int prev = -1;
        for (int i = 0; i < m; i++) {
            int p = (int) Math.min(rowCount - 1, (long) (i * step + r.nextInt(Math.max(1, (int) step))));
            if (p <= prev) {
                p = prev + 1;
            }
            s[i] = p;
            prev = p;
        }
        return s;
    }

    private static Path firstFile(String dir)
            throws IOException
    {
        try (var s = Files.list(Path.of(dir))) {
            return s.filter(Files::isRegularFile)
                    .filter(f -> !f.getFileName().toString().startsWith("."))
                    .sorted()
                    .findFirst()
                    .orElseThrow();
        }
    }

    private static final class FileDataSource
            extends AbstractParquetDataSource
    {
        private final File file;
        private final RandomAccessFile input;

        private FileDataSource(File file, ParquetReaderOptions options)
                throws java.io.FileNotFoundException
        {
            super(new ParquetDataSourceId(file.toString()), file.length(), options);
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
