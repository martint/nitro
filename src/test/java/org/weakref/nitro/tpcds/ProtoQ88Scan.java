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
 * Prototype (dormant unless -Dnitro.protoSkip=true): the q88 dynamic-filter SCAN pattern on real store_sales,
 * end-to-end through the Trino reader. Decodes the filter key ss_store_sk fully, builds a survivor mask for one
 * store (~1%), then SKIP-decodes the other join keys (ss_sold_time_sk, ss_hdemo_sk) for survivors only via
 * SkipFlatColumnReader + a runtime FilteredRowRanges. Compares total scan: full-decode-3-cols vs
 * decode-key + skip-decode-2-cols. Run with -Dlicense.skip=true.
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
import io.trino.parquet.reader.ColumnChunk;
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

public class ProtoQ88Scan
{
    private static final String[] COLUMNS = {"ss_store_sk", "ss_sold_time_sk", "ss_hdemo_sk"};

    @Test
    public void proto()
            throws Exception
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.protoSkip"), "set -Dnitro.protoSkip=true");
        Path file = firstFile("/root/data/tpcds-parquet-sf10/sf10/store_sales");
        ParquetReaderOptions options = ParquetReaderOptions.builder().build();
        FileDataSource ds = new FileDataSource(file.toFile(), options);
        ParquetMetadata md = MetadataReader.readFooter(ds, Optional.empty());
        MessageType schema = md.getFileMetaData().getSchema();
        BlockMetadata block = md.getBlocks().get(0);
        long rowCount = block.rowCount();
        System.out.printf("store_sales row group 0: %,d rows%n", rowCount);

        // filter key full decode -> survivor mask for one store (~1%)
        long[] storeKey = new long[(int) rowCount];
        long tKey = time(() -> decode(ds, options, schema, block, "ss_store_sk", rowCount, Optional.empty(), storeKey));
        long pick = storeKey[0];
        long[] survivors = new long[(int) rowCount];
        int m = 0;
        for (int i = 0; i < rowCount; i++) {
            if (storeKey[i] == pick) {
                survivors[m++] = i;
            }
        }
        double sel = 100.0 * m / rowCount;
        System.out.printf("filter ss_store_sk == %d : %,d survivors (%.1f%%)%n", pick, m, sel);
        FilteredRowRangesHolder ranges = new FilteredRowRangesHolder(survivors, m);

        // FULL: decode all three key columns fully
        long full = tKey
                + time(() -> decode(ds, options, schema, block, "ss_sold_time_sk", rowCount, Optional.empty(), null))
                + time(() -> decode(ds, options, schema, block, "ss_hdemo_sk", rowCount, Optional.empty(), null));

        // SKIP: key full (tKey) + skip-decode the other two for survivors only
        long skip = tKey
                + time(() -> decode(ds, options, schema, block, "ss_sold_time_sk", rowCount, Optional.of(ranges.build()), null))
                + time(() -> decode(ds, options, schema, block, "ss_hdemo_sk", rowCount, Optional.of(ranges.build()), null));

        System.out.printf("%nFULL scan (decode 3 keys, all rows):        %6.1f ms%n", full / 1e6);
        System.out.printf("SKIP scan (decode key + skip 2 for %.1f%%):  %6.1f ms   speedup = %.2fx%n",
                sel, skip / 1e6, (double) full / skip);
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

    private static void decode(FileDataSource template, ParquetReaderOptions options, MessageType schema,
            BlockMetadata block, String column, long rowCount, Optional<org.apache.parquet.internal.filter2.columnindex.RowRanges> ranges, long[] collect)
    {
        try {
            FileDataSource ds = new FileDataSource(template.file, options);
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
            PageReader pageReader = PageReader.createPageReader(ds.getId(), chunks.get(0), chunkMeta, descriptor,
                    null, Optional.empty(), Optional.empty(), 8 * 1024 * 1024);
            SkipFlatColumnReader<long[]> reader = SkipFlatColumnReader.createForLong(field, options.isVectorizedDecodingEnabled(),
                    AggregatedMemoryContext.newSimpleAggregatedMemoryContext().newLocalMemoryContext("proto"));
            Optional<io.trino.parquet.reader.FilteredRowRanges> filtered =
                    ranges.map(io.trino.parquet.reader.FilteredRowRanges::new);
            reader.setPageReader(pageReader, filtered);
            long total = filtered.isPresent() ? filtered.orElseThrow().getRowCount() : rowCount;
            long remaining = total;
            int out = 0;
            while (remaining > 0) {
                int batch = (int) Math.min(8192, remaining);
                reader.prepareNextRead(batch);
                ColumnChunk chunk = reader.readPrimitive();
                Block b = chunk.getBlock();
                for (int i = 0; i < b.getPositionCount(); i++) {
                    long v = b.isNull(i) ? 0 : BIGINT.getLong(b, i);
                    if (collect != null) {
                        collect[out++] = v;
                    }
                }
                remaining -= batch;
            }
            ds.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private record FilteredRowRangesHolder(long[] survivors, int count)
    {
        org.apache.parquet.internal.filter2.columnindex.RowRanges build()
        {
            return RowRangesShim.fromSortedPositions(survivors, count);
        }
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
