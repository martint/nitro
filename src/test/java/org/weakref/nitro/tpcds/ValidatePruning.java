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
 * Validation tool (dormant unless -Dnitro.validatePruning=true): measures Parquet row-group pruning potential for
 * candidate filter columns. For each column it reports row-group min/max clustering (mean per-group span / global
 * span; <<1 = clustered = prunable) and, for a concrete filter range, the fraction of row groups and rows a
 * statistics-based predicate pushdown could SKIP without decoding. This decides whether Stage-1 predicate pushdown
 * is worth building before writing it.
 */
package org.weakref.nitro.tpcds;

import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import org.apache.parquet.column.statistics.Statistics;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class ValidatePruning
{
    private record Group(long min, long max, long rows) {}

    @Test
    public void validate()
            throws Exception
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.validatePruning"), "set -Dnitro.validatePruning=true");
        String tpcds = System.getProperty("nitro.tpcds.dir", "/root/data/tpcds-parquet-sf10/sf10");
        String tpch = System.getProperty("nitro.tpch.dir", "/root/data/tpch-parquet-sf10/sf10");
        String cb = System.getProperty("nitro.clickbench.hits.path", "/tmp/cb_subset");

        // column, concrete filter [lo,hi] (inclusive) modeling a real query predicate / dynamic-filter target
        report(tpch + "/lineitem", "l_shipdate", 8766, 9130);          // q6: 1994 (days since epoch)
        report(tpcds + "/store_sales", "ss_sold_date_sk", 2451545, 2451910);  // ~1 year of d_date_sk
        report(tpcds + "/store_sales", "ss_store_sk", 1, 2);           // q88 'ese' store (very selective)
        report(tpcds + "/catalog_sales", "cs_sold_date_sk", 2451545, 2451910);
        report(cb, "EventDate", 0, 0);                                 // clustering only (filter range unknown)
        report(cb, "CounterID", 0, 0);
    }

    private static void report(String dir, String column, long lo, long hi)
    {
        try {
            List<Path> files = listParquet(dir);
            if (files.isEmpty()) {
                System.out.printf("%-28s %-18s: no files at %s%n", shortDir(dir), column, dir);
                return;
            }
            ParquetReaderOptions options = ParquetReaderOptions.builder().build();
            List<Group> groups = new ArrayList<>();
            for (Path file : files) {
                io.trino.parquet.ParquetDataSource ds = new FileDataSource(file.toFile(), options);
                ParquetMetadata md = MetadataReader.readFooter(ds, Optional.empty());
                for (BlockMetadata block : md.getBlocks()) {
                    for (ColumnChunkMetadata c : block.columns()) {
                        if (!c.getPath().toDotString().equalsIgnoreCase(column)) {
                            continue;
                        }
                        Statistics<?> s = c.getStatistics();
                        if (s == null || !s.hasNonNullValue()) {
                            continue;
                        }
                        Object mn = s.genericGetMin();
                        Object mx = s.genericGetMax();
                        if (mn instanceof Number lmn && mx instanceof Number lmx) {
                            groups.add(new Group(lmn.longValue(), lmx.longValue(), block.rowCount()));
                        }
                    }
                }
                ds.close();
            }
            if (groups.isEmpty()) {
                System.out.printf("%-28s %-18s: no numeric stats%n", shortDir(dir), column);
                return;
            }
            long gmin = groups.stream().mapToLong(Group::min).min().orElse(0);
            long gmax = groups.stream().mapToLong(Group::max).max().orElse(0);
            long totalRows = groups.stream().mapToLong(Group::rows).sum();
            double span = Math.max(1, gmax - gmin);
            double meanSpanRatio = groups.stream().mapToDouble(g -> (g.max() - g.min()) / span).average().orElse(1);
            // pruning for the given filter [lo,hi] (skip a group whose [min,max] is disjoint from [lo,hi])
            long rgRead = 0;
            long rowsRead = 0;
            boolean hasFilter = !(lo == 0 && hi == 0);
            for (Group g : groups) {
                boolean overlap = g.max() >= lo && g.min() <= hi;
                if (overlap) {
                    rgRead++;
                    rowsRead += g.rows();
                }
            }
            System.out.printf("%-22s %-18s rg=%-4d rows=%,-12d globalspan=%d..%d meanGroupSpan/global=%.3f",
                    shortDir(dir), column, groups.size(), totalRows, gmin, gmax, meanSpanRatio);
            if (hasFilter) {
                System.out.printf("  | filter[%d..%d]: read %d/%d rg (%.0f%% rows skipped)",
                        lo, hi, rgRead, groups.size(), 100.0 * (totalRows - rowsRead) / totalRows);
            }
            System.out.println();
        }
        catch (Exception e) {
            System.out.printf("%-22s %-18s: ERROR %s%n", shortDir(dir), column, e);
        }
    }

    private static List<Path> listParquet(String dir)
            throws Exception
    {
        Path p = Path.of(dir);
        if (Files.isRegularFile(p)) {
            return List.of(p);
        }
        if (!Files.isDirectory(p)) {
            return List.of();
        }
        try (var s = Files.list(p)) {
            return s.filter(Files::isRegularFile)
                    .filter(f -> !f.getFileName().toString().startsWith("."))
                    .sorted(Comparator.naturalOrder())
                    .limit(8)
                    .toList();
        }
    }

    private static String shortDir(String dir)
    {
        String[] parts = dir.split("/");
        return parts[parts.length - 1];
    }

    private static final class FileDataSource
            extends io.trino.parquet.AbstractParquetDataSource
    {
        private final java.io.RandomAccessFile input;

        private FileDataSource(java.io.File file, ParquetReaderOptions options)
                throws java.io.FileNotFoundException
        {
            super(new io.trino.parquet.ParquetDataSourceId(file.toString()), file.length(), options);
            this.input = new java.io.RandomAccessFile(file, "r");
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws java.io.IOException
        {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }

        @Override
        public void close()
                throws java.io.IOException
        {
            input.close();
        }
    }
}
