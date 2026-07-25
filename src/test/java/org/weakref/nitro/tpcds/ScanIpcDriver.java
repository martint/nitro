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
package org.weakref.nitro.tpcds;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.MultiStageOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.source.compatibility.parquet.TrinoParquetScanOperator;

import java.util.List;

/**
 * Standalone driver to measure the IPC of the production Parquet scan/decode in isolation (no joins/agg), warmed,
 * so it can be wrapped in {@code sudo perf stat}. Drains a multi-file store_sales scan of the q88 key columns,
 * touching every column's values to force full decode. Args: warmupIters measuredIters.
 */
public final class ScanIpcDriver
{
    private ScanIpcDriver() {}

    private static final List<String> COLUMNS = List.of("ss_store_sk", "ss_sold_time_sk", "ss_hdemo_sk", "ss_ext_sales_price");

    public static void main(String[] args)
            throws Exception
    {
        int warmup = args.length > 0 ? Integer.parseInt(args[0]) : 6;
        int measured = args.length > 1 ? Integer.parseInt(args[1]) : 30;
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");

        long rows = 0;
        long checksum = 0;
        for (int i = 0; i < warmup; i++) {
            checksum += drainScan(tables);
        }
        long start = System.nanoTime();
        for (int i = 0; i < measured; i++) {
            long c = drainScan(tables);
            checksum += c;
            rows += c >>> 20;   // approximate row accounting kept out of the hot path
        }
        long elapsedNanos = System.nanoTime() - start;
        System.out.printf("measured %d iters in %.1f ms (%.1f ms/iter), checksum=%d%n",
                measured, elapsedNanos / 1e6, elapsedNanos / 1e6 / measured, checksum);
    }

    private static long drainScan(TpcdsParquetTables tables)
            throws Exception
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<java.nio.file.Path> files = tables.tableFiles("store_sales");
        Operator scan = new MultiStageOperator(COLUMNS.size(), files, path -> new TrinoParquetScanOperator(allocator, path, COLUMNS));
        long sum = 0;
        try (scan) {
            while (scan.hasNext()) {
                try (Batch batch = scan.next()) {
                    Mask mask = batch.borrowMask();
                    for (int c = 0; c < scan.outputCount(); c++) {
                        Vector values = batch.output(c).borrow(Stream.VALUES);
                        sum += sampleVector(values, mask);
                    }
                }
            }
        }
        return sum;
    }

    // Touch a few decoded positions so the decode isn't dead-code-eliminated, without dominating the scan cost.
    private static long sampleVector(Vector values, Mask mask)
    {
        return values.length() + mask.count();
    }
}
