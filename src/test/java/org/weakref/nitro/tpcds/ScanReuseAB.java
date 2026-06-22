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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.HardwoodParquetScanOperator;
import org.weakref.nitro.operator.NitroParquetScanOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.nio.file.Path;
import java.util.List;

/**
 * A/B driver isolating the Parquet SCAN/decode step for q39's inventory columns, full-decode on both
 * sides, so we can measure whether Hardwood's buffer reuse (pooled decode arrays + mmap input) closes the
 * decode gap vs the Trino-vendored reader. Drains all batches and sums values to force materialization.
 * Run single-thread under taskset -c 0 with mode arg "trino" or "hardwood", plus iters and warmup.
 * Hardwood worker threads via the nitro.hardwood.threads system property.
 */
public final class ScanReuseAB
{
    private static final List<String> COLUMNS = List.of("inv_date_sk", "inv_item_sk", "inv_quantity_on_hand");

    private ScanReuseAB() {}

    public static void main(String[] args)
    {
        String mode = args.length > 0 ? args[0] : "trino";
        int iters = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        int warmup = args.length > 2 ? Integer.parseInt(args[2]) : 4;

        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        List<Path> files = tables.tableFiles("inventory");

        long sink = 0;
        for (int i = 0; i < warmup; i++) {
            sink += run(mode, files);
        }
        System.err.println("[warmup done, sink=" + sink + "] --- BEGIN MEASURED " + iters + "x " + mode + " ---");
        long start = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            sink += run(mode, files);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        System.err.println("[MEASURED] " + mode + " " + iters + " iters in " + elapsedMs + "ms = "
                + (elapsedMs / iters) + "ms/iter  sink=" + sink);
    }

    private static long run(String mode, List<Path> files)
    {
        Allocator allocator = new Allocator();
        long sum = 0;
        try (Operator operator = switch (mode) {
            case "hardwood" -> new HardwoodParquetScanOperator(allocator, files, COLUMNS);
            case "nitro" -> new NitroParquetScanOperator(allocator, files, COLUMNS);
            default -> new TrinoParquetScanOperator(allocator, files, COLUMNS);
        }) {
            int columnCount = operator.outputCount();
            while (operator.hasNext()) {
                Batch batch = operator.next();
                try {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }
                    int count = mask.count();
                    for (int c = 0; c < columnCount; c++) {
                        Output output = batch.output(c);
                        Vector values = output.borrow(Stream.VALUES);
                        Vector nullsVector = output.borrowOrNull(Stream.NULLS);
                        boolean[] nulls = nullsVector instanceof org.weakref.nitro.data.BooleanVector bv ? bv.values() : null;
                        if (values instanceof I32Vector i32) {
                            int[] raw = i32.values();
                            for (int p = 0; p < count; p++) {
                                if (nulls == null || !nulls[p]) {
                                    sum += raw[p];
                                }
                            }
                        }
                    }
                }
                finally {
                    batch.close();
                }
            }
        }
        return sum;
    }
}
