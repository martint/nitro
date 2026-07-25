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
 * Measurement (dormant unless -Dnitro.measureDf=true): demonstrates Velox-style dynamic filtering in the INTERPRETED
 * operator pipeline. A selective dimension (a single store key) is the build side of a hash join whose probe is a
 * SkipDecodeScanOperator over store_sales; the join pushes the key membership into the scan, which decodes the join
 * key, narrows to the matching rows, and SKIP-decodes the other columns for survivors only. Correctness is checked
 * against an ordinary TrinoParquetScanOperator tree; decode engagement and wall time are reported. Run with
 * -Dnitro.dynamicFilter=true to enable the push (and -Dnitro.tpcds.parquet.path, -Dlicense.skip=true).
 */
package org.weakref.nitro.tpcds;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.source.compatibility.parquet.SkipDecodeScanOperator;
import org.weakref.nitro.operator.source.compatibility.parquet.TrinoParquetScanOperator;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

public class MeasureDynamicFilter
{
    private static final List<String> COLUMNS = List.of("ss_store_sk", "ss_sold_time_sk", "ss_hdemo_sk");
    private static final long STORE_KEY = 1L;   // a single store: ~1/200 of the fact rows

    @Test
    public void measure()
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.measureDf"), "set -Dnitro.measureDf=true");
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        Assumptions.assumeTrue(tables != null, "set -Dnitro.tpcds.parquet.path");
        List<Path> files = tables.tableFiles("store_sales");
        boolean dynamicFilter = Boolean.getBoolean("nitro.dynamicFilter");

        // Correctness: the skip-decode-scan tree must match the ordinary-scan tree exactly.
        long[] reference = run(() -> new TrinoParquetScanOperator(new Allocator(EngineResources.createDefault()), files, COLUMNS, true));
        SkipDecodeScanOperator.Profile profile = new SkipDecodeScanOperator.Profile();
        long[] skip = run(() -> new SkipDecodeScanOperator(new Allocator(EngineResources.createDefault()), files, COLUMNS, profile));
        System.out.printf("reference rows=%d checksum=%d ; skipScan rows=%d checksum=%d ; match=%b ; %s%n",
                reference[0], reference[1], skip[0], skip[1], reference[0] == skip[0] && reference[1] == skip[1],
                profile.summary());

        // Timing of the skip-decode-scan tree (2 warmup + min of 5).
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 7; i++) {
            long start = System.nanoTime();
            run(() -> new SkipDecodeScanOperator(new Allocator(EngineResources.createDefault()), files, COLUMNS));
            long elapsed = System.nanoTime() - start;
            if (i >= 2) {
                best = Math.min(best, elapsed);
            }
        }
        System.out.printf("dynamicFilter=%b : store_sales join skip-scan %6.1f ms%n", dynamicFilter, best / 1e6);
    }

    /** Build store_sales(probe) ⋈ {STORE_KEY}(build) on ss_store_sk; drain and return {rowCount, checksum}. */
    private static long[] run(Supplier<Operator> probeFactory)
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator probe = probeFactory.get();
        Operator dimension = new TableOperator(1, List.of(
                new TableOperator.Page(1, new Streams[] {Streams.ofValues(new I64Vector(new long[] {STORE_KEY}))}, Mask.all(1))));
        Operator join = new HashJoinOperator(allocator, probe, 0, dimension, 0);

        long rowCount = 0;
        long checksum = 0;
        try (join) {
            while (join.hasNext()) {
                try (Batch batch = join.next()) {
                    Mask mask = batch.borrowMask();
                    int count = mask.count();
                    if (count == 0) {
                        continue;
                    }
                    Vector values = batch.output(1).borrow(Stream.VALUES);   // ss_sold_time_sk
                    Vector nulls = batch.output(1).borrowOrNull(Stream.NULLS);
                    for (int i = 0; i < count; i++) {
                        int position = mask.position(i);
                        rowCount++;
                        if (nulls == null || !VectorAccess.isNull(nulls, position)) {
                            checksum += longAt(values, position);
                        }
                    }
                }
            }
        }
        return new long[] {rowCount, checksum};
    }

    private static long longAt(Vector vector, int position)
    {
        if (vector instanceof I64Vector i64) {
            return i64.values()[position];
        }
        if (vector instanceof DictionaryVector dictionary) {
            return longAt(dictionary.values(), dictionary.ids()[position]);
        }
        throw new IllegalArgumentException("Unsupported vector: " + vector.getClass().getName());
    }
}
