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
 * Measurement (dormant unless -Dnitro.measureSkip=true): times the q88 dynamic-filter composite end-to-end through
 * the compiled streaming engine, reporting total and scan-only wall time. Run twice -- once with -Dnitro.skipDecode=true
 * (default) and once with -Dnitro.skipDecode=false -- to isolate the value-level skip-decode win on a real scan-bound
 * query. Requires -Dnitro.tpcds.parquet.path; run with -Dlicense.skip=true.
 */
package org.weakref.nitro.tpcds;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.execution.EngineResources;

import java.util.HashMap;
import java.util.Map;

public class MeasureSkipDecode
{
    @Test
    public void measure()
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.measureSkip"), "set -Dnitro.measureSkip=true");
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        Assumptions.assumeTrue(tables != null, "set -Dnitro.tpcds.parquet.path");

        boolean skip = !"false".equals(System.getProperty("nitro.skipDecode", "true"));
        CompiledTpcdsQueries.Composite q88 = CompiledTpcdsQueries.query88();

        long bestTotal = Long.MAX_VALUE;
        long bestScan = Long.MAX_VALUE;
        int warmups = 3;
        int iterations = 8;
        for (int i = 0; i < warmups + iterations; i++) {
            CompiledQuerySupport.ScanProfile.enable(true);
            CompiledQuerySupport.ScanProfile.reset();
            long start = System.nanoTime();
            int rows = runQ88(tables, q88);
            long total = System.nanoTime() - start;
            long scan = CompiledQuerySupport.ScanProfile.scanNanos();
            CompiledQuerySupport.ScanProfile.enable(false);
            if (i >= warmups) {
                bestTotal = Math.min(bestTotal, total);
                bestScan = Math.min(bestScan, scan);
            }
            if (i == 0) {
                System.out.printf("q88 rows = %d ; %s%n", rows, CompiledQuerySupport.ScanProfile.skipStats());
            }
        }
        System.out.printf("q88 skipDecode=%s : total %6.1f ms | scan %6.1f ms (%.0f%% of total)%n",
                skip, bestTotal / 1e6, bestScan / 1e6, 100.0 * bestScan / bestTotal);
    }

    private static int runQ88(TpcdsParquetTables tables, CompiledTpcdsQueries.Composite q88)
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : q88.stages()) {
            virtuals.put(stage.virtualName(),
                    CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, q88.main().lower(), virtuals);
        return run.result().rowCount();
    }
}
