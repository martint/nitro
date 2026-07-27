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
 * Measurement (dormant unless -Dnitro.measureQ88Df=true; run with -Dnitro.dynamicFilter=true and
 * -Dnitro.tpcds.parquet.path): the WHOLE q88 interpreted operator tree with Velox-style dynamic filtering. q88's
 * store_sales probe joins three selective dimensions (time_dim by hour, household_demographics, store='ese'); each
 * join pushes its key membership down to the one SkipDecodeScanOperator, which decodes the most-selective key,
 * narrows, and skip-decodes the rest for survivors. The ordinary-scan tree (skipScan off) is the correctness
 * reference and the timing baseline; the skip-scan tree is the dynamic-filtering result. Toggled per run via the
 * -Dnitro.skipScan property (read at scan-construction time).
 */
package org.weakref.nitro.tpcds;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

public class MeasureQ88DynamicFilter
{
    @Test
    public void measure()
    {
        Assumptions.assumeTrue(Boolean.getBoolean("nitro.measureQ88Df"), "set -Dnitro.measureQ88Df=true");
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        Assumptions.assumeTrue(tables != null, "set -Dnitro.tpcds.parquet.path");
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();

        System.setProperty("nitro.skipScan", "false");
        long referenceChecksum = runQ88(tables, registry);
        long referenceTime = timeQ88(tables, registry);

        System.setProperty("nitro.skipScan", "true");
        long skipChecksum = runQ88(tables, registry);
        long skipTime = timeQ88(tables, registry);

        System.out.printf("q88 reference checksum=%d (%.0f ms) ; skipScan checksum=%d (%.0f ms) ; match=%b%n",
                referenceChecksum, referenceTime / 1e6, skipChecksum, skipTime / 1e6, referenceChecksum == skipChecksum);
        System.out.printf("q88 dynamic-filter speedup = %.2fx%n", (double) referenceTime / skipTime);
    }

    private static long timeQ88(TpcdsParquetTables tables, PrimitiveRegistry registry)
    {
        for (int i = 0; i < 2; i++) {
            runQ88(tables, registry);
        }
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            long start = System.nanoTime();
            runQ88(tables, registry);
            best = Math.min(best, System.nanoTime() - start);
        }
        return best;
    }

    private static long runQ88(TpcdsParquetTables tables, PrimitiveRegistry registry)
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator q88 = TpcdsParquetSupport.query88(allocator, registry, tables);
        long checksum = 0;
        try (q88) {
            while (q88.hasNext()) {
                try (Batch batch = q88.next()) {
                    Mask mask = batch.borrowMask();
                    int count = mask.count();
                    for (int c = 0; c < q88.outputCount(); c++) {
                        Vector values = batch.output(c).borrow(Stream.VALUES);
                        for (int i = 0; i < count; i++) {
                            checksum += longAt(values, mask.position(i));
                        }
                    }
                }
            }
        }
        return checksum;
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
