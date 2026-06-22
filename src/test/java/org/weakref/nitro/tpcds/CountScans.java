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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Operator;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Audit tool: builds each TPC-DS Nitro operator tree and records how many table-scan helper calls it makes
 * (one per table reference), so the per-query scan count can be diffed against the canonical Trino EXPLAIN
 * plan and against the Velox harness. Writes one "qNN,count" line per query to /tmp/nitro_scans.csv. Run with:
 *   mvn test -Dtest=CountScans -Dnitro.tpcds.parquet.path=/root/data/tpcds-parquet-sf10 -Dnitro.tpcds.parquet.schema=sf10
 */
public class CountScans
{
    @Test
    void countAll()
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        StringBuilder out = new StringBuilder("query,nitro_scans\n");
        for (int q = 1; q <= 99; q++) {
            String name = String.format("query%02d", q);
            Method method;
            try {
                method = TpcdsParquetSupport.class.getDeclaredMethod(name, Allocator.class, org.weakref.nitro.operator.evaluator.PrimitiveRegistry.class, TpcdsParquetTables.class);
            }
            catch (NoSuchMethodException e) {
                continue;
            }
            method.setAccessible(true);
            TpcdsParquetSupport.SCAN_COUNT.set(0);
            try {
                Operator operator = (Operator) method.invoke(null, new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
                if (operator == null) {
                    continue;
                }
                out.append(q).append(',').append(TpcdsParquetSupport.SCAN_COUNT.get()).append('\n');
                System.out.println(name + ": " + TpcdsParquetSupport.SCAN_COUNT.get() + " scans");
            }
            catch (Throwable t) {
                System.out.println(name + ": BUILD-FAILED " + t);
            }
        }
        Files.writeString(Path.of("/tmp/nitro_scans.csv"), out.toString(), StandardCharsets.UTF_8);
    }
}
