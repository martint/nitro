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

import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Runs one TPC-DS query through the Nitro operator tree and prints a digest of its result rows. Intended to
 * be run twice in separate JVMs — once with {@code -Dnitro.parquet.useNitroReader=false} (Trino reader) and
 * once with {@code true} (Nitro reader) — to confirm the new decoder yields identical whole-query results.
 */
public final class QueryDigest
{
    private QueryDigest() {}

    public static void main(String[] args)
            throws Exception
    {
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        int iters = Integer.getInteger("iters", 0);
        for (String name : args) {
            Method method = TpcdsParquetSupport.class.getDeclaredMethod(
                    name, Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class);
            method.setAccessible(true);
            List<?> rows;
            long digest = 1125899906842597L;
            for (int w = 0; w < iters; w++) {
                try (Operator operator = (Operator) method.invoke(null, new Allocator(), registry, tables)) {
                    OperatorAssertions.OperatorAssert.toRows(operator);
                }
            }
            long start = System.nanoTime();
            int measured = Math.max(1, iters);
            int size = 0;
            for (int m = 0; m < measured; m++) {
                try (Operator operator = (Operator) method.invoke(null, new Allocator(), registry, tables)) {
                    rows = OperatorAssertions.OperatorAssert.toRows(operator);
                }
                size = rows.size();
                digest = 1125899906842597L;
                for (Object row : rows) {
                    digest = digest * 1000003L + String.valueOf(row).hashCode();
                }
            }
            long ms = (System.nanoTime() - start) / 1_000_000 / measured;
            System.out.println(name + " rows=" + size + " digest=" + digest + (iters > 0 ? " ms=" + ms : ""));
        }
    }
}
