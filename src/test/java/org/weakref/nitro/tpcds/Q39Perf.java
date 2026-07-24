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
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.lang.reflect.Method;

/**
 * Standalone perf-counter driver: warm up, then run one TPC-DS query N times in steady state so an external
 * `sudo perf stat` over the whole process yields ~per-query HW counters (instructions, IPC, cache/branch misses)
 * — for an apples-to-apples microarchitectural comparison against the single-core Velox numbers. Run:
 *   sudo perf stat -e ... taskset -c 0 java ... org.weakref.nitro.tpcds.Q39Perf <queryNN> <iters>
 */
public final class Q39Perf
{
    private Q39Perf() {}

    public static void main(String[] args)
            throws Exception
    {
        String name = args.length > 0 ? args[0] : "query39";
        int iters = args.length > 1 ? Integer.parseInt(args[1]) : 12;
        int warmup = args.length > 2 ? Integer.parseInt(args[2]) : 6;

        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        Method method = TpcdsParquetSupport.class.getDeclaredMethod(
                name, Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class);
        method.setAccessible(true);

        long sink = 0;
        for (int i = 0; i < warmup; i++) {
            sink += run(method, registry, tables);
        }
        System.err.println("[warmup done, sink=" + sink + "] --- BEGIN MEASURED " + iters + "x " + name + " ---");
        long start = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            sink += run(method, registry, tables);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        System.err.println("[MEASURED] " + name + " " + iters + " iters in " + elapsedMs + "ms = "
                + (elapsedMs / iters) + "ms/iter  sink=" + sink);
    }

    private static long run(Method method, PrimitiveRegistry registry, TpcdsParquetTables tables)
            throws Exception
    {
        try (Operator operator = (Operator) method.invoke(null, new Allocator(EngineResources.createDefault()), registry, tables)) {
            return OperatorAssertions.OperatorAssert.toRows(operator).size();
        }
    }
}
