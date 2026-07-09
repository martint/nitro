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

import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.lang.reflect.Method;

/**
 * Runs one named interpreted TPC-DS query (the same operator tree the JMH harness uses) in a warmed loop so it can be
 * wrapped in {@code sudo perf record}/{@code stat} to bucket scan vs compute and read IPC. Args: queryNN warmup measured.
 * Pass -Dnitro.skipScan -Dnitro.dynamicFilter to exercise the dynamic-filter path.
 */
public final class QueryDriver
{
    private QueryDriver() {}

    public static void main(String[] args)
            throws Exception
    {
        int q = Integer.parseInt(args[0]);
        int warmup = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        int measured = args.length > 2 ? Integer.parseInt(args[2]) : 20;
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        Method method = TpcdsParquetSupport.class.getDeclaredMethod(
                String.format("query%02d", q), Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class);
        method.setAccessible(true);

        boolean rowSink = Boolean.getBoolean("nitro.queryDriver.rowSink");
        boolean operatorCpuProfile = Boolean.getBoolean("nitro.operatorCpuProfile");
        long sink = 0;
        for (int i = 0; i < warmup; i++) {
            sink += run(method, registry, tables, rowSink);
        }
        if (operatorCpuProfile) {
            OperatorCpuProfile profile = new OperatorCpuProfile();
            long start = System.nanoTime();
            for (int i = 0; i < measured; i++) {
                sink += TpcdsParquetSupport.withOperatorCpuProfile(profile, () -> runUnchecked(method, registry, tables, rowSink));
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("q%d: %d iters, %.1f ms/iter, rows-sink=%d%n", q, measured, nanos / 1e6 / measured, sink);
            System.out.println(profile.formatReport());
            return;
        }
        if (Boolean.getBoolean("nitro.joinMaterializationProfile")) {
            JoinMaterializationProfile profile = new JoinMaterializationProfile();
            long start = System.nanoTime();
            for (int i = 0; i < measured; i++) {
                sink += HashJoinOperator.withMaterializationProfile(profile, () -> runUnchecked(method, registry, tables, rowSink));
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("q%d: %d iters, %.1f ms/iter, rows-sink=%d%n", q, measured, nanos / 1e6 / measured, sink);
            System.out.println(profile.formatReport());
            return;
        }
        long start = System.nanoTime();
        for (int i = 0; i < measured; i++) {
            sink += run(method, registry, tables, rowSink);
        }
        long nanos = System.nanoTime() - start;
        System.out.printf("q%d: %d iters, %.1f ms/iter, rows-sink=%d%n", q, measured, nanos / 1e6 / measured, sink);
    }

    private static long run(Method method, PrimitiveRegistry registry, TpcdsParquetTables tables, boolean rowSink)
            throws Exception
    {
        Operator operator = (Operator) method.invoke(null, new Allocator(), registry, tables);
        if (rowSink) {
            return org.weakref.nitro.OperatorAssertions.OperatorAssert.toRows(operator).size();
        }
        return consume(operator);
    }

    private static long runUnchecked(Method method, PrimitiveRegistry registry, TpcdsParquetTables tables, boolean rowSink)
    {
        try {
            return run(method, registry, tables, rowSink);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long consume(Operator operator)
    {
        long sink = 0;
        try (operator) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    var mask = batch.borrowMask();
                    sink += mask.count();
                    for (int column = 0; column < operator.outputCount(); column++) {
                        sink += consume(batch.output(column).borrow(Stream.VALUES));
                    }
                }
            }
        }
        return sink;
    }

    private static long consume(Vector vector)
    {
        return vector.length();
    }
}
