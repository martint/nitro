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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.lang.reflect.Method;

/**
 * Runs one named interpreted TPC-DS query (the same operator tree the JMH harness uses) in a warmed loop so it can be
 * wrapped in {@code sudo perf record}/{@code stat} to bucket scan vs compute and read IPC. The first argument is a
 * query number or a diagnostic method name, followed by warmup and measured iteration counts.
 */
public final class QueryDriver
{
    private QueryDriver() {}

    public static void main(String[] args)
            throws Exception
    {
        String query = args[0].matches("\\d+") ? String.format("query%02d", Integer.parseInt(args[0])) : args[0];
        int warmup = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        int measured = args.length > 2 ? Integer.parseInt(args[2]) : 20;
        TpcdsParquetTables tables = TpcdsParquetTables.requiredActual("sf10");
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        Method method = TpcdsParquetSupport.class.getDeclaredMethod(
                query, Allocator.class, PrimitiveRegistry.class, TpcdsParquetTables.class);
        method.setAccessible(true);

        boolean rowSink = Boolean.getBoolean("nitro.queryDriver.rowSink");
        boolean operatorCpuProfile = Boolean.getBoolean("nitro.operatorCpuProfile");
        boolean steadyStateAllocationProfile = Boolean.getBoolean("nitro.steadyStateAllocationProfile");
        long sink = 0;
        for (int i = 0; i < warmup; i++) {
            sink += run(method, registry, tables, rowSink);
        }
        if (operatorCpuProfile) {
            OperatorCpuProfile profile = new OperatorCpuProfile();
            Method contextualMethod;
            try {
                contextualMethod = TpcdsParquetSupport.class.getDeclaredMethod(query, TpcdsQueryContext.class);
                contextualMethod.setAccessible(true);
            }
            catch (NoSuchMethodException ignored) {
                contextualMethod = null;
            }
            long start = System.nanoTime();
            for (int i = 0; i < measured; i++) {
                if (contextualMethod != null) {
                    sink += runContextualUnchecked(contextualMethod, registry, tables, rowSink, profile);
                }
                else {
                    sink += TpcdsParquetSupport.withOperatorCpuProfile(profile, () -> runUnchecked(method, registry, tables, rowSink));
                }
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("%s: %d iters, %.1f ms/iter, rows-sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
            System.out.println(profile.formatReport());
            return;
        }
        if (steadyStateAllocationProfile) {
            int highWaterBatches = Integer.getInteger("nitro.allocationProfile.highWaterBatches", 8);
            for (int i = 0; i < measured; i++) {
                SteadyStateAllocationProfile.Report report = SteadyStateAllocationProfile.measure(
                        () -> constructUnchecked(method, registry, tables),
                        highWaterBatches);
                sink += report.sink();
                System.out.printf("%s allocation pass %d: %s%n", query, i + 1, report.formatReport());
            }
            return;
        }
        if (Boolean.getBoolean("nitro.joinMaterializationProfile")) {
            JoinMaterializationProfile profile = new JoinMaterializationProfile();
            long start = System.nanoTime();
            for (int i = 0; i < measured; i++) {
                sink += runUnchecked(method, registry, tables, rowSink, profile);
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("%s: %d iters, %.1f ms/iter, rows-sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
            System.out.println(profile.formatReport());
            return;
        }
        long start = System.nanoTime();
        for (int i = 0; i < measured; i++) {
            sink += run(method, registry, tables, rowSink);
        }
        long nanos = System.nanoTime() - start;
        System.out.printf("%s: %d iters, %.1f ms/iter, rows-sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
    }

    private static long run(Method method, PrimitiveRegistry registry, TpcdsParquetTables tables, boolean rowSink)
            throws Exception
    {
        Operator operator = construct(method, registry, tables);
        if (rowSink) {
            return org.weakref.nitro.OperatorAssertions.OperatorAssert.toRows(operator).size();
        }
        return consume(operator);
    }

    private static Operator construct(Method method, PrimitiveRegistry registry, TpcdsParquetTables tables)
            throws Exception
    {
        return (Operator) method.invoke(null, new Allocator(EngineResources.createDefault()), registry, tables);
    }

    private static Operator constructUnchecked(Method method, PrimitiveRegistry registry, TpcdsParquetTables tables)
    {
        try {
            return construct(method, registry, tables);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private static long runUnchecked(
            Method method,
            PrimitiveRegistry registry,
            TpcdsParquetTables tables,
            boolean rowSink,
            JoinMaterializationProfile profile)
    {
        try {
            Operator operator = (Operator) method.invoke(null, profile.newAllocator(), registry, tables);
            if (rowSink) {
                return org.weakref.nitro.OperatorAssertions.OperatorAssert.toRows(operator).size();
            }
            return consume(operator);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long runContextualUnchecked(
            Method method,
            PrimitiveRegistry registry,
            TpcdsParquetTables tables,
            boolean rowSink,
            OperatorCpuProfile profile)
    {
        try {
            Operator operator = (Operator) method.invoke(
                    null,
                    new TpcdsQueryContext(new Allocator(EngineResources.createDefault()), registry, tables, profile));
            if (rowSink) {
                return org.weakref.nitro.OperatorAssertions.OperatorAssert.toRows(operator).size();
            }
            return consume(operator);
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
