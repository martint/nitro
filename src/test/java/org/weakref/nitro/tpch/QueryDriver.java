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
package org.weakref.nitro.tpch;

import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.tpcds.JoinMaterializationProfile;
import org.weakref.nitro.tpcds.OperatorCpuProfile;
import org.weakref.nitro.tpcds.SteadyStateAllocationProfile;

/** Runs focused operator-based TPC-H profiling loops. */
public final class QueryDriver
{
    private QueryDriver() {}

    public static void main(String[] args)
    {
        String query = args[0].matches("\\d+") ? String.format("query%02d", Integer.parseInt(args[0])) : args[0];
        if (!query.equals("query05") && !query.equals("query07") && !query.equals("query08") && !query.equals("query09") && !query.equals("query11") && !query.equals("query12") && !query.equals("query13") && !query.equals("query14") && !query.equals("query15") && !query.equals("query16") && !query.equals("query19") && !query.equals("query20") && !query.equals("query21") && !query.equals("query22")) {
            throw new IllegalArgumentException("Operator profiling is not yet wired for " + query);
        }
        int warmup = args.length > 1 ? Integer.parseInt(args[1]) : 2;
        int measured = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        TpchParquetTables tables = TpchParquetTables.requiredActual();
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        long sink = 0;
        for (int iteration = 0; iteration < warmup; iteration++) {
            sink += consume(query(query, new Allocator(), registry, tables));
        }

        if (Boolean.getBoolean("nitro.steadyStateAllocationProfile")) {
            int highWaterBatches = Integer.getInteger("nitro.allocationProfile.highWaterBatches", 8);
            for (int iteration = 0; iteration < measured; iteration++) {
                SteadyStateAllocationProfile.Report report = SteadyStateAllocationProfile.measure(
                        () -> query(query, new Allocator(), registry, tables),
                        highWaterBatches);
                sink += report.sink();
                System.out.printf("%s allocation pass %d: %s%n", query, iteration + 1, report.formatReport());
            }
            return;
        }

        if (Boolean.getBoolean("nitro.joinMaterializationProfile")) {
            JoinMaterializationProfile profile = new JoinMaterializationProfile();
            long start = System.nanoTime();
            for (int iteration = 0; iteration < measured; iteration++) {
                sink += HashJoinOperator.withMaterializationProfile(
                        profile,
                        () -> consume(query(query, new Allocator(), registry, tables)));
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("%s: %d iters, %.1f ms/iter, sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
            System.out.println(profile.formatReport());
            return;
        }

        if (!Boolean.getBoolean("nitro.operatorCpuProfile")) {
            long start = System.nanoTime();
            for (int iteration = 0; iteration < measured; iteration++) {
                sink += consume(query(query, new Allocator(), registry, tables));
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("%s: %d iters, %.1f ms/iter, sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
            return;
        }

        OperatorCpuProfile profile = new OperatorCpuProfile();
        long start = System.nanoTime();
        for (int iteration = 0; iteration < measured; iteration++) {
            sink += consume(TpchParquetSupport.withOperatorCpuProfile(
                    profile,
                    () -> query(query, new Allocator(), registry, tables)));
        }
        long nanos = System.nanoTime() - start;
        System.out.printf("%s: %d iters, %.1f ms/iter, sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
        System.out.println(profile.formatReport());
    }

    private static Operator query(String query, Allocator allocator, PrimitiveRegistry registry, TpchParquetTables tables)
    {
        return switch (query) {
            case "query05" -> TpchParquetSupport.query05(allocator, registry, tables);
            case "query07" -> TpchParquetSupport.query07(allocator, registry, tables);
            case "query08" -> TpchParquetSupport.query08(allocator, registry, tables);
            case "query09" -> TpchParquetSupport.query09(allocator, registry, tables);
            case "query11" -> TpchParquetSupport.query11(allocator, registry, tables);
            case "query12" -> TpchParquetSupport.query12(allocator, registry, tables);
            case "query13" -> TpchParquetSupport.query13(allocator, registry, tables);
            case "query14" -> TpchParquetSupport.query14(allocator, registry, tables);
            case "query15" -> TpchParquetSupport.query15(allocator, registry, tables);
            case "query16" -> TpchParquetSupport.query16(allocator, registry, tables);
            case "query19" -> TpchParquetSupport.query19(allocator, registry, tables);
            case "query20" -> TpchParquetSupport.query20(allocator, registry, tables);
            case "query21" -> TpchParquetSupport.query21(allocator, registry, tables);
            case "query22" -> TpchParquetSupport.query22(allocator, registry, tables);
            default -> throw new IllegalArgumentException("Unsupported query: " + query);
        };
    }

    private static long consume(Operator operator)
    {
        long sink = 0;
        try (operator) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    sink += batch.borrowMask().count();
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
