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
package org.weakref.nitro.clickbench;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.tpcds.OperatorCpuProfile;

import java.nio.file.Path;

/**
 * Runs the operator-based ClickBench harness in a warmed loop for focused profiling.
 *
 * <p>Arguments use ClickBench's published zero-based IDs. The underlying support methods retain their historical
 * one-based names.
 */
public final class QueryDriver
{
    private QueryDriver() {}

    public static void main(String[] args)
    {
        String query = args[0].matches("\\d+") ? String.format("query%02d", Integer.parseInt(args[0])) : args[0];
        if (!query.equals("query04") && !query.equals("query05") && !query.equals("query08") && !query.equals("query09") && !query.equals("query11") && !query.equals("query17") && !query.equals("query21") && !query.equals("query28") && !query.equals("query29") && !query.equals("query32") && !query.equals("query33") && !query.equals("query35") && !query.equals("query39") && !query.equals("query42")) {
            throw new IllegalArgumentException("Operator profiling is not yet wired for " + query);
        }
        int warmup = args.length > 1 ? Integer.parseInt(args[1]) : 2;
        int measured = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        Path hits = ClickBenchHitsSupport.requiredActualHitsDirectory();
        try (EngineResources resources = EngineResources.createDefault()) {
            run(query, warmup, measured, hits, resources);
        }
    }

    private static void run(String query, int warmup, int measured, Path hits, EngineResources resources)
    {
        long sink = 0;
        for (int iteration = 0; iteration < warmup; iteration++) {
            try (Allocator allocator = new Allocator(resources)) {
                sink += consume(query(query, allocator, hits, null));
            }
        }

        if (!Boolean.getBoolean("nitro.operatorCpuProfile")) {
            long start = System.nanoTime();
            for (int iteration = 0; iteration < measured; iteration++) {
                try (Allocator allocator = new Allocator(resources)) {
                    sink += consume(query(query, allocator, hits, null));
                }
            }
            long nanos = System.nanoTime() - start;
            System.out.printf("%s: %d iters, %.1f ms/iter, sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
            return;
        }

        OperatorCpuProfile profile = new OperatorCpuProfile();
        long start = System.nanoTime();
        for (int iteration = 0; iteration < measured; iteration++) {
            try (Allocator allocator = new Allocator(resources)) {
                sink += consume(query(query, allocator, hits, profile));
            }
        }
        long nanos = System.nanoTime() - start;
        System.out.printf("%s: %d iters, %.1f ms/iter, sink=%d%n", query, measured, nanos / 1e6 / measured, sink);
        System.out.println(profile.formatReport());
    }

    private static Operator query(String query, Allocator allocator, Path hits, OperatorCpuProfile profile)
    {
        return switch (query) {
            case "query04" -> ClickBenchHitsSupport.query05(allocator, hits, profile);
            case "query05" -> ClickBenchHitsSupport.query06(allocator, hits);
            case "query08" -> ClickBenchHitsSupport.query09(allocator, hits, profile);
            case "query09" -> ClickBenchHitsSupport.query10(allocator, hits, profile);
            case "query11" -> ClickBenchHitsSupport.query12(allocator, org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(), hits, profile);
            case "query17" -> ClickBenchHitsSupport.query18(allocator, hits, profile);
            case "query21" -> ClickBenchHitsSupport.query22(allocator, org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(), hits, profile);
            case "query28" -> ClickBenchHitsSupport.query29(allocator, org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(), hits);
            case "query29" -> ClickBenchHitsSupport.query30(allocator, org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(), hits);
            case "query32" -> ClickBenchHitsSupport.query33(allocator, hits, profile);
            case "query33" -> ClickBenchHitsSupport.query34(allocator, hits, profile);
            case "query35" -> ClickBenchHitsSupport.query36(allocator, org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(), hits, profile);
            case "query39" -> ClickBenchHitsSupport.query40(allocator, org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(), hits, profile);
            case "query42" -> ClickBenchHitsSupport.query43(allocator, org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(), hits, profile);
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
