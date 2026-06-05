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
package org.weakref.nitro;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.PipelineCompiler;
import org.weakref.nitro.jit.Plan;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Grouped aggregation, interpreted vs JIT-compiled: {@code SELECT k, sum(v) GROUP BY k} over 16M rows.
 * Sweeps the distinct-group count, since that controls whether the grouping table is cache-resident
 * (where fusion/dispatch removal dominates) or memory-bound (where both pay similarly).
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledGroupBy
{
    private static final int ROWS = 16_000_000;
    private static final int CHUNK = 4096;

    @Param({"1000", "100000", "2000000"})
    private int groups;

    private final Allocator allocator = new Allocator();

    private long[] k;
    private long[] v;
    private List<TableOperator.Page> pages;
    private CompiledPipeline compiled;

    @Setup
    public void setup()
    {
        k = new long[ROWS];
        v = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            k[i] = i % groups;
            v[i] = (i % 100) + 1;
        }

        pages = new ArrayList<>();
        for (int off = 0; off < ROWS; off += CHUNK) {
            int len = Math.min(CHUNK, ROWS - off);
            long[] ck = new long[len];
            long[] cv = new long[len];
            System.arraycopy(k, off, ck, 0, len);
            System.arraycopy(v, off, cv, 0, len);
            pages.add(new TableOperator.Page(
                    len,
                    new Streams[] {Streams.ofValues(new I64Vector(ck)), Streams.ofValues(new I64Vector(cv))},
                    Mask.all(len)));
        }

        Plan.Pipeline plan = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));
        compiled = PipelineCompiler.compile(plan);

        if (jitCompiled() != interpreted()) {
            throw new IllegalStateException("mismatch: jit=" + jitCompiled() + " interpreted=" + interpreted());
        }
    }

    @Benchmark
    public long jitCompiled()
    {
        CompiledPipeline.Result result = compiled.execute(new long[][] {k, v}, ROWS);
        long checksum = 0;
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            checksum += sums[g];
        }
        return checksum;
    }

    @Benchmark
    public long interpreted()
    {
        Operator source = new TableOperator(2, pages);
        Operator aggregation = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1)), source);

        long checksum = 0;
        try (aggregation) {
            while (aggregation.hasNext()) {
                try (var batch = aggregation.next()) {
                    Mask mask = batch.borrowMask();
                    I64Vector sums = (I64Vector) batch.output(1).borrow(Stream.VALUES);
                    for (int index = 0; index < mask.selectedCount(); index++) {
                        checksum += sums.values()[mask.position(index)];
                    }
                }
            }
        }
        return checksum;
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkCompiledGroupBy.class).run();
    }
}
