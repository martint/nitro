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
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Composite-key inner join: fact(fk0, fk1, measure) JOIN dim(dk0, dk1, dattr) ON two columns, GROUP BY a
 * dimension attribute, sum a fact measure — interpreted (multi-key HashJoin -> GroupedAggregation) vs
 * JIT-compiled (one fused routine: composite build, folded-hash probe with a two-component equality check,
 * grouped aggregation).
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 4, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledMultiKeyJoin
{
    private static final int FACT_ROWS = 16_000_000;
    private static final int DIM0 = 2_000;
    private static final int DIM1 = 50;
    private static final int DIM_ROWS = DIM0 * DIM1;   // 100k dim rows, unique (dk0, dk1)
    private static final int GROUPS = 100;
    private static final int CHUNK = 4096;

    private final Allocator allocator = new Allocator();

    private long[] fk0;
    private long[] fk1;
    private long[] measure;
    private long[] dk0;
    private long[] dk1;
    private long[] dattr;
    private List<TableOperator.Page> factPages;
    private List<TableOperator.Page> dimPages;
    private CompiledPipeline compiled;

    @Setup
    public void setup()
    {
        dk0 = new long[DIM_ROWS];
        dk1 = new long[DIM_ROWS];
        dattr = new long[DIM_ROWS];
        for (int a0 = 0; a0 < DIM0; a0++) {
            for (int a1 = 0; a1 < DIM1; a1++) {
                int row = a0 * DIM1 + a1;
                dk0[row] = a0;
                dk1[row] = a1;
                dattr[row] = row % GROUPS;
            }
        }
        fk0 = new long[FACT_ROWS];
        fk1 = new long[FACT_ROWS];
        measure = new long[FACT_ROWS];
        for (int i = 0; i < FACT_ROWS; i++) {
            fk0[i] = i % DIM0;
            fk1[i] = i % DIM1;
            measure[i] = (i % 50) + 1;
        }

        factPages = pages(fk0, fk1, measure);
        dimPages = pages(dk0, dk1, dattr);

        Plan.Pipeline plan = new Plan.Pipeline(
                3,
                new Plan.Build(3, new int[] {0, 1}),
                new int[] {0, 1},
                List.of(),
                List.of(new Plan.Col(5)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));
        compiled = PipelineCompiler.compile(plan);

        if (jitCompiled() != interpreted()) {
            throw new IllegalStateException("mismatch: jit=" + jitCompiled() + " interpreted=" + interpreted());
        }
    }

    private static List<TableOperator.Page> pages(long[] c0, long[] c1, long[] c2)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int off = 0; off < c0.length; off += CHUNK) {
            int len = Math.min(CHUNK, c0.length - off);
            long[] a0 = new long[len];
            long[] a1 = new long[len];
            long[] a2 = new long[len];
            System.arraycopy(c0, off, a0, 0, len);
            System.arraycopy(c1, off, a1, 0, len);
            System.arraycopy(c2, off, a2, 0, len);
            pages.add(new TableOperator.Page(
                    len,
                    new Streams[] {
                            Streams.ofValues(new I64Vector(a0)),
                            Streams.ofValues(new I64Vector(a1)),
                            Streams.ofValues(new I64Vector(a2))},
                    Mask.all(len)));
        }
        return pages;
    }

    @Benchmark
    public long jitCompiled()
    {
        CompiledPipeline.Result result = compiled.execute(
                new long[][][] {{fk0, fk1, measure}, {dk0, dk1, dattr}},
                new int[] {FACT_ROWS, DIM_ROWS});
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
        Operator fact = new TableOperator(3, factPages);
        Operator joined = new HashJoinOperator(allocator, fact, new int[] {0, 1}, new TableOperator(3, dimPages), new int[] {0, 1});
        // joined columns: 0=fk0, 1=fk1, 2=measure, 3=dk0, 4=dk1, 5=dattr
        Operator aggregation = new GroupedAggregationOperator(allocator, List.of(5), List.of(new Sum(2)), joined);

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
        Benchmarks.benchmark(BenchmarkCompiledMultiKeyJoin.class).run();
    }
}
