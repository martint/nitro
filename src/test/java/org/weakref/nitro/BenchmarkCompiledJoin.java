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
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.legacy.pipeline.CompiledPipeline;
import org.weakref.nitro.legacy.pipeline.CompilerResources;
import org.weakref.nitro.legacy.pipeline.PipelineCompiler;
import org.weakref.nitro.legacy.pipeline.Plan;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.aggregation.Sum;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The canonical TPC-DS shape — fact JOIN dimension, GROUP BY a dimension attribute, sum a fact measure —
 * interpreted (HashJoin -> GroupedAggregation) vs JIT-compiled (one fused build+probe+group routine).
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 4, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledJoin
{
    private static final int FACT_ROWS = 16_000_000;
    private static final int DIM_ROWS = 100_000;
    private static final int GROUPS = 100;
    private static final int CHUNK = 4096;

    private final Allocator allocator = new Allocator(EngineResources.createDefault());

    private static final long SPARSE_STRIDE = 1_000_000;

    private long[] fk;
    private long[] measure;
    private long[] dkey;
    private long[] dattr;
    private long[] fkSparse;
    private long[] dkeySparse;
    private List<TableOperator.Page> factPages;
    private List<TableOperator.Page> dimPages;
    private CompiledPipeline compiled;

    @Setup
    public void setup()
    {
        dkey = new long[DIM_ROWS];
        dattr = new long[DIM_ROWS];
        for (int d = 0; d < DIM_ROWS; d++) {
            dkey[d] = d;
            dattr[d] = d % GROUPS;
        }
        fk = new long[FACT_ROWS];
        measure = new long[FACT_ROWS];
        for (int i = 0; i < FACT_ROWS; i++) {
            fk[i] = i % DIM_ROWS;
            measure[i] = (i % 50) + 1;
        }

        // A sparse copy of the same join: build keys spread out by SPARSE_STRIDE so the key range far exceeds
        // the row count. The same compiled plan must fall back to the hash table for this input.
        dkeySparse = new long[DIM_ROWS];
        for (int d = 0; d < DIM_ROWS; d++) {
            dkeySparse[d] = d * SPARSE_STRIDE;
        }
        fkSparse = new long[FACT_ROWS];
        for (int i = 0; i < FACT_ROWS; i++) {
            fkSparse[i] = dkeySparse[i % DIM_ROWS];
        }

        factPages = pages(fk, measure);
        dimPages = pages(dkey, dattr);

        // fact(fk=0, measure=1) JOIN dim(dkey=0, dattr=1) ON fk=dkey; GROUP BY dattr (combined col 3), sum(measure col 1)
        // One adaptive plan: it measures the build key range at runtime and picks array vs hash from the data.
        Plan.Pipeline plan = new Plan.Pipeline(
                2,
                new Plan.Build(2, 0),
                0,
                List.of(),
                List.of(new Plan.Col(3)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));
        compiled = new PipelineCompiler(CompilerResources.createDefault()).compile(plan);

        long interp = interpreted();
        // Dense and sparse inputs map every fact row to the same dimension attribute, so all three agree.
        if (jitDenseBuild() != interp || jitSparseBuild() != interp) {
            throw new IllegalStateException("mismatch: dense=" + jitDenseBuild() + " sparse=" + jitSparseBuild() + " interpreted=" + interp);
        }
    }

    @Benchmark
    public long jitDenseBuild()
    {
        CompiledPipeline.Result result = compiled.execute(
                new long[][][] {{fk, measure}, {dkey, dattr}},
                new int[] {FACT_ROWS, DIM_ROWS});
        long checksum = 0;
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            checksum += sums[g];
        }
        return checksum;
    }

    @Benchmark
    public long jitSparseBuild()
    {
        CompiledPipeline.Result result = compiled.execute(
                new long[][][] {{fkSparse, measure}, {dkeySparse, dattr}},
                new int[] {FACT_ROWS, DIM_ROWS});
        long checksum = 0;
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            checksum += sums[g];
        }
        return checksum;
    }

    private static List<TableOperator.Page> pages(long[] c0, long[] c1)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int off = 0; off < c0.length; off += CHUNK) {
            int len = Math.min(CHUNK, c0.length - off);
            long[] a = new long[len];
            long[] b = new long[len];
            System.arraycopy(c0, off, a, 0, len);
            System.arraycopy(c1, off, b, 0, len);
            pages.add(new TableOperator.Page(
                    len,
                    new Streams[] {Streams.ofValues(new I64Vector(a)), Streams.ofValues(new I64Vector(b))},
                    Mask.all(len)));
        }
        return pages;
    }

    @Benchmark
    public long interpreted()
    {
        Operator fact = new TableOperator(2, factPages);
        Operator joined = new HashJoinOperator(allocator, fact, 0, new TableOperator(2, dimPages), 0);
        // joined columns: 0=fk, 1=measure, 2=dkey, 3=dattr
        Operator aggregation = new GroupedAggregationOperator(allocator, List.of(3), List.of(new Sum(1)), joined);

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
        Benchmarks.benchmark(BenchmarkCompiledJoin.class).run();
    }
}
