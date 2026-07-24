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
import org.weakref.nitro.legacy.pipeline.CompiledPipeline;
import org.weakref.nitro.legacy.pipeline.CompilerResources;
import org.weakref.nitro.legacy.pipeline.PipelineCompiler;
import org.weakref.nitro.legacy.pipeline.Plan;
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
 * Composite (two-key) grouped aggregation, interpreted vs JIT-compiled:
 * {@code SELECT k0, k1, sum(v) GROUP BY k0, k1} over 16M rows. The compiled path stores both key components
 * in parallel slot arrays, folds them into one hash, and compares both on collision — measuring whether
 * compilation's dispatch removal still pays once the grouping key is composite. Keys are scrambled into
 * fact-scan order so the hash table sees a realistic, non-sequential probe stream.
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 4, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledMultiKeyGroupBy
{
    private static final int ROWS = 16_000_000;
    private static final int CARDINALITY0 = 2_000;
    private static final int CARDINALITY1 = 500;
    private static final int CHUNK = 4096;

    private final Allocator allocator = new Allocator(EngineResources.createDefault());

    private long[] k0;
    private long[] k1;
    private long[] v;
    private List<TableOperator.Page> pages;
    private CompiledPipeline compiled;

    @Setup
    public void setup()
    {
        k0 = new long[ROWS];
        k1 = new long[ROWS];
        v = new long[ROWS];
        long scramble = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < ROWS; i++) {
            long h = i * scramble;
            h ^= h >>> 29;
            k0[i] = Math.floorMod(h, CARDINALITY0);
            k1[i] = Math.floorMod(h >>> 17, CARDINALITY1);
            v[i] = (i % 100) + 1;
        }

        pages = new ArrayList<>();
        for (int off = 0; off < ROWS; off += CHUNK) {
            int len = Math.min(CHUNK, ROWS - off);
            long[] a0 = new long[len];
            long[] a1 = new long[len];
            long[] av = new long[len];
            System.arraycopy(k0, off, a0, 0, len);
            System.arraycopy(k1, off, a1, 0, len);
            System.arraycopy(v, off, av, 0, len);
            pages.add(new TableOperator.Page(
                    len,
                    new Streams[] {
                            Streams.ofValues(new I64Vector(a0)),
                            Streams.ofValues(new I64Vector(a1)),
                            Streams.ofValues(new I64Vector(av))},
                    Mask.all(len)));
        }

        Plan.Pipeline plan = new Plan.Pipeline(
                3,
                List.of(),
                List.of(new Plan.Col(0), new Plan.Col(1)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(2))));
        compiled = new PipelineCompiler(CompilerResources.createDefault()).compile(plan);

        if (jitCompiled() != interpreted()) {
            throw new IllegalStateException("mismatch: jit=" + jitCompiled() + " interpreted=" + interpreted());
        }
    }

    @Benchmark
    public long jitCompiled()
    {
        CompiledPipeline.Result result = compiled.execute(new long[][][] {{k0, k1, v}}, new int[] {ROWS});
        long checksum = 0;
        long[] sums = result.columns()[2];
        for (int g = 0; g < result.rowCount(); g++) {
            checksum += sums[g];
        }
        return checksum;
    }

    @Benchmark
    public long interpreted()
    {
        Operator source = new TableOperator(3, pages);
        Operator aggregation = new GroupedAggregationOperator(allocator, List.of(0, 1), List.of(new Sum(2)), source);

        long checksum = 0;
        try (aggregation) {
            while (aggregation.hasNext()) {
                try (var batch = aggregation.next()) {
                    Mask mask = batch.borrowMask();
                    I64Vector sums = (I64Vector) batch.output(2).borrow(Stream.VALUES);
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
        Benchmarks.benchmark(BenchmarkCompiledMultiKeyGroupBy.class).run();
    }
}
