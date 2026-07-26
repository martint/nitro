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
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Three-way comparison of the same compute-bound pipeline {@code SELECT sum(a * b) WHERE a > k} over
 * identical in-memory data:
 * <ul>
 *     <li>{@link #interpreted()}: Nitro's operator tree (TableOperator -> Filter -> Project -> Aggregation),</li>
 *     <li>{@link #fused()}: a hand-written fused loop (the compiler's theoretical ceiling),</li>
 *     <li>{@link #jitCompiled()}: the prototype query compiler — generated Java, compiled in-process, run.</li>
 * </ul>
 * jitCompiled tracking fused (and both crushing interpreted) is the proof the codegen mechanism reaches the
 * ceiling.
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledPipeline
{
    private static final int ROWS = 16_000_000;
    private static final int CHUNK = 4096;
    private static final long THRESHOLD = 500;

    private final Allocator allocator = new Allocator(EngineResources.createDefault());
    private final PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();

    private long[] a;
    private long[] b;
    private List<TableOperator.Page> pages;
    private CompiledPipeline compiled;

    @Setup
    public void setup()
    {
        a = new long[ROWS];
        b = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            a[i] = i % 1000;            // 0..999 -> ~50% pass a > 500
            b[i] = (i % 7) + 1;         // 1..7
        }

        // Pages for the interpreted operator tree, chunked like a real scan.
        pages = new ArrayList<>();
        for (int off = 0; off < ROWS; off += CHUNK) {
            int len = Math.min(CHUNK, ROWS - off);
            long[] ca = new long[len];
            long[] cb = new long[len];
            System.arraycopy(a, off, ca, 0, len);
            System.arraycopy(b, off, cb, 0, len);
            pages.add(new TableOperator.Page(
                    len,
                    new Streams[] {Streams.ofValues(new I64Vector(ca)), Streams.ofValues(new I64Vector(cb))},
                    Mask.all(len)));
        }

        // The compiled pipeline: sum(a*b) WHERE a > THRESHOLD.
        Plan.Pipeline plan = new Plan.Pipeline(
                2,
                List.of(new Plan.Predicate(">", new Plan.Col(0), new Plan.Lit(THRESHOLD))),
                List.of(),
                List.of(new Plan.Aggregate("sum", new Plan.Bin("*", new Plan.Col(0), new Plan.Col(1)))));
        compiled = new PipelineCompiler(CompilerResources.createDefault()).compile(plan);

        long f = fused();
        long i = interpreted();
        long j = jitCompiled();
        if (f != i || f != j) {
            throw new IllegalStateException("mismatch: fused=" + f + " interpreted=" + i + " jit=" + j);
        }
    }

    @Benchmark
    public long fused()
    {
        long sum = 0;
        for (int i = 0; i < ROWS; i++) {
            if (a[i] > THRESHOLD) {
                sum += a[i] * b[i];
            }
        }
        return sum;
    }

    @Benchmark
    public long jitCompiled()
    {
        return compiled.execute(new long[][][] {{a, b}}, new int[] {ROWS}).columns()[0][0];
    }

    @Benchmark
    public long interpreted()
    {
        Variable literal = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal(THRESHOLD), AllMask.ALL),
                        new Assignment(predicate, new Call("lt", List.of(
                                new Reference(literal, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL)),
                List.of());

        Operator source = new TableOperator(2, pages);
        Operator filter = new FilterOperator(
                source,
                filterPlan,
                primitiveRegistry,
                new ReferenceMask(new Reference(predicate, Stream.VALUES)),
                allocator,
                allocator.engineResources().operatorResources().filter());

        Variable product = new Variable(0);
        EvaluationPlan projectPlan = new EvaluationPlan(
                List.of(new Assignment(product, new Call("multiply", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(product, Stream.VALUES)));
        Operator project = new ProjectOperator(allocator, projectPlan, primitiveRegistry, filter);
        Operator aggregation = new AggregationOperator(allocator, List.of(new Sum(0)), project);

        long sum = 0;
        try (aggregation) {
            while (aggregation.hasNext()) {
                try (var batch = aggregation.next()) {
                    Mask mask = batch.borrowMask();
                    I64Vector result = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    for (int index = 0; index < mask.selectedCount(); index++) {
                        sum += result.values()[mask.position(index)];
                    }
                }
            }
        }
        return sum;
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkCompiledPipeline.class).run();
    }
}
