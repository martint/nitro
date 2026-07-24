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
package org.weakref.nitro.jit;

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
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.MultiplyI64;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The batch-function-fusion thesis, measured: {@code a*b + c*d} over a dense I64 batch, computed two ways with
 * reused (allocation-free) buffers so the comparison is pure compute. {@link #fused} is one compiled bespoke
 * {@link PrimitiveFunction} -- a single loop, intermediates in registers, the auto-vectorizable shape;
 * {@link #interpreted} is the per-node model the evaluator uses -- three atomic batch primitives (multiply,
 * multiply, add) each making a pass and materializing an intermediate buffer.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBatchFunction
{
    private static final Set<Stream> VALUES = Set.of(Stream.VALUES);

    @Param("65536")
    public int rows;

    private PrimitiveFunction fused;
    private PrimitiveFunction fusedVector;
    private final PrimitiveFunction multiply = new MultiplyI64();
    private final PrimitiveFunction add = new AddI64();
    private List<Streams> inputs;
    private Mask mask;
    private PrimitiveExecutionContext context;
    private Streams fusedOut;
    private Streams ab;
    private Streams cd;
    private Streams sum;

    @Setup
    public void setup()
    {
        Plan.Expr expression = new Plan.Bin("+",
                new Plan.Bin("*", new Plan.Col(0), new Plan.Col(1)),
                new Plan.Bin("*", new Plan.Col(2), new Plan.Col(3)));
        fused = new BatchFunctionCompiler(CompilerResources.createDefault()).compile(expression, false);
        fusedVector = new BatchFunctionCompiler(CompilerResources.createDefault()).compile(expression, true);
        long[] a = new long[rows];
        long[] b = new long[rows];
        long[] c = new long[rows];
        long[] d = new long[rows];
        for (int i = 0; i < rows; i++) {
            a[i] = i;
            b[i] = (i % 7) + 1;
            c[i] = (i % 13) + 1;
            d[i] = (i % 5) + 1;
        }
        inputs = List.of(Streams.ofValues(new I64Vector(a)), Streams.ofValues(new I64Vector(b)),
                Streams.ofValues(new I64Vector(c)), Streams.ofValues(new I64Vector(d)));
        mask = Mask.all(rows);
        context = new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault()));
        // Reusable output buffers -> allocation-free steady state for both sides.
        fusedOut = Streams.ofValues(new I64Vector(rows));
        ab = Streams.ofValues(new I64Vector(rows));
        cd = Streams.ofValues(new I64Vector(rows));
        sum = Streams.ofValues(new I64Vector(rows));
    }

    @Benchmark
    public Object fused()
    {
        return fused.apply(inputs, mask, VALUES, fusedOut, context);
    }

    @Benchmark
    public Object fusedVector()
    {
        return fusedVector.apply(inputs, mask, VALUES, fusedOut, context);
    }

    @Benchmark
    public Object interpreted()
    {
        Streams t0 = multiply.apply(List.of(inputs.get(0), inputs.get(1)), mask, VALUES, ab, context);
        Streams t1 = multiply.apply(List.of(inputs.get(2), inputs.get(3)), mask, VALUES, cd, context);
        return add.apply(List.of(t0, t1), mask, VALUES, sum, context);
    }
}
