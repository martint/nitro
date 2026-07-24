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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkQueries
{
    private final EngineResources engineResources = EngineResources.createDefault();
    private Allocator allocator;
    private final PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
    private TpchParquetTables tables;

    @Setup
    public void setup()
    {
        // Schema overridable via -Dnitro.tpch.parquet.schema (defaults to sf10).
        tables = TpchParquetTables.requiredActual();
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(engineResources);
    }

    @Benchmark
    public void query01()
    {
        consume(TpchParquetSupport.query01(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query02()
    {
        consume(TpchParquetSupport.query02(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query03()
    {
        consume(TpchParquetSupport.query03(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query04()
    {
        consume(TpchParquetSupport.query04(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query05()
    {
        consume(TpchParquetSupport.query05(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query06()
    {
        consume(TpchParquetSupport.query06(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query07()
    {
        consume(TpchParquetSupport.query07(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query08()
    {
        consume(TpchParquetSupport.query08(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query09()
    {
        consume(TpchParquetSupport.query09(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query10()
    {
        consume(TpchParquetSupport.query10(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query11()
    {
        consume(TpchParquetSupport.query11(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query12()
    {
        consume(TpchParquetSupport.query12(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query13()
    {
        consume(TpchParquetSupport.query13(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query14()
    {
        consume(TpchParquetSupport.query14(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query15()
    {
        consume(TpchParquetSupport.query15(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query16()
    {
        consume(TpchParquetSupport.query16(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query17()
    {
        consume(TpchParquetSupport.query17(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query18()
    {
        consume(TpchParquetSupport.query18(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query19()
    {
        consume(TpchParquetSupport.query19(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query20()
    {
        consume(TpchParquetSupport.query20(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query21()
    {
        consume(TpchParquetSupport.query21(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query22()
    {
        consume(TpchParquetSupport.query22(allocator, primitiveRegistry, tables));
    }

    private void consume(Operator operator)
    {
        allocator.beginExecution();
        try (operator) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    var mask = batch.borrowMask();
                    for (int column = 0; column < operator.outputCount(); column++) {
                        consume(batch.output(column).borrow(Stream.VALUES));
                    }
                    consume(mask.count());
                }
            }
        }
    }

    private static void consume(Vector vector)
    {
        consume(vector.length());
    }

    private static void consume(long value)
    {
        if (value == Long.MIN_VALUE) {
            throw new AssertionError();
        }
    }
}
