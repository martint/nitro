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
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkQueries
{
    private final Allocator allocator = new Allocator();
    private final PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
    private TpcdsParquetTables tables;

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual("sf10");
    }

    @Benchmark
    public void query01()
    {
        consume(TpcdsParquetSupport.query01(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query06()
    {
        consume(TpcdsParquetSupport.query06(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query41()
    {
        consume(TpcdsParquetSupport.query41(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query44()
    {
        consume(TpcdsParquetSupport.query44(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query45()
    {
        consume(TpcdsParquetSupport.query45(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query51()
    {
        consume(TpcdsParquetSupport.query51(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query53()
    {
        consume(TpcdsParquetSupport.query53(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query54()
    {
        consume(TpcdsParquetSupport.query54(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query58()
    {
        consume(TpcdsParquetSupport.query58(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query61()
    {
        consume(TpcdsParquetSupport.query61(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query57()
    {
        consume(TpcdsParquetSupport.query57(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query80()
    {
        consume(TpcdsParquetSupport.query80(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query81()
    {
        consume(TpcdsParquetSupport.query81(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query97()
    {
        consume(TpcdsParquetSupport.query97(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query10()
    {
        consume(TpcdsParquetSupport.query10(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query35()
    {
        consume(TpcdsParquetSupport.query35(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query62()
    {
        consume(TpcdsParquetSupport.query62(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query73()
    {
        consume(TpcdsParquetSupport.query73(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query69()
    {
        consume(TpcdsParquetSupport.query69(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query67()
    {
        consume(TpcdsParquetSupport.query67(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query70()
    {
        consume(TpcdsParquetSupport.query70(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query84()
    {
        consume(TpcdsParquetSupport.query84(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query90()
    {
        consume(TpcdsParquetSupport.query90(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query88()
    {
        consume(TpcdsParquetSupport.query88(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query96()
    {
        consume(TpcdsParquetSupport.query96(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query99()
    {
        consume(TpcdsParquetSupport.query99(allocator, primitiveRegistry, tables));
    }

    private static void consume(Operator operator)
    {
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
