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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.tpcds.TpcdsParquetTables;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTpcdsParquet
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
    public void query41()
    {
        consume(TpcdsParquetSupport.query41(allocator, primitiveRegistry, tables));
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
