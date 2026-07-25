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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.Benchmarks;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkQueries
{
    private final EngineResources engineResources = EngineResources.createDefault();
    private final Allocator allocator = new Allocator(engineResources);
    private final PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
    private Path clickBenchHitsDirectory;

    @Setup
    public void setup()
    {
        clickBenchHitsDirectory = ClickBenchHitsSupport.requiredActualHitsDirectory();
    }

    @Benchmark
    public void query00()
    {
        consume(ClickBenchHitsSupport.query00(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query01()
    {
        consume(ClickBenchHitsSupport.query01(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query02()
    {
        consume(ClickBenchHitsSupport.query02(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query03()
    {
        consume(ClickBenchHitsSupport.query03(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query04()
    {
        consume(ClickBenchHitsSupport.query04(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query05()
    {
        consume(ClickBenchHitsSupport.query05(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query06()
    {
        consume(ClickBenchHitsSupport.query06(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query07()
    {
        consume(ClickBenchHitsSupport.query07(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query08()
    {
        consume(ClickBenchHitsSupport.query08(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query09()
    {
        consume(ClickBenchHitsSupport.query09(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query10()
    {
        consume(ClickBenchHitsSupport.query10(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query11()
    {
        consume(ClickBenchHitsSupport.query11(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query12()
    {
        consume(ClickBenchHitsSupport.query12(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query13()
    {
        consume(ClickBenchHitsSupport.query13(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query14()
    {
        consume(ClickBenchHitsSupport.query14(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query15()
    {
        consume(ClickBenchHitsSupport.query15(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query16()
    {
        consume(ClickBenchHitsSupport.query16(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query17()
    {
        consume(ClickBenchHitsSupport.query17(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query18()
    {
        consume(ClickBenchHitsSupport.query18(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query19()
    {
        consume(ClickBenchHitsSupport.query19(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query20()
    {
        consume(ClickBenchHitsSupport.query20(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query22()
    {
        consume(ClickBenchHitsSupport.query22(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query23()
    {
        consume(ClickBenchHitsSupport.query23(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query24()
    {
        consume(ClickBenchHitsSupport.query24(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query25()
    {
        consume(ClickBenchHitsSupport.query25(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query21()
    {
        consume(ClickBenchHitsSupport.query21(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query26()
    {
        consume(ClickBenchHitsSupport.query26(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query27()
    {
        consume(ClickBenchHitsSupport.query27(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query28()
    {
        consume(ClickBenchHitsSupport.query28(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query29()
    {
        consume(ClickBenchHitsSupport.query29(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query30()
    {
        consume(ClickBenchHitsSupport.query30(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query31()
    {
        consume(ClickBenchHitsSupport.query31(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query32()
    {
        consume(ClickBenchHitsSupport.query32(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query33()
    {
        consume(ClickBenchHitsSupport.query33(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query34()
    {
        consume(ClickBenchHitsSupport.query34(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query35()
    {
        consume(ClickBenchHitsSupport.query35(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query36()
    {
        consume(ClickBenchHitsSupport.query36(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query37()
    {
        consume(ClickBenchHitsSupport.query37(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query38()
    {
        consume(ClickBenchHitsSupport.query38(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query39()
    {
        consume(ClickBenchHitsSupport.query39(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query40()
    {
        consume(ClickBenchHitsSupport.query40(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query41()
    {
        consume(ClickBenchHitsSupport.query41(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query42()
    {
        consume(ClickBenchHitsSupport.query42(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query43()
    {
        consume(ClickBenchHitsSupport.query43(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    private void consume(Operator operator)
    {
        allocator.beginExecution();
        try (operator) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }
                    for (int column = 0; column < operator.outputCount(); column++) {
                        consume(batch.output(column).borrow(Stream.VALUES));
                    }
                }
            }
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private static void consume(Vector vector)
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkQueries.class)
                .run();
    }
}
