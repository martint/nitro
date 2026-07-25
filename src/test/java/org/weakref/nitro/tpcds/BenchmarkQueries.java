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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

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
    private TpcdsParquetTables tables;

    @Setup
    public void setup()
    {
        // Schema overridable via -Dnitro.tpcds.parquet.schema (defaults to sf10).
        tables = TpcdsParquetTables.requiredActual();
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(engineResources);
    }

    @Benchmark
    public void query01()
    {
        consume(TpcdsParquetSupport.query01(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query02()
    {
        consume(TpcdsParquetSupport.query02(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query06()
    {
        consume(TpcdsParquetSupport.query06(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query12()
    {
        consume(TpcdsParquetSupport.query12(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query13()
    {
        consume(TpcdsParquetSupport.query13(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query20()
    {
        consume(TpcdsParquetSupport.query20(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query16()
    {
        consume(TpcdsParquetSupport.query16(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query17()
    {
        consume(TpcdsParquetSupport.query17(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query18()
    {
        consume(TpcdsParquetSupport.query18(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query22()
    {
        consume(TpcdsParquetSupport.query22(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query27()
    {
        consume(TpcdsParquetSupport.query27(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query28()
    {
        consume(TpcdsParquetSupport.query28(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query42()
    {
        consume(TpcdsParquetSupport.query42(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query43()
    {
        consume(TpcdsParquetSupport.query43(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query46()
    {
        consume(TpcdsParquetSupport.query46(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query34()
    {
        consume(TpcdsParquetSupport.query34(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query50()
    {
        consume(TpcdsParquetSupport.query50(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query52()
    {
        consume(TpcdsParquetSupport.query52(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query68()
    {
        consume(TpcdsParquetSupport.query68(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query79()
    {
        consume(TpcdsParquetSupport.query79(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query93()
    {
        consume(TpcdsParquetSupport.query93(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query91()
    {
        consume(TpcdsParquetSupport.query91(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query82()
    {
        consume(TpcdsParquetSupport.query82(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query37()
    {
        consume(TpcdsParquetSupport.query37(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query40()
    {
        consume(TpcdsParquetSupport.query40(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query55()
    {
        consume(TpcdsParquetSupport.query55(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query71()
    {
        consume(TpcdsParquetSupport.query71(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query33()
    {
        consume(TpcdsParquetSupport.query33(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query56()
    {
        consume(TpcdsParquetSupport.query56(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query60()
    {
        consume(TpcdsParquetSupport.query60(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query65()
    {
        consume(TpcdsParquetSupport.query65(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query66()
    {
        consume(TpcdsParquetSupport.query66(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query98()
    {
        consume(TpcdsParquetSupport.query98(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query89()
    {
        consume(TpcdsParquetSupport.query89(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query63()
    {
        consume(TpcdsParquetSupport.query63(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query64()
    {
        consume(TpcdsParquetSupport.query64(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query86()
    {
        consume(TpcdsParquetSupport.query86(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query36()
    {
        consume(TpcdsParquetSupport.query36(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query49()
    {
        consume(TpcdsParquetSupport.query49(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query47()
    {
        consume(TpcdsParquetSupport.query47(allocator, primitiveRegistry, tables));
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
    public void query59()
    {
        consume(TpcdsParquetSupport.query59(allocator, primitiveRegistry, tables));
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
    public void query30()
    {
        consume(TpcdsParquetSupport.query30(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query29()
    {
        consume(TpcdsParquetSupport.query29(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query76()
    {
        consume(TpcdsParquetSupport.query76(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query31()
    {
        consume(TpcdsParquetSupport.query31(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query32()
    {
        consume(TpcdsParquetSupport.query32(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query81()
    {
        consume(TpcdsParquetSupport.query81(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query83()
    {
        consume(TpcdsParquetSupport.query83(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query74()
    {
        consume(TpcdsParquetSupport.query74(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query75()
    {
        consume(TpcdsParquetSupport.query75(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query78()
    {
        consume(TpcdsParquetSupport.query78(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query87()
    {
        consume(TpcdsParquetSupport.query87(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query85()
    {
        consume(TpcdsParquetSupport.query85(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query97()
    {
        consume(TpcdsParquetSupport.query97(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query23()
    {
        consume(TpcdsParquetSupport.query23(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query24()
    {
        consume(TpcdsParquetSupport.query24(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query25()
    {
        consume(TpcdsParquetSupport.query25(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query77()
    {
        consume(TpcdsParquetSupport.query77(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query05()
    {
        consume(TpcdsParquetSupport.query05(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query94()
    {
        consume(TpcdsParquetSupport.query94(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query95()
    {
        consume(TpcdsParquetSupport.query95(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query38()
    {
        consume(TpcdsParquetSupport.query38(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query48()
    {
        consume(TpcdsParquetSupport.query48(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query09()
    {
        consume(TpcdsParquetSupport.query09(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query03()
    {
        consume(TpcdsParquetSupport.query03(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query04()
    {
        consume(TpcdsParquetSupport.query04(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query07()
    {
        consume(TpcdsParquetSupport.query07(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query08()
    {
        consume(TpcdsParquetSupport.query08(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query11()
    {
        consume(TpcdsParquetSupport.query11(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query14()
    {
        consume(TpcdsParquetSupport.query14(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query15()
    {
        consume(TpcdsParquetSupport.query15(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query19()
    {
        consume(TpcdsParquetSupport.query19(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query21()
    {
        consume(TpcdsParquetSupport.query21(allocator, primitiveRegistry, tables));
    }

    @Benchmark
    public void query26()
    {
        consume(TpcdsParquetSupport.query26(allocator, primitiveRegistry, tables));
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
    public void query39()
    {
        consume(TpcdsParquetSupport.query39(allocator, primitiveRegistry, tables));
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
    public void query72()
    {
        consume(TpcdsParquetSupport.query72(allocator, primitiveRegistry, tables));
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
    public void query92()
    {
        consume(TpcdsParquetSupport.query92(allocator, primitiveRegistry, tables));
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
