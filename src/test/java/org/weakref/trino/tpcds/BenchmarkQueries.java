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
package org.weakref.trino.tpcds;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.tpcds.TpcdsParquetTables;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkQueries
{
    private TrinoTpcdsParquetSupport support;
    private TpcdsParquetTables tables;

    @Setup
    public void setup()
    {
        support = new TrinoTpcdsParquetSupport();
        tables = TpcdsParquetTables.requiredActual("sf10");
    }

    @TearDown
    public void tearDown()
    {
        support.close();
    }

    @Benchmark
    public Object query01()
    {
        return support.query01(tables);
    }

    @Benchmark
    public Object query02()
    {
        return support.query02(tables);
    }

    @Benchmark
    public Object query06()
    {
        return support.query06(tables);
    }

    @Benchmark
    public Object query12()
    {
        return support.query12(tables);
    }

    @Benchmark
    public Object query13()
    {
        return support.query13(tables);
    }

    @Benchmark
    public Object query20()
    {
        return support.query20(tables);
    }

    @Benchmark
    public Object query16()
    {
        return support.query16(tables);
    }

    @Benchmark
    public Object query18()
    {
        return support.query18(tables);
    }

    @Benchmark
    public Object query22()
    {
        return support.query22(tables);
    }

    @Benchmark
    public Object query27()
    {
        return support.query27(tables);
    }

    @Benchmark
    public Object query28()
    {
        return support.query28(tables);
    }

    @Benchmark
    public Object query42()
    {
        return support.query42(tables);
    }

    @Benchmark
    public Object query43()
    {
        return support.query43(tables);
    }

    @Benchmark
    public Object query46()
    {
        return support.query46(tables);
    }

    @Benchmark
    public Object query34()
    {
        return support.query34(tables);
    }

    @Benchmark
    public Object query50()
    {
        return support.query50(tables);
    }

    @Benchmark
    public Object query52()
    {
        return support.query52(tables);
    }

    @Benchmark
    public Object query68()
    {
        return support.query68(tables);
    }

    @Benchmark
    public Object query79()
    {
        return support.query79(tables);
    }

    @Benchmark
    public Object query93()
    {
        return support.query93(tables);
    }

    @Benchmark
    public Object query91()
    {
        return support.query91(tables);
    }

    @Benchmark
    public Object query82()
    {
        return support.query82(tables);
    }

    @Benchmark
    public Object query37()
    {
        return support.query37(tables);
    }

    @Benchmark
    public Object query40()
    {
        return support.query40(tables);
    }

    @Benchmark
    public Object query55()
    {
        return support.query55(tables);
    }

    @Benchmark
    public Object query71()
    {
        return support.query71(tables);
    }

    @Benchmark
    public Object query33()
    {
        return support.query33(tables);
    }

    @Benchmark
    public Object query56()
    {
        return support.query56(tables);
    }

    @Benchmark
    public Object query60()
    {
        return support.query60(tables);
    }

    @Benchmark
    public Object query65()
    {
        return support.query65(tables);
    }

    @Benchmark
    public Object query66()
    {
        return support.query66(tables);
    }

    @Benchmark
    public Object query98()
    {
        return support.query98(tables);
    }

    @Benchmark
    public Object query89()
    {
        return support.query89(tables);
    }

    @Benchmark
    public Object query63()
    {
        return support.query63(tables);
    }

    @Benchmark
    public Object query86()
    {
        return support.query86(tables);
    }

    @Benchmark
    public Object query36()
    {
        return support.query36(tables);
    }

    @Benchmark
    public Object query49()
    {
        return support.query49(tables);
    }

    @Benchmark
    public Object query47()
    {
        return support.query47(tables);
    }

    @Benchmark
    public Object query41()
    {
        return support.query41(tables);
    }

    @Benchmark
    public Object query44()
    {
        return support.query44(tables);
    }

    @Benchmark
    public Object query45()
    {
        return support.query45(tables);
    }

    @Benchmark
    public Object query51()
    {
        return support.query51(tables);
    }

    @Benchmark
    public Object query53()
    {
        return support.query53(tables);
    }

    @Benchmark
    public Object query54()
    {
        return support.query54(tables);
    }

    @Benchmark
    public Object query58()
    {
        return support.query58(tables);
    }

    @Benchmark
    public Object query59()
    {
        return support.query59(tables);
    }

    @Benchmark
    public Object query61()
    {
        return support.query61(tables);
    }

    @Benchmark
    public Object query57()
    {
        return support.query57(tables);
    }

    @Benchmark
    public Object query80()
    {
        return support.query80(tables);
    }

    @Benchmark
    public Object query30()
    {
        return support.query30(tables);
    }

    @Benchmark
    public Object query29()
    {
        return support.query29(tables);
    }

    @Benchmark
    public Object query76()
    {
        return support.query76(tables);
    }

    @Benchmark
    public Object query31()
    {
        return support.query31(tables);
    }

    @Benchmark
    public Object query32()
    {
        return support.query32(tables);
    }

    @Benchmark
    public Object query81()
    {
        return support.query81(tables);
    }

    @Benchmark
    public Object query83()
    {
        return support.query83(tables);
    }

    @Benchmark
    public Object query74()
    {
        return support.query74(tables);
    }

    @Benchmark
    public Object query75()
    {
        return support.query75(tables);
    }

    @Benchmark
    public Object query78()
    {
        return support.query78(tables);
    }

    @Benchmark
    public Object query87()
    {
        return support.query87(tables);
    }

    @Benchmark
    public Object query85()
    {
        return support.query85(tables);
    }

    @Benchmark
    public Object query97()
    {
        return support.query97(tables);
    }

    @Benchmark
    public Object query23()
    {
        return support.query23(tables);
    }

    @Benchmark
    public Object query24()
    {
        return support.query24(tables);
    }

    @Benchmark
    public Object query25()
    {
        return support.query25(tables);
    }

    @Benchmark
    public Object query77()
    {
        return support.query77(tables);
    }

    @Benchmark
    public Object query05()
    {
        return support.query05(tables);
    }

    @Benchmark
    public Object query94()
    {
        return support.query94(tables);
    }

    @Benchmark
    public Object query95()
    {
        return support.query95(tables);
    }

    @Benchmark
    public Object query38()
    {
        return support.query38(tables);
    }

    @Benchmark
    public Object query48()
    {
        return support.query48(tables);
    }

    @Benchmark
    public Object query09()
    {
        return support.query09(tables);
    }

    @Benchmark
    public Object query03()
    {
        return support.query03(tables);
    }

    @Benchmark
    public Object query04()
    {
        return support.query04(tables);
    }

    @Benchmark
    public Object query07()
    {
        return support.query07(tables);
    }

    @Benchmark
    public Object query08()
    {
        return support.query08(tables);
    }

    @Benchmark
    public Object query11()
    {
        return support.query11(tables);
    }

    @Benchmark
    public Object query14()
    {
        return support.query14(tables);
    }

    @Benchmark
    public Object query15()
    {
        return support.query15(tables);
    }

    @Benchmark
    public Object query19()
    {
        return support.query19(tables);
    }

    @Benchmark
    public Object query21()
    {
        return support.query21(tables);
    }

    @Benchmark
    public Object query26()
    {
        return support.query26(tables);
    }

    @Benchmark
    public Object query10()
    {
        return support.query10(tables);
    }

    @Benchmark
    public Object query35()
    {
        return support.query35(tables);
    }

    @Benchmark
    public Object query62()
    {
        return support.query62(tables);
    }

    @Benchmark
    public Object query73()
    {
        return support.query73(tables);
    }

    @Benchmark
    public Object query69()
    {
        return support.query69(tables);
    }

    @Benchmark
    public Object query67()
    {
        return support.query67(tables);
    }

    @Benchmark
    public Object query70()
    {
        return support.query70(tables);
    }

    @Benchmark
    public Object query84()
    {
        return support.query84(tables);
    }

    @Benchmark
    public Object query90()
    {
        return support.query90(tables);
    }

    @Benchmark
    public Object query92()
    {
        return support.query92(tables);
    }

    @Benchmark
    public Object query88()
    {
        return support.query88(tables);
    }

    @Benchmark
    public Object query96()
    {
        return support.query96(tables);
    }

    @Benchmark
    public Object query99()
    {
        return support.query99(tables);
    }
}
