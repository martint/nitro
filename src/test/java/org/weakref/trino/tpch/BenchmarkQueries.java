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
package org.weakref.trino.tpch;

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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.tpch.TpchParquetTables;
import org.weakref.nitro.trino.TrinoTpchParquetSupport;

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
    private TrinoTpchParquetSupport support;
    private TpchParquetTables tables;

    @Setup
    public void setup()
    {
        support = new TrinoTpchParquetSupport();
        // Schema overridable via -Dnitro.tpch.parquet.schema (defaults to sf10).
        tables = TpchParquetTables.requiredActual();
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
    public Object query05()
    {
        return support.query05(tables);
    }

    @Benchmark
    public Object query06()
    {
        return support.query06(tables);
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
    public Object query09()
    {
        return support.query09(tables);
    }

    @Benchmark
    public Object query10()
    {
        return support.query10(tables);
    }

    @Benchmark
    public Object query11()
    {
        return support.query11(tables);
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
    public Object query16()
    {
        return support.query16(tables);
    }

    @Benchmark
    public Object query17()
    {
        return support.query17(tables);
    }

    @Benchmark
    public Object query18()
    {
        return support.query18(tables);
    }

    @Benchmark
    public Object query19()
    {
        return support.query19(tables);
    }

    @Benchmark
    public Object query20()
    {
        return support.query20(tables);
    }

    @Benchmark
    public Object query21()
    {
        return support.query21(tables);
    }

    @Benchmark
    public Object query22()
    {
        return support.query22(tables);
    }
}
