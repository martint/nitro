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
    public Object query06()
    {
        return support.query06(tables);
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
    public Object query97()
    {
        return support.query97(tables);
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
