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
package org.weakref.trino.clickbench;

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
import org.weakref.nitro.trino.TrinoClickBenchSupport;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Trino operator controls named with ClickBench's published zero-based query IDs.
 *
 * <p>The underlying support methods retain their historical one-based names, so benchmark {@code queryNN} invokes
 * support method {@code query(NN + 1)}. The non-query full-column reader probe is exposed separately as
 * {@link #allColumnsScan()}.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkQueries
{
    private TrinoClickBenchSupport support;
    private Path clickBenchHitsPath;

    @Setup
    public void setup()
    {
        support = new TrinoClickBenchSupport();
        clickBenchHitsPath = support.requiredActualHitsPath();
    }

    @TearDown
    public void tearDown()
    {
        support.close();
    }

    @Benchmark
    public void allColumnsScan()
    {
        support.consumeQuery00(clickBenchHitsPath);
    }

    @Benchmark
    public Object query00()
    {
        return support.query01(clickBenchHitsPath);
    }

    @Benchmark
    public Object query01()
    {
        return support.query02(clickBenchHitsPath);
    }

    @Benchmark
    public Object query02()
    {
        return support.query03(clickBenchHitsPath);
    }

    @Benchmark
    public Object query03()
    {
        return support.query04(clickBenchHitsPath);
    }

    @Benchmark
    public Object query04()
    {
        return support.query05(clickBenchHitsPath);
    }

    @Benchmark
    public Object query05()
    {
        return support.query06(clickBenchHitsPath);
    }

    @Benchmark
    public Object query06()
    {
        return support.query07(clickBenchHitsPath);
    }

    @Benchmark
    public Object query07()
    {
        return support.query08(clickBenchHitsPath);
    }

    @Benchmark
    public Object query08()
    {
        return support.query09(clickBenchHitsPath);
    }

    @Benchmark
    public Object query09()
    {
        return support.query10(clickBenchHitsPath);
    }

    @Benchmark
    public Object query10()
    {
        return support.query11(clickBenchHitsPath);
    }

    @Benchmark
    public Object query11()
    {
        return support.query12(clickBenchHitsPath);
    }

    @Benchmark
    public Object query12()
    {
        return support.query13(clickBenchHitsPath);
    }

    @Benchmark
    public Object query13()
    {
        return support.query14(clickBenchHitsPath);
    }

    @Benchmark
    public Object query14()
    {
        return support.query15(clickBenchHitsPath);
    }

    @Benchmark
    public Object query15()
    {
        return support.query16(clickBenchHitsPath);
    }

    @Benchmark
    public Object query16()
    {
        return support.query17(clickBenchHitsPath);
    }

    @Benchmark
    public Object query17()
    {
        return support.query18(clickBenchHitsPath);
    }

    @Benchmark
    public Object query18()
    {
        return support.query19(clickBenchHitsPath);
    }

    @Benchmark
    public Object query19()
    {
        return support.query20(clickBenchHitsPath);
    }

    @Benchmark
    public Object query20()
    {
        return support.query21(clickBenchHitsPath);
    }

    @Benchmark
    public Object query21()
    {
        return support.query22(clickBenchHitsPath);
    }

    @Benchmark
    public Object query22()
    {
        return support.query23(clickBenchHitsPath);
    }

    @Benchmark
    public Object query23()
    {
        return support.query24(clickBenchHitsPath);
    }

    @Benchmark
    public Object query24()
    {
        return support.query25(clickBenchHitsPath);
    }

    @Benchmark
    public Object query25()
    {
        return support.query26(clickBenchHitsPath);
    }

    @Benchmark
    public Object query26()
    {
        return support.query27(clickBenchHitsPath);
    }

    @Benchmark
    public Object query27()
    {
        return support.query28(clickBenchHitsPath);
    }

    @Benchmark
    public Object query28()
    {
        return support.query29(clickBenchHitsPath);
    }

    @Benchmark
    public Object query29()
    {
        return support.query30(clickBenchHitsPath);
    }

    @Benchmark
    public Object query30()
    {
        return support.query31(clickBenchHitsPath);
    }

    @Benchmark
    public Object query31()
    {
        return support.query32(clickBenchHitsPath);
    }

    @Benchmark
    public Object query32()
    {
        return support.query33(clickBenchHitsPath);
    }

    @Benchmark
    public Object query33()
    {
        return support.query34(clickBenchHitsPath);
    }

    @Benchmark
    public Object query34()
    {
        return support.query35(clickBenchHitsPath);
    }

    @Benchmark
    public Object query35()
    {
        return support.query36(clickBenchHitsPath);
    }

    @Benchmark
    public Object query36()
    {
        return support.query37(clickBenchHitsPath);
    }

    @Benchmark
    public Object query37()
    {
        return support.query38(clickBenchHitsPath);
    }

    @Benchmark
    public Object query38()
    {
        return support.query39(clickBenchHitsPath);
    }

    @Benchmark
    public Object query39()
    {
        return support.query40(clickBenchHitsPath);
    }

    @Benchmark
    public Object query40()
    {
        return support.query41(clickBenchHitsPath);
    }

    @Benchmark
    public Object query41()
    {
        return support.query42(clickBenchHitsPath);
    }

    @Benchmark
    public Object query42()
    {
        return support.query43(clickBenchHitsPath);
    }
}
