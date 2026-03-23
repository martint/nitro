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
package org.weakref.nitro.trino;

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

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTrinoClickBenchHits
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
    public void query0SelectAll()
    {
        support.consumeQuery0SelectAll(clickBenchHitsPath);
    }

    @Benchmark
    public Object query1CountAll()
    {
        return support.query1CountAll(clickBenchHitsPath);
    }

    @Benchmark
    public Object query3SumAdvEngineAndAvgResolutionWidth()
    {
        return support.query3SumAdvEngineAndAvgResolutionWidth(clickBenchHitsPath);
    }

    @Benchmark
    public Object query7MinAndMaxEventDate()
    {
        return support.query7MinAndMaxEventDate(clickBenchHitsPath);
    }

    @Benchmark
    public Object query8GroupByAdvEngineId()
    {
        return support.query8GroupByAdvEngineId(clickBenchHitsPath);
    }

    @Benchmark
    public Object query13TopSearchPhrases()
    {
        return support.query13TopSearchPhrases(clickBenchHitsPath);
    }

    @Benchmark
    public Object query34TopUrls()
    {
        return support.query34TopUrls(clickBenchHitsPath);
    }
}
