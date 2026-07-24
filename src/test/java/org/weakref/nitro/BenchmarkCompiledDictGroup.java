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
import org.weakref.nitro.jit.Column;
import org.weakref.nitro.jit.ColumnEncoding;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.CompilerResources;
import org.weakref.nitro.jit.PipelineCompiler;
import org.weakref.nitro.jit.Plan;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The dictionary group-on-id win: {@code SELECT k, sum(v) GROUP BY k} over 16M rows where {@code k} has a
 * small dictionary (1000 entries) but sparse values (so the value domain far exceeds the array cap). Grouping
 * the column as flat falls back to the hash table; grouping the same column as dictionary-encoded groups on
 * the dense id (array mode) and reconstructs the value at finalize. Both produce identical results.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 4, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledDictGroup
{
    private static final int ROWS = 16_000_000;
    private static final int DICT_SIZE = 1_000;
    private static final long VALUE_STRIDE = 200_000;   // value range ~2e8 > array cap, so flat grouping hashes

    private long[] flatKey;
    private long[] value;
    private int[] ids;
    private long[] dictionary;
    private CompiledPipeline flatCompiled;
    private CompiledPipeline dictCompiled;

    @Setup
    public void setup()
    {
        dictionary = new long[DICT_SIZE];
        for (int d = 0; d < DICT_SIZE; d++) {
            dictionary[d] = d * VALUE_STRIDE;
        }
        flatKey = new long[ROWS];
        value = new long[ROWS];
        ids = new int[ROWS];
        long scramble = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < ROWS; i++) {
            long h = i * scramble;
            h ^= h >>> 29;
            int id = (int) Math.floorMod(h, DICT_SIZE);
            ids[i] = id;
            flatKey[i] = dictionary[id];
            value[i] = (i % 100) + 1;
        }

        Plan.Pipeline plan = new Plan.Pipeline(
                2,
                List.of(),
                List.of(new Plan.Col(0)),
                List.of(new Plan.Aggregate("sum", new Plan.Col(1))));
        flatCompiled = new PipelineCompiler(CompilerResources.createDefault()).compile(plan);
        dictCompiled = new PipelineCompiler(CompilerResources.createDefault()).compile(plan, new ColumnEncoding[][] {{ColumnEncoding.DICTIONARY, ColumnEncoding.FLAT}});

        if (flatGroup() != dictGroup()) {
            throw new IllegalStateException("mismatch: flat=" + flatGroup() + " dict=" + dictGroup());
        }
    }

    @Benchmark
    public long flatGroup()
    {
        CompiledPipeline.Result result = flatCompiled.execute(new long[][][] {{flatKey, value}}, new int[] {ROWS});
        return checksum(result);
    }

    @Benchmark
    public long dictGroup()
    {
        Column[][] inputs = {{new Column.DictionaryColumn(ids, dictionary), new Column.FlatColumn(value)}};
        CompiledPipeline.Result result = dictCompiled.execute(inputs, new int[] {ROWS});
        return checksum(result);
    }

    private static long checksum(CompiledPipeline.Result result)
    {
        long checksum = 0;
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            checksum += sums[g];
        }
        return checksum;
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkCompiledDictGroup.class).run();
    }
}
