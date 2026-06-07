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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.QueryLowering;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Apples-to-apples benchmark of the data-centric compiler against the Nitro and Trino operator-chain harnesses
 * (their {@code BenchmarkQueries}), using the same framework and the same per-invocation read of sf10 Parquet --
 * no preloading. {@link #full} is the compiled engine's whole "from Parquet to result" cost; {@link #loadOnly}
 * scans the inputs into columns without running the compiled routine, so {@code full - loadOnly} isolates the
 * compute. The one-time Java compile is amortized in {@link #setup}, like a query plan.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledQueries
{
    @Param({"03", "07", "15", "26", "42", "48", "43", "52", "55", "62", "91", "96", "99", "32", "34", "73", "92"})
    public String query;

    private Allocator allocator;
    private TpcdsParquetTables tables;
    private final Map<String, QueryLowering.Lowered> lowered = new LinkedHashMap<>();
    private final Map<String, CompiledPipeline> compiled = new LinkedHashMap<>();
    private final Map<String, org.weakref.nitro.jit.StreamingPipeline> streaming = new LinkedHashMap<>();
    private final Map<String, CompiledTpcdsQueries.MultiStage> multiStage = new LinkedHashMap<>();
    private final Map<String, QueryLowering.Lowered> multiStageSub = new LinkedHashMap<>();
    private final Map<String, org.weakref.nitro.jit.StreamingPipeline> multiStageSubCompiled = new LinkedHashMap<>();
    private final Map<String, QueryLowering.Lowered> multiStageMain = new LinkedHashMap<>();
    private final Map<String, CompiledPipeline> multiStageMainCompiled = new LinkedHashMap<>();

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual();
        compile("03", CompiledTpcdsQueries.query03());
        compile("07", CompiledTpcdsQueries.query07());
        compile("15", CompiledTpcdsQueries.query15());
        compile("26", CompiledTpcdsQueries.query26());
        compile("42", CompiledTpcdsQueries.query42());
        compile("48", CompiledTpcdsQueries.query48());
        compile("43", CompiledTpcdsQueries.query43());
        compile("52", CompiledTpcdsQueries.query52());
        compile("55", CompiledTpcdsQueries.query55());
        compile("62", CompiledTpcdsQueries.query62());
        compile("91", CompiledTpcdsQueries.query91());
        compile("96", CompiledTpcdsQueries.query96());
        compile("99", CompiledTpcdsQueries.query99());
        compileMultiStage("32", CompiledTpcdsQueries.query32());
        compileMultiStage("34", CompiledTpcdsQueries.query34());
        compileMultiStage("73", CompiledTpcdsQueries.query73());
        compileMultiStage("92", CompiledTpcdsQueries.query92());
    }

    private void compile(String name, CompiledTpcdsQueries.Ported ported)
    {
        QueryLowering.Lowered plan = ported.query().lower();
        lowered.put(name, plan);
        compiled.put(name, plan.compile());   // one-time, like building a query plan
        streaming.put(name, org.weakref.nitro.jit.PipelineCompiler.compileStreaming(plan.pipeline(), plan.encodings(), plan.nullable()));
    }

    private void compileMultiStage(String name, CompiledTpcdsQueries.MultiStage staged)
    {
        multiStage.put(name, staged);
        QueryLowering.Lowered sub = staged.subquery().lower();
        QueryLowering.Lowered main = staged.main().lower();
        multiStageSub.put(name, sub);
        multiStageMain.put(name, main);
        multiStageSubCompiled.put(name, org.weakref.nitro.jit.PipelineCompiler.compileStreaming(sub.pipeline(), sub.encodings(), sub.nullable()));
        multiStageMainCompiled.put(name, main.compile());
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator();
    }

    @Benchmark
    public Object full()
    {
        if (multiStage.containsKey(query)) {
            return CompiledQuerySupport.runMultiStage(allocator, tables,
                    multiStageSub.get(query), multiStageSubCompiled.get(query),
                    multiStageMain.get(query), multiStageMainCompiled.get(query), multiStage.get(query).virtualTable());
        }
        CompiledQuerySupport.LoadedInputs inputs = CompiledQuerySupport.loadLoweredInputs(allocator, tables, lowered.get(query));
        return compiled.get(query).execute(inputs.inputs(), inputs.rowCounts());
    }

    @Benchmark
    public Object loadOnly()
    {
        // For a multi-stage query the dominant Parquet read is the subquery's fact scan; the main stage's other
        // input is the (in-memory) materialized subquery result, which has no Parquet table to load.
        QueryLowering.Lowered plan = multiStage.containsKey(query) ? multiStageSub.get(query) : lowered.get(query);
        return CompiledQuerySupport.loadLoweredInputs(allocator, tables, plan);
    }

    @Benchmark
    public Object streamingLazy()
    {
        // Single-stage queries stream the probe through the lazy source (selection-/join-driven late
        // materialization). Multi-stage queries are not single-pipeline; they fall back to the eager path so the
        // parameter stays comparable.
        if (multiStage.containsKey(query)) {
            return full();
        }
        return CompiledQuerySupport.runStreamingLowered(allocator, tables, lowered.get(query), streaming.get(query), true);
    }
}
