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
import org.weakref.nitro.jit.PipelineCompiler;
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
    @Param({"03", "07", "26", "42", "52", "55", "96"})
    public String query;

    private Allocator allocator;
    private TpcdsParquetTables tables;
    private final Map<String, QueryLowering.Lowered> lowered = new LinkedHashMap<>();
    private final Map<String, CompiledPipeline> compiled = new LinkedHashMap<>();
    private final Map<String, org.weakref.nitro.jit.StreamingPipeline> streaming = new LinkedHashMap<>();

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual();
        compile("03", CompiledTpcdsQueries.query03());
        compile("07", CompiledTpcdsQueries.query07());
        compile("26", CompiledTpcdsQueries.query26());
        compile("42", CompiledTpcdsQueries.query42());
        compile("52", CompiledTpcdsQueries.query52());
        compile("55", CompiledTpcdsQueries.query55());
        compile("96", CompiledTpcdsQueries.query96());
    }

    private void compile(String name, CompiledTpcdsQueries.Ported ported)
    {
        QueryLowering.Lowered plan = ported.query().lower();
        lowered.put(name, plan);
        compiled.put(name, plan.compile());   // one-time, like building a query plan
        streaming.put(name, PipelineCompiler.compileStreaming(plan.pipeline(), plan.encodings(), plan.nullable()));
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator();
    }

    @Benchmark
    public Object full()
    {
        CompiledQuerySupport.LoadedInputs inputs = CompiledQuerySupport.loadLoweredInputs(allocator, tables, lowered.get(query));
        return compiled.get(query).execute(inputs.inputs(), inputs.rowCounts());
    }

    @Benchmark
    public Object loadOnly()
    {
        return CompiledQuerySupport.loadLoweredInputs(allocator, tables, lowered.get(query));
    }

    @Benchmark
    public Object streaming()
    {
        return CompiledQuerySupport.runStreamingLowered(allocator, tables, lowered.get(query), streaming.get(query));
    }
}
