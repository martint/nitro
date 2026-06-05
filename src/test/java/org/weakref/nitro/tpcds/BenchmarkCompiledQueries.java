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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.QueryLowering;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Apples-to-apples benchmark of the data-centric compiler against the Nitro and Trino operator-chain harnesses
 * (their {@code BenchmarkQueries}), using the same framework and the same per-invocation read of sf10 Parquet.
 * Each invocation scans the inputs into columns and runs the compiled routine -- the compiled engine's full
 * "from Parquet to result" cost, including the materialization it currently requires (no streaming scan yet).
 * Java compilation is a one-time cost (a query plan), so it happens once in {@link #setup}.
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
    private Allocator allocator;
    private TpcdsParquetTables tables;
    private final Map<String, QueryLowering.Lowered> lowered = new LinkedHashMap<>();
    private final Map<String, CompiledPipeline> compiled = new LinkedHashMap<>();

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
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator();
    }

    @Benchmark
    public Object query03()
    {
        return run("03");
    }

    @Benchmark
    public Object query07()
    {
        return run("07");
    }

    @Benchmark
    public Object query26()
    {
        return run("26");
    }

    @Benchmark
    public Object query42()
    {
        return run("42");
    }

    @Benchmark
    public Object query52()
    {
        return run("52");
    }

    @Benchmark
    public Object query55()
    {
        return run("55");
    }

    @Benchmark
    public Object query96()
    {
        return run("96");
    }

    private Object run(String name)
    {
        CompiledQuerySupport.LoadedInputs inputs = CompiledQuerySupport.loadLoweredInputs(allocator, tables, lowered.get(name));
        return compiled.get(name).execute(inputs.inputs(), inputs.rowCounts());
    }
}
