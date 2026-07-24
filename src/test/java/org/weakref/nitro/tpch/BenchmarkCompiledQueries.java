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
package org.weakref.nitro.tpch;

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
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.jit.QueryLowering.Lowered;
import org.weakref.nitro.tpcds.CompiledQuerySupport;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Apples-to-apples benchmark of the data-centric compiler over every ported TPC-H query, against the Nitro
 * and Trino operator-chain harnesses ({@code org.weakref.nitro.tpch.BenchmarkQueries} and
 * {@code org.weakref.trino.tpch.BenchmarkQueries}) on the same per-invocation read of sf10 Parquet.
 * Compilation is amortized like a query plan (compiled pipelines are cached by source), so after warmup
 * the measured iterations only execute. Each query dispatches to the shape it was ported as: a streamed
 * single-stage pipeline, or a tree of stages (the decorrelated subqueries materialized as virtual tables).
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompiledQueries
{
    @Param({"01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22"})
    public String query;

    private Allocator allocator;
    private TpchParquetTables tables;
    private final Map<String, Supplier<Object>> runners = new LinkedHashMap<>();

    @Setup
    public void setup()
    {
        // Schema overridable via -Dnitro.tpch.parquet.schema (defaults to sf10).
        tables = TpchParquetTables.requiredActual();

        ported("01", CompiledTpchQueries.query01());
        composite("02", CompiledTpchQueries.query02());
        ported("03", CompiledTpchQueries.query03());
        ported("04", CompiledTpchQueries.query04());
        ported("05", CompiledTpchQueries.query05());
        ported("06", CompiledTpchQueries.query06());
        ported("07", CompiledTpchQueries.query07());
        ported("08", CompiledTpchQueries.query08());
        ported("09", CompiledTpchQueries.query09());
        ported("10", CompiledTpchQueries.query10());
        composite("11", CompiledTpchQueries.query11());
        ported("12", CompiledTpchQueries.query12());
        composite("13", CompiledTpchQueries.query13());
        ported("14", CompiledTpchQueries.query14());
        composite("15", CompiledTpchQueries.query15());
        composite("16", CompiledTpchQueries.query16());
        composite("17", CompiledTpchQueries.query17());
        composite("18", CompiledTpchQueries.query18());
        ported("19", CompiledTpchQueries.query19());
        composite("20", CompiledTpchQueries.query20());
        composite("21", CompiledTpchQueries.query21());
        composite("22", CompiledTpchQueries.query22());
    }

    /** A single-stage query: the probe fact is streamed batch-by-batch, as the operator harnesses stream it. */
    private void ported(String name, CompiledTpcdsQueries.Ported ported)
    {
        Lowered lowered = ported.query().lower();
        runners.put(name, () -> CompiledQuerySupport.runStreamingPorted(allocator, tables, lowered));
    }

    /** A multi-stage query expressed as a tree of pipelines (each stage materialized under a virtual table name). */
    private void composite(String name, CompiledTpcdsQueries.Composite composite)
    {
        record StagePlan(Lowered plan, String virtualName, List<CompiledTpcdsQueries.DictRef> stringColumns) {}

        List<StagePlan> stages = new ArrayList<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            stages.add(new StagePlan(stage.plan().lower(), stage.virtualName(), stage.stringColumns()));
        }
        Lowered main = composite.main().lower();
        runners.put(name, () -> {
            Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
            for (StagePlan stage : stages) {
                virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan(), virtuals, stage.stringColumns()));
            }
            return CompiledQuerySupport.runStage(allocator, tables, main, virtuals);
        });
    }

    @Setup(Level.Invocation)
    public void setupInvocation()
    {
        allocator = new Allocator(EngineResources.createDefault());
    }

    @Benchmark
    public Object full()
    {
        return runners.get(query).get();
    }
}
