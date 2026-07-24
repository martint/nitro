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
package org.weakref.nitro.clickbench;

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
import org.weakref.nitro.legacy.pipeline.QueryLowering.Lowered;
import org.weakref.nitro.tpcds.CompiledQuerySupport;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Apples-to-apples benchmark of the data-centric compiler over every ported ClickBench query, against the Nitro
 * and Trino operator-chain harnesses ({@code org.weakref.nitro.clickbench.BenchmarkQueries} and
 * {@code org.weakref.trino.clickbench.BenchmarkQueries}) on the same per-invocation read of the split hits
 * Parquet. Compilation is amortized like a query plan (compiled pipelines are cached by source), so after warmup
 * the measured iterations only execute. Each query dispatches to the shape it was ported as: a streamed
 * single-stage pipeline, or a tree of stages (the harness's MarkDistinct/TopN/Project pre-stages).
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
            "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
            "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
            "41", "42", "43"})
    public String query;

    private Allocator allocator;
    private ClickBenchParquetTables tables;
    private final Map<String, Supplier<Object>> runners = new LinkedHashMap<>();

    @Setup
    public void setup()
    {
        tables = ClickBenchParquetTables.actualIfPresent()
                .orElseThrow(() -> new IllegalStateException("ClickBench hits data not found (set -Dnitro.clickbench.hits.path)"));
        Path hits = tables.directory();

        ported("01", CompiledClickBenchQueries.query01());
        ported("02", CompiledClickBenchQueries.query02());
        ported("03", CompiledClickBenchQueries.query03());
        ported("04", CompiledClickBenchQueries.query04());
        ported("05", CompiledClickBenchQueries.query05());
        ported("06", CompiledClickBenchQueries.query06());
        ported("07", CompiledClickBenchQueries.query07());
        ported("08", CompiledClickBenchQueries.query08());
        ported("09", CompiledClickBenchQueries.query09());
        ported("10", CompiledClickBenchQueries.query10());
        ported("11", CompiledClickBenchQueries.query11());
        ported("12", CompiledClickBenchQueries.query12());
        ported("13", CompiledClickBenchQueries.query13());
        ported("14", CompiledClickBenchQueries.query14());
        ported("15", CompiledClickBenchQueries.query15());
        ported("16", CompiledClickBenchQueries.query16());
        ported("17", CompiledClickBenchQueries.query17());
        ported("18", CompiledClickBenchQueries.query18());
        ported("19", CompiledClickBenchQueries.query19());
        ported("20", CompiledClickBenchQueries.query20());
        ported("21", CompiledClickBenchQueries.query21());
        ported("22", CompiledClickBenchQueries.query22());
        ported("23", CompiledClickBenchQueries.query23());
        ported("24", CompiledClickBenchQueries.query24());
        composite("25", CompiledClickBenchQueries.query25());
        ported("26", CompiledClickBenchQueries.query26());
        composite("27", CompiledClickBenchQueries.query27());
        ported("28", CompiledClickBenchQueries.query28());
        ported("29", CompiledClickBenchQueries.query29());
        ported("30", CompiledClickBenchQueries.query30());
        ported("31", CompiledClickBenchQueries.query31());
        ported("32", CompiledClickBenchQueries.query32());
        ported("33", CompiledClickBenchQueries.query33());
        ported("34", CompiledClickBenchQueries.query34());
        ported("35", CompiledClickBenchQueries.query35());
        composite("36", CompiledClickBenchQueries.query36());
        ported("37", CompiledClickBenchQueries.query37(hits));
        ported("38", CompiledClickBenchQueries.query38(hits));
        ported("39", CompiledClickBenchQueries.query39(hits));
        ported("40", CompiledClickBenchQueries.query40(hits));
        ported("41", CompiledClickBenchQueries.query41(hits));
        ported("42", CompiledClickBenchQueries.query42(hits));
        ported("43", CompiledClickBenchQueries.query43(hits));
    }

    /** A single-stage query: the hits fact is streamed batch-by-batch, as the operator harnesses stream it. */
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
