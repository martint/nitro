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
import org.weakref.nitro.jit.QueryLowering.Lowered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Apples-to-apples benchmark of the data-centric compiler over every ported TPC-DS query, against the Nitro and
 * Trino operator-chain harnesses ({@code org.weakref.nitro.tpcds.BenchmarkQueries} and
 * {@code org.weakref.trino.tpcds.BenchmarkQueries}) on the same per-invocation read of sf10 Parquet. {@link #full}
 * runs the whole "Parquet to result" for one query; compilation is amortized like a query plan -- the
 * {@link org.weakref.nitro.jit.PipelineCompiler} caches compiled pipelines by source, so after warmup the measured
 * iterations only execute. Each query dispatches to the execution shape it was ported as (single-stage star,
 * streamed single-stage, multi-stage pipeline-breaker, UNION ALL, or a tree of stages).
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
    @Param({"01", "03", "06", "10", "35", "90", "07", "13", "15", "22", "25", "26", "27", "29", "32", "33", "34", "38", "87", "37", "40", "42", "43", "48", "50", "52", "53", "55", "56",
            "60", "62", "66", "63", "65", "71", "73", "79", "82", "85", "89", "91", "04", "11", "12", "16", "19", "20", "21", "31", "36", "46", "59", "68", "74", "86", "92", "93", "94", "95", "96", "99"})
    public String query;

    private Allocator allocator;
    private TpcdsParquetTables tables;
    private final Map<String, Supplier<Object>> runners = new LinkedHashMap<>();

    @Setup
    public void setup()
    {
        tables = TpcdsParquetTables.requiredActual();

        ported("03", CompiledTpcdsQueries.query03());
        ported("07", CompiledTpcdsQueries.query07());
        ported("13", CompiledTpcdsQueries.query13());
        ported("15", CompiledTpcdsQueries.query15());
        ported("19", CompiledTpcdsQueries.query19());
        ported("26", CompiledTpcdsQueries.query26());
        ported("27", CompiledTpcdsQueries.query27());
        ported("42", CompiledTpcdsQueries.query42());
        ported("43", CompiledTpcdsQueries.query43());
        ported("25", CompiledTpcdsQueries.query25());
        ported("29", CompiledTpcdsQueries.query29());
        ported("48", CompiledTpcdsQueries.query48());
        ported("50", CompiledTpcdsQueries.query50());
        ported("52", CompiledTpcdsQueries.query52());
        ported("55", CompiledTpcdsQueries.query55());
        ported("62", CompiledTpcdsQueries.query62());
        ported("91", CompiledTpcdsQueries.query91());
        ported("96", CompiledTpcdsQueries.query96());
        ported("85", CompiledTpcdsQueries.query85());
        composite("36", CompiledTpcdsQueries.query36());
        composite("86", CompiledTpcdsQueries.query86());
        composite("93", CompiledTpcdsQueries.query93());
        ported("99", CompiledTpcdsQueries.query99());

        ported("40", CompiledTpcdsQueries.query40());
        ported("37", CompiledTpcdsQueries.query37());
        ported("82", CompiledTpcdsQueries.query82());

        multiStage("32", CompiledTpcdsQueries.query32());
        multiStage("34", CompiledTpcdsQueries.query34());
        multiStage("73", CompiledTpcdsQueries.query73());
        multiStage("92", CompiledTpcdsQueries.query92());

        unionComposite("10", CompiledTpcdsQueries.query10());
        unionComposite("35", CompiledTpcdsQueries.query35());
        unionComposite("38", CompiledTpcdsQueries.query38());
        unionComposite("87", CompiledTpcdsQueries.query87());
        union("33", CompiledTpcdsQueries.query33());
        union("56", CompiledTpcdsQueries.query56());
        union("60", CompiledTpcdsQueries.query60());
        union("71", CompiledTpcdsQueries.query71());
        union("66", CompiledTpcdsQueries.query66());

        composite("06", CompiledTpcdsQueries.query06());
        composite("90", CompiledTpcdsQueries.query90());
        composite("01", CompiledTpcdsQueries.query01());
        composite("22", CompiledTpcdsQueries.query22());
        composite("53", CompiledTpcdsQueries.query53());
        composite("63", CompiledTpcdsQueries.query63());
        composite("89", CompiledTpcdsQueries.query89());
        composite("94", CompiledTpcdsQueries.query94());
        composite("95", CompiledTpcdsQueries.query95());
        composite("16", CompiledTpcdsQueries.query16());
        composite("12", CompiledTpcdsQueries.query12());
        composite("20", CompiledTpcdsQueries.query20());
        composite("21", CompiledTpcdsQueries.query21());
        composite("59", CompiledTpcdsQueries.query59());
        composite("46", CompiledTpcdsQueries.query46());
        composite("68", CompiledTpcdsQueries.query68());
        composite("04", CompiledTpcdsQueries.query04());
        composite("11", CompiledTpcdsQueries.query11());
        composite("31", CompiledTpcdsQueries.query31());
        composite("74", CompiledTpcdsQueries.query74());
        composite("79", CompiledTpcdsQueries.query79());
        composite("65", CompiledTpcdsQueries.query65());
    }

    /**
     * A single-stage star query. The probe fact is streamed batch-by-batch (its dimensions built into hash tables) --
     * the engine's production execution and the apples-to-apples match for the operator harnesses, which also stream
     * the fact. (Draining the whole fact into arrays first would dominate the measurement with load-side allocation.)
     */
    private void ported(String name, CompiledTpcdsQueries.Ported ported)
    {
        Lowered lowered = ported.query().lower();
        runners.put(name, () -> CompiledQuerySupport.runStreamingPorted(allocator, tables, lowered));
    }

    /** A two-stage pipeline-breaker query (decorrelated subquery feeding the main stage). */
    private void multiStage(String name, CompiledTpcdsQueries.MultiStage staged)
    {
        Lowered subquery = staged.subquery().lower();
        Lowered main = staged.main().lower();
        String virtualTable = staged.virtualTable();
        runners.put(name, () -> CompiledQuerySupport.runMultiStage(allocator, tables, subquery, main, virtualTable));
    }

    /** A UNION ALL of independently computed branches feeding a final stage. */
    private void union(String name, CompiledTpcdsQueries.Union union)
    {
        List<Lowered> branches = new ArrayList<>();
        for (org.weakref.nitro.jit.QueryLowering branch : union.branches()) {
            branches.add(branch.lower());
        }
        Lowered main = union.main().lower();
        String virtualTable = union.virtualTable();
        List<CompiledTpcdsQueries.DictRef> branchStringColumns = union.branchStringColumns();
        runners.put(name, () -> CompiledQuerySupport.runUnion(allocator, tables, branches, main, virtualTable, branchStringColumns));
    }

    /** A UNION ALL materialized into a virtual table that downstream stages then consume (union feeding an aggregate). */
    private void unionComposite(String name, CompiledTpcdsQueries.UnionComposite composite)
    {
        record StagePlan(Lowered plan, String virtualName, List<CompiledTpcdsQueries.DictRef> stringColumns) {}

        List<Lowered> branches = new ArrayList<>();
        for (org.weakref.nitro.jit.QueryLowering branch : composite.branches()) {
            branches.add(branch.lower());
        }
        String unionVirtualName = composite.unionVirtualName();
        List<CompiledTpcdsQueries.DictRef> branchStringColumns = composite.branchStringColumns();
        List<StagePlan> stages = new ArrayList<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            stages.add(new StagePlan(stage.plan().lower(), stage.virtualName(), stage.stringColumns()));
        }
        Lowered main = composite.main().lower();
        runners.put(name, () -> {
            Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
            virtuals.put(unionVirtualName, CompiledQuerySupport.materializeUnion(allocator, tables, branches, branchStringColumns));
            for (StagePlan stage : stages) {
                virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan(), virtuals, stage.stringColumns()));
            }
            return CompiledQuerySupport.runStage(allocator, tables, main, virtuals);
        });
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
        allocator = new Allocator();
    }

    @Benchmark
    public Object full()
    {
        return runners.get(query).get();
    }
}
