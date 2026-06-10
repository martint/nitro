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
    @Param({"01", "03", "06", "10", "35", "88", "90", "07", "13", "15", "22", "25", "26", "27", "29", "32", "33", "34", "39", "78", "51", "38", "87", "97", "61", "83", "58", "57", "47", "05", "80", "77", "08", "49", "30", "81", "09", "28", "37", "40", "42", "43", "48", "50", "52", "53", "55", "56",
            "60", "62", "66", "63", "65", "71", "73", "75", "76", "79", "82", "85", "89", "91", "04", "11", "12", "16", "19", "20", "21", "31", "36", "46", "59", "68", "69", "70", "74", "86", "92", "93", "94", "95", "96", "99"})
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
        composite("39", CompiledTpcdsQueries.query39());
        composite("86", CompiledTpcdsQueries.query86());
        composite("70", CompiledTpcdsQueries.query70());
        composite("78", CompiledTpcdsQueries.query78());
        runningCumulative("51", CompiledTpcdsQueries.query51());
        channelUnion("05", CompiledTpcdsQueries.query05());
        labeledUnion("80", CompiledTpcdsQueries.query80());
        labeledUnion("77", CompiledTpcdsQueries.query77());
        labeledUnion("49", CompiledTpcdsQueries.query49());
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
        unionComposite("69", CompiledTpcdsQueries.query69());
        unionComposite("35", CompiledTpcdsQueries.query35());
        unionComposite("38", CompiledTpcdsQueries.query38());
        unionComposite("87", CompiledTpcdsQueries.query87());
        unionComposite("97", CompiledTpcdsQueries.query97());
        union("33", CompiledTpcdsQueries.query33());
        union("76", CompiledTpcdsQueries.query76());
        unionSelfJoin("75", CompiledTpcdsQueries.query75());
        union("56", CompiledTpcdsQueries.query56());
        union("60", CompiledTpcdsQueries.query60());
        union("71", CompiledTpcdsQueries.query71());
        union("66", CompiledTpcdsQueries.query66());

        composite("06", CompiledTpcdsQueries.query06());
        composite("90", CompiledTpcdsQueries.query90());
        composite("88", CompiledTpcdsQueries.query88());
        composite("61", CompiledTpcdsQueries.query61());
        composite("08", CompiledTpcdsQueries.query08());
        composite("30", CompiledTpcdsQueries.query30());
        composite("09", CompiledTpcdsQueries.query09());
        composite("28", CompiledTpcdsQueries.query28());
        composite("81", CompiledTpcdsQueries.query81());
        composite("83", CompiledTpcdsQueries.query83());
        composite("58", CompiledTpcdsQueries.query58());
        composite("57", CompiledTpcdsQueries.query57());
        composite("47", CompiledTpcdsQueries.query47());
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

    /** A year-over-year union self-join (Q75): the union-aggregate subquery assembled twice and self-joined. */
    private void unionSelfJoin(String name, CompiledTpcdsQueries.UnionSelfJoin query)
    {
        List<org.weakref.nitro.jit.QueryLowering> branchPlans = query.branches();
        String currentUnion = query.currentUnion();
        String previousUnion = query.previousUnion();
        Lowered currentGroup = query.currentGroup().lower();
        Lowered previousGroup = query.previousGroup().lower();
        String currentVirtual = query.currentVirtual();
        String previousVirtual = query.previousVirtual();
        Lowered main = query.main().lower();
        runners.put(name, () -> {
            Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
            List<Lowered> branchesCurrent = new ArrayList<>();
            for (org.weakref.nitro.jit.QueryLowering branch : branchPlans) {
                branchesCurrent.add(branch.lower());
            }
            virtuals.put(currentUnion, CompiledQuerySupport.materializeUnion(allocator, tables, branchesCurrent, List.of()));
            virtuals.put(currentVirtual, CompiledQuerySupport.materializeStage(allocator, tables, currentGroup, virtuals, List.of()));
            List<Lowered> branchesPrevious = new ArrayList<>();
            for (org.weakref.nitro.jit.QueryLowering branch : branchPlans) {
                branchesPrevious.add(branch.lower());
            }
            virtuals.put(previousUnion, CompiledQuerySupport.materializeUnion(allocator, tables, branchesPrevious, List.of()));
            virtuals.put(previousVirtual, CompiledQuerySupport.materializeStage(allocator, tables, previousGroup, virtuals, List.of()));
            return CompiledQuerySupport.runStage(allocator, tables, main, virtuals);
        });
    }

    /** The Q51 cumulative shape: two grouped+windowed channel stages per side, concatenated, merged, then the running-max/filter/order main. */
    private void runningCumulative(String name, CompiledTpcdsQueries.RunningCumulative query)
    {
        Lowered webGrouped = query.webGrouped().lower();
        Lowered webWindow = query.webWindow().lower();
        Lowered storeGrouped = query.storeGrouped().lower();
        Lowered storeWindow = query.storeWindow().lower();
        Lowered merged = query.merged().lower();
        Lowered main = query.main().lower();
        runners.put(name, () -> {
            Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
            virtuals.put("q51_web_grouped", CompiledQuerySupport.materializeStage(allocator, tables, webGrouped, virtuals, List.of()));
            virtuals.put("q51_web_window", CompiledQuerySupport.materializeStage(allocator, tables, webWindow, virtuals, List.of()));
            virtuals.put("q51_store_grouped", CompiledQuerySupport.materializeStage(allocator, tables, storeGrouped, virtuals, List.of()));
            virtuals.put("q51_store_window", CompiledQuerySupport.materializeStage(allocator, tables, storeWindow, virtuals, List.of()));
            virtuals.put("q51_union", CompiledQuerySupport.concatenate(virtuals.get("q51_web_window"), virtuals.get("q51_store_window")));
            virtuals.put("q51_merged", CompiledQuerySupport.materializeStage(allocator, tables, merged, virtuals, List.of()));
            return CompiledQuerySupport.runStage(allocator, tables, main, virtuals);
        });
    }

    /** The Q05 channel-rollup shape: per channel two leaves concatenated and grouped, the labeled channels concatenated, then the ordering main. */
    private void channelUnion(String name, CompiledTpcdsQueries.ChannelUnion query)
    {
        record ChannelPlan(Lowered sales, Lowered returns, String unionVirtual, Lowered grouped, String groupedVirtual,
                List<CompiledTpcdsQueries.DictRef> groupedStrings) {}

        Lowered webReturns = query.webReturns().lower();
        List<ChannelPlan> channels = new ArrayList<>();
        for (CompiledTpcdsQueries.Channel channel : query.channels()) {
            channels.add(new ChannelPlan(channel.sales().lower(), channel.returns().lower(), channel.unionVirtual(),
                    channel.grouped().lower(), channel.groupedVirtual(), channel.groupedStrings()));
        }
        List<CompiledTpcdsQueries.DictRef> leafStrings = query.leafStrings();
        Lowered main = query.main().lower();
        runners.put(name, () -> {
            Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
            virtuals.put("q05_web_returns", CompiledQuerySupport.materializeStage(allocator, tables, webReturns, virtuals, List.of()));
            List<CompiledQuerySupport.Materialized> grouped = new ArrayList<>();
            for (ChannelPlan channel : channels) {
                CompiledQuerySupport.Materialized sales = CompiledQuerySupport.materializeStage(allocator, tables, channel.sales(), virtuals, leafStrings);
                CompiledQuerySupport.Materialized returns = CompiledQuerySupport.materializeStage(allocator, tables, channel.returns(), virtuals, leafStrings);
                virtuals.put(channel.unionVirtual(), CompiledQuerySupport.concatenate(sales, returns));
                CompiledQuerySupport.Materialized channelGrouped = CompiledQuerySupport.materializeStage(allocator, tables, channel.grouped(), virtuals, channel.groupedStrings());
                virtuals.put(channel.groupedVirtual(), channelGrouped);
                grouped.add(channelGrouped);
            }
            virtuals.put("q05_channels", CompiledQuerySupport.concatenate(
                    CompiledQuerySupport.concatenate(grouped.get(0), grouped.get(1)), grouped.get(2)));
            return CompiledQuerySupport.runStage(allocator, tables, main, virtuals);
        });
    }

    /** A union of independently-materialized branch stages concatenated under one virtual name, then the main. */
    private void labeledUnion(String name, CompiledTpcdsQueries.LabeledUnion query)
    {
        record BranchPlan(Lowered plan, String virtualName, List<CompiledTpcdsQueries.DictRef> stringColumns) {}

        List<BranchPlan> stages = new ArrayList<>();
        for (CompiledTpcdsQueries.Stage stage : query.stages()) {
            stages.add(new BranchPlan(stage.plan().lower(), stage.virtualName(), stage.stringColumns()));
        }
        List<BranchPlan> branches = new ArrayList<>();
        for (CompiledTpcdsQueries.Stage branch : query.branches()) {
            branches.add(new BranchPlan(branch.plan().lower(), branch.virtualName(), branch.stringColumns()));
        }
        String unionVirtual = query.unionVirtual();
        Lowered main = query.main().lower();
        runners.put(name, () -> {
            Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
            for (BranchPlan stage : stages) {
                virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan(), virtuals, stage.stringColumns()));
            }
            CompiledQuerySupport.Materialized union = null;
            for (BranchPlan branch : branches) {
                CompiledQuerySupport.Materialized part = CompiledQuerySupport.materializeStage(allocator, tables, branch.plan(), virtuals, branch.stringColumns());
                union = union == null ? part : CompiledQuerySupport.concatenate(union, part);
            }
            virtuals.put(unionVirtual, union);
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
