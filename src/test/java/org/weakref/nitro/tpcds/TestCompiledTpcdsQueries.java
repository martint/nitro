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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.CompiledOperator;
import org.weakref.nitro.operator.Operator;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Validates the TPC-DS queries ported to the data-centric compiler, apples-to-apples against the Nitro
 * operator-chain harness ({@link TpcdsParquetSupport}) on real sf10 Parquet: each query (built by
 * {@link CompiledTpcdsQueries}) is lowered by column name, compiled, bridged back into an {@link Operator}, and
 * asserted row-for-row (order included, for top-N queries) identical to the harness chain. Skipped when the
 * dataset is not configured.
 */
public class TestCompiledTpcdsQueries
{
    @Test
    void discoversNumericPhysicalWidthsFromData()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // store_sales: a 64-bit surrogate key + measure, a 32-bit quantity; date_dim: 32-bit d_year. The widths are
        // read from the actual file, not the logical type -- the foundation for per-file variant specialization.
        int[] ss = CompiledQuerySupport.discoverNumericWidths(new Allocator(), tables, "store_sales",
                List.of("ss_item_sk", "ss_ext_sales_price", "ss_quantity"));
        assertThat(ss).containsExactly(64, 64, 32);
        int[] dd = CompiledQuerySupport.discoverNumericWidths(new Allocator(), tables, "date_dim", List.of("d_date_sk", "d_year"));
        assertThat(dd).containsExactly(64, 32);
    }

    @Test
    void query03()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query03(), TpcdsParquetSupport::query03);
    }

    @Test
    void query07()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query07(), TpcdsParquetSupport::query07);
    }

    @Test
    void query15()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query15(), TpcdsParquetSupport::query15);
    }

    @Test
    void query26()
    {
        assertQuery26MatchesHarness(CompiledTpcdsQueries.query26(), TpcdsParquetSupport::query26);
    }

    @Test
    void query27()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query27(), TpcdsParquetSupport::query27);
    }

    @Test
    void query22()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query22(), TpcdsParquetSupport::query22);
    }

    @Test
    void query32()
    {
        assertMultiStageMatchesHarness(CompiledTpcdsQueries.query32(), TpcdsParquetSupport::query32);
    }

    @Test
    void query53()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query53(), TpcdsParquetSupport::query53);
    }

    @Test
    void query63()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query63(), TpcdsParquetSupport::query63);
    }

    @Test
    void query89()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query89(), TpcdsParquetSupport::query89);
    }

    @Test
    void query94()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query94(), TpcdsParquetSupport::query94);
    }

    @Test
    void query95()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query95(), TpcdsParquetSupport::query95);
    }

    @Test
    void query16()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query16(), TpcdsParquetSupport::query16);
    }

    @Test
    void query33()
    {
        assertUnionMatchesHarness(CompiledTpcdsQueries.query33(), TpcdsParquetSupport::query33);
    }

    @Test
    void query76()
    {
        assertUnionMatchesHarness(CompiledTpcdsQueries.query76(), TpcdsParquetSupport::query76);
    }

    @Test
    void query75()
    {
        assertUnionSelfJoinMatchesHarness(CompiledTpcdsQueries.query75(), TpcdsParquetSupport::query75);
    }

    @Test
    void query66()
    {
        assertUnionMatchesHarness(CompiledTpcdsQueries.query66(), TpcdsParquetSupport::query66);
    }

    @Test
    void query56()
    {
        assertUnionMatchesHarness(CompiledTpcdsQueries.query56(), TpcdsParquetSupport::query56);
    }

    @Test
    void query60()
    {
        assertUnionMatchesHarness(CompiledTpcdsQueries.query60(), TpcdsParquetSupport::query60);
    }

    @Test
    void query71()
    {
        assertUnionMatchesHarness(CompiledTpcdsQueries.query71(), TpcdsParquetSupport::query71);
    }

    @Test
    void query34()
    {
        assertMultiStageMatchesHarness(CompiledTpcdsQueries.query34(), TpcdsParquetSupport::query34);
    }

    @Test
    void query42()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query42(), TpcdsParquetSupport::query42);
    }

    @Test
    void query43()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query43(), TpcdsParquetSupport::query43);
    }

    @Test
    void query13()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query13(), TpcdsParquetSupport::query13);
    }

    @Test
    void query48()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query48(), TpcdsParquetSupport::query48);
    }

    @Test
    void query25()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query25(), TpcdsParquetSupport::query25);
    }

    @Test
    void query40()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query40(), TpcdsParquetSupport::query40);
    }

    @Test
    void query29()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query29(), TpcdsParquetSupport::query29);
    }

    @Test
    void query50()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query50(), TpcdsParquetSupport::query50);
    }

    @Test
    void query52()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query52(), TpcdsParquetSupport::query52);
    }

    @Test
    void query55()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query55(), TpcdsParquetSupport::query55);
    }

    @Test
    void query19()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query19(), TpcdsParquetSupport::query19);
    }

    @Test
    void query68()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query68(), TpcdsParquetSupport::query68);
    }

    @Test
    void query46()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query46(), TpcdsParquetSupport::query46);
    }

    @Test
    void query12()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query12(), TpcdsParquetSupport::query12);
    }

    @Test
    void query20()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query20(), TpcdsParquetSupport::query20);
    }

    @Test
    void query59()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query59(), TpcdsParquetSupport::query59);
    }

    @Test
    void query21()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query21(), TpcdsParquetSupport::query21);
    }

    @Test
    void query74()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query74(), TpcdsParquetSupport::query74);
    }

    @Test
    void query31()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query31(), TpcdsParquetSupport::query31);
    }

    @Test
    void query04()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query04(), TpcdsParquetSupport::query04);
    }

    @Test
    void query11()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query11(), TpcdsParquetSupport::query11);
    }

    @Test
    void query85()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query85(), TpcdsParquetSupport::query85);
    }

    @Test
    void query36()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query36(), TpcdsParquetSupport::query36);
    }

    @Test
    void query86()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query86(), TpcdsParquetSupport::query86);
    }

    @Test
    void query70()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query70(), TpcdsParquetSupport::query70);
    }

    @Test
    void query78()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query78(), TpcdsParquetSupport::query78);
    }

    @Test
    void query51()
    {
        assertRunningCumulativeMatchesHarness(CompiledTpcdsQueries.query51(), TpcdsParquetSupport::query51);
    }

    @Test
    void query39()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query39(), TpcdsParquetSupport::query39);
    }

    @Test
    void query93()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query93(), TpcdsParquetSupport::query93);
    }

    @Test
    void query62()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query62(), TpcdsParquetSupport::query62);
    }

    @Test
    void query73()
    {
        assertMultiStageMatchesHarness(CompiledTpcdsQueries.query73(), TpcdsParquetSupport::query73);
    }

    @Test
    void query91()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query91(), TpcdsParquetSupport::query91);
    }

    @Test
    void query92()
    {
        assertMultiStageMatchesHarness(CompiledTpcdsQueries.query92(), TpcdsParquetSupport::query92);
    }

    @Test
    void query96()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query96(), TpcdsParquetSupport::query96);
    }

    @Test
    void query99()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query99(), TpcdsParquetSupport::query99);
    }

    @Test
    void streamingLazyMatchesEagerForPortedQueries()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // Every single-stage ported query, run through the lazy streaming path (selection-driven / join-driven late
        // materialization), must produce the same rows as the eager run -- so the lazy path can carry the headline
        // benchmark without changing answers.
        java.util.Map<String, CompiledTpcdsQueries.Ported> ported = new java.util.LinkedHashMap<>();
        ported.put("03", CompiledTpcdsQueries.query03());
        ported.put("07", CompiledTpcdsQueries.query07());
        ported.put("13", CompiledTpcdsQueries.query13());
        ported.put("15", CompiledTpcdsQueries.query15());
        ported.put("26", CompiledTpcdsQueries.query26());
        ported.put("42", CompiledTpcdsQueries.query42());
        ported.put("25", CompiledTpcdsQueries.query25());
        ported.put("40", CompiledTpcdsQueries.query40());
        ported.put("29", CompiledTpcdsQueries.query29());
        ported.put("48", CompiledTpcdsQueries.query48());
        ported.put("50", CompiledTpcdsQueries.query50());
        ported.put("43", CompiledTpcdsQueries.query43());
        ported.put("52", CompiledTpcdsQueries.query52());
        ported.put("55", CompiledTpcdsQueries.query55());
        ported.put("19", CompiledTpcdsQueries.query19());   // exercises a column-vs-column string compare on the streaming probe path
        ported.put("85", CompiledTpcdsQueries.query85());   // two build-side column-vs-column string compares on the streaming path
        ported.put("62", CompiledTpcdsQueries.query62());
        ported.put("91", CompiledTpcdsQueries.query91());
        ported.put("96", CompiledTpcdsQueries.query96());
        ported.put("99", CompiledTpcdsQueries.query99());

        for (var entry : ported.entrySet()) {
            org.weakref.nitro.jit.QueryLowering.Lowered lowered = entry.getValue().query().lower();
            org.weakref.nitro.jit.CompiledPipeline.Result eager = CompiledQuerySupport.runLowered(new Allocator(), tables, lowered).result();
            org.weakref.nitro.jit.StreamingPipeline streaming =
                    org.weakref.nitro.jit.PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
            org.weakref.nitro.jit.CompiledPipeline.Result lazy =
                    CompiledQuerySupport.runStreamingLowered(new Allocator(), tables, lowered, streaming, true);
            assertThat(rows(lazy)).as("Q%s streaming-lazy vs eager", entry.getKey()).isEqualTo(rows(eager));
        }
    }

    @Test
    void lateMaterializationJoinMatchesEager()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // A selective star query: store_sales JOIN date_dim (d_year = 2001), GROUP BY ss_item_sk, sum measure.
        // Streamed with a lazy probe source so join-driven late materialization converts ss_item_sk and the
        // measure only for fact rows that join a surviving date -- byte-identical to the eager run.
        org.weakref.nitro.jit.QueryLowering joinQuery = org.weakref.nitro.jit.QueryLowering.scan("store_sales",
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_sold_date_sk", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_item_sk", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_ext_sales_price", org.weakref.nitro.jit.ColumnEncoding.FLAT, true))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new org.weakref.nitro.jit.QueryLowering.Column("d_date_sk"),
                        new org.weakref.nitro.jit.QueryLowering.Column("d_year"));
        joinQuery.where(new org.weakref.nitro.jit.Plan.Predicate("=", joinQuery.column("d_year"), new org.weakref.nitro.jit.Plan.Lit(2001)))
                .groupBy("ss_item_sk")
                .aggregate("sum", "ss_ext_sales_price");
        org.weakref.nitro.jit.QueryLowering.Lowered joinLowered = joinQuery.lower();

        org.weakref.nitro.jit.CompiledPipeline.Result joinEager = CompiledQuerySupport.runLowered(new Allocator(), tables, joinLowered).result();
        org.weakref.nitro.jit.StreamingPipeline joinStreaming =
                org.weakref.nitro.jit.PipelineCompiler.compileStreaming(joinLowered.pipeline(), joinLowered.encodings(), joinLowered.nullable());
        org.weakref.nitro.jit.CompiledPipeline.Result joinLazy =
                CompiledQuerySupport.runStreamingLowered(new Allocator(), tables, joinLowered, joinStreaming, true);

        assertThat(rows(joinLazy)).isEqualTo(rows(joinEager));
        assertThat(joinLazy.rowCount()).isGreaterThan(0);
    }

    @Test
    void lazyMaterializationMatchesEager()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // A selective single-table scan with two conjuncts on different columns:
        // SELECT sum(ss_ext_sales_price) FROM store_sales WHERE ss_quantity < 50 AND ss_sales_price > 100.
        // Run streamed two ways -- an eager source (every column converted for every row) and a selection-driven
        // lazy source (staged: ss_sales_price converted only for ss_quantity survivors, the measure only for both).
        org.weakref.nitro.jit.QueryLowering query = org.weakref.nitro.jit.QueryLowering.scan("store_sales",
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_quantity", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_sales_price", org.weakref.nitro.jit.ColumnEncoding.FLAT, true),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_ext_sales_price", org.weakref.nitro.jit.ColumnEncoding.FLAT, true));
        query.where(
                        new org.weakref.nitro.jit.Plan.Predicate("<", query.column("ss_quantity"), new org.weakref.nitro.jit.Plan.Lit(50)),
                        new org.weakref.nitro.jit.Plan.Predicate(">", query.column("ss_sales_price"), new org.weakref.nitro.jit.Plan.Lit(100)))
                .aggregate("sum", "ss_ext_sales_price");
        org.weakref.nitro.jit.QueryLowering.Lowered lowered = query.lower();
        org.weakref.nitro.jit.StreamingPipeline streaming =
                org.weakref.nitro.jit.PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());

        var probe = lowered.inputs().get(0);
        org.weakref.nitro.jit.CompiledPipeline.Result eager = streaming.execute(
                CompiledQuerySupport.parquetFlatSource(new Allocator(), tables, probe.table(), probe.columns()),
                new org.weakref.nitro.jit.Column[0][], new int[0]);
        org.weakref.nitro.jit.CompiledPipeline.Result lazy = streaming.execute(
                CompiledQuerySupport.parquetLazySource(new Allocator(), tables, probe.table(), probe.columns(), lowered.pipeline()),
                new org.weakref.nitro.jit.Column[0][], new int[0]);

        assertThat(lazy.rowCount()).isEqualTo(1).isEqualTo(eager.rowCount());
        assertThat(lazy.columns()[0][0]).isEqualTo(eager.columns()[0][0]);
    }

    @Test
    void streamingJoinMatchesEager()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // The Q42 star query (2 joins, build-side string group key, decimal sum, top-100) run two ways: eager
        // (probe materialized) and streaming (dimensions built once, fact streamed batch-by-batch). The build-side
        // i_category dictionary is materialized once, so its group-key ids stay consistent across probe batches.
        org.weakref.nitro.jit.QueryLowering.Lowered lowered = CompiledTpcdsQueries.query42().query().lower();

        org.weakref.nitro.jit.CompiledPipeline.Result eager = CompiledQuerySupport.runLowered(new Allocator(), tables, lowered).result();

        org.weakref.nitro.jit.StreamingPipeline streaming =
                org.weakref.nitro.jit.PipelineCompiler.compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
        org.weakref.nitro.jit.CompiledPipeline.Result streamed =
                CompiledQuerySupport.runStreamingLowered(new Allocator(), tables, lowered, streaming);

        assertThat(rows(streamed)).isEqualTo(rows(eager));
        assertThat(streamed.rowCount()).isGreaterThan(0);
    }

    /** A result as the set of its rows (each a list of the column slot values), order-insensitive. */
    private static java.util.Set<List<Long>> rows(org.weakref.nitro.jit.CompiledPipeline.Result result)
    {
        java.util.Set<List<Long>> set = new java.util.HashSet<>();
        for (int r = 0; r < result.rowCount(); r++) {
            List<Long> row = new ArrayList<>();
            for (long[] column : result.columns()) {
                row.add(column[r]);
            }
            set.add(row);
        }
        return set;
    }

    /** A harness operator chain built from a fresh allocator + the shared primitive registry over the given tables. */
    private interface HarnessChain
    {
        Operator build(Allocator allocator, org.weakref.nitro.operator.evaluator.PrimitiveRegistry registry, TpcdsParquetTables tables);
    }

    /**
     * Lower + compile + bridge {@code ported}, and assert its rows equal the harness operator chain's, in order
     * (for top-N queries). The bridge reconstructs a dictionary-string result column from its source input.
     */
    private static void assertMatchesHarness(CompiledTpcdsQueries.Ported ported, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runLowered(new Allocator(), tables, ported.query().lower());
        assertBridgedRowsMatch(run, ported.stringColumns(), harness, tables);
    }

    private static void assertQuery26MatchesHarness(CompiledTpcdsQueries.Ported ported, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runLowered(new Allocator(), tables, ported.query().lower());
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        for (CompiledTpcdsQueries.DictRef ref : ported.stringColumns()) {
            dictionaries[ref.resultColumn()] = CompiledQuerySupport.dictionaryFor(run.inputs(), ref);
        }
        Operator compiled = new CompiledOperator(run.result(), dictionaries);
        Operator harnessChain = harness.build(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
        List<Row> actual = normalizeQuery26CompiledAverages(OperatorAssertions.OperatorAssert.toRows(compiled), true);
        List<Row> expected = normalizeQuery26CompiledAverages(OperatorAssertions.OperatorAssert.toRows(harnessChain), false);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private static List<Row> normalizeQuery26CompiledAverages(List<Row> rows, boolean roundMoneyAverages)
    {
        List<Row> normalized = normalize(rows);
        if (!roundMoneyAverages) {
            return normalized;
        }
        List<Row> rounded = new ArrayList<>(normalized.size());
        for (Row row : normalized) {
            Object[] values = row.values().clone();
            for (int index = 2; index < values.length; index++) {
                if (values[index] != null) {
                    values[index] = Math.round(((Number) values[index]).doubleValue());
                }
            }
            rounded.add(new Row(values));
        }
        return rounded;
    }

    /** As {@link #assertMatchesHarness}, but for a UNION ALL query run via {@code runUnion} (branches + final stage). */
    private static void assertUnionMatchesHarness(CompiledTpcdsQueries.Union union, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        List<org.weakref.nitro.jit.QueryLowering.Lowered> branches = new ArrayList<>();
        for (org.weakref.nitro.jit.QueryLowering branch : union.branches()) {
            branches.add(branch.lower());
        }
        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : union.stages()) {
            virtuals.put(stage.virtualName(),
                    CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        virtuals.put(union.virtualTable(),
                CompiledQuerySupport.materializeUnion(allocator, tables, branches, union.branchStringColumns(), virtuals));
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(
                allocator, tables, union.main().lower(), virtuals);
        assertBridgedRowsMatch(run, union.stringColumns(), harness, tables);
    }

    @Test
    void query01()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query01(), TpcdsParquetSupport::query01);
    }

    @Test
    void query65()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query65(), TpcdsParquetSupport::query65);
    }

    /**
     * Q98's full unlimited sort has no unique answer: distinct groups can tie on all five sort keys (category,
     * class, item id, item description, ratio), so the harness's and the compiled engine's tie orders legitimately
     * differ. Validate the sort CONTRACT instead: the row multisets match exactly and the compiled output is
     * non-decreasing under the query's sort keys.
     */
    @Test
    void query98()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledTpcdsQueries.Composite composite = CompiledTpcdsQueries.query98();
        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, composite.main().lower(), virtuals);
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        for (CompiledTpcdsQueries.DictRef ref : composite.stringColumns()) {
            dictionaries[ref.resultColumn()] = CompiledQuerySupport.dictionaryFor(run.inputs(), ref);
        }
        List<Row> actual = normalize(OperatorAssertions.OperatorAssert.toRows(new CompiledOperator(run.result(), dictionaries)));

        Operator harnessChain = TpcdsParquetSupport.query98(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
        List<Row> expected = normalize(OperatorAssertions.OperatorAssert.toRows(harnessChain));
        assertThat(expected).isNotEmpty();

        // The compiled output honors ORDER BY category, class, item id, item description, ratio.
        java.util.Comparator<Row> sortKeys = rowComparator(2, 3, 0, 1, 6);
        for (int row = 1; row < actual.size(); row++) {
            assertThat(sortKeys.compare(actual.get(row - 1), actual.get(row)))
                    .as("sort contract at row %d", row)
                    .isLessThanOrEqualTo(0);
        }

        // The same rows, compared under a total order refining the sort keys (so tie order cannot differ).
        java.util.Comparator<Row> total = rowComparator(2, 3, 0, 1, 6, 4, 5);
        List<Row> actualCanonical = new ArrayList<>(actual);
        actualCanonical.sort(total);
        List<Row> expectedCanonical = new ArrayList<>(expected);
        expectedCanonical.sort(total);
        assertThat(actualCanonical).containsExactlyElementsOf(expectedCanonical);
    }

    /** Compare normalized rows on {@code columns} in order: longs numerically, strings lexicographically. */
    private static java.util.Comparator<Row> rowComparator(int... columns)
    {
        return (left, right) -> {
            for (int column : columns) {
                Object a = left.values()[column];
                Object b = right.values()[column];
                int comparison;
                if (a == null || b == null) {
                    comparison = Boolean.compare(a == null, b == null);   // nulls last, matching the engines' sort
                }
                else if (a instanceof String string) {
                    comparison = string.compareTo((String) b);
                }
                else {
                    comparison = Long.compare((Long) a, (Long) b);
                }
                if (comparison != 0) {
                    return comparison;
                }
            }
            return 0;
        };
    }

    /** Run a multi-stage query as a tree of compiled pipelines: each stage materialized under its virtual name, then the main. */
    private static void assertCompositeMatchesHarness(CompiledTpcdsQueries.Composite composite, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, composite.main().lower(), virtuals);
        assertBridgedRowsMatch(run, composite.stringColumns(), harness, tables);
    }

    @Test
    void query35()
    {
        assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.query35(), TpcdsParquetSupport::query35);
    }

    @Test
    void query10()
    {
        assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.query10(), TpcdsParquetSupport::query10);
    }

    @Test
    void query69()
    {
        assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.query69(), TpcdsParquetSupport::query69);
    }

    @Test
    void query38()
    {
        assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.query38(), TpcdsParquetSupport::query38);
    }

    @Test
    void query87()
    {
        assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.query87(), TpcdsParquetSupport::query87);
    }

    @Test
    void query97()
    {
        assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.query97(), TpcdsParquetSupport::query97);
    }

    /**
     * Run a UNION-feeding-aggregate query: materialize the union into a virtual table, then run the downstream stages
     * (each materialized under its virtual name) and the main, exactly as a {@link CompiledTpcdsQueries.Composite} but
     * with the union pre-registered as the first virtual relation.
     */
    private static void assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.UnionComposite composite, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        List<org.weakref.nitro.jit.QueryLowering.Lowered> branches = new ArrayList<>();
        for (org.weakref.nitro.jit.QueryLowering branch : composite.branches()) {
            branches.add(branch.lower());
        }
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        virtuals.put(composite.unionVirtualName(),
                CompiledQuerySupport.materializeUnion(allocator, tables, branches, composite.branchStringColumns()));
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, composite.main().lower(), virtuals);
        assertBridgedRowsMatch(run, composite.stringColumns(), harness, tables);
    }

    /**
     * Run a year-over-year union self-join (Q75): the union-aggregate subquery is assembled TWICE (current/previous),
     * each materialized into its own grouped virtual relation, and the two are joined by the main stage.
     */
    private static void assertUnionSelfJoinMatchesHarness(CompiledTpcdsQueries.UnionSelfJoin query, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        // Assemble the union subquery independently for each year (no reuse), then group and filter each to its year.
        virtuals.put(query.currentUnion(), CompiledQuerySupport.materializeUnion(allocator, tables, lowerBranches(query.branches()), List.of()));
        virtuals.put(query.currentVirtual(), CompiledQuerySupport.materializeStage(allocator, tables, query.currentGroup().lower(), virtuals, List.of()));
        for (CompiledTpcdsQueries.Stage stage : query.currentStages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        virtuals.put(query.previousUnion(), CompiledQuerySupport.materializeUnion(allocator, tables, lowerBranches(query.branches()), List.of()));
        virtuals.put(query.previousVirtual(), CompiledQuerySupport.materializeStage(allocator, tables, query.previousGroup().lower(), virtuals, List.of()));
        for (CompiledTpcdsQueries.Stage stage : query.previousStages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, query.main().lower(), virtuals);
        assertBridgedRowsMatch(run, query.stringColumns(), harness, tables);
    }

    private static List<org.weakref.nitro.jit.QueryLowering.Lowered> lowerBranches(List<org.weakref.nitro.jit.QueryLowering> branches)
    {
        List<org.weakref.nitro.jit.QueryLowering.Lowered> lowered = new ArrayList<>();
        for (org.weakref.nitro.jit.QueryLowering branch : branches) {
            lowered.add(branch.lower());
        }
        return lowered;
    }

    /**
     * Run the Q51 cumulative web-vs-store shape: each channel grouped then running-summed (two stages), the two windowed
     * channels concatenated, re-grouped to merge them, then a running-max window + filter + order in the main.
     */
    private static void assertRunningCumulativeMatchesHarness(CompiledTpcdsQueries.RunningCumulative query, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        virtuals.put("q51_web_grouped", CompiledQuerySupport.materializeStage(allocator, tables, query.webGrouped().lower(), virtuals, List.of()));
        virtuals.put("q51_web_window", CompiledQuerySupport.materializeStage(allocator, tables, query.webWindow().lower(), virtuals, List.of()));
        virtuals.put("q51_store_grouped", CompiledQuerySupport.materializeStage(allocator, tables, query.storeGrouped().lower(), virtuals, List.of()));
        virtuals.put("q51_store_window", CompiledQuerySupport.materializeStage(allocator, tables, query.storeWindow().lower(), virtuals, List.of()));
        virtuals.put("q51_union", CompiledQuerySupport.concatenate(virtuals.get("q51_web_window"), virtuals.get("q51_store_window")));
        virtuals.put("q51_merged", CompiledQuerySupport.materializeStage(allocator, tables, query.merged().lower(), virtuals, List.of()));
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, query.main().lower(), virtuals);
        assertBridgedRowsMatch(run, List.of(), harness, tables);
    }

    /**
     * Run the Q05 channel-rollup shape: per channel a sales and a returns leaf concatenated (string dictionaries
     * unified), grouped by the dimension id and labeled; the three labeled channels concatenated again, ordered by
     * (channel, id) in the main.
     */
    private static void assertChannelUnionMatchesHarness(CompiledTpcdsQueries.ChannelUnion query, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        virtuals.put("q05_web_returns", CompiledQuerySupport.materializeStage(allocator, tables, query.webReturns().lower(), virtuals, List.of()));
        List<CompiledQuerySupport.Materialized> channels = new ArrayList<>();
        for (CompiledTpcdsQueries.Channel channel : query.channels()) {
            CompiledQuerySupport.Materialized sales = CompiledQuerySupport.materializeStage(allocator, tables, channel.sales().lower(), virtuals, query.leafStrings());
            CompiledQuerySupport.Materialized returns = CompiledQuerySupport.materializeStage(allocator, tables, channel.returns().lower(), virtuals, query.leafStrings());
            virtuals.put(channel.unionVirtual(), CompiledQuerySupport.concatenate(sales, returns));
            CompiledQuerySupport.Materialized grouped = CompiledQuerySupport.materializeStage(allocator, tables, channel.grouped().lower(), virtuals, channel.groupedStrings());
            virtuals.put(channel.groupedVirtual(), grouped);
            channels.add(grouped);
        }
        virtuals.put("q05_channels", CompiledQuerySupport.concatenate(
                CompiledQuerySupport.concatenate(channels.get(0), channels.get(1)), channels.get(2)));
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, query.main().lower(), virtuals);
        assertBridgedRowsMatch(run, List.of(new CompiledTpcdsQueries.DictRef(0, 0, 0), new CompiledTpcdsQueries.DictRef(1, 0, 1)), harness, tables);
    }

    @Test
    void query05()
    {
        assertChannelUnionMatchesHarness(CompiledTpcdsQueries.query05(), TpcdsParquetSupport::query05);
    }

    /**
     * Run a {@link CompiledTpcdsQueries.LabeledUnion}: each branch stage materialized with its own string
     * reconstruction, concatenated (dictionaries unified) under the union's virtual name, then the main.
     */
    private static void assertLabeledUnionMatchesHarness(CompiledTpcdsQueries.LabeledUnion query, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        for (CompiledTpcdsQueries.PreStage pre : query.stages()) {
            switch (pre) {
                case CompiledTpcdsQueries.Stage stage -> virtuals.put(stage.virtualName(),
                        CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
                case CompiledTpcdsQueries.UnionStage unionStage -> {
                    CompiledQuerySupport.Materialized parts = null;
                    for (CompiledTpcdsQueries.Stage part : unionStage.parts()) {
                        CompiledQuerySupport.Materialized materialized =
                                CompiledQuerySupport.materializeStage(allocator, tables, part.plan().lower(), virtuals, part.stringColumns());
                        parts = parts == null ? materialized : CompiledQuerySupport.concatenate(parts, materialized);
                    }
                    virtuals.put(unionStage.virtualName(), parts);
                }
            }
        }
        CompiledQuerySupport.Materialized union = null;
        for (CompiledTpcdsQueries.Stage branch : query.branches()) {
            CompiledQuerySupport.Materialized part =
                    CompiledQuerySupport.materializeStage(allocator, tables, branch.plan().lower(), virtuals, branch.stringColumns());
            union = union == null ? part : CompiledQuerySupport.concatenate(union, part);
        }
        virtuals.put(query.unionVirtual(), union);
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, query.main().lower(), virtuals);
        assertBridgedRowsMatch(run, query.stringColumns(), harness, tables);
    }

    @Test
    void query80()
    {
        assertLabeledUnionMatchesHarness(CompiledTpcdsQueries.query80(), TpcdsParquetSupport::query80);
    }

    @Test
    void query77()
    {
        assertLabeledUnionMatchesHarness(CompiledTpcdsQueries.query77(), TpcdsParquetSupport::query77);
    }

    @Test
    void query49()
    {
        assertLabeledUnionMatchesHarness(CompiledTpcdsQueries.query49(), TpcdsParquetSupport::query49);
    }

    @Test
    void query79()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query79(), TpcdsParquetSupport::query79);
    }

    @Test
    void query06()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query06(), TpcdsParquetSupport::query06);
    }

    @Test
    void query90()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query90(), TpcdsParquetSupport::query90);
    }

    @Test
    void query88()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query88(), TpcdsParquetSupport::query88);
    }

    @Test
    void query61()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query61(), TpcdsParquetSupport::query61);
    }

    @Test
    void query08()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query08(), TpcdsParquetSupport::query08);
    }

    @Test
    void query09()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query09(), TpcdsParquetSupport::query09);
    }

    @Test
    void query54()
    {
        assertUnionCompositeMatchesHarness(CompiledTpcdsQueries.query54(), TpcdsParquetSupport::query54);
    }

    /** Q17's three-fact join combination is empty at sf10: both engines must agree at zero rows (the empty oracle is permitted). */
    @Test
    void query17()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledTpcdsQueries.Ported ported = CompiledTpcdsQueries.query17();
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runLowered(new Allocator(), tables, ported.query().lower());
        assertBridgedRowsMatch(run, ported.stringColumns(), TpcdsParquetSupport::query17, tables, true);
    }

    /** Q18's cohort filter is empty at sf10: both engines must agree at zero rows (the empty oracle is permitted). */
    @Test
    void query18()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledTpcdsQueries.Ported ported = CompiledTpcdsQueries.query18();
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runLowered(new Allocator(), tables, ported.query().lower());
        assertBridgedRowsMatch(run, ported.stringColumns(), TpcdsParquetSupport::query18, tables, true);
    }

    @Test
    void query67()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query67(), TpcdsParquetSupport::query67);
    }

    @Test
    void query02()
    {
        assertUnionSelfJoinMatchesHarness(CompiledTpcdsQueries.query02(), TpcdsParquetSupport::query02);
    }

    @Test
    void query64()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query64(), TpcdsParquetSupport::query64);
    }

    @Test
    void query14()
    {
        assertLabeledUnionMatchesHarness(CompiledTpcdsQueries.query14(), TpcdsParquetSupport::query14);
    }

    @Test
    void query23()
    {
        assertLabeledUnionMatchesHarness(CompiledTpcdsQueries.query23(), TpcdsParquetSupport::query23);
    }

    @Test
    void query24()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query24(), TpcdsParquetSupport::query24);
    }

    @Test
    void query41()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query41(), TpcdsParquetSupport::query41);
    }

    /**
     * Run Q84's composite and concatenate the customer name at the bridge: the pipeline carries (id, last, first)
     * and the output row is (id, last || ", " || first), null when either name part is null -- matching the
     * harness's concat primitive. The name never participates in a comparison, so the concat is purely an
     * output-edge rendering.
     */
    @Test
    void query84()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledTpcdsQueries.Composite composite = CompiledTpcdsQueries.query84();
        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            virtuals.put(stage.virtualName(),
                    CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, composite.main().lower(), virtuals);

        byte[][][] dictionaries = new byte[3][][];
        for (CompiledTpcdsQueries.DictRef ref : composite.stringColumns()) {
            dictionaries[ref.resultColumn()] = CompiledQuerySupport.dictionaryFor(run.inputs(), ref);
        }
        org.weakref.nitro.jit.CompiledPipeline.Result result = run.result();
        List<Row> actual = new ArrayList<>();
        for (int r = 0; r < result.rowCount(); r++) {
            String id = new String(dictionaries[0][(int) result.columns()[0][r]], UTF_8);
            boolean lastNull = result.nulls() != null && result.nulls()[1] != null && result.nulls()[1][r];
            boolean firstNull = result.nulls() != null && result.nulls()[2] != null && result.nulls()[2][r];
            String name = lastNull || firstNull
                    ? null
                    : new String(dictionaries[1][(int) result.columns()[1][r]], UTF_8) + ", "
                            + new String(dictionaries[2][(int) result.columns()[2][r]], UTF_8);
            actual.add(new Row(new Object[] {id, name}));
        }

        Operator harnessChain = TpcdsParquetSupport.query84(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
        List<Row> expected = normalize(OperatorAssertions.OperatorAssert.toRows(harnessChain));
        assertThat(expected).isNotEmpty();
        assertThat(normalize(actual)).containsExactlyElementsOf(expected);
    }

    @Test
    void query45()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query45(), TpcdsParquetSupport::query45);
    }

    @Test
    void query72()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query72(), TpcdsParquetSupport::query72);
    }

    @Test
    void query44()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query44(), TpcdsParquetSupport::query44);
    }

    @Test
    void query28()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query28(), TpcdsParquetSupport::query28);
    }

    @Test
    void query30()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query30(), TpcdsParquetSupport::query30);
    }

    @Test
    void query81()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query81(), TpcdsParquetSupport::query81);
    }

    @Test
    void query83()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query83(), TpcdsParquetSupport::query83);
    }

    @Test
    void query58()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query58(), TpcdsParquetSupport::query58);
    }

    @Test
    void query57()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query57(), TpcdsParquetSupport::query57);
    }

    @Test
    void query47()
    {
        assertCompositeMatchesHarness(CompiledTpcdsQueries.query47(), TpcdsParquetSupport::query47);
    }

    @Test
    void query37()
    {
        assertStreamingPortedMatchesHarness(CompiledTpcdsQueries.query37(), TpcdsParquetSupport::query37);
    }

    @Test
    void query82()
    {
        assertStreamingPortedMatchesHarness(CompiledTpcdsQueries.query82(), TpcdsParquetSupport::query82);
    }

    /** As {@link #assertMatchesHarness}, but streams the probe fact (for a single-stage query whose probe is too large to drain). */
    private static void assertStreamingPortedMatchesHarness(CompiledTpcdsQueries.Ported ported, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStreamingPorted(new Allocator(), tables, ported.query().lower());
        assertBridgedRowsMatch(run, ported.stringColumns(), harness, tables);
    }

    /** As {@link #assertMatchesHarness}, but for a two-stage (pipeline-breaker) query run via {@code runMultiStage}. */
    private static void assertMultiStageMatchesHarness(CompiledTpcdsQueries.MultiStage staged, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runMultiStage(
                new Allocator(), tables, staged.subquery().lower(), staged.main().lower(), staged.virtualTable());
        assertBridgedRowsMatch(run, staged.stringColumns(), harness, tables);
    }

    /** Bridge a compiled result back to an operator, reconstruct its dictionary-string columns, and assert it equals the harness chain. */
    private static void assertBridgedRowsMatch(CompiledQuerySupport.LoweredResult run, List<CompiledTpcdsQueries.DictRef> stringColumns,
            HarnessChain harness, TpcdsParquetTables tables)
    {
        assertBridgedRowsMatch(run, stringColumns, harness, tables, false);
    }

    /**
     * As {@link #assertBridgedRowsMatch(CompiledQuerySupport.LoweredResult, List, HarnessChain, TpcdsParquetTables)},
     * but {@code allowEmptyOracle} skips the non-empty guard -- for queries whose filter combination is genuinely
     * empty at this scale (Q17/Q18), where exact agreement at zero rows is the strongest available check.
     */
    private static void assertBridgedRowsMatch(CompiledQuerySupport.LoweredResult run, List<CompiledTpcdsQueries.DictRef> stringColumns,
            HarnessChain harness, TpcdsParquetTables tables, boolean allowEmptyOracle)
    {
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        for (CompiledTpcdsQueries.DictRef ref : stringColumns) {
            dictionaries[ref.resultColumn()] = CompiledQuerySupport.dictionaryFor(run.inputs(), ref);
        }
        Operator compiled = new CompiledOperator(run.result(), dictionaries);

        List<Row> actual = normalize(OperatorAssertions.OperatorAssert.toRows(compiled));
        // Materialize the compiled rows before executing the independent harness oracle. Keeping both query
        // consumers active at once turns this result check into an accidental overlapping-lifetime stress test.
        Operator harnessChain = harness.build(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
        List<Row> expected = normalize(OperatorAssertions.OperatorAssert.toRows(harnessChain));
        if (!allowEmptyOracle) {
            assertThat(expected).isNotEmpty();
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    /**
     * Coerce values to a representation-independent form for comparison: integral numbers (byte/short/int/long)
     * to {@code long}, floating-point to {@code double} (kept exact, not truncated), and raw bytes to a UTF-8
     * string. This makes the compiled result (e.g. an {@code I64} key vs the harness's {@code I32}) comparable
     * without losing an aggregate's fractional value.
     */
    private static List<Row> normalize(List<Row> rows)
    {
        List<Row> normalized = new ArrayList<>(rows.size());
        for (Row row : rows) {
            Object[] values = row.values().clone();
            for (int i = 0; i < values.length; i++) {
                if (values[i] instanceof Double || values[i] instanceof Float) {
                    values[i] = ((Number) values[i]).doubleValue();
                }
                else if (values[i] instanceof Number number) {
                    values[i] = number.longValue();
                }
                else if (values[i] instanceof byte[] bytes) {
                    values[i] = new String(bytes, UTF_8);
                }
            }
            normalized.add(new Row(values));
        }
        return normalized;
    }
}
