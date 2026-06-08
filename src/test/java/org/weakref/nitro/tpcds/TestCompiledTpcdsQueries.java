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
import org.weakref.nitro.jit.Column;
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
        assertMatchesHarness(CompiledTpcdsQueries.query26(), TpcdsParquetSupport::query26);
    }

    @Test
    void query27()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query27(), TpcdsParquetSupport::query27);
    }

    @Test
    void query22()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query22(), TpcdsParquetSupport::query22);
    }

    @Test
    void query32()
    {
        assertMultiStageMatchesHarness(CompiledTpcdsQueries.query32(), TpcdsParquetSupport::query32);
    }

    @Test
    void query33()
    {
        assertUnionMatchesHarness(CompiledTpcdsQueries.query33(), TpcdsParquetSupport::query33);
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
        ported.put("48", CompiledTpcdsQueries.query48());
        ported.put("43", CompiledTpcdsQueries.query43());
        ported.put("52", CompiledTpcdsQueries.query52());
        ported.put("55", CompiledTpcdsQueries.query55());
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
                CompiledQuerySupport.parquetLazySource(new Allocator(), tables, probe.table(), probe.columns()),
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

    /** As {@link #assertMatchesHarness}, but for a UNION ALL query run via {@code runUnion} (branches + final stage). */
    private static void assertUnionMatchesHarness(CompiledTpcdsQueries.Union union, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        List<org.weakref.nitro.jit.QueryLowering.Lowered> branches = new ArrayList<>();
        for (org.weakref.nitro.jit.QueryLowering branch : union.branches()) {
            branches.add(branch.lower());
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runUnion(
                new Allocator(), tables, branches, union.main().lower(), union.virtualTable(), union.branchStringColumns());
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

    /** Run a multi-stage query as a tree of compiled pipelines: each stage materialized under its virtual name, then the main. */
    private static void assertCompositeMatchesHarness(CompiledTpcdsQueries.Composite composite, HarnessChain harness)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        Allocator allocator = new Allocator();
        java.util.Map<String, CompiledQuerySupport.Materialized> virtuals = new java.util.HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, composite.main().lower(), virtuals);
        assertBridgedRowsMatch(run, composite.stringColumns(), harness, tables);
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
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        for (CompiledTpcdsQueries.DictRef ref : stringColumns) {
            dictionaries[ref.resultColumn()] = ((Column.StringColumn) run.inputs()[ref.dictInput()][ref.dictColumn()]).dictionary();
        }
        Operator compiled = new CompiledOperator(run.result(), dictionaries);

        Operator harnessChain = harness.build(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
        List<Row> expected = normalize(OperatorAssertions.OperatorAssert.toRows(harnessChain));
        List<Row> actual = normalize(OperatorAssertions.OperatorAssert.toRows(compiled));
        assertThat(expected).isNotEmpty();
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
