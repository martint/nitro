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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.CompilerResources;
import org.weakref.nitro.jit.PipelineCompiler;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Validates that the data-centric compiler produces byte-identical results to the interpreted operator tree
 * for a real TPC-DS star-schema query over real sf10 Parquet (see {@link CompiledQuerySupport}). Skipped when
 * the dataset is not configured.
 */
public class TestCompiledQuery
{
    @Test
    void compiledMatchesInterpretedOnStoreSalesByItem()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledQuerySupport.Loaded data = CompiledQuerySupport.load(new Allocator(EngineResources.createDefault()), tables);

        Map<Long, Long> interpreted = runInterpreted(data);
        Map<Long, Long> compiled = runCompiled(data);

        assertThat(compiled).isEqualTo(interpreted);
        assertThat(compiled).isNotEmpty();

        // The same query lowered from column names must match the hand-built plan on the real data.
        Map<Long, Long> lowered = runLowered(data);
        assertThat(lowered).isEqualTo(compiled);
    }

    @Test
    void lowersStringFilterQueryOnRealData()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // SELECT count(*) FROM item WHERE i_category = 'Books' -- lowered by name, string filter over the
        // dictionary, loaded from real Parquet (dictionary built at load).
        org.weakref.nitro.jit.QueryLowering query = org.weakref.nitro.jit.QueryLowering.scan("item",
                new org.weakref.nitro.jit.QueryLowering.Column("i_category", org.weakref.nitro.jit.ColumnEncoding.STRING, true));
        query.where(new org.weakref.nitro.jit.Plan.StringMatch(query.position("i_category"), List.of("Books"), false)).count();

        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runLowered(new Allocator(EngineResources.createDefault()), tables, query.lower());
        long compiledCount = run.result().columns()[0][0];

        // Reference: count non-null 'Books' rows directly from the loaded dictionary column.
        org.weakref.nitro.jit.Column.StringColumn category = (org.weakref.nitro.jit.Column.StringColumn) run.inputs()[0][0];
        byte[] books = "Books".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long reference = 0;
        for (int i = 0; i < category.ids().length; i++) {
            boolean isNull = category.nulls() != null && category.nulls()[i];
            if (!isNull && java.util.Arrays.equals(category.dictionary()[category.ids()[i]], books)) {
                reference++;
            }
        }

        assertThat(reference).as("'Books' items in sf10").isGreaterThan(0);
        assertThat(compiledCount).isEqualTo(reference);
    }

    @Test
    void bridgedCompiledMatchesInterpretedOperatorRows()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // The lowered store_sales JOIN date_dim, sum(ss_quantity) GROUP BY ss_item_sk query, compiled and then
        // bridged back into the operator world, must produce the same rows as the interpreted Nitro operator chain
        // -- compared through the very same harness machinery (OperatorAssertions.toRows) used for operator trees.
        org.weakref.nitro.jit.QueryLowering query = org.weakref.nitro.jit.QueryLowering.scan("store_sales",
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_sold_date_sk"),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_item_sk"),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_quantity"))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new org.weakref.nitro.jit.QueryLowering.Column("d_date_sk"),
                        new org.weakref.nitro.jit.QueryLowering.Column("d_year"));
        // Same d_year = 2001 predicate the interpreted reference pushes into the date_dim load.
        query.where(new org.weakref.nitro.jit.Plan.Predicate("=", query.column("d_year"), new org.weakref.nitro.jit.Plan.Lit(2001)))
                .groupBy("ss_item_sk")
                .aggregate("sum", "ss_quantity");

        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runLowered(new Allocator(EngineResources.createDefault()), tables, query.lower());
        Operator bridged = new org.weakref.nitro.operator.CompiledOperator(run.result());

        List<Row> interpreted = OperatorAssertions.OperatorAssert.toRows(
                CompiledQuerySupport.interpreted(new Allocator(EngineResources.createDefault()), CompiledQuerySupport.load(new Allocator(EngineResources.createDefault()), tables)));

        assertThat(interpreted).isNotEmpty();
        assertThat(OperatorAssertions.operator(bridged)).matches(interpreted);
    }

    @Test
    void streamingMatchesEagerOnStoreSalesAggregate()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        // SELECT ss_item_sk, sum(ss_quantity) FROM store_sales GROUP BY ss_item_sk -- run eagerly (whole columns
        // materialized) and by streaming the Parquet scan batch-by-batch; the two must agree.
        org.weakref.nitro.jit.QueryLowering query = org.weakref.nitro.jit.QueryLowering.scan("store_sales",
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_item_sk"),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_quantity"))
                .groupBy("ss_item_sk")
                .aggregate("sum", "ss_quantity");
        org.weakref.nitro.jit.QueryLowering.Lowered lowered = query.lower();

        Map<Long, Long> eager = toMap(CompiledQuerySupport.runLowered(new Allocator(EngineResources.createDefault()), tables, lowered).result());

        org.weakref.nitro.jit.StreamingPipeline streaming =
                new org.weakref.nitro.jit.PipelineCompiler(org.weakref.nitro.jit.CompilerResources.createDefault()).compileStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable());
        Map<Long, Long> streamed = toMap(streaming.execute(
                CompiledQuerySupport.parquetFlatSource(new Allocator(EngineResources.createDefault()), tables, "store_sales", "ss_item_sk", "ss_quantity"),
                new org.weakref.nitro.jit.Column[0][], new int[0]));
        Map<Long, Long> zeroCopy = toMap(streaming.execute(
                CompiledQuerySupport.parquetColumnarSource(new Allocator(EngineResources.createDefault()), tables, "store_sales", "ss_item_sk", "ss_quantity"),
                new org.weakref.nitro.jit.Column[0][], new int[0]));

        assertThat(streamed).isEqualTo(eager);
        assertThat(zeroCopy).isEqualTo(eager);
        assertThat(streamed).isNotEmpty();
    }

    private static Map<Long, Long> toMap(CompiledPipeline.Result result)
    {
        Map<Long, Long> map = new HashMap<>();
        for (int g = 0; g < result.rowCount(); g++) {
            map.put(result.columns()[0][g], result.columns()[1][g]);
        }
        return map;
    }

    private static Map<Long, Long> runLowered(CompiledQuerySupport.Loaded data)
    {
        org.weakref.nitro.jit.QueryLowering query = org.weakref.nitro.jit.QueryLowering.scan("store_sales",
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_sold_date_sk"),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_item_sk"),
                        new org.weakref.nitro.jit.QueryLowering.Column("ss_quantity"))
                .join("date_dim", "ss_sold_date_sk", "d_date_sk",
                        new org.weakref.nitro.jit.QueryLowering.Column("d_date_sk"))
                .groupBy("ss_item_sk")
                .aggregate("sum", "ss_quantity");
        org.weakref.nitro.jit.CompiledPipeline.Result result = query.lower().compile(new PipelineCompiler(CompilerResources.createDefault())).execute(
                new long[][][] {{data.soldDateSk(), data.itemSk(), data.quantity()}, {data.dateSk()}},
                new int[] {data.storeSalesRows(), data.dateDimRows()});
        Map<Long, Long> map = new HashMap<>();
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        for (int g = 0; g < result.rowCount(); g++) {
            map.put(keys[g], sums[g]);
        }
        return map;
    }

    private static Map<Long, Long> runInterpreted(CompiledQuerySupport.Loaded data)
    {
        Map<Long, Long> result = new HashMap<>();
        try (Operator aggregation = CompiledQuerySupport.interpreted(new Allocator(EngineResources.createDefault()), data)) {
            while (aggregation.hasNext()) {
                try (Batch batch = aggregation.next()) {
                    Mask mask = batch.borrowMask();
                    I64Vector keys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    I64Vector sums = (I64Vector) batch.output(1).borrow(Stream.VALUES);
                    for (int index = 0; index < mask.count(); index++) {
                        int position = mask.position(index);
                        result.put(keys.values()[position], sums.values()[position]);
                    }
                }
            }
        }
        return result;
    }

    private static Map<Long, Long> runCompiled(CompiledQuerySupport.Loaded data)
    {
        CompiledPipeline.Result result = CompiledQuerySupport.runCompiled(CompiledQuerySupport.compile(), data);
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        Map<Long, Long> map = new HashMap<>();
        for (int g = 0; g < result.rowCount(); g++) {
            map.put(keys[g], sums[g]);
        }
        return map;
    }
}
