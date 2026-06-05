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
    void query26()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query26(), TpcdsParquetSupport::query26);
    }

    @Test
    void query42()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query42(), TpcdsParquetSupport::query42);
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
    void query96()
    {
        assertMatchesHarness(CompiledTpcdsQueries.query96(), TpcdsParquetSupport::query96);
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
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        if (ported.stringResultColumn() >= 0) {
            dictionaries[ported.stringResultColumn()] = ((Column.StringColumn) run.inputs()[ported.dictInput()][ported.dictColumn()]).dictionary();
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
