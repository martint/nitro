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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.CompiledOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.tpcds.CompiledQuerySupport;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Validates every compiled ClickBench query byte-exact against the {@link ClickBenchHitsSupport} operator chain
 * on the real split hits data (the same oracle {@link TestRealData} pins).
 */
public class TestCompiledClickBenchQueries
{
    @FunctionalInterface
    interface HarnessChain
    {
        Operator build(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path hits);
    }

    @Test
    void query01()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query01(), (allocator, registry, hits) -> ClickBenchHitsSupport.query01(allocator, hits));
    }

    @Test
    void query02()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query02(), ClickBenchHitsSupport::query02);
    }

    @Test
    void query03()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query03(), (allocator, registry, hits) -> ClickBenchHitsSupport.query03(allocator, hits));
    }

    @Test
    void query04()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query04(), (allocator, registry, hits) -> ClickBenchHitsSupport.query04(allocator, hits));
    }

    @Test
    void query05()
    {
        assertCompositeMatchesHarness(CompiledClickBenchQueries.query05(), (allocator, registry, hits) -> ClickBenchHitsSupport.query05(allocator, hits));
    }

    @Test
    void query07()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query07(), (allocator, registry, hits) -> ClickBenchHitsSupport.query07(allocator, hits));
    }

    @Test
    void query08()
    {
        assertTopKMatchesHarness(CompiledClickBenchQueries.query08(), ClickBenchHitsSupport::query08, 1);
    }

    @Test
    void query16()
    {
        assertTopKMatchesHarness(CompiledClickBenchQueries.query16(), (allocator, registry, hits) -> ClickBenchHitsSupport.query16(allocator, hits), 1);
    }

    @Test
    void query31()
    {
        assertTopKMatchesHarness(CompiledClickBenchQueries.query31(), ClickBenchHitsSupport::query31, 2);
    }

    @Test
    void query32()
    {
        assertTopKMatchesHarness(CompiledClickBenchQueries.query32(), ClickBenchHitsSupport::query32, 2);
    }

    @Test
    void query33()
    {
        assertTopKMatchesHarness(CompiledClickBenchQueries.query33(), (allocator, registry, hits) -> ClickBenchHitsSupport.query33(allocator, hits), 2);
    }

    @Test
    void query09()
    {
        assertCompositeMatchesHarness(CompiledClickBenchQueries.query09(), (allocator, registry, hits) -> ClickBenchHitsSupport.query09(allocator, hits));
    }

    @Test
    void query20()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query20(), ClickBenchHitsSupport::query20);
    }

    @Test
    void query21()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query21(), ClickBenchHitsSupport::query21);
    }

    @Test
    void query30()
    {
        assertMatchesHarness(CompiledClickBenchQueries.query30(), ClickBenchHitsSupport::query30);
    }

    @Test
    void query36()
    {
        assertCompositeMatchesHarness(CompiledClickBenchQueries.query36(), ClickBenchHitsSupport::query36);
    }

    @Test
    void query41()
    {
        assertTopKMatchesHarness(CompiledClickBenchQueries::query41, ClickBenchHitsSupport::query41, 2, true);
    }

    @Test
    void query42()
    {
        assertTopKMatchesHarness(CompiledClickBenchQueries::query42, ClickBenchHitsSupport::query42, 2, true);
    }

    @Test
    void query43()
    {
        assertMatchesHarness(CompiledClickBenchQueries::query43, ClickBenchHitsSupport::query43);
    }

    private static void assertMatchesHarness(CompiledTpcdsQueries.Ported ported, HarnessChain harness)
    {
        assertMatchesHarness(hits -> ported, harness);
    }

    private static void assertMatchesHarness(Function<Path, CompiledTpcdsQueries.Ported> portedForData, HarnessChain harness)
    {
        ClickBenchParquetTables tables = requireHits();
        CompiledTpcdsQueries.Ported ported = portedForData.apply(tables.directory());
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStreamingPorted(new Allocator(), tables, ported.query().lower());
        assertBridgedRowsMatch(run, ported.stringColumns(), harness, tables);
    }

    /**
     * As {@link #assertMatchesHarness}, but for a top-K-by-aggregate query whose LIMIT can cut inside a tie group:
     * the SQL does not determine which of the tied rows survives, so the engines may legitimately differ there.
     * Asserts the sort-key sequence matches positionally everywhere, and rows match exactly (as multisets within
     * each maximal tie run) for every run except the one the limit truncates.
     */
    private static void assertTopKMatchesHarness(CompiledTpcdsQueries.Ported ported, HarnessChain harness, int sortColumn)
    {
        assertTopKMatchesHarness(hits -> ported, harness, sortColumn, false);
    }

    private static void assertTopKMatchesHarness(Function<Path, CompiledTpcdsQueries.Ported> portedForData, HarnessChain harness, int sortColumn, boolean offsetCut)
    {
        ClickBenchParquetTables tables = requireHits();
        CompiledTpcdsQueries.Ported ported = portedForData.apply(tables.directory());
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStreamingPorted(new Allocator(), tables, ported.query().lower());
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        for (CompiledTpcdsQueries.DictRef ref : ported.stringColumns()) {
            dictionaries[ref.resultColumn()] = CompiledQuerySupport.dictionaryFor(run.inputs(), ref);
        }
        List<Row> actual = normalize(OperatorAssertions.OperatorAssert.toRows(new CompiledOperator(run.result(), dictionaries)));
        Operator harnessChain = harness.build(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables.directory());
        List<Row> expected = normalize(OperatorAssertions.OperatorAssert.toRows(harnessChain));
        assertThat(expected).isNotEmpty();
        assertTopKRows(actual, expected, sortColumn, offsetCut);
    }

    private static void assertTopKRows(List<Row> actual, List<Row> expected, int sortColumn, boolean offsetCut)
    {
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i).values()[sortColumn]).as("sort key at row %d", i).isEqualTo(expected.get(i).values()[sortColumn]);
        }
        int start = 0;
        while (start < expected.size()) {
            Object key = expected.get(start).values()[sortColumn];
            int end = start;
            while (end < expected.size() && key.equals(expected.get(end).values()[sortColumn])) {
                end++;
            }
            // OFFSET can cut into the window's first tie run just like LIMIT cuts the last; both ends'
            // incomplete runs are checked on sort key only (above), complete runs on exact rows.
            boolean cutByOffset = offsetCut && start == 0;
            if (end < expected.size() && !cutByOffset) {
                // A complete tie run: the same rows in some order.
                assertThat(actual.subList(start, end)).as("tie run [%d, %d)", start, end)
                        .containsExactlyInAnyOrderElementsOf(expected.subList(start, end));
            }
            start = end;
        }
    }

    private static void assertCompositeMatchesHarness(CompiledTpcdsQueries.Composite composite, HarnessChain harness)
    {
        ClickBenchParquetTables tables = requireHits();
        Allocator allocator = new Allocator();
        Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStage(allocator, tables, composite.main().lower(), virtuals);
        assertBridgedRowsMatch(run, composite.stringColumns(), harness, tables);
    }

    private static ClickBenchParquetTables requireHits()
    {
        var tables = ClickBenchParquetTables.actualIfPresent().orElse(null);
        assumeTrue(tables != null, "Set -D" + "nitro.clickbench.hits.path=/path/to/clickbench or place the split parquet files at ~/tmp/clickbench");
        return tables;
    }

    private static void assertBridgedRowsMatch(CompiledQuerySupport.LoweredResult run, List<CompiledTpcdsQueries.DictRef> stringColumns,
            HarnessChain harness, ClickBenchParquetTables tables)
    {
        byte[][][] dictionaries = new byte[run.result().columns().length][][];
        for (CompiledTpcdsQueries.DictRef ref : stringColumns) {
            dictionaries[ref.resultColumn()] = CompiledQuerySupport.dictionaryFor(run.inputs(), ref);
        }
        Operator compiled = new CompiledOperator(run.result(), dictionaries);

        Operator harnessChain = harness.build(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables.directory());
        List<Row> expected = normalize(OperatorAssertions.OperatorAssert.toRows(harnessChain));
        List<Row> actual = normalize(OperatorAssertions.OperatorAssert.toRows(compiled));
        assertThat(expected).isNotEmpty();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    /** Coerce values to a representation-independent form: integral to long, floating-point to double, bytes to UTF-8. */
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
