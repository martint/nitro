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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.jit.Types;
import org.weakref.nitro.tpcds.CompiledQuerySupport;
import org.weakref.nitro.tpcds.CompiledTpcdsQueries;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The compiled TPC-H queries against the same checked-in reference results the operator harness validates
 * against (see {@link TestTpchQueries} for the comparison semantics: longs and strings exact, doubles within
 * relative tolerance, DATE epoch-day longs against ISO dates).
 */
public class TestCompiledTpchQueries
{
    private static final double RELATIVE_TOLERANCE = 1e-9;

    @Test
    void query01()
    {
        assertMatchesReference("01", CompiledTpchQueries.query01());
    }

    @Test
    void query06()
    {
        assertMatchesReference("06", CompiledTpchQueries.query06());
    }

    @Test
    void query03()
    {
        assertMatchesReference("03", CompiledTpchQueries.query03());
    }

    @Test
    void query05()
    {
        assertMatchesReference("05", CompiledTpchQueries.query05());
    }

    @Test
    void query10()
    {
        assertMatchesReference("10", CompiledTpchQueries.query10());
    }

    @Test
    void query12()
    {
        assertMatchesReference("12", CompiledTpchQueries.query12());
    }

    @Test
    void query14()
    {
        assertMatchesReference("14", CompiledTpchQueries.query14());
    }

    @Test
    void query19()
    {
        assertMatchesReference("19", CompiledTpchQueries.query19());
    }

    @Test
    void query07()
    {
        assertMatchesReference("07", CompiledTpchQueries.query07());
    }

    @Test
    void query08()
    {
        assertMatchesReference("08", CompiledTpchQueries.query08());
    }

    @Test
    void query09()
    {
        assertMatchesReference("09", CompiledTpchQueries.query09());
    }

    @Test
    void query04()
    {
        assertMatchesReference("04", CompiledTpchQueries.query04());
    }

    @Test
    void query18()
    {
        assertMatchesReference("18", CompiledTpchQueries.query18());
    }

    @Test
    void query13()
    {
        assertMatchesReference("13", CompiledTpchQueries.query13());
    }

    @Test
    void query16()
    {
        assertMatchesReference("16", CompiledTpchQueries.query16());
    }

    @Test
    void query02()
    {
        assertMatchesReference("02", CompiledTpchQueries.query02());
    }

    @Test
    void query15()
    {
        assertMatchesReference("15", CompiledTpchQueries.query15());
    }

    @Test
    void query17()
    {
        assertMatchesReference("17", CompiledTpchQueries.query17());
    }

    @Test
    void query11()
    {
        assertMatchesReferenceWithTies("11", 1, CompiledTpchQueries.query11());
    }

    @Test
    void query20()
    {
        assertMatchesReference("20", CompiledTpchQueries.query20());
    }

    @Test
    void query21()
    {
        assertMatchesReference("21", CompiledTpchQueries.query21());
    }

    @Test
    void query22()
    {
        assertMatchesReference("22", CompiledTpchQueries.query22());
    }

    /**
     * As {@link #assertMatchesReference(String, CompiledTpcdsQueries.Composite)}, but tolerance-tied runs of the
     * double sort key compare as sets of their first column -- the row order within a near-tie is accumulation-order
     * dependent across engines (the same oracle the operator harness uses for Q11).
     */
    private static void assertMatchesReferenceWithTies(String queryId, int sortKeyColumn, CompiledTpcdsQueries.Composite composite)
    {
        var tables = TpchParquetTables.actualIfPresent();
        assumeTrue(tables.isPresent(), "TPC-H parquet data not present");

        CompiledQuerySupport.LoweredResult run = runComposite(tables.orElseThrow(), composite);
        CompiledPipeline.Result result = run.result();
        List<List<String>> expected = readExpected(queryId);
        assertThat(result.rowCount()).as("q%s row count", queryId).isEqualTo(expected.size());

        int rowIndex = 0;
        while (rowIndex < expected.size()) {
            double key = Double.parseDouble(expected.get(rowIndex).get(sortKeyColumn));
            int runEnd = rowIndex + 1;
            while (runEnd < expected.size() && nearlyEqual(Double.parseDouble(expected.get(runEnd).get(sortKeyColumn)), key)) {
                runEnd++;
            }
            if (runEnd - rowIndex == 1) {
                List<String> expectedRow = expected.get(rowIndex);
                for (int column = 0; column < expectedRow.size(); column++) {
                    assertCell(queryId, rowIndex, column, expectedRow.get(column), result, new byte[result.columns().length][][]);
                }
            }
            else {
                Set<String> expectedKeys = new HashSet<>();
                Set<String> actualKeys = new HashSet<>();
                for (int row = rowIndex; row < runEnd; row++) {
                    expectedKeys.add(expected.get(row).get(0));
                    actualKeys.add(String.valueOf(result.columns()[0][row]));
                }
                assertThat(actualKeys).as("q%s tie run at %s", queryId, rowIndex).isEqualTo(expectedKeys);
            }
            rowIndex = runEnd;
        }
    }

    private static boolean nearlyEqual(double actual, double expected)
    {
        if (expected == 0.0) {
            return Math.abs(actual) < 1e-9;
        }
        return Math.abs(actual / expected - 1.0) < RELATIVE_TOLERANCE;
    }

    private static void assertMatchesReference(String queryId, CompiledTpcdsQueries.Composite composite)
    {
        var tables = TpchParquetTables.actualIfPresent();
        assumeTrue(tables.isPresent(), "TPC-H parquet data not present");

        CompiledQuerySupport.LoweredResult run = runComposite(tables.orElseThrow(), composite);
        assertMatchesReference(queryId, run, composite.stringColumns());
    }

    private static CompiledQuerySupport.LoweredResult runComposite(TpchParquetTables tables, CompiledTpcdsQueries.Composite composite)
    {
        Allocator allocator = new Allocator();
        Map<String, CompiledQuerySupport.Materialized> virtuals = new HashMap<>();
        for (CompiledTpcdsQueries.Stage stage : composite.stages()) {
            virtuals.put(stage.virtualName(), CompiledQuerySupport.materializeStage(allocator, tables, stage.plan().lower(), virtuals, stage.stringColumns()));
        }
        return CompiledQuerySupport.runStage(allocator, tables, composite.main().lower(), virtuals);
    }

    private static void assertMatchesReference(String queryId, CompiledTpcdsQueries.Ported ported)
    {
        var tables = TpchParquetTables.actualIfPresent();
        assumeTrue(tables.isPresent(), "TPC-H parquet data not present");

        CompiledQuerySupport.LoweredResult run = CompiledQuerySupport.runStreamingPorted(new Allocator(), tables.orElseThrow(), ported.query().lower());
        assertMatchesReference(queryId, run, ported.stringColumns());
    }

    private static void assertMatchesReference(String queryId, CompiledQuerySupport.LoweredResult run, List<CompiledTpcdsQueries.DictRef> stringColumns)
    {
        CompiledPipeline.Result result = run.result();
        byte[][][] dictionaries = new byte[result.columns().length][][];
        for (CompiledTpcdsQueries.DictRef ref : stringColumns) {
            dictionaries[ref.resultColumn()] = CompiledQuerySupport.dictionaryFor(run.inputs(), ref);
        }

        List<List<String>> expected = readExpected(queryId);
        assertThat(result.rowCount()).as("q%s row count", queryId).isEqualTo(expected.size());
        for (int row = 0; row < expected.size(); row++) {
            List<String> expectedRow = expected.get(row);
            assertThat(result.columns().length).as("q%s width", queryId).isEqualTo(expectedRow.size());
            for (int column = 0; column < expectedRow.size(); column++) {
                assertCell(queryId, row, column, expectedRow.get(column), result, dictionaries);
            }
        }
    }

    private static void assertCell(String queryId, int row, int column, String expected, CompiledPipeline.Result result, byte[][][] dictionaries)
    {
        String context = "q%s row %s column %s".formatted(queryId, row, column);
        boolean isNull = result.nulls() != null && result.nulls()[column] != null && result.nulls()[column][row];
        if (expected.equals("\\N")) {
            assertThat(isNull).as(context).isTrue();
            return;
        }
        assertThat(isNull).as(context + " null").isFalse();
        long slot = result.columns()[column][row];
        if (dictionaries[column] != null) {
            String actual = new String(dictionaries[column][(int) slot], StandardCharsets.UTF_8);
            assertThat(actual).as(context).isEqualTo(expected);
            return;
        }
        if (result.types()[column] == Types.DOUBLE) {
            double actual = Double.longBitsToDouble(slot);
            double expectedDouble = Double.parseDouble(expected);
            if (expectedDouble == 0.0) {
                assertThat(actual).as(context).isEqualTo(0.0, within(1e-9));
            }
            else {
                assertThat(actual / expectedDouble).as(context).isEqualTo(1.0, within(RELATIVE_TOLERANCE));
            }
            return;
        }
        if (expected.length() == 10 && expected.charAt(4) == '-' && expected.charAt(7) == '-') {
            assertThat(slot).as(context).isEqualTo(LocalDate.parse(expected).toEpochDay());
            return;
        }
        assertThat(slot).as(context).isEqualTo(Long.parseLong(expected));
    }

    private static List<List<String>> readExpected(String queryId)
    {
        String resource = "/tpch/q" + queryId + ".expected.tsv";
        try (InputStream input = TestCompiledTpchQueries.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("Missing reference resource: " + resource);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
                    .map(line -> List.of(line.split("\t", -1)))
                    .toList();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to read " + resource, exception);
        }
    }
}
