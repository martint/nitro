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
package org.weakref.trino.tpch;

import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.weakref.nitro.tpch.TpchParquetTables;
import org.weakref.nitro.trino.TrinoTpchParquetSupport;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Trino operator-tree TPC-H harness against the checked-in reference results (the same TSVs the Nitro
 * harness validates against). Longs and strings compare exactly; doubles within relative tolerance --
 * cross-engine floating-point sums are accumulation-order dependent. DATE cells materialize as
 * {@link LocalDate} and compare against the reference's ISO dates.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestQueries
{
    private static final double RELATIVE_TOLERANCE = 1e-9;

    private TrinoTpchParquetSupport support;

    @BeforeAll
    void setUp()
    {
        support = new TrinoTpchParquetSupport();
    }

    @AfterAll
    void tearDown()
    {
        if (support != null) {
            support.close();
            support = null;
        }
    }

    @Test
    void query01()
    {
        assertMatchesReference("01", tables -> support.query01(tables));
    }

    @Test
    void query02()
    {
        assertMatchesReference("02", tables -> support.query02(tables));
    }

    @Test
    void query03()
    {
        assertMatchesReference("03", tables -> support.query03(tables));
    }

    @Test
    void query04()
    {
        assertMatchesReference("04", tables -> support.query04(tables));
    }

    @Test
    void query05()
    {
        assertMatchesReference("05", tables -> support.query05(tables));
    }

    @Test
    void query06()
    {
        assertMatchesReference("06", tables -> support.query06(tables));
    }

    @Test
    void query07()
    {
        assertMatchesReference("07", tables -> support.query07(tables));
    }

    @Test
    void query08()
    {
        assertMatchesReference("08", tables -> support.query08(tables));
    }

    @Test
    void query09()
    {
        assertMatchesReference("09", tables -> support.query09(tables));
    }

    @Test
    void query10()
    {
        assertMatchesReference("10", tables -> support.query10(tables));
    }

    @Test
    void query11()
    {
        // The output sorts by a double sum: near-equal values order differently across engines, so rows
        // within a tolerance-tied run of the sort key compare as sets of their first column.
        assertMatchesReferenceWithTies("11", 1, tables -> support.query11(tables));
    }

    @Test
    void query12()
    {
        assertMatchesReference("12", tables -> support.query12(tables));
    }

    @Test
    void query13()
    {
        assertMatchesReference("13", tables -> support.query13(tables));
    }

    @Test
    void query14()
    {
        assertMatchesReference("14", tables -> support.query14(tables));
    }

    @Test
    void query15()
    {
        assertMatchesReference("15", tables -> support.query15(tables));
    }

    @Test
    void query16()
    {
        assertMatchesReference("16", tables -> support.query16(tables));
    }

    @Test
    void query17()
    {
        assertMatchesReference("17", tables -> support.query17(tables));
    }

    @Test
    void query18()
    {
        assertMatchesReference("18", tables -> support.query18(tables));
    }

    @Test
    void query19()
    {
        assertMatchesReference("19", tables -> support.query19(tables));
    }

    @Test
    void query20()
    {
        assertMatchesReference("20", tables -> support.query20(tables));
    }

    @Test
    void query21()
    {
        assertMatchesReference("21", tables -> support.query21(tables));
    }

    @Test
    void query22()
    {
        assertMatchesReference("22", tables -> support.query22(tables));
    }

    /**
     * Like {@link #assertMatchesReference}, but rows whose double sort key ties within tolerance compare
     * as a set of their first column: cross-engine accumulation order makes near-equal sums reorder.
     */
    private void assertMatchesReferenceWithTies(String queryId, int sortKeyColumn, Function<TpchParquetTables, MaterializedResult> query)
    {
        var tables = TpchParquetTables.actualIfPresent();
        assumeTrue(tables.isPresent(), "TPC-H parquet data not present");

        List<MaterializedRow> actual = query.apply(tables.orElseThrow()).getMaterializedRows();
        List<List<String>> expected = readExpected(queryId);
        assertThat(actual).as("q%s row count", queryId).hasSize(expected.size());

        int rowIndex = 0;
        while (rowIndex < expected.size()) {
            // A tie run: consecutive expected rows whose sort keys agree within tolerance.
            double key = Double.parseDouble(expected.get(rowIndex).get(sortKeyColumn));
            int runEnd = rowIndex + 1;
            while (runEnd < expected.size() && nearlyEqual(Double.parseDouble(expected.get(runEnd).get(sortKeyColumn)), key)) {
                runEnd++;
            }
            if (runEnd - rowIndex == 1) {
                List<String> expectedRow = expected.get(rowIndex);
                MaterializedRow actualRow = actual.get(rowIndex);
                assertThat(actualRow.getFieldCount()).as("q%s row %s width", queryId, rowIndex).isEqualTo(expectedRow.size());
                for (int column = 0; column < expectedRow.size(); column++) {
                    assertCell(queryId, rowIndex, column, expectedRow.get(column), actualRow.getField(column));
                }
            }
            else {
                Set<String> expectedKeys = new HashSet<>();
                Set<String> actualKeys = new HashSet<>();
                for (int run = rowIndex; run < runEnd; run++) {
                    expectedKeys.add(expected.get(run).get(0));
                    actualKeys.add(String.valueOf(actual.get(run).getField(0)));
                }
                assertThat(actualKeys).as("q%s tie run at %s", queryId, rowIndex).isEqualTo(expectedKeys);
            }
            rowIndex = runEnd;
        }
    }

    private static boolean nearlyEqual(double left, double right)
    {
        if (left == right) {
            return true;
        }
        return Math.abs(left - right) <= Math.max(Math.abs(left), Math.abs(right)) * 1e-9;
    }

    private void assertMatchesReference(String queryId, Function<TpchParquetTables, MaterializedResult> query)
    {
        var tables = TpchParquetTables.actualIfPresent();
        assumeTrue(tables.isPresent(), "TPC-H parquet data not present");

        List<MaterializedRow> actual = query.apply(tables.orElseThrow()).getMaterializedRows();
        List<List<String>> expected = readExpected(queryId);

        assertThat(actual).as("q%s row count", queryId).hasSize(expected.size());
        for (int rowIndex = 0; rowIndex < expected.size(); rowIndex++) {
            List<String> expectedRow = expected.get(rowIndex);
            MaterializedRow actualRow = actual.get(rowIndex);
            assertThat(actualRow.getFieldCount()).as("q%s row %s width", queryId, rowIndex).isEqualTo(expectedRow.size());
            for (int column = 0; column < expectedRow.size(); column++) {
                assertCell(queryId, rowIndex, column, expectedRow.get(column), actualRow.getField(column));
            }
        }
    }

    private static void assertCell(String queryId, int rowIndex, int column, String expected, Object actual)
    {
        String context = "q%s row %s column %s".formatted(queryId, rowIndex, column);
        if (expected.equals("\\N")) {
            assertThat(actual).as(context).isNull();
            return;
        }
        switch (actual) {
            case null -> assertThat(expected).as(context).isEqualTo("\\N");
            case Double actualDouble -> {
                double expectedDouble = Double.parseDouble(expected);
                if (expectedDouble == 0.0) {
                    assertThat(actualDouble).as(context).isEqualTo(0.0, within(1e-9));
                }
                else {
                    assertThat(actualDouble / expectedDouble).as(context).isEqualTo(1.0, within(RELATIVE_TOLERANCE));
                }
            }
            case Long actualLong -> assertThat(actualLong).as(context).isEqualTo(Long.parseLong(expected));
            case LocalDate actualDate -> assertThat(actualDate.toEpochDay()).as(context).isEqualTo(LocalDate.parse(expected).toEpochDay());
            case String actualString -> assertThat(actualString).as(context).isEqualTo(expected);
            default -> assertThat(actual.toString()).as(context).isEqualTo(expected);
        }
    }

    private static List<List<String>> readExpected(String queryId)
    {
        String resource = "/tpch/q" + queryId + ".expected.tsv";
        try (InputStream input = TestQueries.class.getResourceAsStream(resource)) {
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
