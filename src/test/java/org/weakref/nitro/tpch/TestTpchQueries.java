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
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Operator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Nitro TPC-H harness against the checked-in reference results (independent engine over the same sf10
 * parquet; see TpchQueryCatalog). Longs and strings compare exactly; doubles within relative tolerance --
 * cross-engine floating-point sums are accumulation-order dependent. DATE columns surface as epoch-day integers
 * and compare against the reference's ISO dates.
 */
public class TestTpchQueries
{
    private static final double RELATIVE_TOLERANCE = 1e-9;

    @Test
    void query01()
    {
        assertMatchesReference("01", tables -> TpchParquetSupport.query01(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query03()
    {
        assertMatchesReference("03", tables -> TpchParquetSupport.query03(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query04()
    {
        assertMatchesReference("04", tables -> TpchParquetSupport.query04(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query05()
    {
        assertMatchesReference("05", tables -> TpchParquetSupport.query05(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query06()
    {
        assertMatchesReference("06", tables -> TpchParquetSupport.query06(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query10()
    {
        assertMatchesReference("10", tables -> TpchParquetSupport.query10(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query12()
    {
        assertMatchesReference("12", tables -> TpchParquetSupport.query12(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query14()
    {
        assertMatchesReference("14", tables -> TpchParquetSupport.query14(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query19()
    {
        assertMatchesReference("19", tables -> TpchParquetSupport.query19(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query07()
    {
        assertMatchesReference("07", tables -> TpchParquetSupport.query07(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query08()
    {
        assertMatchesReference("08", tables -> TpchParquetSupport.query08(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query09()
    {
        assertMatchesReference("09", tables -> TpchParquetSupport.query09(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query13()
    {
        assertMatchesReference("13", tables -> TpchParquetSupport.query13(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query16()
    {
        assertMatchesReference("16", tables -> TpchParquetSupport.query16(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query18()
    {
        assertMatchesReference("18", tables -> TpchParquetSupport.query18(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query11()
    {
        // The output sorts by a double sum: near-equal values order differently across engines, so rows
        // within a tolerance-tied run of the sort key compare as sets.
        assertMatchesReferenceWithTies("11", 1, tables -> TpchParquetSupport.query11(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query15()
    {
        assertMatchesReference("15", tables -> TpchParquetSupport.query15(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query17()
    {
        assertMatchesReference("17", tables -> TpchParquetSupport.query17(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query02()
    {
        assertMatchesReference("02", tables -> TpchParquetSupport.query02(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query20()
    {
        assertMatchesReference("20", tables -> TpchParquetSupport.query20(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query21()
    {
        assertMatchesReference("21", tables -> TpchParquetSupport.query21(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query22()
    {
        assertMatchesReference("22", tables -> TpchParquetSupport.query22(new Allocator(EngineResources.createDefault()), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    private static void assertMatchesReferenceWithTies(String queryId, int sortKeyColumn, java.util.function.Function<TpchParquetTables, Operator> query)
    {
        var tables = TpchParquetTables.actualIfPresent();
        assumeTrue(tables.isPresent(), "TPC-H parquet data not present");

        List<Row> actual = OperatorAssertions.OperatorAssert.toRows(query.apply(tables.orElseThrow()));
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
                Object[] actualRow = actual.get(rowIndex).values();
                for (int column = 0; column < expectedRow.size(); column++) {
                    assertCell(queryId, rowIndex, column, expectedRow.get(column), actualRow[column]);
                }
            }
            else {
                // Within the run, compare the rows' non-key cells as a multiset keyed by their first column.
                java.util.Set<String> expectedKeys = new java.util.HashSet<>();
                java.util.Set<String> actualKeys = new java.util.HashSet<>();
                for (int run = rowIndex; run < runEnd; run++) {
                    expectedKeys.add(expected.get(run).get(0));
                    Object value = actual.get(run).values()[0];
                    actualKeys.add(value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value));
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

    private static void assertMatchesReference(String queryId, java.util.function.Function<TpchParquetTables, Operator> query)
    {
        var tables = TpchParquetTables.actualIfPresent();
        assumeTrue(tables.isPresent(), "TPC-H parquet data not present");

        List<Row> actual = OperatorAssertions.OperatorAssert.toRows(query.apply(tables.orElseThrow()));
        List<List<String>> expected = readExpected(queryId);

        assertThat(actual).as("q%s row count", queryId).hasSize(expected.size());
        for (int rowIndex = 0; rowIndex < expected.size(); rowIndex++) {
            List<String> expectedRow = expected.get(rowIndex);
            Object[] actualRow = actual.get(rowIndex).values();
            assertThat(actualRow.length).as("q%s row %s width", queryId, rowIndex).isEqualTo(expectedRow.size());
            for (int column = 0; column < expectedRow.size(); column++) {
                assertCell(queryId, rowIndex, column, expectedRow.get(column), actualRow[column]);
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
            case Long actualLong -> {
                // A DATE column surfaces as epoch days; the reference holds an ISO date.
                if (expected.length() == 10 && expected.charAt(4) == '-' && expected.charAt(7) == '-') {
                    assertThat(actualLong).as(context).isEqualTo(LocalDate.parse(expected).toEpochDay());
                }
                else {
                    assertThat(actualLong).as(context).isEqualTo(Long.parseLong(expected));
                }
            }
            case Integer actualInteger -> {
                // Generic operators preserve the Parquet physical width. DATE can therefore remain
                // an I32 epoch day when it is carried through joins/TopN rather than becoming an I64
                // grouping key.
                if (expected.length() == 10 && expected.charAt(4) == '-' && expected.charAt(7) == '-') {
                    assertThat(actualInteger).as(context).isEqualTo(toIntExact(LocalDate.parse(expected).toEpochDay()));
                }
                else {
                    assertThat(actualInteger).as(context).isEqualTo(Integer.parseInt(expected));
                }
            }
            case String actualString -> assertThat(actualString).as(context).isEqualTo(expected);
            case byte[] actualBytes -> assertThat(new String(actualBytes, StandardCharsets.UTF_8)).as(context).isEqualTo(expected);
            default -> assertThat(actual.toString()).as(context).isEqualTo(expected);
        }
    }

    private static List<List<String>> readExpected(String queryId)
    {
        String resource = "/tpch/q" + queryId + ".expected.tsv";
        try (InputStream input = TestTpchQueries.class.getResourceAsStream(resource)) {
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
