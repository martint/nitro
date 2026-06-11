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
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Operator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Nitro TPC-H harness against the checked-in reference results (independent engine over the same sf10
 * parquet; see TpchQueryCatalog). Longs and strings compare exactly; doubles within relative tolerance --
 * cross-engine floating-point sums are accumulation-order dependent. DATE columns surface as epoch-day longs
 * and compare against the reference's ISO dates.
 */
public class TestTpchQueries
{
    private static final double RELATIVE_TOLERANCE = 1e-9;

    @Test
    void query01()
    {
        assertMatchesReference("01", tables -> TpchParquetSupport.query01(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query03()
    {
        assertMatchesReference("03", tables -> TpchParquetSupport.query03(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query04()
    {
        assertMatchesReference("04", tables -> TpchParquetSupport.query04(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query05()
    {
        assertMatchesReference("05", tables -> TpchParquetSupport.query05(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query06()
    {
        assertMatchesReference("06", tables -> TpchParquetSupport.query06(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query10()
    {
        assertMatchesReference("10", tables -> TpchParquetSupport.query10(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query12()
    {
        assertMatchesReference("12", tables -> TpchParquetSupport.query12(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query14()
    {
        assertMatchesReference("14", tables -> TpchParquetSupport.query14(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void query19()
    {
        assertMatchesReference("19", tables -> TpchParquetSupport.query19(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
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
