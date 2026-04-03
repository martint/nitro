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

import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.trino.TrinoOperatorCpuProfile;
import org.weakref.nitro.trino.TrinoTpcdsParquetSqlSupport;
import org.weakref.nitro.trino.TrinoTpcdsParquetSupport;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestQueries
{
    @Test
    void testQuery01()
    {
        assertOperatorMatches("01", tables -> TpcdsParquetSupport.query01(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query01(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery01Sql()
    {
        assertNitroMatchesSql("01", tables -> TpcdsParquetSupport.query01(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery01TrinoSql()
    {
        assertTrinoOperatorMatchesSql("01", support -> support.query01(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery02()
    {
        assertOperatorMatches("02", tables -> TpcdsParquetSupport.query02(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query02(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery06()
    {
        assertOperatorMatches("06", tables -> TpcdsParquetSupport.query06(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query06(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery12()
    {
        assertOperatorMatches("12", tables -> TpcdsParquetSupport.query12(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query12(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery13()
    {
        assertOperatorMatches("13", tables -> TpcdsParquetSupport.query13(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query13(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery20()
    {
        assertOperatorMatches("20", tables -> TpcdsParquetSupport.query20(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query20(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery16()
    {
        assertOperatorMatches("16", tables -> TpcdsParquetSupport.query16(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query16(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery18()
    {
        assertOperatorMatches("18", tables -> TpcdsParquetSupport.query18(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query18(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery22()
    {
        assertOperatorMatches("22", tables -> TpcdsParquetSupport.query22(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query22(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeValue);
    }

    @Test
    void testQuery27()
    {
        assertOperatorMatches("27", tables -> TpcdsParquetSupport.query27(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query27(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeValue);
    }

    @Test
    void testQuery28()
    {
        assertOperatorMatches("28", tables -> TpcdsParquetSupport.query28(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query28(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery42()
    {
        assertOperatorMatches("42", tables -> TpcdsParquetSupport.query42(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query42(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery43()
    {
        assertOperatorMatches("43", tables -> TpcdsParquetSupport.query43(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query43(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery46()
    {
        assertOperatorMatches("46", tables -> TpcdsParquetSupport.query46(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query46(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery34()
    {
        assertOperatorMatches("34", tables -> TpcdsParquetSupport.query34(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query34(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery50()
    {
        assertOperatorMatches("50", tables -> TpcdsParquetSupport.query50(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query50(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery52()
    {
        assertOperatorMatches("52", tables -> TpcdsParquetSupport.query52(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query52(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery68()
    {
        assertOperatorMatches("68", tables -> TpcdsParquetSupport.query68(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query68(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery79()
    {
        assertOperatorMatches("79", tables -> TpcdsParquetSupport.query79(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query79(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery93()
    {
        assertOperatorMatches("93", tables -> TpcdsParquetSupport.query93(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query93(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery91()
    {
        assertOperatorMatches("91", tables -> TpcdsParquetSupport.query91(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query91(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery82()
    {
        assertOperatorMatches("82", tables -> TpcdsParquetSupport.query82(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query82(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery37()
    {
        assertOperatorMatches("37", tables -> TpcdsParquetSupport.query37(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query37(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery40()
    {
        assertOperatorMatches("40", tables -> TpcdsParquetSupport.query40(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query40(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery55()
    {
        assertOperatorMatches("55", tables -> TpcdsParquetSupport.query55(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query55(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery71()
    {
        assertOperatorMatches("71", tables -> TpcdsParquetSupport.query71(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query71(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery05()
    {
        assertOperatorMatches("05", tables -> TpcdsParquetSupport.query05(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query05(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery77()
    {
        assertOperatorMatches("77", tables -> TpcdsParquetSupport.query77(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query77(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery94()
    {
        assertOperatorMatches("94", tables -> TpcdsParquetSupport.query94(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query94(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery95()
    {
        assertOperatorMatches("95", tables -> TpcdsParquetSupport.query95(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query95(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery33()
    {
        assertOperatorMatches("33", tables -> TpcdsParquetSupport.query33(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query33(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery56()
    {
        assertOperatorMatches("56", tables -> TpcdsParquetSupport.query56(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query56(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery60()
    {
        assertOperatorMatches("60", tables -> TpcdsParquetSupport.query60(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query60(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery65()
    {
        assertOperatorMatches("65", tables -> TpcdsParquetSupport.query65(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query65(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery66()
    {
        assertOperatorMatches("66", tables -> TpcdsParquetSupport.query66(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query66(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery98()
    {
        assertOperatorMatches("98", tables -> TpcdsParquetSupport.query98(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query98(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery89()
    {
        assertOperatorMatches("89", tables -> TpcdsParquetSupport.query89(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query89(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery63()
    {
        assertOperatorMatches("63", tables -> TpcdsParquetSupport.query63(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query63(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery86()
    {
        assertOperatorMatches("86", tables -> TpcdsParquetSupport.query86(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query86(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery36()
    {
        assertOperatorMatches("36", tables -> TpcdsParquetSupport.query36(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query36(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery49()
    {
        assertOperatorMatches("49", tables -> TpcdsParquetSupport.query49(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query49(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery47()
    {
        assertOperatorMatches("47", tables -> TpcdsParquetSupport.query47(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query47(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery09()
    {
        assertOperatorMatches("09", tables -> TpcdsParquetSupport.query09(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query09(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery03()
    {
        assertOperatorMatches("03", tables -> TpcdsParquetSupport.query03(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query03(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery04()
    {
        assertOperatorMatches("04", tables -> TpcdsParquetSupport.query04(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query04(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery07()
    {
        assertOperatorMatches("07", tables -> TpcdsParquetSupport.query07(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query07(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery08()
    {
        assertOperatorMatches("08", tables -> TpcdsParquetSupport.query08(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query08(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery11()
    {
        assertOperatorMatches("11", tables -> TpcdsParquetSupport.query11(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query11(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery14()
    {
        assertOperatorMatches("14", tables -> TpcdsParquetSupport.query14(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query14(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery15()
    {
        assertOperatorMatches("15", tables -> TpcdsParquetSupport.query15(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query15(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery19()
    {
        assertOperatorMatches("19", tables -> TpcdsParquetSupport.query19(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query19(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery21()
    {
        assertOperatorMatches("21", tables -> TpcdsParquetSupport.query21(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query21(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery26()
    {
        assertOperatorMatches("26", tables -> TpcdsParquetSupport.query26(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query26(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery41()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = TpcdsParquetSupport.query41(new Allocator(), primitiveRegistry, tables)) {
            nitroRows = OperatorAssertions.OperatorAssert.toRows(query);
        }

        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            MaterializedResult result = support.query41(tables);
            assertThat(result.getMaterializedRows().stream()
                    .map(row -> new org.weakref.nitro.data.Row(row.getFields().stream()
                            .map(field -> field instanceof String value ? normalize(value) : field)
                            .toArray()))
                    .toList())
                    .containsExactlyElementsOf(nitroRows.stream()
                            .map(row -> new org.weakref.nitro.data.Row(java.util.Arrays.stream(row.values())
                                    .map(value -> value instanceof String string ? normalize(string) : value)
                                    .toArray()))
                            .toList());
        }
    }

    @Test
    void testQuery41Sql()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        List<String> nitroRows;
        try (Operator query = TpcdsParquetSupport.query41(new Allocator(), primitiveRegistry, tables)) {
            nitroRows = OperatorAssertions.OperatorAssert.toRows(query).stream()
                    .map(row -> normalize((String) row.values()[0]))
                    .toList();
        }

        try (TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery("41");
            assertThat(nitroRows)
                    .containsExactlyElementsOf(sqlResult.getMaterializedRows().stream()
                            .map(row -> normalize(Objects.toString(row.getField(0))))
                            .toList());
        }
    }

    @Test
    void testQuery41TrinoSql()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        try (TrinoTpcdsParquetSupport operatorSupport = new TrinoTpcdsParquetSupport();
                TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult operatorResult = operatorSupport.query41(tables);
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery("41");
            assertThat(operatorResult.getMaterializedRows().stream()
                    .map(row -> normalize((String) row.getField(0)))
                    .toList())
                    .containsExactlyElementsOf(sqlResult.getMaterializedRows().stream()
                            .map(row -> normalize(Objects.toString(row.getField(0))))
                            .toList());
        }
    }

    @Test
    void testQuery44()
    {
        assertOperatorMatches("44", tables -> TpcdsParquetSupport.query44(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query44(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery44Sql()
    {
        assertNitroMatchesSql("44", tables -> TpcdsParquetSupport.query44(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery44TrinoSql()
    {
        assertTrinoOperatorMatchesSql("44", support -> support.query44(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery10()
    {
        assertOperatorMatches("10", tables -> TpcdsParquetSupport.query10(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query10(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery35()
    {
        assertOperatorMatches("35", tables -> TpcdsParquetSupport.query35(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query35(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery10Sql()
    {
        assertNitroMatchesSql("10", tables -> TpcdsParquetSupport.query10(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery10TrinoSql()
    {
        assertTrinoOperatorMatchesSql("10", support -> support.query10(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery35Sql()
    {
        assertNitroMatchesSql("35", tables -> TpcdsParquetSupport.query35(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery35TrinoSql()
    {
        assertTrinoOperatorMatchesSql("35", support -> support.query35(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery62Sql()
    {
        assertNitroMatchesSql("62", tables -> TpcdsParquetSupport.query62(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery62TrinoSql()
    {
        assertTrinoOperatorMatchesSql("62", support -> support.query62(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery45Sql()
    {
        assertNitroMatchesSql("45", tables -> TpcdsParquetSupport.query45(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery51Sql()
    {
        assertNitroMatchesSql("51", tables -> TpcdsParquetSupport.query51(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDateAndDecimalValue);
    }

    @Test
    void testQuery53Sql()
    {
        assertNitroMatchesSql("53", tables -> TpcdsParquetSupport.query53(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery45TrinoSql()
    {
        assertTrinoOperatorMatchesSql("45", support -> support.query45(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery51TrinoSql()
    {
        assertTrinoOperatorMatchesSql("51", support -> support.query51(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDateAndDecimalValue);
    }

    @Test
    void testQuery53TrinoSql()
    {
        assertTrinoOperatorMatchesSql("53", support -> support.query53(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery58Sql()
    {
        assertNitroMatchesSql("58", tables -> TpcdsParquetSupport.query58(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery58TrinoSql()
    {
        assertTrinoOperatorMatchesSql("58", support -> support.query58(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery61Sql()
    {
        assertNitroMatchesSql("61", tables -> TpcdsParquetSupport.query61(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery61TrinoSql()
    {
        assertTrinoOperatorMatchesSql("61", support -> support.query61(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery57Sql()
    {
        assertNitroMatchesSql("57", tables -> TpcdsParquetSupport.query57(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery57OperatorAssembly()
    {
        assertApplesToApplesOperatorMatches(
                "57",
                tables -> TpcdsParquetSupport.query57(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables),
                support -> support.query57(TpcdsParquetTables.requiredActual("sf10")),
                TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery57StageCounts()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        try (Operator joinedFacts = TpcdsParquetSupport.query57JoinedFacts(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
                Operator groupedSales = TpcdsParquetSupport.query57MonthlyGroupedSales(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
                Operator rankedSales = TpcdsParquetSupport.query57MonthlyRankedSales(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables);
                TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(joinedFacts)).hasSize(support.query57JoinedFacts(tables).getMaterializedRows().size());
            assertThat(OperatorAssertions.OperatorAssert.toRows(groupedSales)).hasSize(support.query57MonthlyGroupedSales(tables).getMaterializedRows().size());
            assertThat(OperatorAssertions.OperatorAssert.toRows(rankedSales)).hasSize(support.query57MonthlyRankedSales(tables).getMaterializedRows().size());
        }
    }

    @Test
    void testQuery57TrinoSql()
    {
        assertTrinoOperatorMatchesSql("57", support -> support.query57(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void profileQuery57OperatorCpu()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        OperatorCpuProfile profile = new OperatorCpuProfile();
        try (Operator query = TpcdsParquetSupport.withOperatorCpuProfile(
                profile,
                () -> TpcdsParquetSupport.query57(new Allocator(), primitiveRegistry, tables))) {
            consumeOperator(query);
        }

        System.out.println(profile.formatReport());
    }

    @Test
    void profileQuery57TrinoOperatorCpu()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        TrinoOperatorCpuProfile profile = new TrinoOperatorCpuProfile();
        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            TrinoTpcdsParquetSupport.withOperatorCpuProfile(profile, () -> support.query57(tables));
        }

        System.out.println(profile.formatReport());
    }

    @Test
    void testQuery96()
    {
        assertOperatorMatches("96", tables -> TpcdsParquetSupport.query96(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery73()
    {
        assertOperatorMatches("73", tables -> TpcdsParquetSupport.query73(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query73(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery73Sql()
    {
        assertNitroMatchesSql("73", tables -> TpcdsParquetSupport.query73(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery73TrinoSql()
    {
        assertTrinoOperatorMatchesSql("73", support -> support.query73(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery69()
    {
        assertOperatorMatches("69", tables -> TpcdsParquetSupport.query69(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query69(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery69Sql()
    {
        assertNitroMatchesSql("69", tables -> TpcdsParquetSupport.query69(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery69TrinoSql()
    {
        assertTrinoOperatorMatchesSql("69", support -> support.query69(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery67()
    {
        assertOperatorMatches("67", tables -> TpcdsParquetSupport.query67(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query67(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery67Sql()
    {
        assertNitroMatchesSql("67", tables -> TpcdsParquetSupport.query67(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery67TrinoSql()
    {
        assertTrinoOperatorMatchesSql("67", support -> support.query67(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery70()
    {
        assertOperatorMatches("70", tables -> TpcdsParquetSupport.query70(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query70(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery70Sql()
    {
        assertNitroMatchesSql("70", tables -> TpcdsParquetSupport.query70(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery70TrinoSql()
    {
        assertTrinoOperatorMatchesSql("70", support -> support.query70(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery80()
    {
        assertOperatorMatches("80", tables -> TpcdsParquetSupport.query80(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query80(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery30()
    {
        assertOperatorMatches("30", tables -> TpcdsParquetSupport.query30(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query30(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery29()
    {
        assertOperatorMatches("29", tables -> TpcdsParquetSupport.query29(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query29(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery76()
    {
        assertOperatorMatches("76", tables -> TpcdsParquetSupport.query76(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query76(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery31()
    {
        assertOperatorMatches("31", tables -> TpcdsParquetSupport.query31(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query31(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery32()
    {
        assertOperatorMatches("32", tables -> TpcdsParquetSupport.query32(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query32(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery81()
    {
        assertOperatorMatches("81", tables -> TpcdsParquetSupport.query81(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query81(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery83()
    {
        assertOperatorMatches("83", tables -> TpcdsParquetSupport.query83(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query83(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery74()
    {
        assertOperatorMatches("74", tables -> TpcdsParquetSupport.query74(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query74(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery75()
    {
        assertOperatorMatches("75", tables -> TpcdsParquetSupport.query75(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query75(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery78()
    {
        assertOperatorMatches("78", tables -> TpcdsParquetSupport.query78(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query78(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery87()
    {
        assertOperatorMatches("87", tables -> TpcdsParquetSupport.query87(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query87(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery85()
    {
        assertOperatorMatches("85", tables -> TpcdsParquetSupport.query85(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query85(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery80Sql()
    {
        assertNitroMatchesSql("80", tables -> TpcdsParquetSupport.query80(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery30Sql()
    {
        assertNitroMatchesSql("30", tables -> TpcdsParquetSupport.query30(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery32Sql()
    {
        assertNitroMatchesSql("32", tables -> TpcdsParquetSupport.query32(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery81Sql()
    {
        assertNitroMatchesSql("81", tables -> TpcdsParquetSupport.query81(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery80TrinoSql()
    {
        assertTrinoOperatorMatchesSql("80", support -> support.query80(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery30TrinoSql()
    {
        assertTrinoOperatorMatchesSql("30", support -> support.query30(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery32TrinoSql()
    {
        assertTrinoOperatorMatchesSql("32", support -> support.query32(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery81TrinoSql()
    {
        assertTrinoOperatorMatchesSql("81", support -> support.query81(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery97()
    {
        assertOperatorMatches("97", tables -> TpcdsParquetSupport.query97(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query97(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery23()
    {
        assertOperatorMatches("23", tables -> TpcdsParquetSupport.query23(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query23(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery24()
    {
        assertOperatorMatches("24", tables -> TpcdsParquetSupport.query24(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query24(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery25()
    {
        assertOperatorMatches("25", tables -> TpcdsParquetSupport.query25(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query25(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery38()
    {
        assertOperatorMatches("38", tables -> TpcdsParquetSupport.query38(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query38(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery48()
    {
        assertOperatorMatches("48", tables -> TpcdsParquetSupport.query48(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query48(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery97Sql()
    {
        assertNitroMatchesSql("97", tables -> TpcdsParquetSupport.query97(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery23Sql()
    {
        assertNitroMatchesSql("23", tables -> TpcdsParquetSupport.query23(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery24Sql()
    {
        assertNitroMatchesSql("24", tables -> TpcdsParquetSupport.query24(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery97TrinoSql()
    {
        assertTrinoOperatorMatchesSql("97", support -> support.query97(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery23TrinoSql()
    {
        assertTrinoOperatorMatchesSql("23", support -> support.query23(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery24TrinoSql()
    {
        assertTrinoOperatorMatchesSql("24", support -> support.query24(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery09Sql()
    {
        assertNitroMatchesSql("09", tables -> TpcdsParquetSupport.query09(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery14Sql()
    {
        assertNitroMatchesSql("14", tables -> TpcdsParquetSupport.query14(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery09TrinoSql()
    {
        assertTrinoOperatorMatchesSql("09", support -> support.query09(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery14TrinoSql()
    {
        assertTrinoOperatorMatchesSql("14", support -> support.query14(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery84()
    {
        assertOperatorMatches("84", tables -> TpcdsParquetSupport.query84(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query84(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery84Sql()
    {
        assertNitroMatchesSql("84", tables -> TpcdsParquetSupport.query84(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeQuery84Value);
    }

    @Test
    void testQuery84TrinoSql()
    {
        assertTrinoOperatorMatchesSql("84", support -> support.query84(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeQuery84Value);
    }

    @Test
    void testQuery90()
    {
        assertOperatorMatches("90", tables -> TpcdsParquetSupport.query90(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query90(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery92()
    {
        assertOperatorMatches("92", tables -> TpcdsParquetSupport.query92(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query92(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery90Sql()
    {
        assertApproximateNitroMatchesSql("90", tables -> TpcdsParquetSupport.query90(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery92Sql()
    {
        assertNitroMatchesSql("92", tables -> TpcdsParquetSupport.query92(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery90TrinoSql()
    {
        assertApproximateTrinoOperatorMatchesSql("90", support -> support.query90(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery92TrinoSql()
    {
        assertTrinoOperatorMatchesSql("92", support -> support.query92(TpcdsParquetTables.requiredActual("sf10")), TestQueries::normalizeDecimalCentsValue);
    }

    @Test
    void testQuery96Sql()
    {
        assertNitroMatchesSql("96", tables -> TpcdsParquetSupport.query96(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery96TrinoSql()
    {
        assertTrinoOperatorMatchesSql("96", support -> support.query96(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery99()
    {
        assertOperatorMatches("99", tables -> TpcdsParquetSupport.query99(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query99(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery88()
    {
        assertOperatorMatches("88", tables -> TpcdsParquetSupport.query88(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables), support -> support.query88(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery88Sql()
    {
        assertNitroMatchesSql("88", tables -> TpcdsParquetSupport.query88(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery88TrinoSql()
    {
        assertTrinoOperatorMatchesSql("88", support -> support.query88(TpcdsParquetTables.requiredActual("sf10")));
    }

    @Test
    void testQuery99Sql()
    {
        assertNitroMatchesSql("99", tables -> TpcdsParquetSupport.query99(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), tables));
    }

    @Test
    void testQuery99TrinoSql()
    {
        assertTrinoOperatorMatchesSql("99", support -> support.query99(TpcdsParquetTables.requiredActual("sf10")));
    }

    private static void assertOperatorMatches(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        assertOperatorMatches(queryId, nitroQuery, trinoQuery, TestQueries::normalizeValue);
    }

    private static void assertOperatorMatches(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery, java.util.function.Function<Object, Object> valueNormalizer)
    {
        assertNitroMatchesSql(queryId, nitroQuery, valueNormalizer);
    }

    private static void consumeOperator(Operator operator)
    {
        while (operator.hasNext()) {
            try (Batch batch = operator.next()) {
                int count = batch.borrowMask().count();
                for (int column = 0; column < operator.outputCount(); column++) {
                    consumeVector(batch.output(column).borrow(Stream.VALUES));
                }
                if (count == Integer.MIN_VALUE) {
                    throw new AssertionError();
                }
            }
        }
    }

    private static void consumeVector(Vector vector)
    {
        if (vector.length() == Integer.MIN_VALUE) {
            throw new AssertionError();
        }
    }

    private static void assertApplesToApplesOperatorMatches(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        assertApplesToApplesOperatorMatches(queryId, nitroQuery, trinoQuery, TestQueries::normalizeValue);
    }

    private static void assertApplesToApplesOperatorMatches(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery, java.util.function.Function<Object, Object> valueNormalizer)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");
        assumeTrue(TrinoTpcdsParquetSupport.supportsOperatorAssembly(queryId), "Trino operator assembly is not implemented for Q" + queryId);

        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = nitroQuery.apply(tables)) {
            nitroRows = normalizeNitroRows(OperatorAssertions.OperatorAssert.toRows(query), valueNormalizer);
        }

        try (TrinoTpcdsParquetSupport support = new TrinoTpcdsParquetSupport()) {
            assertThat(normalizeTrinoRows(trinoQuery.apply(support), valueNormalizer))
                    .as("TPC-DS Q%s operator assembly result", queryId)
                    .containsExactlyElementsOf(nitroRows);
        }
    }

    private static void assertNitroMatchesSql(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery)
    {
        assertNitroMatchesSql(queryId, nitroQuery, TestQueries::normalizeValue);
    }

    private static void assertNitroMatchesSql(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery, java.util.function.Function<Object, Object> valueNormalizer)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        List<org.weakref.nitro.data.Row> nitroRows;
        try (Operator query = nitroQuery.apply(tables)) {
            nitroRows = normalizeNitroRows(OperatorAssertions.OperatorAssert.toRows(query), valueNormalizer);
        }

        try (TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            assertThat(nitroRows)
                    .as("TPC-DS Q%s Nitro vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId), valueNormalizer));
        }
    }

    private static void assertTrinoOperatorMatchesSql(String queryId, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        assertTrinoOperatorMatchesSql(queryId, trinoQuery, TestQueries::normalizeValue);
    }

    private static void assertTrinoOperatorMatchesSql(String queryId, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery, java.util.function.Function<Object, Object> valueNormalizer)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");
        assumeTrue(TrinoTpcdsParquetSupport.supportsOperatorAssembly(queryId), "Trino operator assembly is not implemented for Q" + queryId);

        try (TrinoTpcdsParquetSupport operatorSupport = new TrinoTpcdsParquetSupport();
                TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            assertThat(normalizeTrinoRows(trinoQuery.apply(operatorSupport), valueNormalizer))
                    .as("TPC-DS Q%s Trino operator vs SQL", queryId)
                    .containsExactlyElementsOf(normalizeTrinoRows(sqlSupport.executeBenchmarkQuery(queryId), valueNormalizer));
        }
    }

    private static void assertApproximateNitroMatchesSql(String queryId, java.util.function.Function<TpcdsParquetTables, Operator> nitroQuery)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        double actual;
        try (Operator query = nitroQuery.apply(tables)) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            actual = ((Number) rows.getFirst().values()[0]).doubleValue();
        }

        try (TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult sqlResult = sqlSupport.executeBenchmarkQuery(queryId);
            assertThat(actual)
                    .as("TPC-DS Q%s Nitro vs SQL", queryId)
                    .isCloseTo(bigDecimalValue(sqlResult).doubleValue(), within(1e-9));
        }
    }

    private static void assertApproximateTrinoOperatorMatchesSql(String queryId, java.util.function.Function<TrinoTpcdsParquetSupport, MaterializedResult> trinoQuery)
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        try (TrinoTpcdsParquetSupport operatorSupport = new TrinoTpcdsParquetSupport();
                TrinoTpcdsParquetSqlSupport sqlSupport = new TrinoTpcdsParquetSqlSupport(tables)) {
            MaterializedResult operatorResult = trinoQuery.apply(operatorSupport);
            assertThat(operatorResult.getMaterializedRows()).hasSize(1);
            double actual = ((Number) operatorResult.getMaterializedRows().getFirst().getField(0)).doubleValue();
            assertThat(actual)
                    .as("TPC-DS Q%s Trino operator vs SQL", queryId)
                    .isCloseTo(bigDecimalValue(sqlSupport.executeBenchmarkQuery(queryId)).doubleValue(), within(1e-9));
        }
    }

    private static BigDecimal bigDecimalValue(MaterializedResult result)
    {
        assertThat(result.getMaterializedRows()).hasSize(1);
        Object value = result.getMaterializedRows().getFirst().getField(0);
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof SqlDecimal decimal) {
            return decimal.toBigDecimal();
        }
        return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    private static List<org.weakref.nitro.data.Row> normalizeNitroRows(List<org.weakref.nitro.data.Row> rows)
    {
        return normalizeNitroRows(rows, TestQueries::normalizeValue);
    }

    private static List<org.weakref.nitro.data.Row> normalizeNitroRows(List<org.weakref.nitro.data.Row> rows, java.util.function.Function<Object, Object> valueNormalizer)
    {
        return rows.stream()
                .map(row -> new org.weakref.nitro.data.Row(java.util.Arrays.stream(row.values())
                        .map(valueNormalizer)
                        .toArray()))
                .toList();
    }

    private static List<org.weakref.nitro.data.Row> normalizeTrinoRows(MaterializedResult result)
    {
        return normalizeTrinoRows(result, TestQueries::normalizeValue);
    }

    private static List<org.weakref.nitro.data.Row> normalizeTrinoRows(MaterializedResult result, java.util.function.Function<Object, Object> valueNormalizer)
    {
        return result.getMaterializedRows().stream()
                .map(row -> new org.weakref.nitro.data.Row(row.getFields().stream()
                        .map(valueNormalizer)
                        .toArray()))
                .toList();
    }

    private static Object normalizeValue(Object value)
    {
        return value instanceof String string ? normalize(string) : value;
    }

    private static String normalize(String value)
    {
        return value == null ? null : value.stripTrailing();
    }

    private static Object normalizeQuery84Value(Object value)
    {
        if (!(value instanceof String string)) {
            return value;
        }
        String normalized = normalize(string);
        if (normalized == null) {
            return null;
        }
        int delimiter = normalized.indexOf(", ");
        if (delimiter < 0) {
            return normalized;
        }
        return normalize(normalized.substring(0, delimiter)) + ", " + normalize(normalized.substring(delimiter + 2));
    }

    private static Object valueAt(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector binary -> new String(binary.copyBytes(position), StandardCharsets.UTF_8);
            case I64Vector values -> values.values()[position];
            default -> throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass().getSimpleName());
        };
    }

    private static Object normalizeDecimalCentsValue(Object value)
    {
        if (value instanceof java.time.LocalDate date) {
            return (int) date.toEpochDay();
        }
        if (value instanceof SqlDate date) {
            return date.getDays();
        }
        if (value instanceof SqlDecimal decimal) {
            return decimal.toBigDecimal().unscaledValue().longValueExact();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.unscaledValue().longValueExact();
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        return normalizeValue(value);
    }

    private static Object normalizeDateAndDecimalValue(Object value)
    {
        return normalizeDecimalCentsValue(value);
    }

    private static Streams copyOutputStreams(Allocator allocator, org.weakref.nitro.operator.Output output, org.weakref.nitro.data.Mask mask)
    {
        Allocator.Context allocationContext = new Allocator.Context("TestQueries");
        Streams.Builder streams = Streams.builder();
        for (org.weakref.nitro.operator.evaluator.ir.Stream stream : output.streams()) {
            streams.put(stream, allocator.copyVector(allocationContext, output.borrow(stream), mask));
        }
        return streams.build();
    }

    private static long countRows(Operator operator)
    {
        long rowCount = 0;
        while (operator.hasNext()) {
            try (Batch batch = operator.next()) {
                rowCount += batch.borrowMask().count();
            }
        }
        return rowCount;
    }
}
