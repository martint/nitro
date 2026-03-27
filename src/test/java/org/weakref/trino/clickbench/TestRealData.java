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
package org.weakref.trino.clickbench;

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.trino.TrinoClickBenchSupport;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestRealData
{
    private static final String RUN_ACTUAL_TESTS_PROPERTY = "nitro.clickbench.runActualDataTests";

    private Path actualHitsDirectory()
    {
        assumeTrue(actualDataTestsEnabled(), "Set -D" + RUN_ACTUAL_TESTS_PROPERTY + "=false to skip Trino ClickBench tests against the split real hits parquet data");
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            return support.requiredActualHitsPath();
        }
        catch (IllegalStateException exception) {
            assumeTrue(false, exception.getMessage());
            throw exception;
        }
    }

    private static boolean actualDataTestsEnabled()
    {
        return !"false".equalsIgnoreCase(System.getProperty(RUN_ACTUAL_TESTS_PROPERTY));
    }

    @Test
    void testQuery00()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            support.consumeQuery00(actualHitsDirectory());
        }
    }

    @Test
    void testQuery01()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query01(actualHitsDirectory());
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getOnlyValue()).isEqualTo(99_997_497L);
        }
    }

    @Test
    void testQuery02()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertSingleLongResult(support.query02(actualHitsDirectory()));
        }
    }

    @Test
    void testQuery03()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query03(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).hasSize(3);
                assertThat(row.getFields().get(0)).isInstanceOf(Long.class);
                assertThat(row.getFields().get(1)).isEqualTo(99_997_497L);
                assertThat(row.getFields().get(2)).isInstanceOf(Double.class);
            });
        }
    }

    @Test
    void testQuery04()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertSingleDoubleResult(support.query04(actualHitsDirectory()));
        }
    }

    @Test
    void testQuery05()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertSingleLongResult(support.query05(actualHitsDirectory()));
        }
    }

    @Test
    void testQuery06()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertSingleLongResult(support.query06(actualHitsDirectory()));
        }
    }

    @Test
    void testQuery07()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query07(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).containsExactly(15_888, 15_917);
            });
        }
    }

    @Test
    void testQuery08()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query08(actualHitsDirectory());
            assertThat(result.getMaterializedRows())
                    .extracting(row -> row.getFields())
                    .containsExactly(
                            resultRow(2, 404_602L),
                            resultRow(27, 113_167L),
                            resultRow(13, 45_631L),
                            resultRow(45, 38_960L),
                            resultRow(44, 9_730L),
                            resultRow(3, 6_896L),
                            resultRow(62, 5_266L),
                            resultRow(52, 3_554L),
                            resultRow(50, 938L),
                            resultRow(28, 836L));
        }
    }

    @Test
    void testQuery09()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query09(actualHitsDirectory()), 2);
        }
    }

    @Test
    void testQuery10()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query10(actualHitsDirectory()), 5);
        }
    }

    @Test
    void testQuery11()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query11(actualHitsDirectory()), 2);
        }
    }

    @Test
    void testQuery12()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query12(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery13()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query13(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(2);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(0)).isInstanceOf(String.class);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(1)).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery14()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query14(actualHitsDirectory()), 2);
        }
    }

    @Test
    void testQuery15()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query15(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery16()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query16(actualHitsDirectory()), 2);
        }
    }

    @Test
    void testQuery17()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query17(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery18()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query18(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery19()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query19(actualHitsDirectory()), 4);
        }
    }

    @Test
    void testQuery20()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query20(actualHitsDirectory()), 1);
        }
    }

    @Test
    void testQuery21()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertSingleLongResult(support.query21(actualHitsDirectory()));
        }
    }

    @Test
    void testQuery22()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query22(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery23()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query23(actualHitsDirectory()), 5);
        }
    }

    @Test
    void testQuery24()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query24(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields().size()).isGreaterThan(20);
        }
    }

    @Test
    void testQuery25()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query25(actualHitsDirectory()), 1);
        }
    }

    @Test
    void testQuery26()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query26(actualHitsDirectory()), 1);
        }
    }

    @Test
    void testQuery27()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query27(actualHitsDirectory()), 1);
        }
    }

    @Test
    void testQuery28()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query28(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery29()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query29(actualHitsDirectory()), 4);
        }
    }

    @Test
    void testQuery30()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query30(actualHitsDirectory());
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(90);
        }
    }

    @Test
    void testQuery31()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query31(actualHitsDirectory()), 5);
        }
    }

    @Test
    void testQuery32()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query32(actualHitsDirectory()), 5);
        }
    }

    @Test
    void testQuery33()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query33(actualHitsDirectory()), 5);
        }
    }

    @Test
    void testQuery34()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query34(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(2);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(0)).isInstanceOf(String.class);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(1)).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery35()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query35(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery36()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query36(actualHitsDirectory()), 5);
        }
    }

    @Test
    void testQuery37()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query37(actualHitsDirectory()), 2);
        }
    }

    @Test
    void testQuery38()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query38(actualHitsDirectory()), 2);
        }
    }

    @Test
    void testQuery39()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query39(actualHitsDirectory()), 2);
        }
    }

    @Test
    void testQuery40()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query40(actualHitsDirectory()), 6);
        }
    }

    @Test
    void testQuery41()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query41(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery42()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query42(actualHitsDirectory()), 3);
        }
    }

    @Test
    void testQuery43()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            assertNonEmptyRows(support.query43(actualHitsDirectory()), 2);
        }
    }

    private static void assertSingleLongResult(MaterializedResult result)
    {
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getFields()).singleElement().isInstanceOf(Long.class);
    }

    private static void assertSingleDoubleResult(MaterializedResult result)
    {
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getFields()).singleElement().isInstanceOf(Double.class);
    }

    private static void assertNonEmptyRows(MaterializedResult result, int fieldCount)
    {
        assertThat(result.getMaterializedRows()).isNotEmpty();
        assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(fieldCount);
    }

    private static List<Object> resultRow(Object... values)
    {
        return List.of(values);
    }
}
