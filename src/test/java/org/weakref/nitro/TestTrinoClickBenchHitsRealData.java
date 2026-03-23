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
package org.weakref.nitro;

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.trino.TrinoClickBenchSupport;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestTrinoClickBenchHitsRealData
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
            support.consumeQuery0SelectAll(actualHitsDirectory());
        }
    }

    @Test
    void testQuery01()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query1CountAll(actualHitsDirectory());
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getOnlyValue()).isEqualTo(99_997_497L);
        }
    }

    @Test
    void testQuery03()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query3SumAdvEngineAndAvgResolutionWidth(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).hasSize(3);
                assertThat(row.getFields().get(0)).isInstanceOf(Long.class);
                assertThat(row.getFields().get(1)).isEqualTo(99_997_497L);
                assertThat(row.getFields().get(2)).isInstanceOf(Double.class);
            });
        }
    }

    @Test
    void testQuery07()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query7MinAndMaxEventDate(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).singleElement().satisfies(row -> {
                assertThat(row.getFields()).containsExactly(15_888, 15_917);
            });
        }
    }

    @Test
    void testQuery08()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query8GroupByAdvEngineId(actualHitsDirectory());
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
    void testQuery13()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query13TopSearchPhrases(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(2);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(0)).isInstanceOf(String.class);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(1)).isInstanceOf(Long.class);
        }
    }

    @Test
    void testQuery34()
    {
        try (TrinoClickBenchSupport support = new TrinoClickBenchSupport()) {
            MaterializedResult result = support.query34TopUrls(actualHitsDirectory());
            assertThat(result.getMaterializedRows()).isNotEmpty();
            assertThat(result.getMaterializedRows().getFirst().getFields()).hasSize(2);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(0)).isInstanceOf(String.class);
            assertThat(result.getMaterializedRows().getFirst().getFields().get(1)).isInstanceOf(Long.class);
        }
    }

    private static List<Object> resultRow(Object... values)
    {
        return List.of(values);
    }
}
