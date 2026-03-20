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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Operator;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestClickBenchHitsRealData
{
    private static final String RUN_ACTUAL_TESTS_PROPERTY = "nitro.clickbench.runActualDataTests";
    private static final String RUN_SLOW_ACTUAL_TESTS_PROPERTY = "nitro.clickbench.runSlowActualTests";

    private Path actualHitsFile()
    {
        assumeTrue(Boolean.getBoolean(RUN_ACTUAL_TESTS_PROPERTY), "Set -D" + RUN_ACTUAL_TESTS_PROPERTY + "=true to run ClickBench tests against the real hits.parquet data");
        var file = ClickBenchHitsSupport.actualHitsFileIfPresent();
        assumeTrue(file.isPresent(), "Set -D" + ClickBenchHitsSupport.CLICKBENCH_HITS_PATH_PROPERTY + "=/path/to/hits.parquet or place the file at ~/tmp/clickbench/hits.parquet");
        return file.orElseThrow();
    }

    @Test
    void testClickBenchQuery1CountAllOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query1CountAll(new Allocator(), actualHitsFile())) {
            assertThat(operator(query)).matchesExactly(List.of(row(99_997_497L)));
        }
    }

    @Test
    void testClickBenchQuery2CountNonZeroAdvEngineIdOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query2CountNonZeroAdvEngineId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsFile())) {
            assertThat(operator(query)).matchesExactly(List.of(row(671_775L)));
        }
    }

    @Test
    void testClickBenchQuery7MinAndMaxEventDateOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query7MinAndMaxEventDate(new Allocator(), actualHitsFile())) {
            assertThat(operator(query)).matchesExactly(List.of(row(15_888L, 15_917L)));
        }
    }

    @Test
    void testClickBenchQuery3RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query3SumAdvEngineAndAvgResolutionWidth(new Allocator(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).matches(rowsList -> rowsList.size() == 1);
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isEqualTo(99_997_497L);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Double.class);
        }
    }

    @Test
    void testClickBenchQuery4RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query4AvgUserId(new Allocator(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Double.class);
        }
    }

    @Test
    void testClickBenchQuery5RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query5CountDistinctUserId(new Allocator(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    @Test
    void testClickBenchQuery6RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query6CountDistinctSearchPhrase(new Allocator(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    @Test
    void testClickBenchQuery8GroupByAdvEngineIdOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query8GroupByAdvEngineId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsFile())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(2L, 384_215L),
                    row(27L, 190_473L),
                    row(13L, 53_491L),
                    row(44L, 15_911L),
                    row(45L, 10_891L),
                    row(62L, 8_373L),
                    row(3L, 3_299L),
                    row(52L, 2_600L),
                    row(50L, 1_004L),
                    row(28L, 996L)));
        }
    }

    @Test
    void testClickBenchQuery13RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query13TopSearchPhrases(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testClickBenchQuery16RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query16TopUserIds(new Allocator(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testClickBenchQuery20RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query20SearchPhrasesForUserId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
        }
    }

    @Test
    void testClickBenchQuery26RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query26SearchPhrasesOrderedAscending(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
        }
    }

    @Test
    void testClickBenchQuery30RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query30SumResolutionWidthPlusOffsets(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(10);
            for (Object value : rows.getFirst().values()) {
                assertThat(value).isInstanceOf(Long.class);
            }
        }
    }

    @Test
    void testClickBenchQuery34RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query34TopUrls(new Allocator(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testClickBenchQuery35RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query35ConstantAndTopUrls(new Allocator(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isEqualTo(1L);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testClickBenchQuery21RunsOnActualHits()
    {
        assumeTrue(Boolean.getBoolean(RUN_SLOW_ACTUAL_TESTS_PROPERTY), "Set -D" + RUN_SLOW_ACTUAL_TESTS_PROPERTY + "=true to run slow real-data ClickBench query coverage");
        try (Operator query = ClickBenchHitsSupport.query21CountUrlsContainingGoogle(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsFile())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }
}
