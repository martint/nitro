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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.ir.Stream;

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

    private Path actualHitsDirectory()
    {
        assumeTrue(Boolean.getBoolean(RUN_ACTUAL_TESTS_PROPERTY), "Set -D" + RUN_ACTUAL_TESTS_PROPERTY + "=true to run ClickBench tests against the split real hits parquet data");
        var directory = ClickBenchHitsSupport.actualHitsDirectoryIfPresent();
        assumeTrue(directory.isPresent(), "Set -D" + ClickBenchHitsSupport.CLICKBENCH_HITS_PATH_PROPERTY + "=/path/to/clickbench or place the split files at ~/tmp/clickbench");
        return directory.orElseThrow();
    }

    @Test
    void testClickBenchQuery1CountAllOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query1CountAll(new Allocator(), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(row(99_997_497L)));
        }
    }

    @Test
    void testClickBenchQuery0SelectAllOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query0SelectAll(new Allocator(), actualHitsDirectory())) {
            assertThat(query.outputCount()).isEqualTo(6);
            assertThat(query.hasNext()).isTrue();

            Batch batch = query.next();
            var mask = batch.borrowMask();
            assertThat(mask.none()).isFalse();

            int firstPosition = mask.position(0);
            assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
            assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
            assertThat(batch.output(2).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
            assertThat(batch.output(3).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
            assertThat(batch.output(4).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
            assertThat(batch.output(5).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);

            integerValue(batch.output(0).borrow(Stream.VALUES), firstPosition);
            integerValue(batch.output(1).borrow(Stream.VALUES), firstPosition);
            integerValue(batch.output(2).borrow(Stream.VALUES), firstPosition);
            assertThat(integerValue(batch.output(3).borrow(Stream.VALUES), firstPosition)).isPositive();
            assertThat(((BinaryVector) batch.output(4).borrow(Stream.VALUES)).utf8Value(firstPosition)).isNotNull();
            assertThat(((BinaryVector) batch.output(5).borrow(Stream.VALUES)).utf8Value(firstPosition)).isNotNull();
        }
    }

    @Test
    void testClickBenchQuery2CountNonZeroAdvEngineIdOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query2CountNonZeroAdvEngineId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(row(630_500L)));
        }
    }

    @Test
    void testClickBenchQuery7MinAndMaxEventDateOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query7MinAndMaxEventDate(new Allocator(), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(row(15_888L, 15_917L)));
        }
    }

    @Test
    void testClickBenchQuery3RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query3SumAdvEngineAndAvgResolutionWidth(new Allocator(), actualHitsDirectory())) {
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
        try (Operator query = ClickBenchHitsSupport.query4AvgUserId(new Allocator(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Double.class);
        }
    }

    @Test
    void testClickBenchQuery5RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query5CountDistinctUserId(new Allocator(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    @Test
    void testClickBenchQuery6RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query6CountDistinctSearchPhrase(new Allocator(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    @Test
    void testClickBenchQuery8GroupByAdvEngineIdOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query8GroupByAdvEngineId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(2L, 404_602L),
                    row(27L, 113_167L),
                    row(13L, 45_631L),
                    row(45L, 38_960L),
                    row(44L, 9_730L),
                    row(3L, 6_896L),
                    row(62L, 5_266L),
                    row(52L, 3_554L),
                    row(50L, 938L),
                    row(28L, 836L)));
        }
    }

    @Test
    void testClickBenchQuery13RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query13TopSearchPhrases(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
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
        try (Operator query = ClickBenchHitsSupport.query16TopUserIds(new Allocator(), actualHitsDirectory())) {
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
        try (Operator query = ClickBenchHitsSupport.query20SearchPhrasesForUserId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
        }
    }

    @Test
    void testClickBenchQuery26RunsOnActualHits()
    {
        try (Operator query = ClickBenchHitsSupport.query26SearchPhrasesOrderedAscending(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
        }
    }

    @Test
    void testClickBenchQuery30RunsOnActualHits()
    {
        assumeTrue(Boolean.getBoolean(RUN_SLOW_ACTUAL_TESTS_PROPERTY), "Set -D" + RUN_SLOW_ACTUAL_TESTS_PROPERTY + "=true to run slow real-data ClickBench query coverage");
        try (Operator query = ClickBenchHitsSupport.query30SumResolutionWidthPlusOffsets(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
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
        try (Operator query = ClickBenchHitsSupport.query34TopUrls(new Allocator(), actualHitsDirectory())) {
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
        try (Operator query = ClickBenchHitsSupport.query35ConstantAndTopUrls(new Allocator(), actualHitsDirectory())) {
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
        try (Operator query = ClickBenchHitsSupport.query21CountUrlsContainingGoogle(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), actualHitsDirectory())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().values()).hasSize(1);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat((Long) rows.getFirst().values()[0]).isPositive();
        }
    }

    private static long integerValue(Object vector, int position)
    {
        return switch (vector) {
            case I32Vector typed -> typed.values()[position];
            case I64Vector typed -> typed.values()[position];
            default -> throw new AssertionError("Expected integer vector but got " + vector.getClass().getSimpleName());
        };
    }
}
