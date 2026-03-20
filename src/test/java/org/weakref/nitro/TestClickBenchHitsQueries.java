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
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestClickBenchHitsQueries
{
    @TempDir
    Path tempDirectory;

    private Path writeHitsFixture()
            throws IOException
    {
        return ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("clickbench-hits.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
    }

    private Path writeHitsFixture(int rowCount)
            throws IOException
    {
        return ClickBenchHitsSupport.writeHitsFixture(tempDirectory.resolve("clickbench-hits-" + rowCount + ".parquet"), rowCount);
    }

    private Path writeSplitHitsFixture()
            throws IOException
    {
        Path splitDirectory = tempDirectory.resolve("clickbench-hits-split");
        java.nio.file.Files.createDirectories(splitDirectory);
        ClickBenchHitsSupport.writeHitsFixture(splitDirectory.resolve("hits-part-000.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
        ClickBenchHitsSupport.writeHitsFixture(splitDirectory.resolve("hits-part-001.parquet"), ClickBenchHitsSupport.DEFAULT_ROW_COUNT);
        return splitDirectory;
    }

    @Test
    void testClickBenchQuery0SelectAll()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query0SelectAll(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(0, 1000, 1L, 20130701, "https://google.com", ""),
                    row(10, 1200, 2L, 20130702, "https://example.com", ""),
                    row(10, 900, 2L, 20130703, "https://example.com/page", "phone"),
                    row(20, 800, 1L, 20130701, "https://google.com/maps", "map"),
                    row(20, 700, 2L, 20130731, "https://yandex.ru", "weather"),
                    row(0, 640, 4L, 20130801, "", ""),
                    row(20, 600, 5L, 20130715, "https://google.com/search", "news"),
                    row(0, 500, ClickBenchHitsSupport.QUERY20_USER_ID, 20130716, "https://google.com/search", "news")));
        }
    }

    @Test
    void testClickBenchQuery1CountAll()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query1CountAll(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(8L)));
        }
    }

    @Test
    void testClickBenchQuery2CountNonZeroAdvEngineId()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query2CountNonZeroAdvEngineId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(5L)));
        }
    }

    @Test
    void testClickBenchQuery7MinAndMaxEventDate()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query7MinAndMaxEventDate(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(20130701L, 20130801L)));
        }
    }

    @Test
    void testClickBenchQuery3SumAdvEngineAndAvgResolutionWidth()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query3SumAdvEngineAndAvgResolutionWidth(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(80L, 8L, 792.5)));
        }
    }

    @Test
    void testClickBenchQuery4AvgUserId()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query4AvgUserId(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(54_386_366_612_455_058.0)));
        }
    }

    @Test
    void testClickBenchQuery5CountDistinctUserId()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query5CountDistinctUserId(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(5L)));
        }
    }

    @Test
    void testClickBenchQuery6CountDistinctSearchPhrase()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query6CountDistinctSearchPhrase(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(5L)));
        }
    }

    @Test
    void testClickBenchQuery8GroupByAdvEngineId()
            throws IOException
    {
        Allocator allocator = new Allocator();
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        try (Operator query = ClickBenchHitsSupport.query8GroupByAdvEngineId(allocator, primitiveRegistry, writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(20L, 3L),
                    row(10L, 2L)));
        }
    }

    @Test
    void testClickBenchQuery13TopSearchPhrases()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query13TopSearchPhrases(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(4);
            assertThat(rows.getFirst()).isEqualTo(row("news", 2L));
        }
    }

    @Test
    void testClickBenchQuery13TopSearchPhrasesAcrossManyGroups()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query13TopSearchPhrases(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture(160))) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()[0]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testClickBenchQuery16TopUserIds()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query16TopUserIds(new Allocator(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(5);
            assertThat(rows.getFirst()).isEqualTo(row(2L, 3L));
            assertThat(rows.get(1)).isEqualTo(row(1L, 2L));
        }
    }

    @Test
    void testClickBenchQuery20SearchPhrasesForUserId()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query20SearchPhrasesForUserId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row("news")));
        }
    }

    @Test
    void testClickBenchQuery26SearchPhrasesOrderedAscending()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query26SearchPhrasesOrderedAscending(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("map"),
                    row("news"),
                    row("news"),
                    row("phone"),
                    row("weather")));
        }
    }

    @Test
    void testClickBenchQuery30SumResolutionWidthPlusOffsets()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query30SumResolutionWidthPlusOffsets(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(
                    6_348L,
                    6_356L,
                    6_364L,
                    6_372L,
                    6_380L,
                    6_388L,
                    6_396L,
                    6_404L,
                    6_412L,
                    6_420L)));
        }
    }

    @Test
    void testClickBenchQuery34TopUrls()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query34TopUrls(new Allocator(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(7);
            assertThat(rows.getFirst()).isEqualTo(row("https://google.com/search", 2L));
        }
    }

    @Test
    void testClickBenchQuery35ConstantAndTopUrls()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query35ConstantAndTopUrls(new Allocator(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(7);
            assertThat(rows.getFirst()).isEqualTo(row(1L, "https://google.com/search", 2L));
        }
    }

    @Test
    void testClickBenchQuery21CountUrlsContainingGoogle()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query21CountUrlsContainingGoogle(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(4L)));
        }
    }

    @Test
    void testClickBenchQuery1CountAllOnSplitHitsDirectory()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query1CountAll(new Allocator(), writeSplitHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(16L)));
        }
    }

    @Test
    void testClickBenchQuery34TopUrlsOnSplitHitsDirectory()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query34TopUrls(new Allocator(), writeSplitHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(7);
            assertThat(rows.getFirst()).isEqualTo(row("https://google.com/search", 4L));
        }
    }
}
