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
    void testClickBenchQuery0SelectAllWithTrinoReader()
            throws IOException
    {
        String previousReader = System.getProperty(ClickBenchHitsSupport.CLICKBENCH_PARQUET_READER_PROPERTY);
        System.setProperty(ClickBenchHitsSupport.CLICKBENCH_PARQUET_READER_PROPERTY, "trino");
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
        finally {
            if (previousReader == null) {
                System.clearProperty(ClickBenchHitsSupport.CLICKBENCH_PARQUET_READER_PROPERTY);
            }
            else {
                System.setProperty(ClickBenchHitsSupport.CLICKBENCH_PARQUET_READER_PROPERTY, previousReader);
            }
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
    void testClickBenchQuery9TopRegionsByDistinctUsers()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query9TopRegionsByDistinctUsers(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(9L, 4L),
                    row(7L, 2L),
                    row(8L, 1L)));
        }
    }

    @Test
    void testClickBenchQuery10RegionAggregates()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query10RegionAggregates(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(9L, 40L, 4L, 610.0, 4L),
                    row(7L, 30L, 3L, 1000.0, 2L),
                    row(8L, 10L, 1L, 900.0, 1L)));
        }
    }

    @Test
    void testClickBenchQuery11TopMobilePhoneModelsByDistinctUsers()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query11TopMobilePhoneModelsByDistinctUsers(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("pixel", 3L),
                    row("iphone", 2L)));
        }
    }

    @Test
    void testClickBenchQuery12TopMobilePhonesAndModelsByDistinctUsers()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query12TopMobilePhonesAndModelsByDistinctUsers(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(3L, "pixel", 3L),
                    row(1L, "iphone", 2L)));
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
    void testClickBenchQuery14TopSearchPhrasesByDistinctUsers()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query14TopSearchPhrasesByDistinctUsers(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("news", 2L),
                    row("phone", 1L),
                    row("map", 1L),
                    row("weather", 1L)));
        }
    }

    @Test
    void testClickBenchQuery15TopSearchEngineAndPhrasePairs()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query15TopSearchEngineAndPhrasePairs(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(3L, "news", 2L),
                    row(1L, "phone", 1L),
                    row(1L, "map", 1L),
                    row(2L, "weather", 1L)));
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
    void testClickBenchQuery17TopUserIdAndSearchPhrasePairs()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query17TopUserIdAndSearchPhrasePairs(new Allocator(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(8);
            assertThat(rows.getFirst().values()).hasSize(3);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testClickBenchQuery18FirstUserIdAndSearchPhrasePairs()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query18FirstUserIdAndSearchPhrasePairs(new Allocator(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row(1L, "", 1L),
                    row(2L, "", 1L),
                    row(2L, "phone", 1L),
                    row(1L, "map", 1L),
                    row(2L, "weather", 1L),
                    row(4L, "", 1L),
                    row(5L, "news", 1L),
                    row(ClickBenchHitsSupport.QUERY20_USER_ID, "news", 1L)));
        }
    }

    @Test
    void testClickBenchQuery19TopUserIdMinuteAndSearchPhraseTriples()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query19TopUserIdMinuteAndSearchPhraseTriples(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).hasSize(8);
            assertThat(rows.getFirst().values()).hasSize(4);
            assertThat(rows.getFirst().values()[0]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[1]).isInstanceOf(Long.class);
            assertThat(rows.getFirst().values()[2]).isInstanceOf(String.class);
            assertThat(rows.getFirst().values()[3]).isInstanceOf(Long.class);
        }
    }

    @Test
    void testClickBenchQuery20UserIdsForExactUserId()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query20UserIdsForExactUserId(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(row(ClickBenchHitsSupport.QUERY20_USER_ID)));
        }
    }

    @Test
    void testClickBenchQuery25SearchPhrasesOrderedByEventTime()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query25SearchPhrasesOrderedByEventTime(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("map"),
                    row("phone"),
                    row("news"),
                    row("news"),
                    row("weather")));
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
    void testClickBenchQuery27SearchPhrasesOrderedByEventTimeThenPhrase()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query27SearchPhrasesOrderedByEventTimeThenPhrase(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(operator(query)).matchesExactly(List.of(
                    row("map"),
                    row("phone"),
                    row("news"),
                    row("news"),
                    row("weather")));
        }
    }

    @Test
    void testClickBenchQuery30SumResolutionWidthPlusOffsets()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query30SumResolutionWidthPlusOffsets(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            Object[] sums = new Object[90];
            for (int offset = 0; offset < sums.length; offset++) {
                sums[offset] = 6_340L + (8L * offset);
            }
            assertThat(operator(query)).matchesExactly(List.of(row(sums)));
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
    void testClickBenchQuery22SearchPhrasesWithGoogleUrls()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query22SearchPhrasesWithGoogleUrls(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(3);
        }
    }

    @Test
    void testClickBenchQuery23GoogleTitlesNonGoogleUrls()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query23GoogleTitlesNonGoogleUrls(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testClickBenchQuery24SelectAllGoogleUrlsOrderedByEventTime()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query24SelectAllGoogleUrlsOrderedByEventTime(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values().length).isGreaterThan(20);
        }
    }

    @Test
    void testClickBenchQuery28CounterAverageUrlLength()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query28CounterAverageUrlLength(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testClickBenchQuery29RefererHosts()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query29RefererHosts(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testClickBenchQuery31SearchEngineAndClientIp()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query31SearchEngineAndClientIp(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testClickBenchQuery32WatchIdAndClientIpWithSearchPhrase()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query32WatchIdAndClientIpWithSearchPhrase(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testClickBenchQuery33WatchIdAndClientIp()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query33WatchIdAndClientIp(new Allocator(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testClickBenchQuery36ClientIpArithmeticGroups()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query36ClientIpArithmeticGroups(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(5);
        }
    }

    @Test
    void testClickBenchQuery37TopUrlsForCounter62()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query37TopUrlsForCounter62(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    @Test
    void testClickBenchQuery38TopTitlesForCounter62()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query38TopTitlesForCounter62(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(query);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().values()).hasSize(2);
        }
    }

    @Test
    void testClickBenchQuery39TopUrlsOffset()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query39TopUrlsOffset(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testClickBenchQuery40TrafficSourceGroups()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query40TrafficSourceGroups(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testClickBenchQuery41UrlHashByEventDate()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query41UrlHashByEventDate(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testClickBenchQuery42WindowClientSizes()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query42WindowClientSizes(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
        }
    }

    @Test
    void testClickBenchQuery43PageViewsByMinute()
            throws IOException
    {
        try (Operator query = ClickBenchHitsSupport.query43PageViewsByMinute(new Allocator(), TestPrimitiveFunctions.primitiveRegistry(), writeHitsFixture())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(query)).isEmpty();
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
