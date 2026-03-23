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
package org.weakref.nitro.trino;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTrinoClickBenchHits
{
    private TrinoClickBenchSupport support;
    private Path clickBenchHitsPath;

    @Setup
    public void setup()
    {
        support = new TrinoClickBenchSupport();
        clickBenchHitsPath = support.requiredActualHitsPath();
    }

    @TearDown
    public void tearDown()
    {
        support.close();
    }

    @Benchmark
    public void query0SelectAll()
    {
        support.consumeQuery0SelectAll(clickBenchHitsPath);
    }

    @Benchmark
    public Object query1CountAll()
    {
        return support.query1CountAll(clickBenchHitsPath);
    }

    @Benchmark
    public Object query2CountNonZeroAdvEngineId()
    {
        return support.query2CountNonZeroAdvEngineId(clickBenchHitsPath);
    }

    @Benchmark
    public Object query3SumAdvEngineAndAvgResolutionWidth()
    {
        return support.query3SumAdvEngineAndAvgResolutionWidth(clickBenchHitsPath);
    }

    @Benchmark
    public Object query4AvgUserId()
    {
        return support.query4AvgUserId(clickBenchHitsPath);
    }

    @Benchmark
    public Object query5CountDistinctUserId()
    {
        return support.query5CountDistinctUserId(clickBenchHitsPath);
    }

    @Benchmark
    public Object query6CountDistinctSearchPhrase()
    {
        return support.query6CountDistinctSearchPhrase(clickBenchHitsPath);
    }

    @Benchmark
    public Object query7MinAndMaxEventDate()
    {
        return support.query7MinAndMaxEventDate(clickBenchHitsPath);
    }

    @Benchmark
    public Object query8GroupByAdvEngineId()
    {
        return support.query8GroupByAdvEngineId(clickBenchHitsPath);
    }

    @Benchmark
    public Object query9TopRegionsByDistinctUsers()
    {
        return support.query9TopRegionsByDistinctUsers(clickBenchHitsPath);
    }

    @Benchmark
    public Object query10RegionAggregates()
    {
        return support.query10RegionAggregates(clickBenchHitsPath);
    }

    @Benchmark
    public Object query11TopMobilePhoneModelsByDistinctUsers()
    {
        return support.query11TopMobilePhoneModelsByDistinctUsers(clickBenchHitsPath);
    }

    @Benchmark
    public Object query12TopMobilePhonesAndModelsByDistinctUsers()
    {
        return support.query12TopMobilePhonesAndModelsByDistinctUsers(clickBenchHitsPath);
    }

    @Benchmark
    public Object query13TopSearchPhrases()
    {
        return support.query13TopSearchPhrases(clickBenchHitsPath);
    }

    @Benchmark
    public Object query14TopSearchPhrasesByDistinctUsers()
    {
        return support.query14TopSearchPhrasesByDistinctUsers(clickBenchHitsPath);
    }

    @Benchmark
    public Object query15TopSearchEngineAndPhrasePairs()
    {
        return support.query15TopSearchEngineAndPhrasePairs(clickBenchHitsPath);
    }

    @Benchmark
    public Object query16TopUserIds()
    {
        return support.query16TopUserIds(clickBenchHitsPath);
    }

    @Benchmark
    public Object query17TopUserIdAndSearchPhrasePairs()
    {
        return support.query17TopUserIdAndSearchPhrasePairs(clickBenchHitsPath);
    }

    @Benchmark
    public Object query18FirstUserIdAndSearchPhrasePairs()
    {
        return support.query18FirstUserIdAndSearchPhrasePairs(clickBenchHitsPath);
    }

    @Benchmark
    public Object query19TopUserIdMinuteAndSearchPhraseTriples()
    {
        return support.query19TopUserIdMinuteAndSearchPhraseTriples(clickBenchHitsPath);
    }

    @Benchmark
    public Object query20UserIdsForExactUserId()
    {
        return support.query20UserIdsForExactUserId(clickBenchHitsPath);
    }

    @Benchmark
    public Object query21CountUrlsContainingGoogle()
    {
        return support.query21CountUrlsContainingGoogle(clickBenchHitsPath);
    }

    @Benchmark
    public Object query22SearchPhrasesWithGoogleUrls()
    {
        return support.query22SearchPhrasesWithGoogleUrls(clickBenchHitsPath);
    }

    @Benchmark
    public Object query23GoogleTitlesNonGoogleUrls()
    {
        return support.query23GoogleTitlesNonGoogleUrls(clickBenchHitsPath);
    }

    @Benchmark
    public Object query24SelectAllGoogleUrlsOrderedByEventTime()
    {
        return support.query24SelectAllGoogleUrlsOrderedByEventTime(clickBenchHitsPath);
    }

    @Benchmark
    public Object query25SearchPhrasesOrderedByEventTime()
    {
        return support.query25SearchPhrasesOrderedByEventTime(clickBenchHitsPath);
    }

    @Benchmark
    public Object query26SearchPhrasesOrderedAscending()
    {
        return support.query26SearchPhrasesOrderedAscending(clickBenchHitsPath);
    }

    @Benchmark
    public Object query27SearchPhrasesOrderedByEventTimeThenPhrase()
    {
        return support.query27SearchPhrasesOrderedByEventTimeThenPhrase(clickBenchHitsPath);
    }

    @Benchmark
    public Object query28CounterAverageUrlLength()
    {
        return support.query28CounterAverageUrlLength(clickBenchHitsPath);
    }

    @Benchmark
    public Object query29RefererHosts()
    {
        return support.query29RefererHosts(clickBenchHitsPath);
    }

    @Benchmark
    public Object query30SumResolutionWidthPlusOffsets()
    {
        return support.query30SumResolutionWidthPlusOffsets(clickBenchHitsPath);
    }

    @Benchmark
    public Object query31SearchEngineAndClientIp()
    {
        return support.query31SearchEngineAndClientIp(clickBenchHitsPath);
    }

    @Benchmark
    public Object query32WatchIdAndClientIpWithSearchPhrase()
    {
        return support.query32WatchIdAndClientIpWithSearchPhrase(clickBenchHitsPath);
    }

    @Benchmark
    public Object query33WatchIdAndClientIp()
    {
        return support.query33WatchIdAndClientIp(clickBenchHitsPath);
    }

    @Benchmark
    public Object query34TopUrls()
    {
        return support.query34TopUrls(clickBenchHitsPath);
    }

    @Benchmark
    public Object query35ConstantAndTopUrls()
    {
        return support.query35ConstantAndTopUrls(clickBenchHitsPath);
    }

    @Benchmark
    public Object query36ClientIpArithmeticGroups()
    {
        return support.query36ClientIpArithmeticGroups(clickBenchHitsPath);
    }

    @Benchmark
    public Object query37TopUrlsForCounter62()
    {
        return support.query37TopUrlsForCounter62(clickBenchHitsPath);
    }

    @Benchmark
    public Object query38TopTitlesForCounter62()
    {
        return support.query38TopTitlesForCounter62(clickBenchHitsPath);
    }

    @Benchmark
    public Object query39TopUrlsOffset()
    {
        return support.query39TopUrlsOffset(clickBenchHitsPath);
    }

    @Benchmark
    public Object query40TrafficSourceGroups()
    {
        return support.query40TrafficSourceGroups(clickBenchHitsPath);
    }

    @Benchmark
    public Object query41UrlHashByEventDate()
    {
        return support.query41UrlHashByEventDate(clickBenchHitsPath);
    }

    @Benchmark
    public Object query42WindowClientSizes()
    {
        return support.query42WindowClientSizes(clickBenchHitsPath);
    }

    @Benchmark
    public Object query43PageViewsByMinute()
    {
        return support.query43PageViewsByMinute(clickBenchHitsPath);
    }
}
