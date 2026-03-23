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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkClickBenchHits
{
    private final Allocator allocator = new Allocator();
    private final PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
    private Path clickBenchHitsDirectory;

    @Setup
    public void setup()
    {
        clickBenchHitsDirectory = ClickBenchHitsSupport.requiredActualHitsDirectory();
    }

    @Benchmark
    public void query0SelectAll()
    {
        consume(ClickBenchHitsSupport.query0SelectAll(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query1CountAll()
    {
        consume(ClickBenchHitsSupport.query1CountAll(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query2CountNonZeroAdvEngineId()
    {
        consume(ClickBenchHitsSupport.query2CountNonZeroAdvEngineId(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query3SumAdvEngineAndAvgResolutionWidth()
    {
        consume(ClickBenchHitsSupport.query3SumAdvEngineAndAvgResolutionWidth(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query4AvgUserId()
    {
        consume(ClickBenchHitsSupport.query4AvgUserId(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query5CountDistinctUserId()
    {
        consume(ClickBenchHitsSupport.query5CountDistinctUserId(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query6CountDistinctSearchPhrase()
    {
        consume(ClickBenchHitsSupport.query6CountDistinctSearchPhrase(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query7MinAndMaxEventDate()
    {
        consume(ClickBenchHitsSupport.query7MinAndMaxEventDate(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query8GroupByAdvEngineId()
    {
        consume(ClickBenchHitsSupport.query8GroupByAdvEngineId(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query9TopRegionsByDistinctUsers()
    {
        consume(ClickBenchHitsSupport.query9TopRegionsByDistinctUsers(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query10RegionAggregates()
    {
        consume(ClickBenchHitsSupport.query10RegionAggregates(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query11TopMobilePhoneModelsByDistinctUsers()
    {
        consume(ClickBenchHitsSupport.query11TopMobilePhoneModelsByDistinctUsers(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query12TopMobilePhonesAndModelsByDistinctUsers()
    {
        consume(ClickBenchHitsSupport.query12TopMobilePhonesAndModelsByDistinctUsers(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query13TopSearchPhrases()
    {
        consume(ClickBenchHitsSupport.query13TopSearchPhrases(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query14TopSearchPhrasesByDistinctUsers()
    {
        consume(ClickBenchHitsSupport.query14TopSearchPhrasesByDistinctUsers(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query15TopSearchEngineAndPhrasePairs()
    {
        consume(ClickBenchHitsSupport.query15TopSearchEngineAndPhrasePairs(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query16TopUserIds()
    {
        consume(ClickBenchHitsSupport.query16TopUserIds(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query17TopUserIdAndSearchPhrasePairs()
    {
        consume(ClickBenchHitsSupport.query17TopUserIdAndSearchPhrasePairs(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query18FirstUserIdAndSearchPhrasePairs()
    {
        consume(ClickBenchHitsSupport.query18FirstUserIdAndSearchPhrasePairs(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query19TopUserIdMinuteAndSearchPhraseTriples()
    {
        consume(ClickBenchHitsSupport.query19TopUserIdMinuteAndSearchPhraseTriples(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query20UserIdsForExactUserId()
    {
        consume(ClickBenchHitsSupport.query20UserIdsForExactUserId(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query22SearchPhrasesWithGoogleUrls()
    {
        consume(ClickBenchHitsSupport.query22SearchPhrasesWithGoogleUrls(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query23GoogleTitlesNonGoogleUrls()
    {
        consume(ClickBenchHitsSupport.query23GoogleTitlesNonGoogleUrls(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query24SelectAllGoogleUrlsOrderedByEventTime()
    {
        consume(ClickBenchHitsSupport.query24SelectAllGoogleUrlsOrderedByEventTime(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query25SearchPhrasesOrderedByEventTime()
    {
        consume(ClickBenchHitsSupport.query25SearchPhrasesOrderedByEventTime(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query21CountUrlsContainingGoogle()
    {
        consume(ClickBenchHitsSupport.query21CountUrlsContainingGoogle(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query26SearchPhrasesOrderedAscending()
    {
        consume(ClickBenchHitsSupport.query26SearchPhrasesOrderedAscending(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query27SearchPhrasesOrderedByEventTimeThenPhrase()
    {
        consume(ClickBenchHitsSupport.query27SearchPhrasesOrderedByEventTimeThenPhrase(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query28CounterAverageUrlLength()
    {
        consume(ClickBenchHitsSupport.query28CounterAverageUrlLength(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query29RefererHosts()
    {
        consume(ClickBenchHitsSupport.query29RefererHosts(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query30SumResolutionWidthPlusOffsets()
    {
        consume(ClickBenchHitsSupport.query30SumResolutionWidthPlusOffsets(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query31SearchEngineAndClientIp()
    {
        consume(ClickBenchHitsSupport.query31SearchEngineAndClientIp(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query32WatchIdAndClientIpWithSearchPhrase()
    {
        consume(ClickBenchHitsSupport.query32WatchIdAndClientIpWithSearchPhrase(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query33WatchIdAndClientIp()
    {
        consume(ClickBenchHitsSupport.query33WatchIdAndClientIp(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query34TopUrls()
    {
        consume(ClickBenchHitsSupport.query34TopUrls(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query35ConstantAndTopUrls()
    {
        consume(ClickBenchHitsSupport.query35ConstantAndTopUrls(allocator, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query36ClientIpArithmeticGroups()
    {
        consume(ClickBenchHitsSupport.query36ClientIpArithmeticGroups(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query37TopUrlsForCounter62()
    {
        consume(ClickBenchHitsSupport.query37TopUrlsForCounter62(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query38TopTitlesForCounter62()
    {
        consume(ClickBenchHitsSupport.query38TopTitlesForCounter62(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query39TopUrlsOffset()
    {
        consume(ClickBenchHitsSupport.query39TopUrlsOffset(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query40TrafficSourceGroups()
    {
        consume(ClickBenchHitsSupport.query40TrafficSourceGroups(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query41UrlHashByEventDate()
    {
        consume(ClickBenchHitsSupport.query41UrlHashByEventDate(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query42WindowClientSizes()
    {
        consume(ClickBenchHitsSupport.query42WindowClientSizes(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    @Benchmark
    public void query43PageViewsByMinute()
    {
        consume(ClickBenchHitsSupport.query43PageViewsByMinute(allocator, primitiveRegistry, clickBenchHitsDirectory));
    }

    private static void consume(Operator operator)
    {
        try (operator) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    Mask mask = batch.borrowMask();
                    if (mask.none()) {
                        continue;
                    }
                    for (int column = 0; column < operator.outputCount(); column++) {
                        consume(batch.output(column).borrow(Stream.VALUES));
                    }
                }
            }
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private static void consume(Vector vector)
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkClickBenchHits.class)
                .run();
    }
}
