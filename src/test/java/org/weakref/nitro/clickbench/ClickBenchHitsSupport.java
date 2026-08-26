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
package org.weakref.nitro.clickbench;

import org.weakref.nitro.benchmark.BenchmarkSchemaRegistry;
import org.weakref.nitro.benchmark.BenchmarkTypeRegistry;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.builtin.CastI64ToI64;
import org.weakref.nitro.operator.AdaptiveSqlPartialAggregationOperator;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LazyUnionAllOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.MarkDistinctMarkerOperator;
import org.weakref.nitro.operator.MarkDistinctOperator;
import org.weakref.nitro.operator.MaterializeOperator;
import org.weakref.nitro.operator.OffsetOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SqlStageAggregationOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TopNSession;
import org.weakref.nitro.operator.UnionAllOperator;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.DistinctPhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.FilteredAccumulator;
import org.weakref.nitro.operator.aggregation.MinMaxI64AggregationUnit;
import org.weakref.nitro.operator.aggregation.MinUtf8;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.Operation;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.source.AllocatedSelectionOperatorIngress;
import org.weakref.nitro.operator.source.BatchFeedOperator;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.ColumnViewSourceOperatorIngress;
import org.weakref.nitro.operator.source.LongDomainRuntimeFilterSourceIngress;
import org.weakref.nitro.operator.source.VectorColumnViewOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.NativeSourceOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;
import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanOperator;
import org.weakref.nitro.parquet.NitroParquetBatchSource;
import org.weakref.nitro.parquet.NitroParquetScanResources;
import org.weakref.nitro.parquet.ParquetArenaPolicy;
import org.weakref.nitro.parquet.ParquetFile;
import org.weakref.nitro.parquet.ParquetScanBatchPolicy;
import org.weakref.nitro.tpcds.OperatorCpuProfile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.lang.Math.toIntExact;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.requiredBinary;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.requiredInt32;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.Column.requiredInt64;
import static org.weakref.nitro.parquet.NativeParquetTestFileWriter.write;

public final class ClickBenchHitsSupport
{
    private static final BenchmarkSchemaRegistry SCHEMAS = new BenchmarkSchemaRegistry(new BenchmarkTypeRegistry());
    private static final long SQL_PARTIAL_AGGREGATION_MEMORY_BYTES = 32L * 1024 * 1024;
    private static final double SQL_PARTIAL_AGGREGATION_UNIQUE_ROWS_RATIO = 0.8;
    static final String CLICKBENCH_HITS_PATH_PROPERTY = "nitro.clickbench.hits.path";
    static final long QUERY20_USER_ID = 435_090_932_899_640_449L;
    static final long QUERY41_REFERER_HASH = 3_594_120_000_172_545_465L;
    static final long QUERY42_URL_HASH = 2_868_770_270_353_813_622L;
    private static final List<String> ALL_HITS_COLUMNS = List.of(
            "AdvEngineID",
            "ResolutionWidth",
            "UserID",
            "EventDate",
            "URL",
            "SearchPhrase");
    private static final List<String> ALL_FIXTURE_COLUMNS = List.of(
            "WatchID",
            "Title",
            "EventTime",
            "EventDate",
            "CounterID",
            "ClientIP",
            "RegionID",
            "UserID",
            "URL",
            "Referer",
            "IsRefresh",
            "ResolutionWidth",
            "MobilePhone",
            "MobilePhoneModel",
            "TraficSourceID",
            "SearchEngineID",
            "SearchPhrase",
            "AdvEngineID",
            "WindowClientWidth",
            "WindowClientHeight",
            "IsLink",
            "IsDownload",
            "DontCountHits",
            "RefererHash",
            "URLHash");

    private ClickBenchHitsSupport() {}

    private static Operator profiled(OperatorCpuProfile profile, String name, Operator operator)
    {
        return profile == null ? operator : profile.wrap(name, operator);
    }

    public static final int DEFAULT_ROW_COUNT = 8;
    public static final int BENCHMARK_ROW_COUNT = 100_000;

    public static Optional<Path> actualHitsDirectoryIfPresent()
    {
        String configuredPath = System.getProperty(CLICKBENCH_HITS_PATH_PROPERTY);
        if (configuredPath != null && !configuredPath.isBlank()) {
            Path directory = Path.of(configuredPath);
            return resolveActualHitsDirectory(directory);
        }

        Path clickBenchDirectory = Path.of(System.getProperty("user.home"), "tmp", "clickbench");
        Optional<Path> resolvedDefaultDirectory = resolveActualHitsDirectory(clickBenchDirectory);
        if (resolvedDefaultDirectory.isPresent()) {
            return resolvedDefaultDirectory;
        }
        return Optional.empty();
    }

    public static Path requiredActualHitsDirectory()
    {
        return actualHitsDirectoryIfPresent()
                .orElseThrow(() -> new IllegalStateException("Set -D" + CLICKBENCH_HITS_PATH_PROPERTY + "=/path/to/clickbench, or place the split parquet files at ~/tmp/clickbench"));
    }

    public static Path writeHitsFixture(Path file, int rowCount)
            throws IOException
    {
        List<HitRow> templateRows = templateRows();
        List<HitRow> rows = new ArrayList<>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            rows.add(templateRows.get(rowIndex % templateRows.size()).vary(rowIndex / templateRows.size()));
        }
        write(file, "hits", List.of(
                requiredInt64("WatchID", rows.stream().map(HitRow::watchId).toList()),
                requiredBinary("Title", rows.stream().map(HitRow::title).toList()).asUtf8(),
                requiredInt64("EventTime", rows.stream().map(HitRow::eventTime).toList()),
                requiredInt32("EventDate", rows.stream().map(HitRow::eventDate).toList()),
                requiredInt32("CounterID", rows.stream().map(HitRow::counterId).toList()),
                requiredInt32("ClientIP", rows.stream().map(HitRow::clientIp).toList()),
                requiredInt32("RegionID", rows.stream().map(HitRow::regionId).toList()),
                requiredInt64("UserID", rows.stream().map(HitRow::userId).toList()),
                requiredBinary("URL", rows.stream().map(HitRow::url).toList()).asUtf8(),
                requiredBinary("Referer", rows.stream().map(HitRow::referer).toList()).asUtf8(),
                requiredInt32("IsRefresh", rows.stream().map(HitRow::isRefresh).toList()),
                requiredInt32("AdvEngineID", rows.stream().map(HitRow::advEngineId).toList()),
                requiredInt32("ResolutionWidth", rows.stream().map(HitRow::resolutionWidth).toList()),
                requiredInt32("MobilePhone", rows.stream().map(HitRow::mobilePhone).toList()),
                requiredBinary("MobilePhoneModel", rows.stream().map(HitRow::mobilePhoneModel).toList()).asUtf8(),
                requiredInt32("TraficSourceID", rows.stream().map(HitRow::traficSourceId).toList()),
                requiredInt32("SearchEngineID", rows.stream().map(HitRow::searchEngineId).toList()),
                requiredBinary("SearchPhrase", rows.stream().map(HitRow::searchPhrase).toList()).asUtf8(),
                requiredInt32("WindowClientWidth", rows.stream().map(HitRow::windowClientWidth).toList()),
                requiredInt32("WindowClientHeight", rows.stream().map(HitRow::windowClientHeight).toList()),
                requiredInt32("IsLink", rows.stream().map(HitRow::isLink).toList()),
                requiredInt32("IsDownload", rows.stream().map(HitRow::isDownload).toList()),
                requiredInt32("DontCountHits", rows.stream().map(HitRow::dontCountHits).toList()),
                requiredInt64("RefererHash", rows.stream().map(HitRow::refererHash).toList()),
                requiredInt64("URLHash", rows.stream().map(HitRow::urlHash).toList())), true);
        return file;
    }

    public static Operator query01(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                clickBenchScan(allocator, file));
    }

    public static Operator query00(Allocator allocator, Path file)
    {
        return clickBenchScan(allocator, file, ALL_HITS_COLUMNS.toArray(String[]::new));
    }

    public static Operator query24(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        try {
            List<String> columns = allHitsColumns(file);
            int eventTimeIndex = columns.indexOf("EventTime");
            int watchIdIndex = columns.indexOf("WatchID");
            int clientIpIndex = columns.indexOf("ClientIP");
            int urlIndex = columns.indexOf("URL");
            int searchPhraseIndex = columns.indexOf("SearchPhrase");
            int[] sortChannels = {eventTimeIndex, watchIdIndex, clientIpIndex, urlIndex, searchPhraseIndex};
            boolean[] descending = {false, false, false, false, false};
            NitroParquetScanResources scanResources = NitroParquetScanResources.createDefault(
                    ParquetArenaPolicy.shared(),
                    ParquetScanBatchPolicy.adaptiveHostBoundaryDefaults());

            List<NitroParquetBatchSource.Split> splits = parquetSplits(file, 120L * 1024 * 1024);
            List<Operator> partials = new ArrayList<>(splits.size());
            for (NitroParquetBatchSource.Split split : splits) {
                Allocator splitAllocator = new Allocator(allocator.resourcesOwner());
                Operator partial = null;
                try {
                    Schema scanSchema = SCHEMAS.parquet(split.path(), columns);
                    BatchSource scan = NitroParquetBatchSource.forSplits(
                            scanResources,
                            splitAllocator,
                            List.of(split),
                            scanSchema);
                    BatchFeedOperator feed = new BatchFeedOperator(
                            scan.schema(),
                            new ColumnViewSourceOperatorIngress(
                                    scan.schema(),
                                    new AllocatedSelectionOperatorIngress(
                                            splitAllocator,
                                            new Allocator.Context("ClickBenchQ24SourceIngress")),
                                    new LongDomainRuntimeFilterSourceIngress(),
                                    VectorColumnViewOperatorIngress::new),
                            Set.of(
                                    org.weakref.nitro.core.source.SourceCapability.STABLE_BATCH_BORROW,
                                    org.weakref.nitro.core.source.SourceCapability.CONSTRAINED_REBORROW));
                    Operator filtered = filter(
                            splitAllocator,
                            primitiveRegistry,
                            feed,
                            containsUtf8(urlIndex, "google"));
                    partial = new SessionTopNOperator(
                            splitAllocator,
                            10,
                            sortChannels,
                            descending,
                            scan,
                            feed,
                            filtered);
                    partials.add(new AllocatorOwnedOperator(splitAllocator, partial));
                }
                catch (RuntimeException | Error failure) {
                    if (partial != null) {
                        partial.close();
                    }
                    splitAllocator.close();
                    throw failure;
                }
            }

            Operator exchange = new MaterializeOperator(allocator, new UnionAllOperator(columns.size(), partials));
            return new TopNOperator(allocator, 10, sortChannels, descending, exchange);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list ClickBench splits for " + file, exception);
        }
    }

    public static Operator query02(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                filter(allocator, primitiveRegistry, file, List.of("AdvEngineID"), notEqualI64(0, 0)));
    }

    public static Operator query07(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                PhysicalAggregationProgram.singleUnit(new MinMaxI64AggregationUnit(0)),
                clickBenchScan(allocator, file, "EventDate"));
    }

    public static Operator query03(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountAll(), new Avg(1)),
                clickBenchScan(allocator, file, "AdvEngineID", "ResolutionWidth"));
    }

    public static Operator query04(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new Avg(0)),
                clickBenchScan(allocator, file, "UserID"));
    }

    public static Operator query05(Allocator allocator, Path file)
    {
        return query05(allocator, file, null);
    }

    static Operator query05(Allocator allocator, Path file, OperatorCpuProfile profile)
    {
        return sqlShapedCountDistinct(allocator, file, "UserID", "q05", profile);
    }

    public static Operator query06(Allocator allocator, Path file)
    {
        return sqlShapedCountDistinct(allocator, file, "SearchPhrase", "q06", null);
    }

    public static Operator query08(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        FilterSpec predicate = notEqualI64(0, 0);
        Operator scan = clickBenchScan(allocator, file, "AdvEngineID");
        Operator filtered = new FilterOperator(
                scan,
                predicate.plan(),
                primitiveRegistry,
                predicate.predicate(),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                filtered);
        return new TopNOperator(allocator, 10, 1, aggregated);
    }

    public static Operator query09(Allocator allocator, Path file)
    {
        return query09(allocator, file, null);
    }

    static Operator query09(Allocator allocator, Path file, OperatorCpuProfile profile)
    {
        try {
            List<Path> splits = Files.isDirectory(file) ? parquetFiles(file) : List.of(file);
            List<Operator> partials = new ArrayList<>(splits.size());
            for (Path split : splits) {
                Operator scan = profiled(profile, "q09.partial-scan", clickBenchScan(allocator, split, "RegionID", "UserID"));
                partials.add(profiled(profile, "q09.partial-pairs", new SqlStageAggregationOperator(
                        allocator,
                        scan,
                        1,
                        new int[0],
                        List.of(SqlStageAggregationOperator.distinct(List.of(0, 1))))));
            }
            Operator exchange = profiled(
                    profile,
                    "q09.exchange",
                    new MaterializeOperator(allocator, new UnionAllOperator(2, partials)));
            Operator finalCount = profiled(profile, "q09.final-count", new SqlStageAggregationOperator(
                    allocator,
                    exchange,
                    1,
                    new int[0],
                    List.of(
                            SqlStageAggregationOperator.distinct(List.of(0, 1)),
                            SqlStageAggregationOperator.aggregate(List.of(0), () -> List.of(new CountAll())))));
            return profiled(profile, "q09.topn", new TopNOperator(allocator, 10, 1, finalCount));
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list ClickBench splits for " + file, exception);
        }
    }

    public static Operator query10(Allocator allocator, Path file)
    {
        return query10(allocator, file, null);
    }

    static Operator query10(Allocator allocator, Path file, OperatorCpuProfile profile)
    {
        Operator scan = profiled(profile, "q10.scan", clickBenchScan(allocator, file, "RegionID", "AdvEngineID", "ResolutionWidth", "UserID"));
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("cast_i64_to_i64", new CastI64ToI64());
        Variable advEngineId = new Variable(0);
        Variable resolutionWidth = new Variable(1);
        EvaluationPlan projection = new EvaluationPlan(
                List.of(
                        new Assignment(advEngineId, new Call("cast_i64_to_i64", List.of(new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(resolutionWidth, new Call("cast_i64_to_i64", List.of(new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(advEngineId, Stream.VALUES),
                        new Reference(resolutionWidth, Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES)));
        Operator projected = profiled(profile, "q10.project", new ProjectOperator(allocator, projection, primitiveRegistry, scan));
        PhysicalAggregationProgram aggregations = new PhysicalAggregationProgram(
                List.of(
                        new Sum(1),
                        new CountAll(),
                        new Avg(2),
                        new DistinctPhysicalAggregationUnit(new CountAll(), new int[] {3})),
                List.of(
                        new PhysicalAggregationProgram.Output(0, 0),
                        new PhysicalAggregationProgram.Output(1, 0),
                        new PhysicalAggregationProgram.Output(2, 0),
                        new PhysicalAggregationProgram.Output(3, 0)));
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(0),
                aggregations,
                projected,
                EngineResources.from(allocator).operatorResources());
        aggregated = profiled(profile, "q10.aggregate", aggregated);
        return profiled(profile, "q10.topn", new TopNOperator(allocator, 10, 2, aggregated));
    }

    public static Operator query11(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("MobilePhoneModel", "UserID"), notEqualUtf8(0, ""));
        Operator distinct = new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0, 1},
                filtered,
                true,
                EngineResources.from(allocator).operatorResources());
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new FilteredAccumulator(new CountAll(), 2)),
                distinct);
        return new TopNOperator(allocator, 10, 1, aggregated);
    }

    public static Operator query12(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return query12(allocator, primitiveRegistry, file, null);
    }

    static Operator query12(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, OperatorCpuProfile profile)
    {
        Operator scan = profiled(profile, "q12.scan", clickBenchScan(allocator, file, "MobilePhone", "MobilePhoneModel", "UserID"));
        Operator filtered = profiled(profile, "q12.filter", filter(allocator, primitiveRegistry, scan, notEqualUtf8(1, "")));
        Operator distinct = profiled(profile, "q12.mark-distinct", new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0, 1, 2},
                filtered,
                true,
                EngineResources.from(allocator).operatorResources()));
        Operator aggregated = profiled(profile, "q12.group", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new FilteredAccumulator(new CountAll(), 3)),
                distinct));
        return profiled(profile, "q12.topn", new TopNOperator(allocator, 10, 2, aggregated));
    }

    public static Operator query21(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                filter(allocator, primitiveRegistry, file, List.of("URL"), containsUtf8(0, "google")));
    }

    public static Operator query22(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return query22(allocator, primitiveRegistry, file, null);
    }

    static Operator query22(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, OperatorCpuProfile profile)
    {
        Operator scan = profiled(profile, "q22.scan", clickBenchScan(allocator, file, "SearchPhrase", "URL"));
        Operator filtered = profiled(profile, "q22.filter", filter(allocator, primitiveRegistry, scan, and(notEqualUtf8(0, ""), containsUtf8(1, "google"))));
        Operator grouped = new GroupOperator(allocator, 0, filtered);
        Operator aggregated = profiled(profile, "q22.aggregate", new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new MinUtf8(2), new CountAll()),
                grouped));
        return profiled(profile, "q22.topn", new TopNOperator(allocator, 10, 2, aggregated));
    }

    public static Operator query23(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                file,
                List.of("SearchPhrase", "URL", "Title", "UserID"),
                and(
                        notEqualUtf8(0, ""),
                        containsUtf8(2, "Google"),
                        notContainsUtf8(1, ".google.")));
        Operator distinct = new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0, 3},
                filtered,
                true,
                EngineResources.from(allocator).operatorResources());
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new MinUtf8(1), new MinUtf8(2), new CountAll(), new FilteredAccumulator(new CountAll(), 4)),
                distinct);
        return new TopNOperator(allocator, 10, 3, aggregated);
    }

    public static Operator query13(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return topUtf8Counts(allocator, primitiveRegistry, file, "SearchPhrase", true, null);
    }

    public static Operator query14(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("SearchPhrase", "UserID"), notEqualUtf8(0, ""));
        Operator distinct = new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0, 1},
                filtered,
                true,
                EngineResources.from(allocator).operatorResources());
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new FilteredAccumulator(new CountAll(), 2)),
                distinct);
        return new TopNOperator(allocator, 10, 1, aggregated);
    }

    public static Operator query15(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("SearchEngineID", "SearchPhrase"), notEqualUtf8(1, ""));
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                filtered);
        return new TopNOperator(allocator, 10, 2, aggregated);
    }

    public static Operator query16(Allocator allocator, Path file)
    {
        return topIntegerCounts(allocator, file, "UserID");
    }

    public static Operator query17(Allocator allocator, Path file)
    {
        Operator grouped = new GroupOperator(allocator, new int[] {0, 1}, clickBenchScan(allocator, file, "UserID", "SearchPhrase"));
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1, 2),
                List.of(new CountAll()),
                grouped);
        return new TopNOperator(allocator, 10, 2, aggregated);
    }

    public static Operator query18(Allocator allocator, Path file)
    {
        return query18(allocator, file, null);
    }

    static Operator query18(Allocator allocator, Path file, OperatorCpuProfile profile)
    {
        Operator scan = profiled(profile, "q18.scan", clickBenchScan(allocator, file, "UserID", "SearchPhrase"));
        // Keep GroupOperator visible as GroupedKeySource to its consumer; the outer wrappers still expose inclusive
        // aggregation and scan costs, whose difference is the grouping/aggregation work.
        Operator grouped = new GroupOperator(allocator, new int[] {0, 1}, scan);
        Operator aggregated = profiled(profile, "q18.aggregate", new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1, 2),
                List.of(new CountAll()),
                grouped));
        return profiled(profile, "q18.limit", new LimitOperator(allocator, 10, aggregated));
    }

    public static Operator query19(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator source = clickBenchScan(allocator, file, "UserID", "EventTime", "SearchPhrase");
        Operator projected = projectEventTimeMinute(allocator, primitiveRegistry, source);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new CountAll()),
                projected);
        return new TopNOperator(allocator, 10, 3, aggregated);
    }

    public static Operator query20(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return filter(allocator, primitiveRegistry, file, List.of("UserID"), equalI64(0, QUERY20_USER_ID));
    }

    public static Operator query26(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("SearchPhrase"), notEqualUtf8(0, ""));
        return new TopNOperator(allocator, 10, 0, false, filtered);
    }

    public static Operator query25(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("EventTime", "SearchPhrase"), notEqualUtf8(1, ""));
        Operator ordered = new TopNOperator(allocator, 10, 0, false, filtered);
        return projectInputs(allocator, new PrimitiveRegistry(), ordered, 1);
    }

    public static Operator query27(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("EventTime", "SearchPhrase"), notEqualUtf8(1, ""));
        Operator ordered = new TopNOperator(allocator, 10, new int[] {0, 1}, new boolean[] {false, false}, filtered);
        return projectInputs(allocator, new PrimitiveRegistry(), ordered, 1);
    }

    public static Operator query28(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("CounterID", "URL"), notEqualUtf8(1, ""));
        Operator projected = projectCounterAndUtf8Length(allocator, primitiveRegistry, filtered);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(0),
                List.of(new Avg(1), new CountAll()),
                projected);
        Operator having = filter(
                allocator,
                primitiveRegistry,
                aggregated,
                greaterThan(2, 100_000));
        return new TopNOperator(allocator, 25, 1, having);
    }

    public static Operator query29(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("Referer"), notEqualUtf8(0, ""));
        Operator projected = projectRefererHostAndLength(allocator, primitiveRegistry, filtered);
        Operator grouped = new GroupOperator(allocator, 0, projected);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new Avg(2), new CountAll(), new MinUtf8(3)),
                grouped);
        Operator having = filter(
                allocator,
                primitiveRegistry,
                aggregated,
                greaterThan(2, 100_000));
        return new TopNOperator(allocator, 25, 1, having);
    }

    public static Operator query30(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        try {
            int sumCount = 90;
            NitroParquetScanResources scanResources = NitroParquetScanResources.createDefault(
                    ParquetArenaPolicy.shared(),
                    ParquetScanBatchPolicy.adaptiveHostBoundaryDefaults());
            List<Path> splits = Files.isDirectory(file) ? parquetFiles(file) : List.of(file);
            List<Operator> partials = new ArrayList<>(splits.size());
            for (Path split : splits) {
                partials.add(new AggregationOperator(
                        allocator,
                        PhysicalAggregationProgram.singleUnit(ExactAffineBigintSums.partial(sumCount)),
                        clickBenchScan(scanResources, allocator, split, "ResolutionWidth")));
            }
            Operator exchange = new MaterializeOperator(allocator, new UnionAllOperator(sumCount, partials));
            return new AggregationOperator(allocator, exactOffsetSums(sumCount), exchange);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list ClickBench splits for " + file, exception);
        }
    }

    public static Operator query31(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return sqlShapedTopGroupedCountSumAvg(
                allocator,
                primitiveRegistry,
                file,
                "SearchEngineID",
                true,
                null);
    }

    public static Operator query32(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return sqlShapedTopGroupedCountSumAvg(
                allocator,
                primitiveRegistry,
                file,
                "WatchID",
                true,
                null);
    }

    public static Operator query33(Allocator allocator, Path file)
    {
        return query33(allocator, file, null);
    }

    static Operator query33(Allocator allocator, Path file, OperatorCpuProfile profile)
    {
        return profiled(
                profile,
                "q33.sql-shape",
                sqlShapedTopGroupedCountSumAvg(
                        allocator,
                        org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry(),
                        file,
                        "WatchID",
                        false,
                        profile));
    }

    public static Operator query34(Allocator allocator, Path file)
    {
        return query34(allocator, file, null);
    }

    static Operator query34(Allocator allocator, Path file, OperatorCpuProfile profile)
    {
        return topUtf8Counts(allocator, null, file, "URL", false, profile);
    }

    public static Operator query35(Allocator allocator, Path file)
    {
        Operator topUrls = topUtf8Counts(allocator, null, file, "URL", false, null);
        return prependConstant(allocator, topUrls, 1);
    }

    public static Operator query36(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return query36(allocator, primitiveRegistry, file, null);
    }

    static Operator query36(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, OperatorCpuProfile profile)
    {
        Operator scan = profiled(profile, "q36.scan", clickBenchScan(allocator, file, "ClientIP"));
        Operator projected = profiled(profile, "q36.project", projectClientIpOffsets(allocator, primitiveRegistry, scan));
        Operator aggregated = profiled(profile, "q36.aggregate", new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2, 3),
                List.of(new CountAll()),
                projected));
        return profiled(profile, "q36.topn", new TopNOperator(allocator, 10, 4, aggregated));
    }

    public static Operator query37(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                file,
                List.of("URL", "CounterID", "EventDate", "DontCountHits", "IsRefresh"),
                and(
                        notEqualUtf8(0, ""),
                        counterAndJulyEventDateFilter(file, 1, 2, 4),
                        equalTo(3, 0)));
        return topUtf8Counts(allocator, null, filtered, 0);
    }

    public static Operator query38(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                file,
                List.of("Title", "CounterID", "EventDate", "DontCountHits", "IsRefresh"),
                and(
                        notEqualUtf8(0, ""),
                        counterAndJulyEventDateFilter(file, 1, 2, 4),
                        equalTo(3, 0)));
        return topUtf8Counts(allocator, null, filtered, 0);
    }

    public static Operator query39(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                file,
                List.of("URL", "CounterID", "EventDate", "IsRefresh", "IsLink", "IsDownload"),
                and(
                        counterAndJulyEventDateFilter(file, 1, 2, 3),
                        notEqualTo(4, 0),
                        equalTo(5, 0)));
        Operator grouped = new GroupOperator(allocator, 0, filtered);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                grouped);
        Operator ordered = new TopNOperator(allocator, 1_010, 1, aggregated);
        return new OffsetOperator(allocator, 1_000, ordered);
    }

    public static Operator query40(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return query40(allocator, primitiveRegistry, file, null);
    }

    static Operator query40(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, OperatorCpuProfile profile)
    {
        Operator scan = profiled(profile, "q40.scan", clickBenchScan(
                allocator,
                file,
                "TraficSourceID", "SearchEngineID", "AdvEngineID", "Referer", "URL", "CounterID", "EventDate", "IsRefresh"));
        Operator filtered = profiled(profile, "q40.filter", filter(
                allocator,
                primitiveRegistry,
                scan,
                counterAndJulyEventDateFilter(file, 5, 6, 7)));
        Operator projected = profiled(profile, "q40.project", projectTrafficSourceCase(allocator, primitiveRegistry, filtered));
        Operator grouped = new GroupOperator(allocator, new int[] {0, 1, 2, 3, 4}, projected);
        Operator aggregated = profiled(profile, "q40.aggregate", new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1, 2, 3, 4, 5),
                List.of(new CountAll()),
                grouped));
        Operator ordered = profiled(profile, "q40.topn", new TopNOperator(
                allocator,
                1_010,
                new int[] {5, 3, 4},
                new boolean[] {false, false, true},
                aggregated));
        return profiled(profile, "q40.offset", new OffsetOperator(allocator, 1_000, ordered));
    }

    public static Operator query41(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                file,
                List.of("URLHash", "EventDate", "CounterID", "IsRefresh", "TraficSourceID", "RefererHash"),
                and(
                        counterAndJulyEventDateFilter(file, 2, 1, 3),
                        trafficSourceIn(4),
                        equalTo(5, QUERY41_REFERER_HASH)));
        Operator grouped = new GroupOperator(allocator, new int[] {0, 1}, filtered);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1, 2),
                List.of(new CountAll()),
                grouped);
        Operator ordered = new TopNOperator(allocator, 110, 2, aggregated);
        return new OffsetOperator(allocator, 100, ordered);
    }

    public static Operator query42(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(
                allocator,
                primitiveRegistry,
                file,
                List.of("WindowClientWidth", "WindowClientHeight", "CounterID", "EventDate", "IsRefresh", "DontCountHits", "URLHash"),
                and(
                        counterAndJulyEventDateFilter(file, 2, 3, 4),
                        equalTo(5, 0),
                        equalTo(6, QUERY42_URL_HASH)));
        Operator grouped = new GroupOperator(allocator, new int[] {0, 1}, filtered);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1, 2),
                List.of(new CountAll()),
                grouped);
        Operator ordered = new TopNOperator(allocator, 10_010, 2, aggregated);
        return new OffsetOperator(allocator, 10_000, ordered);
    }

    public static Operator query43(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return query43(allocator, primitiveRegistry, file, null);
    }

    static Operator query43(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, OperatorCpuProfile profile)
    {
        Operator scan = profiled(profile, "q43.scan", clickBenchScan(
                allocator,
                file,
                "EventTime", "CounterID", "EventDate", "DontCountHits", "IsRefresh"));
        Operator filtered = profiled(profile, "q43.filter", filter(
                allocator,
                primitiveRegistry,
                scan,
                and(
                        counterAndEventDateFilter(file, 1, 2, 4, LocalDate.of(2013, 7, 14), LocalDate.of(2013, 7, 16)),
                        equalTo(3, 0))));
        Operator projected = profiled(profile, "q43.project.minute", projectMinuteBucket(allocator, primitiveRegistry, filtered, 0));
        Operator grouped = new GroupOperator(allocator, 0, projected);
        Operator aggregated = profiled(profile, "q43.aggregate", new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                grouped));
        Operator ordered = profiled(profile, "q43.topn", new TopNOperator(allocator, 1_010, 0, false, aggregated));
        return profiled(profile, "q43.offset", new OffsetOperator(allocator, 1_000, ordered));
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, List<String> columns, FilterSpec filterSpec)
    {
        return filter(allocator, primitiveRegistry, clickBenchScan(allocator, file, columns.toArray(String[]::new)), filterSpec);
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, FilterSpec filterSpec)
    {
        return new FilterOperator(
                source,
                filterSpec.plan(),
                primitiveRegistry,
                filterSpec.predicate(),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());
    }

    private static Operator clickBenchScan(Allocator allocator, Path file, String... columns)
    {
        return clickBenchScan(NitroParquetScanResources.createDefault(), allocator, file, columns);
    }

    private static Operator clickBenchScan(
            NitroParquetScanResources resources,
            Allocator allocator,
            Path file,
            String... columns)
    {
        try {
            // The Nitro reader is multi-file aware, so it takes the whole directory's files directly.
            List<Path> paths = Files.isDirectory(file) ? parquetFiles(file) : List.of(file);
            List<String> columnNames = List.of(columns);
            Operator decoder = new NitroParquetScanOperator(
                    resources,
                    allocator,
                    paths,
                    columnNames);
            return new BatchSourceOperator(new OperatorBatchSource(
                    decoder,
                    SCHEMAS.parquet(paths.getFirst(), columnNames)),
                    new NativeSourceOperatorIngress());
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect ClickBench hits file: " + file, exception);
        }
    }

    private static boolean isUsableActualHitsDirectory(Path path)
    {
        if (!Files.isDirectory(path)) {
            return false;
        }
        try (var files = Files.list(path)) {
            return files.anyMatch(file -> Files.isRegularFile(file) && file.getFileName().toString().endsWith(".parquet"));
        }
        catch (IOException exception) {
            return false;
        }
    }

    private static Optional<Path> resolveActualHitsDirectory(Path path)
    {
        if (isUsableActualHitsDirectory(path)) {
            return Optional.of(path);
        }
        return Optional.empty();
    }

    private static List<String> allHitsColumns(Path file)
    {
        if (Files.isDirectory(file)) {
            try {
                file = parquetFiles(file).getFirst();
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to list ClickBench parquet files in " + file, exception);
            }
        }
        try (ParquetFile parquet = ParquetFile.open(file)) {
            return parquet.fieldNames();
        }
    }

    private static List<Path> parquetFiles(Path directory)
            throws IOException
    {
        try (var files = Files.list(directory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(file -> file.getFileName().toString().endsWith(".parquet"))
                    .sorted()
                    .toList();
        }
    }

    private static List<NitroParquetBatchSource.Split> parquetSplits(Path input, long maxSplitSize)
            throws IOException
    {
        List<Path> files = Files.isDirectory(input) ? parquetFiles(input) : List.of(input);
        List<NitroParquetBatchSource.Split> splits = new ArrayList<>();
        for (Path file : files) {
            long size = Files.size(file);
            for (long start = 0; start < size; start += maxSplitSize) {
                splits.add(new NitroParquetBatchSource.Split(file, start, Math.min(maxSplitSize, size - start)));
            }
        }
        return splits;
    }

    /**
     * Models Trino's distributed single-DISTINCT rewrite: each scan split performs local key aggregation, the
     * exchange materializes those partial rows, a final key aggregation removes cross-split duplicates, and only
     * then does the global count run. The direct one-set kernel belongs in a kernel benchmark, not in this
     * SQL-shaped harness.
     */
    private static Operator sqlShapedCountDistinct(
            Allocator allocator,
            Path input,
            String column,
            String profilePrefix,
            OperatorCpuProfile profile)
    {
        try {
            List<Path> splits = Files.isDirectory(input) ? parquetFiles(input) : List.of(input);
            List<Operator> partials = new ArrayList<>(splits.size());
            for (Path split : splits) {
                Operator scan = profiled(profile, profilePrefix + ".partial-scan", clickBenchScan(allocator, split, column));
                partials.add(profiled(
                        profile,
                        profilePrefix + ".partial-distinct",
                        new MarkDistinctOperator(allocator, 0, scan, EngineResources.from(allocator).operatorResources())));
            }

            Operator exchange = profiled(
                    profile,
                    profilePrefix + ".exchange",
                    new MaterializeOperator(allocator, new UnionAllOperator(1, partials)));
            Operator finalDistinct = profiled(
                    profile,
                    profilePrefix + ".final-distinct",
                    new MarkDistinctOperator(allocator, 0, exchange, EngineResources.from(allocator).operatorResources()));
            return profiled(
                    profile,
                    profilePrefix + ".final-count",
                    new AggregationOperator(allocator, List.of(new CountAll()), finalDistinct));
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list ClickBench splits for " + input, exception);
        }
    }

    private static Operator topIntegerCounts(Allocator allocator, Path file, String column)
    {
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                clickBenchScan(allocator, file, column));
        return new TopNOperator(allocator, 10, 1, aggregated);
    }

    private static Operator topUtf8Counts(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int groupColumn)
    {
        Operator grouped = new GroupOperator(allocator, groupColumn, source);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                grouped);
        return new TopNOperator(allocator, 10, 1, aggregated);
    }

    private static Operator topUtf8Counts(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            Path file,
            String column,
            boolean filterEmpty,
            OperatorCpuProfile profile)
    {
        Operator source = profiled(profile, "topUtf8.scan", clickBenchScan(allocator, file, column));
        if (filterEmpty) {
            source = profiled(profile, "topUtf8.filter", new FilterOperator(
                    source,
                    notEqualUtf8(0, "").plan(),
                    primitiveRegistry,
                    notEqualUtf8(0, "").predicate(),
                    allocator,
                    EngineResources.from(allocator).operatorResources().filter()));
        }
        Operator grouped = new GroupOperator(allocator, 0, source);
        Operator aggregated = profiled(profile, "topUtf8.aggregate", new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                grouped));
        return profiled(profile, "topUtf8.topn", new TopNOperator(allocator, 10, 1, aggregated));
    }

    private static Operator projectInputs(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int... inputIndexes)
    {
        List<Reference> outputs = Arrays.stream(inputIndexes)
                .mapToObj(index -> new Reference(new Input(index), Stream.VALUES))
                .toList();
        return new ProjectOperator(allocator, new EvaluationPlan(List.of(), outputs), primitiveRegistry, source);
    }

    private static Operator prependConstant(Allocator allocator, Operator source, long value)
    {
        Variable literal = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(literal, new Literal(value), AllMask.ALL)),
                List.of(
                        new Reference(literal, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES)));
        return new ProjectOperator(allocator, plan, new PrimitiveRegistry(), source);
    }

    private static Operator projectResolutionWidthOffsets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int count)
    {
        java.util.ArrayList<Assignment> assignments = new java.util.ArrayList<>();
        java.util.ArrayList<Reference> outputs = new java.util.ArrayList<>();
        int variableId = 0;
        outputs.add(new Reference(new Input(0), Stream.VALUES));
        for (int offset = 1; offset < count; offset++) {
            Variable literal = new Variable(variableId++);
            assignments.add(new Assignment(literal, new Literal((long) offset), AllMask.ALL));
            Variable sum = new Variable(variableId++);
            assignments.add(new Assignment(sum, new Call("add", List.of(
                    new Reference(new Input(0), Stream.VALUES),
                    new Reference(literal, Stream.VALUES))), AllMask.ALL));
            outputs.add(new Reference(sum, Stream.VALUES));
        }
        return new ProjectOperator(allocator, new EvaluationPlan(assignments, outputs), primitiveRegistry, source);
    }

    private static List<Accumulator> offsetSums(int count)
    {
        java.util.ArrayList<Accumulator> sums = new java.util.ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            sums.add(new Sum(index));
        }
        return sums;
    }

    private static List<Accumulator> exactOffsetSums(int count)
    {
        java.util.ArrayList<Accumulator> sums = new java.util.ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            sums.add(new ExactBigintSum(index));
        }
        return sums;
    }

    private static Operator projectCounterAndUtf8Length(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable length = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(length, new Call("length_utf8", List.of(new Reference(new Input(1), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(new Input(0), Stream.VALUES), new Reference(length, Stream.VALUES)));
        return new ProjectOperator(allocator, plan, primitiveRegistry, source);
    }

    private static Operator projectRefererHostAndLength(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable host = new Variable(0);
        Variable length = new Variable(1);
        Variable pattern = new Variable(2);
        Variable replacement = new Variable(3);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(pattern, new Literal("^https?://(?:www\\.)?([^/]+)/.*$"), AllMask.ALL),
                        new Assignment(replacement, new Literal("\\1"), AllMask.ALL),
                        new Assignment(host, new Call("regexp_replace_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(pattern, Stream.VALUES),
                                new Reference(replacement, Stream.VALUES))), AllMask.ALL),
                        new Assignment(length, new Call("length_utf8", List.of(new Reference(new Input(0), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES), new Reference(length, Stream.VALUES), new Reference(new Input(0), Stream.VALUES)));
        return new ProjectOperator(allocator, plan, primitiveRegistry, source);
    }

    private static Operator projectEventTimeMinute(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable sixty = new Variable(0);
        Variable totalMinutes = new Variable(1);
        Variable minute = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(sixty, new Literal(60L), AllMask.ALL),
                        new Assignment(totalMinutes, new Call("divide", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(sixty, Stream.VALUES))), AllMask.ALL),
                        new Assignment(minute, new Call("modulo", List.of(
                                new Reference(totalMinutes, Stream.VALUES),
                                new Reference(sixty, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(minute, Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES)));
        return new ProjectOperator(allocator, plan, primitiveRegistry, source);
    }

    private static Operator projectMinuteBucket(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source, int eventTimeInputIndex)
    {
        Variable sixty = new Variable(0);
        Variable totalMinutes = new Variable(1);
        Variable minuteStart = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(sixty, new Literal(60L), AllMask.ALL),
                        new Assignment(totalMinutes, new Call("divide", List.of(
                                new Reference(new Input(eventTimeInputIndex), Stream.VALUES),
                                new Reference(sixty, Stream.VALUES))), AllMask.ALL),
                        new Assignment(minuteStart, new Call("multiply", List.of(
                                new Reference(totalMinutes, Stream.VALUES),
                                new Reference(sixty, Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(minuteStart, Stream.VALUES)));
        return new ProjectOperator(allocator, plan, primitiveRegistry, source);
    }

    private static Operator projectTrafficSourceCase(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable zero = new Variable(0);
        Variable searchEquals = new Variable(1);
        Variable advEquals = new Variable(2);
        Variable bothZero = new Variable(3);
        Variable empty = new Variable(4);
        Variable sourceValue = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(searchEquals, new Call("eq", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(advEquals, new Call("eq", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(bothZero, new Call("and", List.of(
                                new Reference(searchEquals, Stream.VALUES),
                                new Reference(advEquals, Stream.VALUES))), AllMask.ALL),
                        new Assignment(empty, new Literal(""), AllMask.ALL),
                        new Assignment(sourceValue, new Call("if_utf8", List.of(
                                new Reference(bothZero, Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES),
                                new Reference(empty, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(sourceValue, Stream.VALUES),
                        new Reference(new Input(4), Stream.VALUES)));
        return new ProjectOperator(allocator, plan, primitiveRegistry, source);
    }

    private static Operator projectClientIpOffsets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Operator source)
    {
        Variable minusOne = new Variable(0);
        Variable minusTwo = new Variable(1);
        Variable minusThree = new Variable(2);
        Variable ipMinusOne = new Variable(3);
        Variable ipMinusTwo = new Variable(4);
        Variable ipMinusThree = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(minusOne, new Literal(-1L), AllMask.ALL),
                        new Assignment(minusTwo, new Literal(-2L), AllMask.ALL),
                        new Assignment(minusThree, new Literal(-3L), AllMask.ALL),
                        new Assignment(ipMinusOne, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(minusOne, Stream.VALUES))), AllMask.ALL),
                        new Assignment(ipMinusTwo, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(minusTwo, Stream.VALUES))), AllMask.ALL),
                        new Assignment(ipMinusThree, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(minusThree, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(ipMinusOne, Stream.VALUES),
                        new Reference(ipMinusTwo, Stream.VALUES),
                        new Reference(ipMinusThree, Stream.VALUES)));
        return new ProjectOperator(allocator, plan, primitiveRegistry, source);
    }

    private static Operator topGroupedCountSumAvg(
            Allocator allocator,
            Operator source,
            int[] groupColumns,
            int sumColumn,
            int avgColumn,
            OperatorCpuProfile profile)
    {
        Operator aggregated = profiled(profile, "topGrouped.aggregate", new GroupedAggregationOperator(
                allocator,
                Arrays.stream(groupColumns).boxed().toList(),
                List.of(new CountAll(), new Sum(sumColumn), new Avg(avgColumn)),
                source));
        return profiled(profile, "topGrouped.topn", new TopNOperator(allocator, 10, groupColumns.length, aggregated));
    }

    private static Operator sqlShapedTopGroupedCountSumAvg(
            Allocator allocator,
            PrimitiveRegistry primitiveRegistry,
            Path input,
            String firstKey,
            boolean filterSearchPhrase,
            OperatorCpuProfile profile)
    {
        try {
            NitroParquetScanResources scanResources = NitroParquetScanResources.createDefault(
                    ParquetArenaPolicy.shared(),
                    ParquetScanBatchPolicy.adaptiveHostBoundaryDefaults());
            List<NitroParquetBatchSource.Split> splits = parquetSplits(input, 120L * 1024 * 1024);
            List<String> columns = filterSearchPhrase
                    ? List.of(firstKey, "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase")
                    : List.of(firstKey, "ClientIP", "IsRefresh", "ResolutionWidth");
            Schema scanSchema = SCHEMAS.parquet(splits.getFirst().path(), columns);
            List<org.weakref.nitro.core.type.Field> exchangeFields = new ArrayList<>(6);
            exchangeFields.add(scanSchema.field(0));
            exchangeFields.add(scanSchema.field(1));
            exchangeFields.addAll(Schema.unspecified(4).fields());
            Schema exchangeSchema = new Schema(exchangeFields);
            int driverCount = Math.min(8, splits.size());
            List<List<NitroParquetBatchSource.Split>> driverAssignments = new ArrayList<>(driverCount);
            for (int driver = 0; driver < driverCount; driver++) {
                driverAssignments.add(new ArrayList<>());
            }
            for (int split = 0; split < splits.size(); split++) {
                driverAssignments.get(split % driverCount).add(splits.get(split));
            }

            List<Supplier<Operator>> partials = new ArrayList<>(driverCount);
            for (List<NitroParquetBatchSource.Split> assignment : driverAssignments) {
                List<NitroParquetBatchSource.Split> driverSplits = List.copyOf(assignment);
                partials.add(() -> {
                    Allocator splitAllocator = new Allocator(allocator.resourcesOwner());
                    Operator partial = null;
                    try {
                        Operator source = profiled(profile, "topGrouped.driver.scan", clickBenchSplitsScan(scanResources, splitAllocator, driverSplits, columns));
                        if (filterSearchPhrase) {
                            source = filter(splitAllocator, primitiveRegistry, source, notEqualUtf8(4, ""));
                            source = projectInputs(splitAllocator, new PrimitiveRegistry(), source, 0, 1, 2, 3);
                        }
                        partial = profiled(profile, "topGrouped.driver.partial", new AdaptiveSqlPartialAggregationOperator(
                                splitAllocator,
                                source,
                                1,
                                new int[0],
                                null,
                                List.of(0, 1),
                                () -> List.of(new CountAll(), new Sum(2), new Sum(3), new CountColumn(3)),
                                SQL_PARTIAL_AGGREGATION_MEMORY_BYTES,
                                SQL_PARTIAL_AGGREGATION_UNIQUE_ROWS_RATIO,
                                false,
                                Integer.MAX_VALUE));
                        return new AllocatorOwnedOperator(splitAllocator, partial);
                    }
                    catch (RuntimeException | Error failure) {
                        if (partial != null) {
                            partial.close();
                        }
                        splitAllocator.close();
                        throw failure;
                    }
                });
            }

            Operator merged = profiled(profile, "topGrouped.final", new SqlStageAggregationOperator(
                    allocator,
                    new LazyUnionAllOperator(exchangeSchema, partials),
                    driverCount,
                    new int[] {0, 1},
                    List.of(SqlStageAggregationOperator.aggregate(
                            List.of(0, 1),
                            () -> List.of(new Sum(2), new Sum(3), new Sum(4), new Sum(5))))));

            Variable average = new Variable(0);
            EvaluationPlan averages = new EvaluationPlan(
                    List.of(new Assignment(average, new Call("divide_i64_to_f64", List.of(
                            new Reference(new Input(4), Stream.VALUES),
                            new Reference(new Input(5), Stream.VALUES))), AllMask.ALL)),
                    List.of(
                            new Reference(new Input(0), Stream.VALUES),
                            new Reference(new Input(1), Stream.VALUES),
                            new Reference(new Input(2), Stream.VALUES),
                            new Reference(new Input(3), Stream.VALUES),
                            new Reference(average, Stream.VALUES)));
            Operator projected = profiled(profile, "topGrouped.average", new ProjectOperator(allocator, averages, primitiveRegistry, merged));
            return profiled(profile, "topGrouped.topn", new TopNOperator(allocator, 10, 2, projected));
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list ClickBench splits for " + input, exception);
        }
    }

    private static Operator clickBenchSplitsScan(
            NitroParquetScanResources resources,
            Allocator allocator,
            List<NitroParquetBatchSource.Split> splits,
            List<String> columns)
    {
        Schema schema = SCHEMAS.parquet(splits.getFirst().path(), columns);
        BatchSource source = NitroParquetBatchSource.forSplits(resources, allocator, splits, schema);
        return new BatchSourceOperator(
                source,
                new ColumnViewSourceOperatorIngress(
                        schema,
                        new AllocatedSelectionOperatorIngress(
                                allocator,
                                new Allocator.Context("ClickBenchSplitScanIngress")),
                        new LongDomainRuntimeFilterSourceIngress(),
                        VectorColumnViewOperatorIngress::new));
    }

    private static FilterSpec notEqualI64(int inputIndex, long constant)
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(equals, new Call("eq", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))));
    }

    private static FilterSpec equalI64(int inputIndex, long constant)
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(equals, new Call("eq", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(equals, Stream.VALUES)));
    }

    private static FilterSpec notEqualUtf8(int inputIndex, String constant)
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(equals, new Call("eq_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))));
    }

    private static FilterSpec equalTo(int inputIndex, long constant)
    {
        return equalI64(inputIndex, constant);
    }

    private static FilterSpec notEqualTo(int inputIndex, long constant)
    {
        return notEqualI64(inputIndex, constant);
    }

    private static FilterSpec containsUtf8(int inputIndex, String needle)
    {
        Variable literal = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(needle), AllMask.ALL),
                new Assignment(contains, new Call("contains_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(contains, Stream.VALUES)));
    }

    private static FilterSpec notContainsUtf8(int inputIndex, String needle)
    {
        Variable literal = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(needle), AllMask.ALL),
                new Assignment(contains, new Call("contains_utf8", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new NotMask(new ReferenceMask(new Reference(contains, Stream.VALUES))));
    }

    private static FilterSpec greaterThan(int inputIndex, long constant)
    {
        Variable literal = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(lessThan, new Call("lt", List.of(
                        new Reference(literal, Stream.VALUES),
                        new Reference(new Input(inputIndex), Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(lessThan, Stream.VALUES)));
    }

    private static FilterSpec and(FilterSpec first, FilterSpec second, FilterSpec... rest)
    {
        FilterSpec result = combineAnd(first, second);
        for (FilterSpec filterSpec : rest) {
            result = combineAnd(result, filterSpec);
        }
        return result;
    }

    private static FilterSpec counterAndJulyEventDateFilter(Path file, int counterIndex, int eventDateIndex, int isRefreshIndex)
    {
        return counterAndEventDateFilter(file, counterIndex, eventDateIndex, isRefreshIndex, LocalDate.of(2013, 7, 1), LocalDate.of(2013, 8, 1));
    }

    private static FilterSpec counterAndEventDateFilter(Path file, int counterIndex, int eventDateIndex, int isRefreshIndex, LocalDate inclusiveLowerBound, LocalDate exclusiveUpperBound)
    {
        return and(
                equalTo(counterIndex, 62),
                and(
                        equalTo(isRefreshIndex, 0),
                        dateRange(file, eventDateIndex, inclusiveLowerBound, exclusiveUpperBound)));
    }

    private static FilterSpec timeRange(int inputIndex, long inclusiveLowerBound, long exclusiveUpperBound)
    {
        FilterSpec lowerBound = greaterThan(inputIndex, inclusiveLowerBound - 1);
        FilterSpec upperBound = lessThan(inputIndex, exclusiveUpperBound);
        return and(lowerBound, upperBound);
    }

    private static FilterSpec dateRange(Path file, int inputIndex, LocalDate inclusiveLowerBound, LocalDate exclusiveUpperBound)
    {
        return timeRange(inputIndex, eventDateLiteral(file, inclusiveLowerBound), eventDateLiteral(file, exclusiveUpperBound));
    }

    static int eventDateLiteral(Path file, LocalDate date)
    {
        if (eventDateUsesEpochDays(file)) {
            return toIntExact(date.toEpochDay());
        }
        return (date.getYear() * 10_000) + (date.getMonthValue() * 100) + date.getDayOfMonth();
    }

    private static boolean eventDateUsesEpochDays(Path file)
    {
        if (Files.isDirectory(file)) {
            try {
                file = parquetFiles(file).getFirst();
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to list ClickBench parquet files in " + file, exception);
            }
        }
        try (ParquetFile parquet = ParquetFile.open(file)) {
            ParquetFile.PrimitiveField field = parquet.primitiveField("EventDate");
            return field.integer() && !field.integerSigned() && field.integerBitWidth() == 16;
        }
    }

    private static FilterSpec lessThan(int inputIndex, long constant)
    {
        Variable literal = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(constant), AllMask.ALL),
                new Assignment(lessThan, new Call("lt", List.of(
                        new Reference(new Input(inputIndex), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        return new FilterSpec(plan, new ReferenceMask(new Reference(lessThan, Stream.VALUES)));
    }

    private static FilterSpec trafficSourceIn(int inputIndex)
    {
        return combineOr(equalTo(inputIndex, -1), equalTo(inputIndex, 6));
    }

    private static FilterSpec combineAnd(FilterSpec left, FilterSpec right)
    {
        return combineMasks(left, right, true);
    }

    private static FilterSpec combineOr(FilterSpec left, FilterSpec right)
    {
        return combineMasks(left, right, false);
    }

    private static FilterSpec combineMasks(FilterSpec left, FilterSpec right, boolean conjunction)
    {
        FilterSpec remappedLeft = remap(left, 0);
        FilterSpec remappedRight = remap(right, maxVariableId(remappedLeft.plan()) + 1);
        java.util.ArrayList<Assignment> assignments = new java.util.ArrayList<>();
        assignments.addAll(remappedLeft.plan().assignments());
        assignments.addAll(remappedRight.plan().assignments());
        MaskExpression predicate = conjunction
                ? new AndMask(List.of(remappedLeft.predicate(), remappedRight.predicate()))
                : new OrMask(List.of(remappedLeft.predicate(), remappedRight.predicate()));
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), predicate);
    }

    private static int maxVariableId(EvaluationPlan... plans)
    {
        int max = -1;
        for (EvaluationPlan plan : plans) {
            for (Assignment assignment : plan.assignments()) {
                max = Math.max(max, assignment.output().id());
            }
        }
        return max;
    }

    private static FilterSpec remap(FilterSpec filterSpec, int variableOffset)
    {
        if (variableOffset == 0) {
            return filterSpec;
        }
        EvaluationPlan plan = filterSpec.plan();
        List<Assignment> assignments = plan.assignments().stream()
                .map(assignment -> new Assignment(
                        new Variable(assignment.output().id() + variableOffset),
                        remapOperation(assignment.operation(), variableOffset),
                        remapMaskExpression(assignment.mask(), variableOffset)))
                .toList();
        return new FilterSpec(new EvaluationPlan(assignments, List.of()), remapMaskExpression(filterSpec.predicate(), variableOffset));
    }

    private static Operation remapOperation(Operation operation, int variableOffset)
    {
        return switch (operation) {
            case Literal literal -> literal;
            case Call call -> new Call(
                    call.name(),
                    call.arguments().stream()
                            .map(reference -> remapReference(reference, variableOffset))
                            .toList(),
                    call.resolvedCall());
            default -> throw new IllegalArgumentException("Unsupported filter operation for remap: " + operation.getClass().getSimpleName());
        };
    }

    private static MaskExpression remapMaskExpression(MaskExpression maskExpression, int variableOffset)
    {
        return switch (maskExpression) {
            case AllMask allMask -> allMask;
            case org.weakref.nitro.operator.evaluator.ir.LongDomainMask domain -> new org.weakref.nitro.operator.evaluator.ir.LongDomainMask(
                    remapReference(domain.input(), variableOffset),
                    domain.domain());
            case org.weakref.nitro.operator.evaluator.ir.RangeConstrainedAndMask range -> new org.weakref.nitro.operator.evaluator.ir.RangeConstrainedAndMask(
                    remapReference(range.input(), variableOffset),
                    range.lowerExclusive(),
                    range.upperExclusive(),
                    range.kernel(),
                    range.remainingTerms().stream().map(term -> remapMaskExpression(term, variableOffset)).toList(),
                    (AndMask) remapMaskExpression(range.fallback(), variableOffset));
            case ReferenceMask referenceMask -> new ReferenceMask(remapReference(referenceMask.reference(), variableOffset));
            case NotMask notMask -> new NotMask(remapMaskExpression(notMask.source(), variableOffset));
            case AndMask andMask -> new AndMask(andMask.terms().stream()
                    .map(term -> remapMaskExpression(term, variableOffset))
                    .toList());
            case OrMask orMask -> new OrMask(orMask.terms().stream()
                    .map(term -> remapMaskExpression(term, variableOffset))
                    .toList());
        };
    }

    private static Reference remapReference(Reference reference, int variableOffset)
    {
        return new Reference(remapProducer(reference.producer(), variableOffset), reference.stream());
    }

    private static Producer remapProducer(Producer producer, int variableOffset)
    {
        return switch (producer) {
            case Input input -> input;
            case Variable variable -> new Variable(variable.id() + variableOffset);
        };
    }

    private static List<HitRow> templateRows()
    {
        return List.of(
                new HitRow(100, "Google home", 1_372_636_800L, 20130701, 62, 1_000, 7, 1, "https://google.com", "https://www.google.com/", 0, 0, 1000, 0, "", -1, 0, "", 1280, 720, 0, 0, 0, QUERY41_REFERER_HASH, QUERY42_URL_HASH),
                new HitRow(101, "Google landing", 1_372_723_200L, 20130702, 62, 1_001, 7, 2, "https://example.com", "https://www.google.com/path", 0, 10, 1200, 0, "", 6, 0, "", 1280, 720, 0, 0, 0, QUERY41_REFERER_HASH, 10),
                new HitRow(102, "Phone page", 1_372_809_600L, 20130703, 62, 1_002, 8, 2, "https://example.com/page", "https://www.yahoo.com/a", 1, 10, 900, 1, "iphone", 2, 1, "phone", 1024, 768, 1, 0, 0, 20, 30),
                new HitRow(103, "Google Maps", 1_372_641_600L, 20130701, 62, 1_000, 7, 1, "https://google.com/maps", "https://www.google.com/maps", 0, 20, 800, 1, "iphone", -1, 1, "map", 1366, 768, 0, 0, 0, QUERY41_REFERER_HASH, QUERY42_URL_HASH),
                new HitRow(104, "Weather", 1_375_228_800L, 20130731, 62, 1_003, 9, 2, "https://yandex.ru", "https://www.example.com/abc", 0, 20, 700, 3, "pixel", 6, 2, "weather", 1440, 900, 1, 0, 0, QUERY41_REFERER_HASH, 40),
                new HitRow(105, "", 1_375_315_200L, 20130801, 7, 1_004, 9, 4, "", "", 0, 0, 640, 0, "", 0, 0, "", 800, 600, 0, 0, 1, 50, 60),
                new HitRow(106, "Google Search", 1_373_846_400L, 20130715, 62, 1_005, 9, 5, "https://google.com/search", "https://www.google.com/search?q=news", 0, 20, 600, 3, "pixel", 6, 3, "news", 1920, 1080, 1, 0, 0, QUERY41_REFERER_HASH, QUERY42_URL_HASH),
                new HitRow(107, "Search results", 1_373_932_800L, 20130716, 62, 1_006, 9, QUERY20_USER_ID, "https://google.com/search", "https://www.google.com/search?q=news2", 0, 0, 500, 3, "pixel", -1, 3, "news", 1920, 1080, 1, 0, 0, QUERY41_REFERER_HASH, QUERY42_URL_HASH));
    }

    private static final class AllocatorOwnedOperator
            implements Operator
    {
        private final Allocator allocator;
        private final Operator delegate;
        private boolean closed;

        private AllocatorOwnedOperator(Allocator allocator, Operator delegate)
        {
            this.allocator = allocator;
            this.delegate = delegate;
        }

        @Override
        public int outputCount()
        {
            return delegate.outputCount();
        }

        @Override
        public Schema outputSchema()
        {
            return delegate.outputSchema();
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public Batch next()
        {
            return delegate.next();
        }

        @Override
        public void constrain(Mask mask)
        {
            delegate.constrain(mask);
        }

        @Override
        public boolean supportsRetainedBatches()
        {
            return delegate.supportsRetainedBatches();
        }

        @Override
        public boolean supportsStableBatchBorrow()
        {
            return delegate.supportsStableBatchBorrow();
        }

        @Override
        public boolean supportsOpenBatchHasNext()
        {
            return delegate.supportsOpenBatchHasNext();
        }

        @Override
        public long exactOutputRows()
        {
            return delegate.exactOutputRows();
        }

        @Override
        public boolean supportsConstrainedReborrow()
        {
            return delegate.supportsConstrainedReborrow();
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            try {
                delegate.close();
            }
            finally {
                allocator.close();
            }
        }
    }

    /** Mirrors the host-driven TopN lifecycle used by the Trino source pipeline. */
    private static final class SessionTopNOperator
            implements Operator
    {
        private final BatchSource source;
        private final BatchFeedOperator feed;
        private final Operator pipeline;
        private final TopNSession session;
        private Batch output;
        private boolean prepared;
        private boolean closed;

        private SessionTopNOperator(
                Allocator allocator,
                int limit,
                int[] orderingColumns,
                boolean[] descending,
                BatchSource source,
                BatchFeedOperator feed,
                Operator pipeline)
        {
            this.source = source;
            this.feed = feed;
            this.pipeline = pipeline;
            session = new TopNSession(allocator, limit, orderingColumns, descending, pipeline.outputSchema());
        }

        @Override
        public int outputCount()
        {
            return pipeline.outputCount();
        }

        @Override
        public Schema outputSchema()
        {
            return session.outputSchema();
        }

        @Override
        public boolean hasNext()
        {
            prepare();
            return output != null;
        }

        @Override
        public Batch next()
        {
            prepare();
            if (output == null) {
                throw new IllegalStateException("TopN session has no output");
            }
            Batch result = output;
            output = null;
            return result;
        }

        @Override
        public void constrain(Mask mask)
        {
            prepare();
            if (output != null) {
                output.constrain(mask);
            }
        }

        private void prepare()
        {
            if (prepared) {
                return;
            }
            prepared = true;
            boolean finished = false;
            while (!finished) {
                switch (source.poll()) {
                    case SourcePoll.Ready(var sourceBatch) -> {
                        feed.addInput(sourceBatch);
                        while (pipeline.hasNext()) {
                            try (Batch batch = pipeline.next()) {
                                session.addInput(batch);
                            }
                        }
                        feed.finishInput();
                    }
                    case SourcePoll.Blocked _ -> throw new IllegalStateException("benchmark source unexpectedly blocked");
                    case SourcePoll.Finished _ -> finished = true;
                }
            }
            output = session.finish().orElse(null);
        }

        @Override
        public boolean supportsRetainedBatches()
        {
            return true;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (output != null) {
                output.close();
                output = null;
            }
            try {
                session.close();
            }
            finally {
                try {
                    pipeline.close();
                }
                finally {
                    source.close();
                }
            }
        }
    }

    private record HitRow(
            long watchId,
            String title,
            long eventTime,
            long eventDate,
            long counterId,
            long clientIp,
            long regionId,
            long userId,
            String url,
            String referer,
            long isRefresh,
            long advEngineId,
            long resolutionWidth,
            long mobilePhone,
            String mobilePhoneModel,
            long traficSourceId,
            long searchEngineId,
            String searchPhrase,
            long windowClientWidth,
            long windowClientHeight,
            long isLink,
            long isDownload,
            long dontCountHits,
            long refererHash,
            long urlHash)
    {
        private HitRow vary(int repeat)
        {
            return new HitRow(
                    watchId + repeat,
                    title,
                    eventTime + (repeat * 60L),
                    eventDate,
                    counterId,
                    clientIp + repeat,
                    regionId,
                    userId + (repeat * 10L),
                    url,
                    referer,
                    isRefresh,
                    advEngineId,
                    resolutionWidth + repeat,
                    mobilePhone,
                    mobilePhoneModel,
                    traficSourceId,
                    searchEngineId,
                    searchPhrase,
                    windowClientWidth,
                    windowClientHeight,
                    isLink,
                    isDownload,
                    dontCountHits,
                    refererHash,
                    urlHash);
        }
    }

    private record FilterSpec(EvaluationPlan plan, MaskExpression predicate)
    {
    }
}
