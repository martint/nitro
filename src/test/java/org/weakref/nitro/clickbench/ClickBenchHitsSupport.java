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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.weakref.nitro.benchmark.BenchmarkSchemaRegistry;
import org.weakref.nitro.benchmark.BenchmarkTypeRegistry;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.builtin.CastI64ToI64;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.MarkDistinctMarkerOperator;
import org.weakref.nitro.operator.MarkDistinctOperator;
import org.weakref.nitro.operator.MaterializeOperator;
import org.weakref.nitro.operator.OffsetOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SqlStageAggregationOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.UnionAllOperator;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.CountAll;
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
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.compatibility.NativeSourceOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;
import org.weakref.nitro.operator.source.compatibility.parquet.NitroParquetScanOperator;
import org.weakref.nitro.parquet.NitroParquetScanResources;
import org.weakref.nitro.parquet.ParquetArenaPolicy;
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

import static java.lang.Math.toIntExact;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public final class ClickBenchHitsSupport
{
    private static final BenchmarkSchemaRegistry SCHEMAS = new BenchmarkSchemaRegistry(new BenchmarkTypeRegistry());
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
        MessageType schema = Types.buildMessage()
                .required(INT64).named("WatchID")
                .required(BINARY).as(stringType()).named("Title")
                .required(INT64).named("EventTime")
                .required(INT32).named("EventDate")
                .required(INT32).named("CounterID")
                .required(INT32).named("ClientIP")
                .required(INT32).named("RegionID")
                .required(INT64).named("UserID")
                .required(BINARY).as(stringType()).named("URL")
                .required(BINARY).as(stringType()).named("Referer")
                .required(INT32).named("IsRefresh")
                .required(INT32).named("AdvEngineID")
                .required(INT32).named("ResolutionWidth")
                .required(INT32).named("MobilePhone")
                .required(BINARY).as(stringType()).named("MobilePhoneModel")
                .required(INT32).named("TraficSourceID")
                .required(INT32).named("SearchEngineID")
                .required(BINARY).as(stringType()).named("SearchPhrase")
                .required(INT32).named("WindowClientWidth")
                .required(INT32).named("WindowClientHeight")
                .required(INT32).named("IsLink")
                .required(INT32).named("IsDownload")
                .required(INT32).named("DontCountHits")
                .required(INT64).named("RefererHash")
                .required(INT64).named("URLHash")
                .named("hits");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        List<HitRow> templateRows = templateRows();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .build()) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                HitRow row = templateRows.get(rowIndex % templateRows.size()).vary(rowIndex / templateRows.size());
                writer.write(groups.newGroup()
                        .append("WatchID", row.watchId())
                        .append("Title", row.title())
                        .append("EventTime", row.eventTime())
                        .append("EventDate", (int) row.eventDate())
                        .append("CounterID", (int) row.counterId())
                        .append("ClientIP", (int) row.clientIp())
                        .append("RegionID", (int) row.regionId())
                        .append("UserID", row.userId())
                        .append("URL", row.url())
                        .append("Referer", row.referer())
                        .append("IsRefresh", (int) row.isRefresh())
                        .append("AdvEngineID", (int) row.advEngineId())
                        .append("ResolutionWidth", (int) row.resolutionWidth())
                        .append("MobilePhone", (int) row.mobilePhone())
                        .append("MobilePhoneModel", row.mobilePhoneModel())
                        .append("TraficSourceID", (int) row.traficSourceId())
                        .append("SearchEngineID", (int) row.searchEngineId())
                        .append("SearchPhrase", row.searchPhrase())
                        .append("WindowClientWidth", (int) row.windowClientWidth())
                        .append("WindowClientHeight", (int) row.windowClientHeight())
                        .append("IsLink", (int) row.isLink())
                        .append("IsDownload", (int) row.isDownload())
                        .append("DontCountHits", (int) row.dontCountHits())
                        .append("RefererHash", row.refererHash())
                        .append("URLHash", row.urlHash()));
            }
        }
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

            List<Path> splits = Files.isDirectory(file) ? parquetFiles(file) : List.of(file);
            List<Operator> partials = new ArrayList<>(splits.size());
            for (Path split : splits) {
                Operator filtered = filter(
                        allocator,
                        primitiveRegistry,
                        clickBenchScan(scanResources, allocator, split, columns.toArray(String[]::new)),
                        containsUtf8(urlIndex, "google"));
                partials.add(new TopNOperator(allocator, 10, sortChannels, descending, filtered));
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
        int sumCount = 90;
        Operator projected = projectResolutionWidthOffsets(allocator, primitiveRegistry, clickBenchScan(allocator, file, "ResolutionWidth"), sumCount);
        return new AggregationOperator(allocator, offsetSums(sumCount), projected);
    }

    public static Operator query31(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"), notEqualUtf8(4, ""));
        Operator projected = projectInputs(allocator, new PrimitiveRegistry(), filtered, 0, 1, 2, 3);
        return topGroupedCountSumAvg(allocator, projected, new int[] {0, 1}, 2, 3, null);
    }

    public static Operator query32(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("WatchID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"), notEqualUtf8(4, ""));
        Operator projected = projectInputs(allocator, new PrimitiveRegistry(), filtered, 0, 1, 2, 3);
        return topGroupedCountSumAvg(allocator, projected, new int[] {0, 1}, 2, 3, null);
    }

    public static Operator query33(Allocator allocator, Path file)
    {
        return query33(allocator, file, null);
    }

    static Operator query33(Allocator allocator, Path file, OperatorCpuProfile profile)
    {
        Operator source = clickBenchScan(allocator, file, "WatchID", "ClientIP", "IsRefresh", "ResolutionWidth");
        return topGroupedCountSumAvg(allocator, source, new int[] {0, 1}, 2, 3, profile);
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
        Operator ordered = profiled(profile, "q40.topn", new TopNOperator(allocator, 1_010, 5, aggregated));
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
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            return reader.getFooter().getFileMetaData().getSchema().getFields().stream()
                    .map(field -> field.getName())
                    .toList();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect ClickBench schema for " + file, exception);
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
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            var field = reader.getFooter().getFileMetaData().getSchema().getType("EventDate").asPrimitiveType();
            return field.getLogicalTypeAnnotation() instanceof org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation logicalType
                    && !logicalType.isSigned()
                    && logicalType.getBitWidth() == 16;
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect ClickBench EventDate encoding for " + file, exception);
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
