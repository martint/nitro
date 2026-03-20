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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ParquetScanOperator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TrinoParquetScanOperator;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpression;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

final class ClickBenchHitsSupport
{
    static final String CLICKBENCH_HITS_PATH_PROPERTY = "nitro.clickbench.hits.path";
    static final String CLICKBENCH_PARQUET_READER_PROPERTY = "nitro.clickbench.parquet.reader";
    static final long QUERY20_USER_ID = 435_090_932_899_640_449L;
    private static final List<String> ALL_HITS_COLUMNS = List.of(
            "AdvEngineID",
            "ResolutionWidth",
            "UserID",
            "EventDate",
            "URL",
            "SearchPhrase");

    private ClickBenchHitsSupport() {}

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
                .required(INT32).named("AdvEngineID")
                .required(INT32).named("ResolutionWidth")
                .required(INT64).named("UserID")
                .required(INT32).named("EventDate")
                .required(BINARY).as(stringType()).named("URL")
                .required(BINARY).as(stringType()).named("SearchPhrase")
                .named("hits");

        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        List<HitRow> templateRows = templateRows();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .build()) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                HitRow row = templateRows.get(rowIndex % templateRows.size()).vary(rowIndex / templateRows.size());
                writer.write(groups.newGroup()
                        .append("AdvEngineID", (int) row.advEngineId())
                        .append("ResolutionWidth", (int) row.resolutionWidth())
                        .append("UserID", row.userId())
                        .append("EventDate", (int) row.eventDate())
                        .append("URL", row.url())
                        .append("SearchPhrase", row.searchPhrase()));
            }
        }
        return file;
    }

    public static Operator query1CountAll(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                clickBenchScan(allocator, file));
    }

    public static Operator query0SelectAll(Allocator allocator, Path file)
    {
        return clickBenchScan(allocator, file, ALL_HITS_COLUMNS.toArray(String[]::new));
    }

    public static Operator query2CountNonZeroAdvEngineId(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                filter(allocator, primitiveRegistry, file, List.of("AdvEngineID"), notEqualI64(0, 0)));
    }

    public static Operator query7MinAndMaxEventDate(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new Min(0), new Max(0)),
                clickBenchScan(allocator, file, "EventDate"));
    }

    public static Operator query3SumAdvEngineAndAvgResolutionWidth(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountAll(), new Avg(1)),
                clickBenchScan(allocator, file, "AdvEngineID", "ResolutionWidth"));
    }

    public static Operator query4AvgUserId(Allocator allocator, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new Avg(0)),
                clickBenchScan(allocator, file, "UserID"));
    }

    public static Operator query5CountDistinctUserId(Allocator allocator, Path file)
    {
        return countDistinct(allocator, clickBenchScan(allocator, file, "UserID"));
    }

    public static Operator query6CountDistinctSearchPhrase(Allocator allocator, Path file)
    {
        return countDistinct(allocator, clickBenchScan(allocator, file, "SearchPhrase"));
    }

    public static Operator query8GroupByAdvEngineId(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        FilterSpec predicate = notEqualI64(0, 0);
        Operator scan = clickBenchScan(allocator, file, "AdvEngineID");
        Operator filtered = new FilterOperator(
                scan,
                predicate.plan(),
                primitiveRegistry,
                predicate.predicate(),
                allocator);
        Operator grouped = new GroupOperator(allocator, 0, filtered);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                grouped);
        return new TopNOperator(allocator, 10, 1, aggregated);
    }

    public static Operator query21CountUrlsContainingGoogle(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                filter(allocator, primitiveRegistry, file, List.of("URL"), containsUtf8(0, "google")));
    }

    public static Operator query13TopSearchPhrases(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        return topUtf8Counts(allocator, primitiveRegistry, file, "SearchPhrase", true);
    }

    public static Operator query16TopUserIds(Allocator allocator, Path file)
    {
        return topIntegerCounts(allocator, file, "UserID");
    }

    public static Operator query20SearchPhrasesForUserId(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("SearchPhrase", "UserID"), equalI64(1, QUERY20_USER_ID));
        Operator projected = projectInputs(allocator, new PrimitiveRegistry(), filtered, 0);
        return new LimitOperator(allocator, 10, projected);
    }

    public static Operator query26SearchPhrasesOrderedAscending(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator filtered = filter(allocator, primitiveRegistry, file, List.of("SearchPhrase"), notEqualUtf8(0, ""));
        return new TopNOperator(allocator, 10, 0, false, filtered);
    }

    public static Operator query30SumResolutionWidthPlusOffsets(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        Operator projected = projectResolutionWidthOffsets(allocator, primitiveRegistry, clickBenchScan(allocator, file, "ResolutionWidth"), 10);
        return new AggregationOperator(
                allocator,
                List.of(
                        new Sum(0),
                        new Sum(1),
                        new Sum(2),
                        new Sum(3),
                        new Sum(4),
                        new Sum(5),
                        new Sum(6),
                        new Sum(7),
                        new Sum(8),
                        new Sum(9)),
                projected);
    }

    public static Operator query34TopUrls(Allocator allocator, Path file)
    {
        return topUtf8Counts(allocator, null, file, "URL", false);
    }

    public static Operator query35ConstantAndTopUrls(Allocator allocator, Path file)
    {
        Operator topUrls = topUtf8Counts(allocator, null, file, "URL", false);
        return prependConstant(allocator, topUrls, 1);
    }

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, List<String> columns, FilterSpec filterSpec)
    {
        return new FilterOperator(
                clickBenchScan(allocator, file, columns.toArray(String[]::new)),
                filterSpec.plan(),
                primitiveRegistry,
                filterSpec.predicate(),
                allocator);
    }

    private static Operator clickBenchScan(Allocator allocator, Path file, String... columns)
    {
        Function<Path, Operator> operatorFactory = switch (configuredReader()) {
            case APACHE -> path -> new ParquetScanOperator(allocator, path, List.of(columns));
            case TRINO -> path -> new TrinoParquetScanOperator(allocator, path, List.of(columns));
        };
        try {
            if (Files.isDirectory(file)) {
                return new MultiFileScanOperator(parquetFiles(file), columns.length, operatorFactory);
            }
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect ClickBench hits file: " + file, exception);
        }
        return operatorFactory.apply(file);
    }

    private static ReaderKind configuredReader()
    {
        return switch (System.getProperty(CLICKBENCH_PARQUET_READER_PROPERTY, "apache").strip().toLowerCase()) {
            case "apache" -> ReaderKind.APACHE;
            case "trino" -> ReaderKind.TRINO;
            default -> throw new IllegalArgumentException("Unsupported ClickBench Parquet reader: " + System.getProperty(CLICKBENCH_PARQUET_READER_PROPERTY));
        };
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

    private static Operator countDistinct(Allocator allocator, Operator source)
    {
        Operator grouped = new GroupOperator(allocator, 0, source);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(new CountAll()),
                grouped);
        return new AggregationOperator(allocator, List.of(new CountAll()), aggregated);
    }

    private static Operator topIntegerCounts(Allocator allocator, Path file, String column)
    {
        Operator grouped = new GroupOperator(allocator, 0, clickBenchScan(allocator, file, column));
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                grouped);
        return new TopNOperator(allocator, 10, 1, aggregated);
    }

    private static Operator topUtf8Counts(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, String column, boolean filterEmpty)
    {
        Operator source = clickBenchScan(allocator, file, column);
        if (filterEmpty) {
            source = new FilterOperator(
                    source,
                    notEqualUtf8(0, "").plan(),
                    primitiveRegistry,
                    notEqualUtf8(0, "").predicate(),
                    allocator);
        }
        Operator grouped = new GroupOperator(allocator, 0, source);
        Operator aggregated = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                grouped);
        return new TopNOperator(allocator, 10, 1, aggregated);
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
        for (int offset = 1; offset <= count; offset++) {
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

    private static List<HitRow> templateRows()
    {
        return List.of(
                new HitRow(0, 1000, 1, 20130701, "https://google.com", ""),
                new HitRow(10, 1200, 2, 20130702, "https://example.com", ""),
                new HitRow(10, 900, 2, 20130703, "https://example.com/page", "phone"),
                new HitRow(20, 800, 1, 20130701, "https://google.com/maps", "map"),
                new HitRow(20, 700, 2, 20130731, "https://yandex.ru", "weather"),
                new HitRow(0, 640, 4, 20130801, "", ""),
                new HitRow(20, 600, 5, 20130715, "https://google.com/search", "news"),
                new HitRow(0, 500, QUERY20_USER_ID, 20130716, "https://google.com/search", "news"));
    }

    private record HitRow(long advEngineId, long resolutionWidth, long userId, long eventDate, String url, String searchPhrase)
    {
        private HitRow vary(int repeat)
        {
            return new HitRow(
                    advEngineId,
                    resolutionWidth + repeat,
                    userId + (repeat * 10L),
                    eventDate,
                    url,
                    searchPhrase);
        }
    }

    private static final class MultiFileScanOperator
            implements Operator
    {
        private final List<Path> files;
        private final int outputCount;
        private final Function<Path, Operator> operatorFactory;

        private int fileIndex;
        private Operator current;

        private MultiFileScanOperator(List<Path> files, int outputCount, Function<Path, Operator> operatorFactory)
        {
            this.files = List.copyOf(files);
            this.outputCount = outputCount;
            this.operatorFactory = operatorFactory;
        }

        @Override
        public int outputCount()
        {
            return outputCount;
        }

        @Override
        public boolean hasNext()
        {
            advanceIfNecessary();
            return current != null && current.hasNext();
        }

        @Override
        public org.weakref.nitro.operator.Batch next()
        {
            if (!hasNext()) {
                throw new IllegalStateException("No more Parquet rows");
            }
            return current.next();
        }

        @Override
        public void constrain(Mask mask)
        {
            if (current != null) {
                current.constrain(mask);
            }
        }

        @Override
        public void close()
        {
            closeCurrent();
        }

        private void advanceIfNecessary()
        {
            while ((current == null || !current.hasNext()) && fileIndex < files.size()) {
                closeCurrent();
                current = operatorFactory.apply(files.get(fileIndex++));
            }
        }

        private void closeCurrent()
        {
            if (current == null) {
                return;
            }
            current.close();
            current = null;
        }
    }

    private record FilterSpec(EvaluationPlan plan, MaskExpression predicate)
    {
    }

    private enum ReaderKind
    {
        APACHE,
        TRINO,
    }
}
