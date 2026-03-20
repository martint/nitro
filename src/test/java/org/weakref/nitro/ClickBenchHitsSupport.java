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
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ParquetScanOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.First;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
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
import java.nio.file.Path;
import java.util.List;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

final class ClickBenchHitsSupport
{
    private ClickBenchHitsSupport() {}

    public static final int DEFAULT_ROW_COUNT = 7;
    public static final int BENCHMARK_ROW_COUNT = 100_000;

    public static Path writeHitsFixture(Path file, int rowCount)
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .required(INT64).named("AdvEngineID")
                .required(INT64).named("ResolutionWidth")
                .required(INT64).named("UserID")
                .required(INT64).named("EventDate")
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
                        .append("AdvEngineID", row.advEngineId())
                        .append("ResolutionWidth", row.resolutionWidth())
                        .append("UserID", row.userId())
                        .append("EventDate", row.eventDate())
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
                new ParquetScanOperator(allocator, file, List.of("UserID")));
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
                new ParquetScanOperator(allocator, file, List.of("EventDate")));
    }

    public static Operator query8GroupByAdvEngineId(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file)
    {
        FilterSpec predicate = notEqualI64(0, 0);
        Operator scan = new ParquetScanOperator(allocator, file, List.of("AdvEngineID"));
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
                List.of(new First(1), new CountAll()),
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

    private static Operator filter(Allocator allocator, PrimitiveRegistry primitiveRegistry, Path file, List<String> columns, FilterSpec filterSpec)
    {
        return new FilterOperator(
                new ParquetScanOperator(allocator, file, columns),
                filterSpec.plan(),
                primitiveRegistry,
                filterSpec.predicate(),
                allocator);
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
                new HitRow(20, 800, 3, 20130701, "https://google.com/maps", "map"),
                new HitRow(20, 700, 1, 20130731, "https://yandex.ru", "weather"),
                new HitRow(0, 640, 4, 20130801, "", ""),
                new HitRow(20, 600, 5, 20130715, "https://google.com/search", "news"));
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

    private record FilterSpec(EvaluationPlan plan, MaskExpression predicate)
    {
    }
}
