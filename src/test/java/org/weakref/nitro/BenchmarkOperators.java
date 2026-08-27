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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(5)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkOperators
{
    private static final int UTF8_GROUP_ROWS = 100_000;
    private static final int UTF8_JOIN_OUTER_ROWS = 100_000;
    private static final int UTF8_JOIN_DISTINCT_KEYS = 1_024;
    private static final int UTF8_JOIN_SECONDARY_DISTINCT_KEYS = 32;
    private static final int UTF8_JOIN_NULL_EVERY = 7;
    private static final int UTF8_JOIN_WIDE_PAYLOAD_LENGTH = 128;

    private final Allocator allocator = new Allocator(EngineResources.createDefault());
    private final PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
    private TableOperator.Page groupUtf8Page;
    private TableOperator.Page outerJoinUtf8Page;
    private TableOperator.Page innerJoinUtf8Page;
    private TableOperator.Page outerJoinUtf8NullablePage;
    private TableOperator.Page innerJoinUtf8NullablePage;
    private TableOperator.Page outerJoinUtf8MultiKeyPage;
    private TableOperator.Page innerJoinUtf8MultiKeyPage;
    private TableOperator.Page outerJoinUtf8PayloadPage;
    private TableOperator.Page innerJoinUtf8PayloadPage;

    @Setup
    public void setup()
    {
        groupUtf8Page = utf8Page(UTF8_GROUP_ROWS, UTF8_JOIN_DISTINCT_KEYS);
        outerJoinUtf8Page = utf8Page(UTF8_JOIN_OUTER_ROWS, UTF8_JOIN_DISTINCT_KEYS);
        innerJoinUtf8Page = utf8Page(UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_DISTINCT_KEYS);
        outerJoinUtf8NullablePage = utf8NullablePage(UTF8_JOIN_OUTER_ROWS, UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_NULL_EVERY);
        innerJoinUtf8NullablePage = utf8NullablePage(UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_NULL_EVERY + 2);
        outerJoinUtf8MultiKeyPage = utf8MultiKeyPage(UTF8_JOIN_OUTER_ROWS, UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_SECONDARY_DISTINCT_KEYS);
        innerJoinUtf8MultiKeyPage = utf8MultiKeyPage(UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_SECONDARY_DISTINCT_KEYS);
        outerJoinUtf8PayloadPage = utf8PayloadPage(UTF8_JOIN_OUTER_ROWS, UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_WIDE_PAYLOAD_LENGTH);
        innerJoinUtf8PayloadPage = utf8PayloadPage(UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_DISTINCT_KEYS, UTF8_JOIN_WIDE_PAYLOAD_LENGTH);
    }

    @Benchmark
    @OperationsPerInvocation(1_000_000_000)
    public void aggregationCountAll()
    {
        Operator operator = new AggregationOperator(
                allocator,
                List.of(new CountAll()),
                new GeneratorOperator(
                        allocator,
                        1_000_000_000L,
                        List.of(new SequenceGenerator(0))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(1_000_000_000)
    public void aggregationCount()
    {
        Operator operator = new AggregationOperator(
                allocator,
                List.of(new CountColumn(0)),
                new GeneratorOperator(
                        allocator,
                        1_000_000_000L,
                        List.of(new SequenceGenerator(0))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(100_000_000)
    public void groupBy()
    {
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(new CountAll()),
                new GeneratorOperator(
                        allocator,
                        100_000_000L,
                        List.of(new SequenceGenerator(0, 10))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(100_000)
    public void group()
    {
        Operator operator = new GroupOperator(
                allocator,
                0,
                new GeneratorOperator(
                        allocator,
                        100_000L,
                        List.of(new SequenceGenerator(0, 10000))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(UTF8_GROUP_ROWS)
    public void groupUtf8RepeatedKeys()
    {
        Operator operator = new GroupOperator(
                allocator,
                0,
                new TableOperator(1, List.of(groupUtf8Page)));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(100_000)
    public void project()
    {
        Variable projected = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        projected,
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(projected, Stream.VALUES)));

        Operator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new GeneratorOperator(
                        allocator,
                        100_000L,
                        List.of(new SequenceGenerator(0, 10000))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(25_000 * 25_000)
    public void nestedLoopJoin()
    {
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new GeneratorOperator(
                        allocator,
                        25_000L,
                        List.of(new SequenceGenerator(100))),
                new GeneratorOperator(
                        allocator,
                        25_000L,
                        List.of(new SequenceGenerator(100))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(3_000 * 500_000)
    public void nestedLoopJoinSmallVsLarge()
    {
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new GeneratorOperator(
                        allocator,
                        3_000L,
                        List.of(new SequenceGenerator(100))),
                new GeneratorOperator(
                        allocator,
                        500_000L,
                        List.of(new SequenceGenerator(100))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(3_000 * 500_000)
    public void nestedLoopJoinLargeVsSmall()
    {
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new GeneratorOperator(
                        allocator,
                        500_000L,
                        List.of(new SequenceGenerator(100))),
                new GeneratorOperator(
                        allocator,
                        3_000,
                        List.of(new SequenceGenerator(100))));

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(UTF8_JOIN_OUTER_ROWS + UTF8_JOIN_DISTINCT_KEYS)
    public void hashJoinUtf8RepeatedKeys()
    {
        Operator operator = new HashJoinOperator(
                allocator,
                new TableOperator(1, List.of(outerJoinUtf8Page)),
                0,
                new TableOperator(1, List.of(innerJoinUtf8Page)),
                0);

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(UTF8_JOIN_OUTER_ROWS + UTF8_JOIN_DISTINCT_KEYS)
    public void nestedLoopEquiJoinUtf8RepeatedKeys()
    {
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new TableOperator(1, List.of(outerJoinUtf8Page)),
                0,
                new TableOperator(1, List.of(innerJoinUtf8Page)),
                0);

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(UTF8_JOIN_OUTER_ROWS + UTF8_JOIN_DISTINCT_KEYS)
    public void hashJoinUtf8NullableKeys()
    {
        Operator operator = new HashJoinOperator(
                allocator,
                new TableOperator(1, List.of(outerJoinUtf8NullablePage)),
                0,
                new TableOperator(1, List.of(innerJoinUtf8NullablePage)),
                0);

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(UTF8_JOIN_OUTER_ROWS + UTF8_JOIN_DISTINCT_KEYS)
    public void hashJoinUtf8MultiKey()
    {
        Operator operator = new HashJoinOperator(
                allocator,
                new TableOperator(2, List.of(outerJoinUtf8MultiKeyPage)),
                new int[] {0, 1},
                new TableOperator(2, List.of(innerJoinUtf8MultiKeyPage)),
                new int[] {0, 1});

        consume(operator);
    }

    @Benchmark
    @OperationsPerInvocation(UTF8_JOIN_OUTER_ROWS + UTF8_JOIN_DISTINCT_KEYS)
    public void hashJoinUtf8RepeatedKeysWithWidePayloads()
    {
        Operator operator = new HashJoinOperator(
                allocator,
                new TableOperator(2, List.of(outerJoinUtf8PayloadPage)),
                0,
                new TableOperator(2, List.of(innerJoinUtf8PayloadPage)),
                0);

        consume(operator);
    }

    private static TableOperator.Page utf8Page(int rowCount, int distinctKeys)
    {
        byte[][] keyBytes = utf8Keys(distinctKeys);
        return new TableOperator.Page(rowCount, new Streams[] {utf8Streams(rowCount, position -> keyBytes[position % distinctKeys], ignored -> false)}, Mask.all(rowCount));
    }

    private static TableOperator.Page utf8NullablePage(int rowCount, int distinctKeys, int nullEvery)
    {
        byte[][] keyBytes = utf8Keys(distinctKeys);
        return new TableOperator.Page(
                rowCount,
                new Streams[] {utf8Streams(rowCount, position -> keyBytes[position % distinctKeys], position -> position % nullEvery == 0)},
                Mask.all(rowCount));
    }

    private static TableOperator.Page utf8MultiKeyPage(int rowCount, int primaryDistinctKeys, int secondaryDistinctKeys)
    {
        byte[][] primaryKeys = utf8Keys(primaryDistinctKeys);
        byte[][] secondaryKeys = utf8Keys("sub-", secondaryDistinctKeys);
        return new TableOperator.Page(
                rowCount,
                new Streams[] {
                        utf8Streams(rowCount, position -> primaryKeys[position % primaryDistinctKeys], ignored -> false),
                        utf8Streams(rowCount, position -> secondaryKeys[(position / primaryDistinctKeys) % secondaryDistinctKeys], ignored -> false),
                },
                Mask.all(rowCount));
    }

    private static TableOperator.Page utf8PayloadPage(int rowCount, int distinctKeys, int payloadLength)
    {
        byte[][] keyBytes = utf8Keys(distinctKeys);
        byte[][] payloadBytes = utf8Keys("payload-", distinctKeys, payloadLength);
        return new TableOperator.Page(
                rowCount,
                new Streams[] {
                        utf8Streams(rowCount, position -> keyBytes[position % distinctKeys], ignored -> false),
                        utf8Streams(rowCount, position -> payloadBytes[position % distinctKeys], ignored -> false),
                },
                Mask.all(rowCount));
    }

    private static byte[][] utf8Keys(int distinctKeys)
    {
        return utf8Keys("key-", distinctKeys);
    }

    private static byte[][] utf8Keys(String prefix, int distinctKeys)
    {
        byte[][] keyBytes = new byte[distinctKeys][];
        for (int key = 0; key < distinctKeys; key++) {
            keyBytes[key] = (prefix + key).getBytes(StandardCharsets.UTF_8);
        }
        return keyBytes;
    }

    private static byte[][] utf8Keys(String prefix, int distinctKeys, int minimumLength)
    {
        byte[][] keyBytes = new byte[distinctKeys][];
        for (int key = 0; key < distinctKeys; key++) {
            StringBuilder builder = new StringBuilder(prefix).append(key);
            while (builder.length() < minimumLength) {
                builder.append('-').append(key);
            }
            keyBytes[key] = builder.toString().getBytes(StandardCharsets.UTF_8);
        }
        return keyBytes;
    }

    private static Streams utf8Streams(int rowCount, IntToByteArray bytes, IntPredicate isNull)
    {
        int byteCapacity = 0;
        for (int position = 0; position < rowCount; position++) {
            if (!isNull.test(position)) {
                byteCapacity += bytes.apply(position).length;
            }
        }

        BinaryVector values = new BinaryVector(rowCount, byteCapacity);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        BooleanVector nulls = null;
        for (int position = 0; position < rowCount; position++) {
            if (isNull.test(position)) {
                if (nulls == null) {
                    nulls = new BooleanVector(rowCount);
                }
                nulls.values()[position] = true;
                values.setNull(position);
                continue;
            }
            values.setBytes(position, bytes.apply(position));
        }

        return nulls == null ? Streams.ofValues(values) : Streams.ofValuesAndNulls(values, nulls);
    }

    @FunctionalInterface
    private interface IntPredicate
    {
        boolean test(int value);
    }

    @FunctionalInterface
    private interface IntToByteArray
    {
        byte[] apply(int value);
    }

    private static void consume(Operator operator)
    {
        try (operator) {
            while (operator.hasNext()) {
                try (var batch = operator.next()) {
                    var mask = batch.borrowMask();
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
        Benchmarks.benchmark(BenchmarkOperators.class)
                .run();
    }
}
