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
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.BatchSliceOperator;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.CountingNextOperator;
import org.weakref.nitro.operator.EnforceSingleRowOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.FullJoinOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupIdOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.LimitSession;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.OffsetOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.PartitionAverageI64WindowFunction;
import org.weakref.nitro.operator.PartitionOffsetI64WindowFunction;
import org.weakref.nitro.operator.PartitionSumI64WindowFunction;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.RankWindowFunction;
import org.weakref.nitro.operator.RunningMaxI64WindowFunction;
import org.weakref.nitro.operator.RunningSumI64WindowFunction;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.SingleBatchOperator;
import org.weakref.nitro.operator.SortSession;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TopNRankingOperator;
import org.weakref.nitro.operator.TopNSession;
import org.weakref.nitro.operator.UnionAllOperator;
import org.weakref.nitro.operator.WindowOperator;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestOperatorBatches
{
    @Test
    void testConstantTableOperatorProducesBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L)));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testConstantTableOperatorProducesI32Batch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new ConstantTableOperator(allocator, 1, List.of(row(1), row(2), row(3)));

        Batch batch = operator.next();
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
        assertThat(((I32Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(1, 2, 3);
    }

    @Test
    void testGeneratorOperatorProducesBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(10)));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(10L, 11L, 12L, 13L, 14L);
    }

    @Test
    void testProjectOperatorProducesProjectedBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();

        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        new Variable(0),
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(new Variable(0), Stream.VALUES)));

        Operator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L), row(2L, 20L))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(11L, 22L);
    }

    @Test
    void testFilterOperatorProducesFilteredBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable threshold = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(
                        new Assignment(threshold, new Literal(3L), AllMask.ALL),
                        new Assignment(
                                predicate,
                                new Call("lt", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(threshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        Operator operator = new FilterOperator(
                new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(0))),
                evaluationPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(batch.borrowMask().count()).isEqualTo(3);
    }

    @Test
    void testFilterOperatorDoesNotTreatNullIntegerValuesAsEqual()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable constant = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(
                        new Assignment(constant, new Literal(4L), AllMask.ALL),
                        new Assignment(
                                predicate,
                                new Call("eq", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(constant, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        Operator operator = new FilterOperator(
                new ConstantTableOperator(allocator, 1, List.of(
                        row((Object) null),
                        row(4L),
                        row(5L),
                        row(4L),
                        row((Object) null))),
                evaluationPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());

        Batch batch = operator.next();
        Mask mask = batch.borrowMask();
        assertThat(mask.count()).isEqualTo(2);
        assertThat(mask.position(0)).isEqualTo(1);
        assertThat(mask.position(1)).isEqualTo(3);
    }

    @Test
    void testAggregationOperatorProducesAggregateBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountAll()),
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(6L);
        assertThat(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values()).containsExactly(3L);
    }

    @Test
    void testAggregationOperatorAcceptsI32Input()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new AggregationOperator(
                allocator,
                List.of(new Sum(0), new CountAll()),
                new ConstantTableOperator(allocator, 1, List.of(row(1), row(2), row(3))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(6L);
        assertThat(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values()).containsExactly(3L);
    }

    @Test
    void testAggregationOperatorSumsDictionaryEncodedIndicatorsWithCompactNulls()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        DictionaryVector values = new DictionaryVector(
                new int[] {1, 0, 1, 1, 0, 1},
                new I64Vector(new long[] {0, 1}));
        RleVector nulls = new RleVector(new int[] {6}, new BooleanVector(new boolean[] {false}));

        Operator source = new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return 1;
            }

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public Batch next()
            {
                hasNext = false;
                return new Batch(Mask.all(6), Output.of(Streams.of(values, nulls, null)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (Operator operator = new AggregationOperator(allocator, List.of(new Sum(0)), source);
                Batch batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(4L);
        }
    }

    @Test
    void testGroupedAggregationOperatorSumsRleEncodedIndicators()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        I64Vector groups = new I64Vector(new long[] {0, 1, 0, 1, 1, 2});
        RleVector values = new RleVector(new int[] {6}, new I64Vector(new long[] {1}));
        RleVector nulls = new RleVector(new int[] {6}, new BooleanVector(new boolean[] {false}));

        Operator source = new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public Batch next()
            {
                hasNext = false;
                return new Batch(
                        Mask.all(6),
                        Output.of(Streams.ofValues(groups)),
                        Output.of(Streams.of(values, nulls, null)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (Operator operator = new GroupedAggregationOperator(allocator, List.of(0), List.of(new Sum(1)), source);
                Batch batch = operator.next()) {
            int resultSize = batch.borrowMask().size();
            assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), resultSize)).containsExactly(0L, 1L, 2L);
            assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), resultSize)).containsExactly(2L, 3L, 1L);
        }
    }

    @Test
    void testAggregationOperatorSkipsEmptyProjectedBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        Variable threshold = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(
                        new Assignment(threshold, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                predicate,
                                new Call("lt", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(threshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());
        Operator filtered = new FilterOperator(
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L))),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());
        Operator projected = new ProjectOperator(
                allocator,
                new EvaluationPlan(List.of(), List.of(new Reference(new Input(0), Stream.VALUES))),
                primitiveRegistry,
                filtered);
        Operator operator = new AggregationOperator(allocator, List.of(new Sum(0)), projected);

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(0L);
    }

    @Test
    void testAvgAccumulatorHandlesDictionaryEncodedNullStream()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Avg accumulator = new Avg(0);
        Streams state = accumulator.allocate(
                new AggregationExecutionContext(
                        allocator,
                        new Allocator.Context("test"),
                        EngineResources.from(allocator).operatorCodeGeneration(),
                        EngineResources.from(allocator).operatorResources().distinctKeySetPolicy(),
                        EngineResources.from(allocator).operatorResources().adaptiveLongGroupingPolicy(),
                        EngineResources.from(allocator).operatorResources().flatKeyTablePolicy(),
                        org.weakref.nitro.core.type.Schema.unspecified(1)),
                1);
        accumulator.initialize(state, 0, 1);

        I64Vector values = new I64Vector(new long[] {10, 20, 30});
        DictionaryVector nulls = new DictionaryVector(new int[] {0, 1, 0}, new BooleanVector(new boolean[] {false, true}));
        accumulator.accumulate(
                state,
                0,
                Mask.all(3),
                (column, stream) -> {
                    assertThat(column).isEqualTo(0);
                    return switch (stream) {
                        case VALUES -> values;
                        case NULLS -> nulls;
                        case ERRORS -> null;
                    };
                });

        Streams result = accumulator.result(0, state, null, allocator, new Allocator.Context("testResult"));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()[0]).isFalse();
        assertThat(((org.weakref.nitro.data.F64Vector) result.get(Stream.VALUES)).values()[0]).isEqualTo(20.0);
    }

    @Test
    void testGroupedAggregationOperatorProducesGroupedAggregateBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(new Sum(2), new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(10L, 1L),
                                row(10L, 2L),
                                row(20L, 3L)))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(3L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorHandlesDictionaryEncodedGroupNulls()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        DictionaryVector groupNulls = new DictionaryVector(new int[] {0, 1, 0}, new BooleanVector(new boolean[] {false, true}));
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(new Sum(1), new CountAll()),
                new TableOperator(
                        2,
                        List.of(new TableOperator.Page(
                                3,
                                new Streams[] {
                                        Streams.builder()
                                                .put(Stream.VALUES, new I64Vector(new long[] {1L, 1L, 2L}))
                                                .put(Stream.NULLS, groupNulls)
                                                .build(),
                                        Streams.ofValues(new I64Vector(new long[] {10L, 20L, 30L}))},
                                Mask.all(3)))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(rowCount).isEqualTo(3);
        boolean[] groupedNulls = Arrays.copyOf(((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values(), rowCount);
        int nullCount = 0;
        for (boolean value : groupedNulls) {
            if (value) {
                nullCount++;
            }
        }
        assertThat(nullCount).isEqualTo(1);
    }

    @Test
    void testGroupedAggregationOperatorCoalescesSharedDictionaryCompositeKeysByValue()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        BinaryVector cities = new BinaryVector(5, 10);
        cities.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        cities.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        cities.setBytes(0, "LA".getBytes(UTF_8));
        cities.setBytes(1, "NY".getBytes(UTF_8));
        cities.setBytes(2, "LA".getBytes(UTF_8));
        cities.setBytes(3, "SF".getBytes(UTF_8));
        cities.setBytes(4, "NA".getBytes(UTF_8));

        BinaryVector zips = new BinaryVector(5, 25);
        zips.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        zips.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        zips.setBytes(0, "90001".getBytes(UTF_8));
        zips.setBytes(1, "10001".getBytes(UTF_8));
        zips.setBytes(2, "90001".getBytes(UTF_8));
        zips.setBytes(3, "94101".getBytes(UTF_8));
        zips.setBytes(4, "00000".getBytes(UTF_8));

        int[] ids = {0, 1, 2, 0, 3, 4};
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                new TableOperator(
                        2,
                        List.of(new TableOperator.Page(
                                ids.length,
                                new Streams[] {
                                        Streams.builder()
                                                .put(Stream.VALUES, DictionaryVector.wrap(Arrays.copyOf(ids, ids.length), cities))
                                                .put(Stream.NULLS, DictionaryVector.wrap(Arrays.copyOf(ids, ids.length), new BooleanVector(new boolean[] {false, false, false, false, true})))
                                                .build(),
                                        Streams.ofValues(DictionaryVector.wrap(Arrays.copyOf(ids, ids.length), zips))},
                                Mask.all(ids.length)))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector groupedCities = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        BooleanVector groupedCityNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
        BinaryVector groupedZips = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(4);
        assertThat(utf8(groupedCities, 0)).isEqualTo("LA");
        assertThat(utf8(groupedZips, 0)).isEqualTo("90001");
        assertThat(utf8(groupedCities, 1)).isEqualTo("NY");
        assertThat(utf8(groupedZips, 1)).isEqualTo("10001");
        assertThat(utf8(groupedCities, 2)).isEqualTo("SF");
        assertThat(utf8(groupedZips, 2)).isEqualTo("94101");
        assertThat(groupedCityNulls.values()[3]).isTrue();
        assertThat(utf8(groupedZips, 3)).isEqualTo("00000");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(3L, 1L, 1L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorKeepsAllNullSumGroupsNull()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1)),
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, null),
                        row(1L, null),
                        row(2L, 0L),
                        row(2L, null))));

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator)).containsExactly(
                row(1L, null),
                row(2L, 0L));
    }

    @Test
    void testFullJoinOperatorNullExtendsUnmatchedRows()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new FullJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 10), row(2L, 20))),
                new int[] {0, 1},
                new ConstantTableOperator(allocator, 2, List.of(row(2L, 20), row(3L, 30))),
                new int[] {0, 1},
                EngineResources.from(allocator).operatorResources().fullJoinPolicy())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 10, null, null),
                            row(2L, 20, 2L, 20),
                            row(null, null, 3L, 30));
        }
    }

    @Test
    void testFullJoinOperatorCrossesPooledRowReferenceChunkBoundary()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<Row> outerRows = new ArrayList<>();
        for (long key = 0; key <= 65_536; key++) {
            outerRows.add(row(key, key + 1));
        }

        try (Operator operator = new FullJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, outerRows),
                new int[] {0},
                new ConstantTableOperator(allocator, 2, List.of(row(65_536L, 7L), row(70_000L, 8L))),
                new int[] {0},
                EngineResources.from(allocator).operatorResources().fullJoinPolicy())) {
            List<Row> rows = OperatorAssertions.OperatorAssert.toRows(operator);
            assertThat(rows).hasSize(65_538);
            assertThat(rows.get(65_535)).isEqualTo(row(65_535L, 65_536L, null, null));
            assertThat(rows.get(65_536)).isEqualTo(row(65_536L, 65_537L, 65_536L, 7L));
            assertThat(rows.get(65_537)).isEqualTo(row(null, null, 70_000L, 8L));
        }
    }

    @Test
    void testSortedFullJoinOperatorPreservesDuplicateMultiplicityAndNullSemantics()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = FullJoinOperator.sorted(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(1L, 11L),
                        row(2L, 20L),
                        row(null, 90L))),
                new int[] {0},
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 30L),
                        row(1L, 31L),
                        row(3L, 40L),
                        row(null, 91L))),
                new int[] {0},
                EngineResources.from(allocator).operatorResources().fullJoinPolicy())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 10L, 1L, 30L),
                            row(1L, 10L, 1L, 31L),
                            row(1L, 11L, 1L, 30L),
                            row(1L, 11L, 1L, 31L),
                            row(2L, 20L, null, null),
                            row(null, null, 3L, 40L),
                            row(null, 90L, null, null),
                            row(null, null, null, 91L));
        }
    }

    @Test
    void testSortedFullJoinOperatorMergesDuplicateRunsAcrossBatchBoundaries()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator outer = new UnionAllOperator(2, List.of(
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L))),
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 11L), row(2L, 20L)))));
        Operator inner = new UnionAllOperator(2, List.of(
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 30L))),
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 31L), row(3L, 40L)))));

        try (Operator operator = FullJoinOperator.sorted(
                allocator,
                outer,
                new int[] {0},
                inner,
                new int[] {0},
                EngineResources.from(allocator).operatorResources().fullJoinPolicy())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 10L, 1L, 30L),
                            row(1L, 10L, 1L, 31L),
                            row(1L, 11L, 1L, 30L),
                            row(1L, 11L, 1L, 31L),
                            row(2L, 20L, null, null),
                            row(null, null, 3L, 40L));
        }
    }

    @Test
    void testWindowOperatorProducesRunningPartitionedAggregates()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, 2, 5L),
                        row(1L, 1, 3L),
                        row(2L, 1, 7L),
                        row(1L, 3, (Object) null),
                        row(2L, 2, 4L))),
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(
                        new RunningSumI64WindowFunction(2),
                        new RunningMaxI64WindowFunction(2)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 1, 3L, 3L, 3L),
                            row(1L, 2, 5L, 8L, 5L),
                            row(1L, 3, null, 8L, 5L),
                            row(2L, 1, 7L, 7L, 7L),
                            row(2L, 2, 4L, 11L, 7L));
        }
    }

    @Test
    void testWindowOperatorProducesPartitionAverageWithoutOrdering()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2L, 7L),
                        row(1L, 2L),
                        row(1L, 3L),
                        row(3L, (Object) null),
                        row(2L, 9L),
                        row(1L, (Object) null))),
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(1)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 2L, 3L),
                            row(1L, 3L, 3L),
                            row(1L, null, 3L),
                            row(2L, 7L, 8L),
                            row(2L, 9L, 8L),
                            row(3L, null, null));
        }
    }

    @Test
    void testWindowOperatorProducesPartitionSumWithoutOrdering()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2L, 7L),
                        row(1L, 2L),
                        row(1L, 3L),
                        row(3L, (Object) null),
                        row(2L, 9L),
                        row(1L, (Object) null))),
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new PartitionSumI64WindowFunction(1)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 2L, 5L),
                            row(1L, 3L, 5L),
                            row(1L, null, 5L),
                            row(2L, 7L, 16L),
                            row(2L, 9L, 16L),
                            row(3L, null, null));
        }
    }

    @Test
    void testWindowOperatorGroupsNullPartitionKeysTogether()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        // PARTITION BY places all null-keyed rows in a single partition, so they share one sum (5 + 7).
        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 3L),
                        row((Object) null, 5L),
                        row(1L, 4L),
                        row((Object) null, 7L))),
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new PartitionSumI64WindowFunction(1)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 3L, 7L),
                            row(1L, 4L, 7L),
                            row(null, 5L, 12L),
                            row(null, 7L, 12L));
        }
    }

    @Test
    void testWindowOperatorHashPartitionsBinaryAndNullKeysExactly()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("sports", 3L),
                        row((Object) null, 5L),
                        row("books", 11L),
                        row("sports", 4L),
                        row((Object) null, 7L),
                        row("books", 13L))),
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new PartitionSumI64WindowFunction(1)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactlyInAnyOrder(
                            row("sports", 3L, 7L),
                            row("sports", 4L, 7L),
                            row("books", 11L, 24L),
                            row("books", 13L, 24L),
                            row(null, 5L, 12L),
                            row(null, 7L, 12L));
        }
    }

    @Test
    void testWindowOperatorProducesRankWithTies()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(3L, "third"),
                        row(1L, "first-a"),
                        row(1L, "first-b"),
                        row(2L, "second"))),
                new int[0],
                new int[] {0},
                new boolean[] {false},
                List.of(new RankWindowFunction(new int[] {0}, new boolean[] {false})))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, "first-a", 1L),
                            row(1L, "first-b", 1L),
                            row(2L, "second", 3L),
                            row(3L, "third", 4L));
        }
    }

    @Test
    void testWindowOperatorRadixOrdersSignedNullableDescendingKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(-1L, -3L, 4L),
                        row(-1L, (Object) null, 5L),
                        row(1L, -8L, 7L),
                        row(-1L, 2L, 6L),
                        row(1L, 9L, 8L))),
                new int[] {0},
                new int[] {1},
                new boolean[] {true},
                List.of(new RunningSumI64WindowFunction(2)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(-1L, null, 5L, 5L),
                            row(-1L, 2L, 6L, 11L),
                            row(-1L, -3L, 4L, 15L),
                            row(1L, 9L, 8L, 8L),
                            row(1L, -8L, 7L, 15L));
        }
    }

    @Test
    void testWindowOperatorEmitsLargeResultsAcrossMultipleBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        for (int value = 1; value <= 5_000; value++) {
            rows.add(row(1L, (long) value));
        }

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, rows),
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(new RunningSumI64WindowFunction(1)))) {
            int batchCount = 0;
            int rowCount = 0;
            long firstValue = Long.MIN_VALUE;
            long boundaryValue = Long.MIN_VALUE;
            long finalValue = Long.MIN_VALUE;
            while (operator.hasNext()) {
                try (Batch batch = operator.next()) {
                    batchCount++;
                    Mask mask = batch.borrowMask();
                    I64Vector runningSum = (I64Vector) batch.output(2).borrow(Stream.VALUES);
                    for (int position : mask) {
                        long value = runningSum.values()[position];
                        if (rowCount == 0) {
                            firstValue = value;
                        }
                        if (rowCount == 4_095) {
                            boundaryValue = value;
                        }
                        finalValue = value;
                        rowCount++;
                    }
                }
            }
            assertThat(batchCount).isEqualTo(2);
            assertThat(rowCount).isEqualTo(5_000);
            assertThat(firstValue).isEqualTo(1L);
            assertThat(boundaryValue).isEqualTo(8_390_656L);
            assertThat(finalValue).isEqualTo(12_502_500L);
        }
    }

    @Test
    void testWindowOperatorRetainedBatchResolvesAfterAdvance()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        for (int value = 5_000; value >= 1; value--) {
            rows.add(row(1L, (long) value));
        }

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, rows),
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(
                        new RunningSumI64WindowFunction(1),
                        new RunningMaxI64WindowFunction(1)));
                Batch first = operator.next();
                Batch second = operator.next()) {
            I64Vector secondValues = (I64Vector) second.output(1).borrow(Stream.VALUES);
            assertThat(secondValues.values()[0]).isEqualTo(4_097L);

            // A retained output may be resolved only after the operator has advanced. Its lazy position mapping
            // must remain tied to this batch rather than whichever batch was produced most recently.
            I64Vector firstValues = (I64Vector) first.output(1).borrow(Stream.VALUES);
            assertThat(firstValues.values()[0]).isEqualTo(1L);
            assertThat(firstValues.values()[4_095]).isEqualTo(4_096L);
        }
    }

    @Test
    void testTopNRankingOperatorEmitsLargeResultsAcrossMultipleBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        for (int value = 5_000; value >= 1; value--) {
            rows.add(row((long) value));
        }

        try (Operator operator = new org.weakref.nitro.operator.TopNRankingOperator(
                allocator,
                5_000,
                new int[] {0},
                new boolean[] {false},
                new ConstantTableOperator(allocator, 1, rows),
                EngineResources.from(allocator).operatorResources().topNRankingPolicy())) {
            int batchCount = 0;
            int rowCount = 0;
            long firstRank = Long.MIN_VALUE;
            long boundaryRank = Long.MIN_VALUE;
            long finalRank = Long.MIN_VALUE;
            long firstValue = Long.MIN_VALUE;
            long boundaryValue = Long.MIN_VALUE;
            long finalValue = Long.MIN_VALUE;
            while (operator.hasNext()) {
                try (Batch batch = operator.next()) {
                    batchCount++;
                    Mask mask = batch.borrowMask();
                    I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    I64Vector ranks = (I64Vector) batch.output(1).borrow(Stream.VALUES);
                    for (int position : mask) {
                        long value = values.values()[position];
                        long rank = ranks.values()[position];
                        if (rowCount == 0) {
                            firstValue = value;
                            firstRank = rank;
                        }
                        if (rowCount == 4_095) {
                            boundaryValue = value;
                            boundaryRank = rank;
                        }
                        finalValue = value;
                        finalRank = rank;
                        rowCount++;
                    }
                }
            }
            assertThat(batchCount).isEqualTo(2);
            assertThat(rowCount).isEqualTo(5_000);
            assertThat(firstValue).isEqualTo(1L);
            assertThat(firstRank).isEqualTo(1L);
            assertThat(boundaryValue).isEqualTo(4_096L);
            assertThat(boundaryRank).isEqualTo(4_096L);
            assertThat(finalValue).isEqualTo(5_000L);
            assertThat(finalRank).isEqualTo(5_000L);
        }
    }

    @Test
    void testBatchSliceOperatorSplitsLargeBatchWithoutDroppingRows()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        for (long value = 0; value < 5_000; value++) {
            rows.add(row(value));
        }

        try (Operator operator = new BatchSliceOperator(
                allocator,
                1_024,
                new ConstantTableOperator(allocator, 1, rows))) {
            List<Long> actual = new ArrayList<>();
            int batchCount = 0;
            while (operator.hasNext()) {
                try (Batch batch = operator.next()) {
                    batchCount++;
                    assertThat(batch.borrowMask().selectedCount()).isLessThanOrEqualTo(1_024);
                    I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    for (int position : batch.borrowMask()) {
                        actual.add(values.values()[position]);
                    }
                }
            }

            assertThat(batchCount).isGreaterThan(1);
            assertThat(actual).hasSize(5_000);
            assertThat(actual.get(0)).isEqualTo(0L);
            assertThat(actual.get(actual.size() - 1)).isEqualTo(4_999L);
        }
    }

    @Test
    void testTopNOperatorSupportsProjectedOrderingAfterBatchSlicing()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();

        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        for (long value = 0; value < 5_000; value++) {
            rows.add(row(value, 2_500L));
        }

        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        new Variable(0),
                        new Call("subtract", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Variable(0), Stream.VALUES)));

        try (Operator operator = new TopNOperator(
                allocator,
                5,
                new int[] {1},
                new boolean[] {false},
                new ProjectOperator(
                        allocator,
                        evaluationPlan,
                        primitiveRegistry,
                        new BatchSliceOperator(
                                allocator,
                                1_024,
                                new ConstantTableOperator(allocator, 2, rows))))) {
            List<Long> actual = new ArrayList<>();
            while (operator.hasNext()) {
                try (Batch batch = operator.next()) {
                    I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    for (int position : batch.borrowMask()) {
                        actual.add(values.values()[position]);
                    }
                }
            }

            assertThat(actual).containsExactly(0L, 1L, 2L, 3L, 4L);
        }
    }

    @Test
    void testTopNRankingOperatorPreservesUtf8PayloadColumns()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new TopNRankingOperator(
                allocator,
                5,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                new ConstantTableOperator(allocator, 2, List.of(
                        row("alpha", 3L),
                        row("alpha", 2L),
                        row("alpha", 1L),
                        row("beta", 2L),
                        row("beta", 1L))),
                EngineResources.from(allocator).operatorResources().topNRankingPolicy())) {
            assertThat(operator(operator)).matchesExactly(List.of(
                    row("alpha", 1L, 1L),
                    row("alpha", 2L, 2L),
                    row("alpha", 3L, 3L),
                    row("beta", 1L, 1L),
                    row("beta", 2L, 2L)));
        }
    }

    @Test
    void testTopNRankingOperatorPreservesUtf8PayloadColumnsAcrossMultipleBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        List<org.weakref.nitro.data.Row> expected = new ArrayList<>();
        for (int partition = 0; partition < 2_500; partition++) {
            String category = "category-" + partition;
            String brand = "brand-" + partition;
            String callCenter = "call-center-" + partition;
            long monthOneSales = partition * 10L;
            long monthTwoSales = monthOneSales + 2;
            rows.add(row(category, brand, callCenter, 1999L, 1L, monthOneSales));
            rows.add(row(category, brand, callCenter, 1999L, 2L, monthTwoSales));
            expected.add(row(category, brand, callCenter, 1999L, 1L, monthOneSales, 1L));
            expected.add(row(category, brand, callCenter, 1999L, 2L, monthTwoSales, 2L));
        }
        sortRowsByQuery57Keys(expected);

        try (Operator operator = new TopNRankingOperator(
                allocator,
                32,
                new int[] {0, 1, 2},
                new int[] {3, 4},
                new boolean[] {false, false},
                new BatchSliceOperator(
                        allocator,
                        257,
                        new ConstantTableOperator(allocator, 6, rows)),
                EngineResources.from(allocator).operatorResources().topNRankingPolicy())) {
            assertThat(operator(operator)).matchesExactly(expected);
        }
    }

    @Test
    void testTopNRankingAndSingleWindowOperatorPreserveUtf8PayloadColumnsAcrossMultipleBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        List<org.weakref.nitro.data.Row> expected = new ArrayList<>();
        for (int partition = 0; partition < 2_500; partition++) {
            String category = "category-" + partition;
            String brand = "brand-" + partition;
            String callCenter = "call-center-" + partition;
            long monthOneSales = partition * 10L;
            long monthTwoSales = monthOneSales + 2;
            long averageSales = monthOneSales + 1;
            rows.add(row(category, brand, callCenter, 1999L, 1L, monthOneSales));
            rows.add(row(category, brand, callCenter, 1999L, 2L, monthTwoSales));
            expected.add(row(category, brand, callCenter, 1999L, 1L, monthOneSales, 1L, averageSales));
            expected.add(row(category, brand, callCenter, 1999L, 2L, monthTwoSales, 2L, averageSales));
        }
        sortRowsByQuery57Keys(expected);

        try (Operator operator = new WindowOperator(
                allocator,
                new TopNRankingOperator(
                        allocator,
                        32,
                        new int[] {0, 1, 2},
                        new int[] {3, 4},
                        new boolean[] {false, false},
                        new BatchSliceOperator(
                                allocator,
                                257,
                                new ConstantTableOperator(allocator, 6, rows)),
                        EngineResources.from(allocator).operatorResources().topNRankingPolicy()),
                new int[] {0, 1, 2, 3},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(5)))) {
            assertThat(operator(operator)).matchesExactly(expected);
        }
    }

    @Test
    void testTopNRankingAndWindowOperatorsPreserveUtf8PayloadColumnsAcrossMultipleBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        List<org.weakref.nitro.data.Row> rows = new ArrayList<>();
        List<org.weakref.nitro.data.Row> expected = new ArrayList<>();
        for (int partition = 0; partition < 2_500; partition++) {
            String category = "category-" + partition;
            String brand = "brand-" + partition;
            String callCenter = "call-center-" + partition;
            long monthOneSales = partition * 10L;
            long monthTwoSales = monthOneSales + 2;
            long averageSales = monthOneSales + 1;
            rows.add(row(category, brand, callCenter, 1999L, 1L, monthOneSales));
            rows.add(row(category, brand, callCenter, 1999L, 2L, monthTwoSales));
            expected.add(row(category, brand, callCenter, 1999L, 1L, monthOneSales, 1L, averageSales, null, monthTwoSales));
            expected.add(row(category, brand, callCenter, 1999L, 2L, monthTwoSales, 2L, averageSales, monthOneSales, null));
        }
        sortRowsByQuery57Keys(expected);

        try (Operator operator = new WindowOperator(
                allocator,
                new WindowOperator(
                        allocator,
                        new TopNRankingOperator(
                                allocator,
                                32,
                                new int[] {0, 1, 2},
                                new int[] {3, 4},
                                new boolean[] {false, false},
                                new BatchSliceOperator(
                                        allocator,
                                        257,
                                        new ConstantTableOperator(allocator, 6, rows)),
                                EngineResources.from(allocator).operatorResources().topNRankingPolicy()),
                        new int[] {0, 1, 2, 3},
                        new int[0],
                        new boolean[0],
                        List.of(new PartitionAverageI64WindowFunction(5))),
                new int[] {0, 1, 2},
                new int[] {3, 4},
                new boolean[] {false, false},
                List.of(
                        new PartitionOffsetI64WindowFunction(5, -1),
                        new PartitionOffsetI64WindowFunction(5, 1)))) {
            assertThat(operator(operator)).matchesExactly(expected);
        }
    }

    private static void sortRowsByQuery57Keys(List<org.weakref.nitro.data.Row> rows)
    {
        rows.sort((left, right) -> {
            int categoryComparison = ((String) left.values()[0]).compareTo((String) right.values()[0]);
            if (categoryComparison != 0) {
                return categoryComparison;
            }
            int brandComparison = ((String) left.values()[1]).compareTo((String) right.values()[1]);
            if (brandComparison != 0) {
                return brandComparison;
            }
            int callCenterComparison = ((String) left.values()[2]).compareTo((String) right.values()[2]);
            if (callCenterComparison != 0) {
                return callCenterComparison;
            }
            int yearComparison = Long.compare((Long) left.values()[3], (Long) right.values()[3]);
            if (yearComparison != 0) {
                return yearComparison;
            }
            return Long.compare((Long) left.values()[4], (Long) right.values()[4]);
        });
    }

    @Test
    void testChainedWindowOperatorsPreserveUtf8PayloadColumns()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new WindowOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 4, List.of(
                                row("alpha", 1998L, 12L, 10L),
                                row("alpha", 1999L, 1L, 20L),
                                row("alpha", 1999L, 2L, 30L),
                                row("beta", 1999L, 1L, 40L),
                                row("beta", 1999L, 2L, 50L))),
                        new int[] {0, 1},
                        new int[0],
                        new boolean[0],
                        List.of(new PartitionAverageI64WindowFunction(3))),
                new int[] {0},
                new int[] {1, 2},
                new boolean[] {false, false},
                List.of(new PartitionOffsetI64WindowFunction(3, -1)))) {
            assertThat(operator(operator)).matchesExactly(List.of(
                    row("alpha", 1998L, 12L, 10L, 10L, null),
                    row("alpha", 1999L, 1L, 20L, 25L, 10L),
                    row("alpha", 1999L, 2L, 30L, 25L, 20L),
                    row("beta", 1999L, 1L, 40L, 45L, null),
                    row("beta", 1999L, 2L, 50L, 45L, 40L)));
        }
    }

    @Test
    void testSingleWindowOperatorPreservesUtf8PayloadColumns()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (Operator operator = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, List.of(
                        row("alpha", 1998L, 12L, 10L),
                        row("alpha", 1999L, 1L, 20L),
                        row("alpha", 1999L, 2L, 30L),
                        row("beta", 1999L, 1L, 40L),
                        row("beta", 1999L, 2L, 50L))),
                new int[] {0, 1},
                new int[0],
                new boolean[0],
                List.of(new PartitionAverageI64WindowFunction(3)))) {
            assertThat(operator(operator)).matchesExactly(List.of(
                    row("alpha", 1998L, 12L, 10L, 10L),
                    row("alpha", 1999L, 1L, 20L, 25L),
                    row("alpha", 1999L, 2L, 30L, 25L),
                    row("beta", 1999L, 1L, 40L, 45L),
                    row("beta", 1999L, 2L, 50L, 45L)));
        }
    }

    @Test
    void testGroupedAggregationOperatorCanFuseGroupingAndAggregation()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountAll()),
                new ConstantTableOperator(allocator, 2, List.of(
                        row(10L, 1L),
                        row(10L, 2L),
                        row(20L, 3L))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(10L, 20L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(3L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(2).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanExposeGroupingKeyBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(
                                row("alpha"),
                                row("alpha"),
                                row("beta")))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(keys.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
        assertThat(utf8(keys, 0)).isEqualTo("alpha");
        assertThat(utf8(keys, 1)).isEqualTo("beta");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanFuseMultipleGroupingKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                new ConstantTableOperator(allocator, 2, List.of(
                        row(10L, "alpha"),
                        row(10L, "alpha"),
                        row(10L, "beta"),
                        row(20L, "alpha"))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector leftKeys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector rightKeys = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(leftKeys.values(), rowCount)).containsExactly(10L, 10L, 20L);
        assertThat(utf8(rightKeys, 0)).isEqualTo("alpha");
        assertThat(utf8(rightKeys, 1)).isEqualTo("beta");
        assertThat(utf8(rightKeys, 2)).isEqualTo("alpha");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanFuseThreeLongGroupingKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new CountAll()),
                new ConstantTableOperator(allocator, 3, List.of(
                        row(10L, 100L, 1000L),
                        row(10L, 100L, 1000L),
                        row(10L, 100L, 2000L),
                        row(20L, 100L, 1000L))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector firstKeys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        I64Vector secondKeys = (I64Vector) batch.output(1).borrow(Stream.VALUES);
        I64Vector thirdKeys = (I64Vector) batch.output(2).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(3).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(firstKeys.values(), rowCount)).containsExactly(10L, 10L, 20L);
        assertThat(Arrays.copyOf(secondKeys.values(), rowCount)).containsExactly(100L, 100L, 100L);
        assertThat(Arrays.copyOf(thirdKeys.values(), rowCount)).containsExactly(1000L, 2000L, 1000L);
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorPreservesEmptyUtf8GroupingKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(
                                row(""),
                                row(""),
                                row("alpha")))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
        I64Vector counts = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(2);
        assertThat(nulls.values()).containsExactly(false, false);
        assertThat(utf8(keys, 0)).isEqualTo("");
        assertThat(keys.endOffset(0)).isZero();
        assertThat(utf8(keys, 1)).isEqualTo("alpha");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanExposeMultipleGroupingKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1, 2),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        new int[] {0, 1},
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(10L, "alpha"),
                                row(10L, "alpha"),
                                row(10L, "beta"),
                                row(20L, "alpha")))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector leftKeys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector rightKeys = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(leftKeys.values(), rowCount)).containsExactly(10L, 10L, 20L);
        assertThat(utf8(rightKeys, 0)).isEqualTo("alpha");
        assertThat(utf8(rightKeys, 1)).isEqualTo("beta");
        assertThat(utf8(rightKeys, 2)).isEqualTo("alpha");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 1L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorCanGroupTwoUtf8KeysAndLongKey()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                List.of(new CountAll()),
                new ConstantTableOperator(allocator, 3, List.of(
                        row("AL", "Walker County", 2L),
                        row("AL", "Walker County", 2L),
                        row("TN", "Williamson County", 2L),
                        row("TN", "Williamson County", 2L),
                        row("TN", "Williamson County", 2L))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector stateKeys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector countyKeys = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector hierarchyKeys = (I64Vector) batch.output(2).borrow(Stream.VALUES);
        I64Vector counts = (I64Vector) batch.output(3).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(2);
        assertThat(utf8(stateKeys, 0)).isEqualTo("AL");
        assertThat(utf8(countyKeys, 0)).isEqualTo("Walker County");
        assertThat(hierarchyKeys.values()[0]).isEqualTo(2L);
        assertThat(counts.values()[0]).isEqualTo(2L);
        assertThat(utf8(stateKeys, 1)).isEqualTo("TN");
        assertThat(utf8(countyKeys, 1)).isEqualTo("Williamson County");
        assertThat(hierarchyKeys.values()[1]).isEqualTo(2L);
        assertThat(counts.values()[1]).isEqualTo(3L);
    }

    @Test
    void testGroupedAggregationOperatorPreservesNullableCompositeKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                new ConstantTableOperator(allocator, 2, List.of(
                        row("80348", "Lakeside"),
                        row((Object) null, "Edgewood"),
                        row((Object) null, (Object) null),
                        row((Object) null, "Edgewood"))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector firstKeys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        BooleanVector firstNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
        BinaryVector secondKeys = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        BooleanVector secondNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
        I64Vector counts = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(3);
        assertThat(firstNulls.values()).containsExactly(false, true, true);
        assertThat(secondNulls.values()).containsExactly(false, false, true);
        assertThat(utf8(firstKeys, 0)).isEqualTo("80348");
        assertThat(utf8(secondKeys, 0)).isEqualTo("Lakeside");
        assertThat(utf8(secondKeys, 1)).isEqualTo("Edgewood");
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(1L, 2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorPreservesNullableCompositeLongKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                new ConstantTableOperator(allocator, 2, List.of(
                        row(10L, 100L),
                        row(10L, 100L),
                        row(10L, (Object) null),
                        row(10L, (Object) null),
                        row((Object) null, 100L),
                        row((Object) null, 100L),
                        row((Object) null, (Object) null))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector firstKeys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BooleanVector firstNulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
        I64Vector secondKeys = (I64Vector) batch.output(1).borrow(Stream.VALUES);
        BooleanVector secondNulls = (BooleanVector) batch.output(1).borrow(Stream.NULLS);
        I64Vector counts = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(4);
        assertThat(Arrays.copyOf(firstKeys.values(), rowCount)).containsExactly(10L, 10L, 0L, 0L);
        assertThat(Arrays.copyOf(firstNulls.values(), rowCount)).containsExactly(false, false, true, true);
        assertThat(Arrays.copyOf(secondKeys.values(), rowCount)).containsExactly(100L, 0L, 100L, 0L);
        assertThat(Arrays.copyOf(secondNulls.values(), rowCount)).containsExactly(false, true, false, true);
        assertThat(Arrays.copyOf(counts.values(), rowCount)).containsExactly(2L, 2L, 2L, 1L);
    }

    @Test
    void testGroupedAggregationOperatorMaterializesOnlyConstrainedBinaryKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(
                                row("alpha"),
                                row("alpha"),
                                row("beta")))));

        Batch batch = operator.next();
        batch.constrain(Mask.sparse(new int[] {1}, 2));

        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);

        assertThat(nulls.values()).containsExactly(true, false);
        assertThat(utf8(keys, 1)).isEqualTo("beta");
        assertThat(keys.endOffset(1)).isEqualTo("beta".length());
    }

    @Test
    void testGroupedAggregationOperatorSupportsRetainedBatchesForTopNPayloadDeferral()
    {
        Operator operator = new GroupedAggregationOperator(
                new Allocator(EngineResources.createDefault()),
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(
                        new Allocator(EngineResources.createDefault()),
                        0,
                        new ConstantTableOperator(new Allocator(EngineResources.createDefault()), 1, List.of(
                                row("alpha"),
                                row("alpha"),
                                row("beta")))));

        assertThat(operator.supportsRetainedBatches()).isTrue();
    }

    void testLimitOperatorProducesLimitedBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new LimitOperator(allocator, 3, new GeneratorOperator(allocator, 5, 5, List.of(new SequenceGenerator(0))));

        Batch batch = operator.next();
        assertThat(batch.borrowMask().count()).isEqualTo(3);
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    void testGroupOperatorProducesGroupBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupOperator(
                allocator,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(10L), row(10L), row(20L))));

        Batch batch = operator.next();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(0L, 0L, 1L);
    }

    @Test
    void testTopNOperatorProducesTopNBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new TopNOperator(
                allocator,
                2,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(5L), row(3L))));

        Batch batch = operator.next();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(5L, 3L);
    }

    @Test
    void testTopNOperatorSupportsI32OrderingAndPayloadColumns()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new TopNOperator(
                allocator,
                2,
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1, 10),
                        row(5, 20),
                        row(3, 30))));

        Batch batch = operator.next();
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
        assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I32Vector.class);
        assertThat(Arrays.copyOf(((I32Vector) batch.output(0).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(5, 3);
        assertThat(Arrays.copyOf(((I32Vector) batch.output(1).borrow(Stream.VALUES)).values(), batch.borrowMask().count())).containsExactly(20, 30);
    }

    @Test
    void testHashJoinOperatorSupportsI32EquiJoin()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1, 10),
                        row(2, 20),
                        row(3, 30))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2, 200),
                        row(3, 300),
                        row(4, 400))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(longValues(batch.output(0).borrow(Stream.VALUES), rowCount)).containsExactly(2L, 3L);
        assertThat(longValues(batch.output(1).borrow(Stream.VALUES), rowCount)).containsExactly(20L, 30L);
        assertThat(longValues(batch.output(3).borrow(Stream.VALUES), rowCount)).containsExactly(200L, 300L);
    }

    @Test
    void testHashJoinOperatorStreamsUnusedUniqueDirectRangeBuildPayload()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<org.weakref.nitro.data.Row> innerRows = new ArrayList<>();
        // Reverse physical order proves that payload-free membership does not depend on a recoverable build-row
        // reference. Keep the range above the completed-direct-range admission floor.
        for (long key = 4096; key >= 1; key--) {
            innerRows.add(row(key));
        }

        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(4096L, 20L),
                        row(4097L, 30L))),
                0,
                new ConstantTableOperator(allocator, 1, innerRows),
                0)
                .withOutputs(1);

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(row(10L), row(20L));
    }

    @Test
    void testProjectOperatorCountsNonNullI32Zeros()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();

        Streams[] columns = new Streams[] {
                Streams.ofValuesAndNulls(
                        new I32Vector(new int[] {0, 7, 0}),
                        new BooleanVector(new boolean[] {false, false, true}))};
        Operator source = new TableOperator(1, List.of(new TableOperator.Page(3, columns, Mask.all(3))));

        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable quantityNull = new Variable(2);
        Variable countContribution = new Variable(3);
        Operator operator = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(one, new Literal(1L), AllMask.ALL),
                                new Assignment(quantityNull, new Call("is_null_i64", List.of(
                                        new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                                new Assignment(countContribution, new Call("if_i64", List.of(
                                        new Reference(quantityNull, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES),
                                        new Reference(one, Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(countContribution, Stream.VALUES))),
                primitiveRegistry,
                source);

        Batch batch = operator.next();
        assertThat(longValues(batch.output(0).borrow(Stream.VALUES), batch.borrowMask().count())).containsExactly(1L, 1L, 0L);
    }

    @Test
    void testHashJoinOperatorPreservesNullableI32ZerosForCountExpression()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();

        Streams[] outerColumns = new Streams[] {
                Streams.ofValues(new I64Vector(new long[] {1L, 1L, 2L})),
                Streams.ofValuesAndNulls(
                        new I32Vector(new int[] {0, 7, 0}),
                        new BooleanVector(new boolean[] {false, false, true}))};
        Operator outer = new TableOperator(2, List.of(new TableOperator.Page(3, outerColumns, Mask.all(3))));
        Operator inner = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L)));
        Operator joined = new HashJoinOperator(allocator, outer, 0, inner, 0);

        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable quantityNull = new Variable(2);
        Variable countContribution = new Variable(3);
        Operator operator = new ProjectOperator(
                allocator,
                new EvaluationPlan(
                        List.of(
                                new Assignment(zero, new Literal(0L), AllMask.ALL),
                                new Assignment(one, new Literal(1L), AllMask.ALL),
                                new Assignment(quantityNull, new Call("is_null_i64", List.of(
                                        new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                                new Assignment(countContribution, new Call("if_i64", List.of(
                                        new Reference(quantityNull, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES),
                                        new Reference(one, Stream.VALUES))), AllMask.ALL)),
                        List.of(new Reference(countContribution, Stream.VALUES))),
                primitiveRegistry,
                joined);

        Batch batch = operator.next();
        assertThat(longValues(batch.output(0).borrow(Stream.VALUES), batch.borrowMask().count())).containsExactly(1L, 1L, 0L);
    }

    @Test
    void testHashJoinOperatorSupportsRlePayloadsAcrossJoinedOuterPositions()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());

        Streams[] outerColumns = new Streams[] {
                Streams.ofValues(new I64Vector(new long[] {1L, 2L, 3L})),
                Streams.ofValues(new RleVector(new int[] {3}, new I64Vector(new long[] {99L})))};
        Operator outer = new TableOperator(2, List.of(new TableOperator.Page(3, outerColumns, Mask.all(3))));
        Operator inner = new ConstantTableOperator(allocator, 2, List.of(
                row(2L, 200L),
                row(3L, 300L)));

        Batch batch = new HashJoinOperator(allocator, outer, 0, inner, 0).next();

        int rowCount = batch.borrowMask().count();
        assertThat(longValues(batch.output(0).borrow(Stream.VALUES), rowCount)).containsExactly(2L, 3L);
        assertThat(longValues(batch.output(1).borrow(Stream.VALUES), rowCount)).containsExactly(99L, 99L);
        assertThat(longValues(batch.output(3).borrow(Stream.VALUES), rowCount)).containsExactly(200L, 300L);
    }

    @Test
    void testHashJoinOperatorPreservesInnerNullsAcrossMultipleInnerPages()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        int firstBatchSize = 250_000;
        int secondBatchSize = 250_001;

        Operator outer = new ConstantTableOperator(allocator, 1, List.of(
                row(1L),
                row((long) firstBatchSize),
                row((long) (firstBatchSize + secondBatchSize))));

        long[] firstKeys = new long[firstBatchSize];
        long[] firstPayloads = new long[firstBatchSize];
        boolean[] firstNulls = new boolean[firstBatchSize];
        for (int index = 0; index < firstBatchSize; index++) {
            firstKeys[index] = index + 1L;
            firstPayloads[index] = (index + 1L) * 10;
        }
        firstNulls[firstBatchSize - 1] = true;

        long[] secondKeys = new long[secondBatchSize];
        long[] secondPayloads = new long[secondBatchSize];
        boolean[] secondNulls = new boolean[secondBatchSize];
        for (int index = 0; index < secondBatchSize; index++) {
            secondKeys[index] = firstBatchSize + index + 1L;
            secondPayloads[index] = (firstBatchSize + index + 1L) * 10;
        }
        secondNulls[secondBatchSize - 1] = true;

        Operator inner = new Operator()
        {
            private int batchIndex;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return batchIndex < 2;
            }

            @Override
            public Batch next()
            {
                if (batchIndex == 0) {
                    batchIndex++;
                    return new Batch(
                            Mask.all(firstBatchSize),
                            Function.identity(),
                            new Output[] {
                                    Output.of(Streams.ofValues(new I64Vector(firstKeys))),
                                    Output.of(Streams.ofValuesAndNulls(new I64Vector(firstPayloads), new BooleanVector(firstNulls)))});
                }
                batchIndex++;
                return new Batch(
                        Mask.all(secondBatchSize),
                        Function.identity(),
                        new Output[] {
                                Output.of(Streams.ofValues(new I64Vector(secondKeys))),
                                Output.of(Streams.ofValuesAndNulls(new I64Vector(secondPayloads), new BooleanVector(secondNulls)))});
            }

            @Override
            public void constrain(Mask mask)
            {
            }

            @Override
            public void close()
            {
            }

            @Override
            public boolean supportsRetainedBatches()
            {
                return true;
            }
        };

        Batch batch = new HashJoinOperator(allocator, outer, 0, inner, 0).next();

        int rowCount = batch.borrowMask().count();
        assertThat(longValues(batch.output(0).borrow(Stream.VALUES), rowCount)).containsExactly(1L, firstBatchSize, firstBatchSize + secondBatchSize);
        assertThat(longValues(batch.output(2).borrow(Stream.VALUES), rowCount)).containsExactly(10L, firstBatchSize * 10L, (firstBatchSize + secondBatchSize) * 10L);
        assertThat(booleanValues(batch.output(2).borrow(Stream.NULLS), rowCount)).containsExactly(false, true, true);

        // The physical side-stream layout is free to be flat or dictionary-wrapped; the regression check is the
        // logical null values above, including matches from both retained inner pages.
    }

    @Test
    void testHashJoinOperatorSupportsI64EquiJoinWithDuplicateMatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(2L, 20L),
                        row(3L, 30L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2L, 200L),
                        row(2L, 201L),
                        row(3L, 300L),
                        row(4L, 400L))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(longValues(batch.output(0).borrow(Stream.VALUES), rowCount)).containsExactly(2L, 2L, 3L);
        assertThat(longValues(batch.output(1).borrow(Stream.VALUES), rowCount)).containsExactly(20L, 20L, 30L);
        assertThat(longValues(batch.output(3).borrow(Stream.VALUES), rowCount)).containsExactly(200L, 201L, 300L);
    }

    @Test
    void testHashJoinDenseSingleBatchReferencesFallBackWhenBuildRowsHaveHoles()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(
                        row(10L),
                        row(11L),
                        row(12L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(10L, 100L),
                        row((Object) null, 999L),
                        row(11L, 200L),
                        row(12L, 300L))),
                0);

        new OperatorAssertions.OperatorAssert(operator).matchesExactly(List.of(
                row(10L, 10L, 100L),
                row(11L, 11L, 200L),
                row(12L, 12L, 300L)));
    }

    @Test
    void testHashJoinDenseSingleBatchRangeOutputCompactsMatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(0L, 100L),
                        row(1L, 101L),
                        row(2L, 102L),
                        row(3L, 103L),
                        row(4L, 104L),
                        row(5L, 105L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2L, 200L),
                        row(3L, 300L),
                        row(4L, 400L))),
                0);

        new OperatorAssertions.OperatorAssert(operator).matchesExactly(List.of(
                row(2L, 102L, 2L, 200L),
                row(3L, 103L, 3L, 300L),
                row(4L, 104L, 4L, 400L)));
    }

    @Test
    void testHashJoinOperatorOutputCanFeedAnotherHashJoin()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new HashJoinOperator(
                allocator,
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(1L, 11L),
                                row(2L, 22L))),
                        0,
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(10L, 1L),
                                row(20L, 2L))),
                        1),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, "one"),
                        row(2L, "two"))),
                0)) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 11L, 10L, 1L, 1L, "one"),
                            row(2L, 22L, 20L, 2L, 2L, "two"));
        }
    }

    @Test
    void testSemiJoinOperatorFiltersUtf8Membership()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new SemiJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("alpha", 10L),
                        row("beta", 20L),
                        row("gamma", 30L))),
                0,
                new ConstantTableOperator(allocator, 1, List.of(
                        row("beta"),
                        row("gamma"),
                        row("gamma"))),
                0);

        new OperatorAssertions.OperatorAssert(operator).matchesExactly(List.of(
                row("beta", 20L),
                row("gamma", 30L)));
    }

    @Test
    void testSemiJoinOperatorFiltersI64Membership()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new SemiJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(10L, "alpha"),
                        row(20L, "beta"),
                        row(30L, "gamma"))),
                0,
                new ConstantTableOperator(allocator, 1, List.of(
                        row(20L),
                        row(30L),
                        row(30L))),
                0);

        new OperatorAssertions.OperatorAssert(operator).matchesExactly(List.of(
                row(20L, "beta"),
                row(30L, "gamma")));
    }

    @Test
    void testSemiJoinOperatorAntiJoinKeepsNullUtf8ProbeKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new SemiJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("alpha", 10L),
                        row("beta", 20L),
                        row((Object) null, 30L),
                        row("gamma", 40L))),
                0,
                new ConstantTableOperator(allocator, 1, List.of(
                        row("beta"),
                        row((Object) null),
                        row("delta"))),
                0,
                false);

        new OperatorAssertions.OperatorAssert(operator).matchesExactly(List.of(
                row("alpha", 10L),
                row((Object) null, 30L),
                row("gamma", 40L)));
    }

    @Test
    void testSemiJoinOperatorCanProjectMatchColumn()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new SemiJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("alpha", 10L),
                        row("beta", 20L),
                        row("gamma", 30L))),
                0,
                new ConstantTableOperator(allocator, 1, List.of(
                        row("beta"),
                        row("gamma"),
                        row("gamma"))),
                0,
                true,
                true)) {
            Batch batch = operator.next();
            assertThat(booleanValues(batch.output(2).borrow(Stream.VALUES), 3)).containsExactly(false, true, true);
            batch.close();
        }
    }

    @Test
    void testGroupIdOperatorExpandsGroupingSets()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new GroupIdOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row("store channel", "storeA", 10L),
                        row("web channel", "webB", 20L))),
                new int[][] {
                        {-1, -1, 2},
                        {0, -1, 2},
                        {0, 1, 2}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(null, null, 10L, 0L),
                            row(null, null, 20L, 0L),
                            row("store channel", null, 10L, 1L),
                            row("web channel", null, 20L, 1L),
                            row("store channel", "storeA", 10L, 2L),
                            row("web channel", "webB", 20L, 2L));
        }
    }

    @Test
    void testGroupIdOperatorUsesMappedTypeForNullExtendedOutput()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new GroupIdOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(row(10L, "detail"))),
                new int[][] {
                        {-1, 0},
                        {1, 0}},
                EngineResources.from(allocator).operatorResources().groupIdPolicy())) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(null, 10L, 0L),
                            row("detail", 10L, 1L));
        }
    }

    @Test
    void testHashJoinOperatorSupportsProbeOuterJoin()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, "matched"),
                        row(2L, "unmatched"))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 100L))),
                0,
                true)) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, "matched", 1L, 100L),
                            row(2L, "unmatched", null, null));
        }
    }

    @Test
    void testHashJoinOperatorEncodesAllUnmatchedBuildOutputAsSingleRuns()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(
                        row(2L),
                        row(3L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 100L))),
                0,
                true);
                Batch batch = operator.next()) {
            assertThat(batch.borrowMask()).containsExactly(0, 1);
            assertThat(batch.output(2).borrow(Stream.VALUES))
                    .isInstanceOfSatisfying(RleVector.class, values -> {
                        assertThat(values.counts()).containsExactly(2);
                        assertThat(values.values()).isInstanceOf(I64Vector.class);
                    });
            assertThat(batch.output(2).borrow(Stream.NULLS))
                    .isInstanceOfSatisfying(RleVector.class, nulls -> {
                        assertThat(nulls.counts()).containsExactly(2);
                        assertThat(((BooleanVector) nulls.values()).values()).containsExactly(true);
                    });
        }
    }

    @Test
    void testProbeOuterJoinUsesTypeFactoryForNullPayloadValues()
    {
        AtomicInteger nullValueConstructions = new AtomicInteger();
        TypeVectorFactory vectorFactory = new TypeVectorFactory()
        {
            @Override
            public Vector constant(VectorAllocator allocator, Object value, int length)
            {
                throw new AssertionError("constant construction is not expected");
            }

            @Override
            public Vector nullValues(VectorAllocator allocator, int length)
            {
                nullValueConstructions.incrementAndGet();
                I64Vector values = allocator.allocate(I64Vector.class, length, I64Vector::new);
                Arrays.fill(values.values(), 999);
                return values;
            }
        };
        TypeBinding payloadType = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("test:provider-owned-long");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(vectorFactory);
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class, RleVector.class);
            }
        };
        TypeBinding unspecified = Schema.unspecified(1).field(0).type();
        Schema outerSchema = new Schema(List.of(new Field(unspecified, false)));
        Schema innerSchema = new Schema(List.of(
                new Field(unspecified, false),
                new Field(payloadType, false)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new HashJoinOperator(
                allocator,
                new TableOperator(
                        outerSchema,
                        List.of(TableOperator.Page.values(
                                2,
                                new Vector[] {new I64Vector(new long[] {2, 3})},
                                Mask.all(2)))),
                0,
                new TableOperator(
                        innerSchema,
                        List.of(TableOperator.Page.values(
                                1,
                                new Vector[] {
                                        new I64Vector(new long[] {1}),
                                        new I64Vector(new long[] {100})},
                                Mask.all(1)))),
                0,
                true);
                Batch batch = operator.next()) {
            assertThat(batch.output(2).borrow(Stream.VALUES))
                    .isInstanceOfSatisfying(RleVector.class, values ->
                            assertThat(((I64Vector) values.values()).values()).containsExactly(999));
            assertThat(booleanValues(batch.output(2).borrow(Stream.NULLS), 2)).containsExactly(true, true);
            assertThat(nullValueConstructions).hasValue(1);
        }
    }

    @Test
    void testProbeOuterJoinNullStreamPreservesNullableBuildValues()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator joined = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(
                        row(1L),
                        row(2L),
                        row(3L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(1L, null),
                        row(2L, null))),
                0,
                true);
                Operator counted = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        List.of(new CountColumn(2)),
                        joined)) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(counted))
                    .containsExactly(
                            row(1L, 1L),
                            row(2L, 0L),
                            row(3L, 0L));
        }
    }

    @Test
    void testHashJoinOperatorPreservesNullableOuterPayloadForAggregations()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator joined = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, 100L, null),
                        row(1L, 100L, 10L),
                        row(2L, 200L, null),
                        row(3L, 300L, 5L))),
                0,
                new ConstantTableOperator(allocator, 1, List.of(
                        row(1L),
                        row(2L),
                        row(3L))),
                0);
                Operator aggregated = new GroupedAggregationOperator(
                        allocator,
                        List.of(1),
                        List.of(1),
                        List.of(new Sum(2), new CountColumn(2)),
                        joined)) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(aggregated))
                    .containsExactly(
                            row(100L, 10L, 1L),
                            row(200L, null, 0L),
                            row(300L, 5L, 1L));
        }
    }

    @Test
    void testEnforceSingleRowOperatorPassesThroughSingleRow()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new EnforceSingleRowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(row(11L, "value"))))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(row(11L, "value"));
        }
    }

    @Test
    void testEnforceSingleRowOperatorProducesNullRowForEmptyInput()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new EnforceSingleRowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of()))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(row(null, null));
        }
    }

    @Test
    void testEnforceSingleRowOperatorRejectsMultipleRows()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new EnforceSingleRowOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(11L), row(12L))))) {
            assertThatThrownBy(() -> OperatorAssertions.OperatorAssert.toRows(operator))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Scalar subquery returned multiple rows");
        }
    }

    @Test
    void testTopNOperatorPreservesBinaryOutputColumn()
    {
        BinaryVector names = new BinaryVector(4, 19);
        names.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        names.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        names.setBytes(0, "alpha".getBytes(UTF_8));
        names.setBytes(1, "beta".getBytes(UTF_8));
        names.setBytes(2, "gamma".getBytes(UTF_8));
        names.setBytes(3, "delta".getBytes(UTF_8));

        Operator operator = new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                0,
                new TableOperator(
                        2,
                        List.of(TableOperator.Page.values(
                                4,
                                new Vector[] {
                                        new I64Vector(new long[] {1L, 5L, 3L, 4L}),
                                        names,
                                },
                                org.weakref.nitro.data.Mask.all(4)))));

        Batch batch = operator.next();
        I64Vector ranks = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector resultNames = (BinaryVector) batch.output(1).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(ranks.values(), batch.borrowMask().count())).containsExactly(5L, 4L);
        assertThat(resultNames.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
        assertThat(resultNames.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isTrue();
        assertThat(utf8(resultNames, 0)).isEqualTo("beta");
        assertThat(utf8(resultNames, 1)).isEqualTo("delta");
    }

    @Test
    void testTopNOperatorPreservesArrayPayloadColumn()
    {
        ArrayVector payload = new ArrayVector(4);
        payload.offsets()[0] = 0;
        payload.offsets()[1] = 2;
        payload.offsets()[2] = 2;
        payload.offsets()[3] = 3;
        payload.offsets()[4] = 6;
        payload.setElements(Streams.ofValues(new I64Vector(new long[] {10L, 11L, 20L, 30L, 31L, 32L})));

        Operator operator = new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                0,
                new TableOperator(
                        2,
                        List.of(TableOperator.Page.values(
                                4,
                                new Vector[] {
                                        new I64Vector(new long[] {1L, 5L, 3L, 4L}),
                                        payload,
                                },
                                org.weakref.nitro.data.Mask.all(4)))));

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(
                        row(5L, List.of()),
                        row(4L, List.of(30L, 31L, 32L)));
    }

    @Test
    void testTopNOperatorOrdersUtf8Keys()
    {
        Operator operator = new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                0,
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        2,
                        List.of(
                                row("apple", 1L),
                                row("pear", 2L),
                                row("banana", 3L))));

        Batch batch = operator.next();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(utf8(keys, 0)).isEqualTo("pear");
        assertThat(utf8(keys, 1)).isEqualTo("banana");
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(2L, 3L);
    }

    @Test
    void testTopNOperatorOrdersNullsLastForAscendingUtf8Keys()
    {
        List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                3,
                0,
                false,
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        1,
                        List.of(
                                row("beta"),
                                row((Object) null),
                                row("alpha")))));

        assertThat(rows).containsExactly(
                row("alpha"),
                row("beta"),
                row((Object) null));
    }

    @Test
    void testTopNOperatorExcludesNullsAtAscendingCutoff()
    {
        List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                0,
                false,
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        1,
                        List.of(
                                row("beta"),
                                row((Object) null),
                                row("alpha"),
                                row((Object) null)))));

        assertThat(rows).containsExactly(
                row("alpha"),
                row("beta"));
    }

    void testTopNOperatorOrdersDoubles()
    {
        Operator operator = new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                0,
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        2,
                        List.of(
                                row(1.5, 10L),
                                row(3.25, 20L),
                                row(2.5, 30L))));

        Batch batch = operator.next();
        org.weakref.nitro.data.F64Vector values = (org.weakref.nitro.data.F64Vector) batch.output(0).borrow(Stream.VALUES);
        I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(values.values(), batch.borrowMask().count())).containsExactly(3.25, 2.5);
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(20L, 30L);
    }

    @Test
    void testTopNOperatorSupportsMultiKeyOrdering()
    {
        Operator operator = new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                3,
                new int[] {0, 1},
                new boolean[] {false, false},
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        3,
                        List.of(
                                row(20L, "pear", 1L),
                                row(10L, "pear", 2L),
                                row(10L, "apple", 3L),
                                row(10L, "banana", 4L))));

        Batch batch = operator.next();
        I64Vector first = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector second = (BinaryVector) batch.output(1).borrow(Stream.VALUES);
        I64Vector payload = (I64Vector) batch.output(2).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(first.values(), batch.borrowMask().count())).containsExactly(10L, 10L, 10L);
        assertThat(utf8(second, 0)).isEqualTo("apple");
        assertThat(utf8(second, 1)).isEqualTo("banana");
        assertThat(utf8(second, 2)).isEqualTo("pear");
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(3L, 4L, 2L);
    }

    @Test
    void testTopNSessionRetainsCandidatesAcrossHostBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator first = new ConstantTableOperator(
                allocator,
                2,
                List.of(
                        row(1L, "first"),
                        row(9L, "second")));
                TopNSession session = new TopNSession(
                        allocator,
                        3,
                        new int[] {0},
                        new boolean[] {true},
                        first.outputSchema())) {
            try (Batch batch = first.next()) {
                session.addInput(batch);
            }
            try (Operator second = new ConstantTableOperator(
                    allocator,
                    2,
                    List.of(
                            row(5L, "third"),
                            row(12L, "fourth")));
                    Batch batch = second.next()) {
                session.addInput(batch);
            }

            try (Batch result = session.finish().orElseThrow()) {
                I64Vector keys = (I64Vector) result.output(0).borrow(Stream.VALUES);
                BinaryVector payload = (BinaryVector) result.output(1).borrow(Stream.VALUES);
                assertThat(Arrays.copyOf(keys.values(), result.borrowMask().count())).containsExactly(12L, 9L, 5L);
                assertThat(utf8(payload, 0)).isEqualTo("fourth");
                assertThat(utf8(payload, 1)).isEqualTo("second");
                assertThat(utf8(payload, 2)).isEqualTo("third");
            }
        }
    }

    @Test
    void testSortSessionRetainsRowsAcrossHostBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator first = new ConstantTableOperator(
                allocator,
                2,
                List.of(
                        row(9L, "second"),
                        row(1L, "first")));
                SortSession session = new SortSession(
                        allocator,
                        new int[] {0},
                        new boolean[] {false},
                        first.outputSchema())) {
            try (Batch batch = first.next()) {
                session.addInput(batch);
            }
            try (Operator second = new ConstantTableOperator(
                    allocator,
                    2,
                    List.of(
                            row(12L, "fourth"),
                            row(5L, "third")));
                    Batch batch = second.next()) {
                session.addInput(batch);
            }

            try (Batch result = session.finish().orElseThrow()) {
                Vector keys = result.output(0).borrow(Stream.VALUES);
                Vector payload = result.output(1).borrow(Stream.VALUES);
                assertThat(longValues(keys, result.borrowMask().count())).containsExactly(1L, 5L, 9L, 12L);
                assertThat(utf8(payload, 0)).isEqualTo("first");
                assertThat(utf8(payload, 1)).isEqualTo("third");
                assertThat(utf8(payload, 2)).isEqualTo("second");
                assertThat(utf8(payload, 3)).isEqualTo("fourth");
            }
        }
    }

    @Test
    void testLimitSessionPreservesRemainingRowsAcrossHostBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (LimitSession session = new LimitSession(allocator, 3);
                Operator first = new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(row(1L), row(2L)));
                Batch firstBatch = first.next()) {
            assertThat(session.select(firstBatch).orElseThrow().count()).isEqualTo(2);
            assertThat(session.isFinished()).isFalse();

            try (Operator second = new ConstantTableOperator(
                    allocator,
                    1,
                    List.of(row(3L), row(4L)));
                    Batch secondBatch = second.next()) {
                assertThat(session.select(secondBatch).orElseThrow().count()).isOne();
                assertThat(longValues(secondBatch.output(0).borrow(Stream.VALUES), secondBatch.borrowMask().count()))
                        .containsExactly(3L);
                assertThat(session.isFinished()).isTrue();
            }
        }
    }

    @Test
    void testTopNOperatorSupportsProjectedUtf8OrderingWithSlotReuse()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveRegistry primitiveRegistry = TestPrimitiveFunctions.primitiveRegistry();
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(),
                List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(new Input(4), Stream.VALUES)));

        Operator operator = new TopNOperator(
                allocator,
                3,
                new int[] {0, 1, 2, 3},
                new boolean[] {false, false, false, false},
                new ProjectOperator(
                        allocator,
                        evaluationPlan,
                        primitiveRegistry,
                        new ConstantTableOperator(
                                allocator,
                                5,
                                List.of(
                                        row("VA", "M", "D", 4L, 40L),
                                        row("MS", "M", "D", 3L, 30L),
                                        row("OR", "M", "S", 5L, 50L),
                                        row("AL", "F", "M", 2L, 20L),
                                        row("AK", "F", "D", 1L, 10L),
                                        row("NJ", "F", "D", 6L, 60L)))));

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(
                        row("AK", "F", "D", 1L, 10L),
                        row("AL", "F", "M", 2L, 20L),
                        row("MS", "M", "D", 3L, 30L));
    }

    @Test
    void testTopNOperatorDefersPayloadBorrowUntilOutputIsRequested()
    {
        AtomicInteger keyBorrows = new AtomicInteger();
        AtomicInteger payloadBorrows = new AtomicInteger();
        I64Vector keys = new I64Vector(new long[] {1L, 5L, 3L, 4L});
        I64Vector payloads = new I64Vector(new long[] {10L, 20L, 30L, 40L});
        Operator source = new SingleBatchOperator(
                2,
                org.weakref.nitro.data.Mask.all(4),
                () -> new Output[] {
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            keyBorrows.incrementAndGet();
                            return keys;
                        }),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            payloadBorrows.incrementAndGet();
                            return payloads;
                        }),
                });

        Operator operator = new TopNOperator(new Allocator(EngineResources.createDefault()), 2, 0, source);

        Batch batch = operator.next();

        assertThat(keyBorrows.get()).isGreaterThan(0);
        assertThat(payloadBorrows.get()).isZero();

        I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);
        assertThat(Arrays.copyOf(payload.values(), batch.borrowMask().count())).containsExactly(20L, 40L);
        assertThat(payloadBorrows.get()).isGreaterThan(0);
    }

    @Test
    void testSingleBatchOperatorPropagatesTopNConstraintToLazyDelegate()
    {
        AtomicInteger payloadRows = new AtomicInteger();
        Mask mask = Mask.all(100_000);
        Batch delegate = new Batch(
                mask,
                constrained -> payloadRows.set(constrained.count()),
                java.util.function.Function.identity(),
                new Output(
                        Set.of(Stream.VALUES),
                        ignored -> new I64Vector(100_000),
                        (stream, vector) -> vector,
                        (stream, vector) -> {},
                        (existing, sourcePosition, outputPosition, size) -> {
                            payloadRows.incrementAndGet();
                            I64Vector output = existing == null ? new I64Vector(size) : (I64Vector) existing.values();
                            output.values()[outputPosition] = sourcePosition;
                            return Streams.ofValues(output);
                        }),
                new Output(
                        Set.of(Stream.VALUES),
                        ignored -> {
                            payloadRows.set(mask.count());
                            return new I64Vector(100_000);
                        },
                        (stream, vector) -> vector,
                        (stream, vector) -> {},
                        (existing, sourcePosition, outputPosition, size) -> {
                            payloadRows.incrementAndGet();
                            I64Vector output = existing == null ? new I64Vector(size) : (I64Vector) existing.values();
                            output.values()[outputPosition] = sourcePosition;
                            return Streams.ofValues(output);
                        }));

        try (Operator operator = new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                0,
                new SingleBatchOperator(Schema.unspecified(2), delegate));
                Batch output = operator.next()) {
            output.output(1).borrow(Stream.VALUES);
            assertThat(payloadRows.get()).isEqualTo(4);
        }
    }

    @Test
    void testTopNOperatorMaterializesOnlyConstrainedPayloadRows()
    {
        BinaryVector payloads = new BinaryVector(3, 16);
        payloads.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        payloads.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        payloads.setBytes(0, "ccc".getBytes(UTF_8));
        payloads.setBytes(1, "bb".getBytes(UTF_8));
        payloads.setBytes(2, "a".getBytes(UTF_8));

        Operator operator = new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                3,
                0,
                new TableOperator(
                        2,
                        List.of(TableOperator.Page.values(
                                3,
                                new Vector[] {
                                        new I64Vector(new long[] {3L, 2L, 1L}),
                                        payloads,
                                },
                                org.weakref.nitro.data.Mask.all(3)))));

        Batch batch = operator.next();
        batch.constrain(Mask.sparse(new int[] {2}, 3));

        BinaryVector payload = (BinaryVector) batch.output(1).borrow(Stream.VALUES);

        assertThat(payload.length()).isEqualTo(3);
        assertThat(payload.length(0)).isZero();
        assertThat(payload.length(1)).isZero();
        assertThat(utf8(payload, 2)).isEqualTo("a");
    }

    @Test
    void testTopNOperatorClearsNullablePayloadWhenSlotIsReused()
    {
        List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                0,
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        3,
                        List.of(
                                row(1L, null, null),
                                row(3L, 0L, 0L),
                                row(2L, 5L, 7L)))));

        assertThat(rows).containsExactly(
                row(3L, 0L, 0L),
                row(2L, 5L, 7L));
    }

    @Test
    void testTopNOperatorClearsNullableOrderingAndPayloadWhenSlotIsReused()
    {
        List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                2,
                new int[] {2},
                new boolean[] {false},
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        3,
                        List.of(
                                row(10L, null, null),
                                row(20L, 0L, 0L),
                                row(30L, 5L, 7L)))));

        assertThat(rows).containsExactly(
                row(20L, 0L, 0L),
                row(30L, 5L, 7L));
    }

    @Test
    void testTopNOperatorPreservesZeroNullablePayloadWithUtf8Ordering()
    {
        List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                6,
                new int[] {0, 1, 2, 5},
                new boolean[] {false, false, false, false},
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        6,
                        List.of(
                                row("Zulu", "Zulu", "Zulu", 1_000_000L, null, null),
                                row("Yankee", "Yankee", "Yankee", 1_000_001L, null, null),
                                row("Abney", "Leif", "Oak Grove", 1_797_015L, 41_140L, -1_657_813L),
                                row("Abney", "Leif", "Oak Grove", 1_797_015L, 0L, -157_588L),
                                row("Abraham", "Ann", "Pleasant Hill", 1_505_848L, 21_592L, -174_653L),
                                row("Abraham", "Ann", "Pleasant Hill", 1_505_848L, 0L, 0L),
                                row("Abrams", "Glayds", "Oak Grove", 638_006L, 6_370L, -998_310L),
                                row("Abrams", "Glayds", "Oak Grove", 638_006L, 0L, 0L)))));

        assertThat(rows).containsExactly(
                row("Abney", "Leif", "Oak Grove", 1_797_015L, 41_140L, -1_657_813L),
                row("Abney", "Leif", "Oak Grove", 1_797_015L, 0L, -157_588L),
                row("Abraham", "Ann", "Pleasant Hill", 1_505_848L, 21_592L, -174_653L),
                row("Abraham", "Ann", "Pleasant Hill", 1_505_848L, 0L, 0L),
                row("Abrams", "Glayds", "Oak Grove", 638_006L, 6_370L, -998_310L),
                row("Abrams", "Glayds", "Oak Grove", 638_006L, 0L, 0L));
    }

    @Test
    void testOffsetOperatorReusesPrefetchedTopNBatch()
    {
        CountingNextOperator source = new CountingNextOperator(new TopNOperator(
                new Allocator(EngineResources.createDefault()),
                3,
                0,
                new ConstantTableOperator(
                        new Allocator(EngineResources.createDefault()),
                        1,
                        List.of(
                                row(5L),
                                row(4L),
                                row(3L),
                                row(2L)))));

        Operator operator = new OffsetOperator(new Allocator(EngineResources.createDefault()), 1, source);

        assertThat(operator.hasNext()).isTrue();
        Batch batch = operator.next();
        Mask mask = batch.borrowMask();
        I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES);

        assertThat(source.nextCount()).isEqualTo(1);
        assertThat(mask.selectedCount()).isEqualTo(2);
        assertThat(mask.position(0)).isEqualTo(1);
        assertThat(mask.position(1)).isEqualTo(2);
        assertThat(values.values()[mask.position(0)]).isEqualTo(4L);
        assertThat(values.values()[mask.position(1)]).isEqualTo(3L);
    }

    @Test
    void testOperatorAssertionsToRowsClosesOffsetBatches()
    {
        Operator operator = new OffsetOperator(
                new Allocator(EngineResources.createDefault()),
                1,
                new TopNOperator(
                        new Allocator(EngineResources.createDefault()),
                        3,
                        0,
                        new ConstantTableOperator(
                                new Allocator(EngineResources.createDefault()),
                                1,
                                List.of(
                                        row(5L),
                                        row(4L),
                                        row(3L),
                                        row(2L)))));

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(row(4L), row(3L));
    }

    @Test
    void testNestedLoopJoinOperatorProducesJoinBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L))),
                new ConstantTableOperator(allocator, 1, List.of(row(10L), row(20L))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(1L, 2L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(10L, 10L);
    }

    @Test
    void testNestedLoopJoinOperatorPreservesBinaryPayloadColumn()
    {
        BinaryVector names = new BinaryVector(2, 9);
        names.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        names.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        names.setBytes(0, "red".getBytes(UTF_8));
        names.setBytes(1, "blue".getBytes(UTF_8));

        Operator operator = new NestedLoopJoinOperator(
                new Allocator(EngineResources.createDefault()),
                new TableOperator(
                        1,
                        List.of(TableOperator.Page.values(
                                2,
                                new Vector[] {new I64Vector(new long[] {1L, 2L})},
                                org.weakref.nitro.data.Mask.all(2)))),
                new TableOperator(
                        1,
                        List.of(TableOperator.Page.values(
                                2,
                                new Vector[] {names},
                                org.weakref.nitro.data.Mask.all(2)))));

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        I64Vector ids = (I64Vector) batch.output(0).borrow(Stream.VALUES);
        BinaryVector payload = (BinaryVector) batch.output(1).borrow(Stream.VALUES);

        assertThat(Arrays.copyOf(ids.values(), rowCount)).containsExactly(1L, 2L);
        assertThat(payload.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
        assertThat(payload.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isTrue();
        assertThat(utf8(payload, 0)).isEqualTo("red");
        assertThat(utf8(payload, 1)).isEqualTo("red");
    }

    @Test
    void testNestedLoopJoinOperatorSupportsI64EquiJoin()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(2L, 20L),
                        row(3L, 30L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(2L, 200L),
                        row(3L, 300L),
                        row(4L, 400L))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        assertThat(Arrays.copyOf(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(20L, 30L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(2).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(2L, 3L);
        assertThat(Arrays.copyOf(((I64Vector) batch.output(3).borrow(Stream.VALUES)).values(), rowCount)).containsExactly(200L, 300L);
    }

    @Test
    void testNestedLoopJoinOperatorSupportsUtf8EquiJoin()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("alpha", 10L),
                        row("beta", 20L),
                        row((Object) null, 30L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("beta", 200L),
                        row("alpha", 100L),
                        row((Object) null, 300L))),
                0);

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        BinaryVector keys = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
        I64Vector leftPayload = (I64Vector) batch.output(1).borrow(Stream.VALUES);
        I64Vector rightPayload = (I64Vector) batch.output(3).borrow(Stream.VALUES);

        assertThat(utf8(keys, 0)).isEqualTo("alpha");
        assertThat(utf8(keys, 1)).isEqualTo("beta");
        assertThat(Arrays.copyOf(leftPayload.values(), rowCount)).containsExactly(10L, 20L);
        assertThat(Arrays.copyOf(rightPayload.values(), rowCount)).containsExactly(100L, 200L);
    }

    @Test
    void testNestedLoopJoinOperatorSupportsMultiKeyEquiJoin()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "alpha", 10L),
                        row(1L, "beta", 20L),
                        row(2L, "alpha", 30L))),
                new int[] {0, 1},
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "beta", 200L),
                        row(1L, "alpha", 100L),
                        row(2L, "beta", 300L))),
                new int[] {0, 1});

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        Vector leftIds = batch.output(0).borrow(Stream.VALUES);
        Vector leftNames = batch.output(1).borrow(Stream.VALUES);
        Vector leftPayload = batch.output(2).borrow(Stream.VALUES);
        Vector rightPayload = batch.output(5).borrow(Stream.VALUES);

        assertThat(longValues(leftIds, rowCount)).containsExactly(1L, 1L);
        assertThat(utf8(leftNames, 0)).isEqualTo("alpha");
        assertThat(utf8(leftNames, 1)).isEqualTo("beta");
        assertThat(longValues(leftPayload, rowCount)).containsExactly(10L, 20L);
        assertThat(longValues(rightPayload, rowCount)).containsExactly(100L, 200L);
    }

    @Test
    void testHashJoinOperatorSupportsMultiKeyEquiJoin()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "alpha", 10L),
                        row(1L, "beta", 20L),
                        row(2L, "alpha", 30L))),
                new int[] {0, 1},
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, "beta", 200L),
                        row(1L, "alpha", 100L),
                        row(2L, "beta", 300L))),
                new int[] {0, 1});

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        Vector leftIds = batch.output(0).borrow(Stream.VALUES);
        Vector leftNames = batch.output(1).borrow(Stream.VALUES);
        Vector leftPayload = batch.output(2).borrow(Stream.VALUES);
        Vector rightPayload = batch.output(5).borrow(Stream.VALUES);

        assertThat(longValues(leftIds, rowCount)).containsExactly(1L, 1L);
        assertThat(utf8(leftNames, 0)).isEqualTo("alpha");
        assertThat(utf8(leftNames, 1)).isEqualTo("beta");
        assertThat(longValues(leftPayload, rowCount)).containsExactly(10L, 20L);
        assertThat(longValues(rightPayload, rowCount)).containsExactly(100L, 200L);
    }

    @Test
    void testHashJoinOperatorPreservesLargeSingleBatchPositionsForDuplicateLongPairs()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        int duplicatePosition = 70_000;
        int rowCount = duplicatePosition + 1;
        long[] firstKeys = new long[rowCount];
        long[] secondKeys = new long[rowCount];
        long[] payloads = new long[rowCount];
        for (int position = 0; position < rowCount; position++) {
            firstKeys[position] = position;
            secondKeys[position] = 0;
            payloads[position] = position;
        }
        // Make the final row duplicate the first pair. Its logical position no longer fits the legacy 16-bit packed
        // row-reference lane, but it does fit the pair index's position-only single-batch representation.
        firstKeys[duplicatePosition] = 0;

        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(row(0L, 0L))),
                new int[] {0, 1},
                new TableOperator(
                        3,
                        List.of(TableOperator.Page.values(
                                rowCount,
                                new Vector[] {
                                        new I64Vector(firstKeys),
                                        new I64Vector(secondKeys),
                                        new I64Vector(payloads),
                                },
                                Mask.all(rowCount)))),
                new int[] {0, 1});

        Batch batch = operator.next();
        int outputRows = batch.borrowMask().count();
        assertThat(outputRows).isEqualTo(2);
        assertThat(longValues(batch.output(4).borrow(Stream.VALUES), outputRows))
                .containsExactly(0L, (long) duplicatePosition);
    }

    @Test
    void testHashJoinOperatorSupportsThreeLongJoinKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 4, List.of(
                        row(1998L, 11L, 101L, 10L),
                        row(1998L, 11L, 102L, 20L),
                        row(1999L, 11L, 101L, 30L))),
                new int[] {0, 1, 2},
                new ConstantTableOperator(allocator, 4, List.of(
                        row(1998L, 11L, 102L, 200L),
                        row(1998L, 11L, 101L, 100L),
                        row(1999L, 11L, 103L, 300L))),
                new int[] {0, 1, 2});

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        Vector years = batch.output(0).borrow(Stream.VALUES);
        Vector items = batch.output(1).borrow(Stream.VALUES);
        Vector customers = batch.output(2).borrow(Stream.VALUES);
        Vector leftPayload = batch.output(3).borrow(Stream.VALUES);
        Vector rightPayload = batch.output(7).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(2);
        assertThat(longValues(years, rowCount)).containsExactly(1998L, 1998L);
        assertThat(longValues(items, rowCount)).containsExactly(11L, 11L);
        assertThat(longValues(customers, rowCount)).containsExactly(101L, 102L);
        assertThat(longValues(leftPayload, rowCount)).containsExactly(10L, 20L);
        assertThat(longValues(rightPayload, rowCount)).containsExactly(100L, 200L);
    }

    @Test
    void testHashJoinOperatorSupportsTwoUtf8JoinKeys()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row("AL", "Walker County", 11L),
                        row("AL", "Madison County", 12L),
                        row("TN", "Williamson County", 13L))),
                new int[] {0, 1},
                new ConstantTableOperator(allocator, 3, List.of(
                        row("AL", "Walker County", 101L),
                        row("AL", "Mobile County", 102L),
                        row("TN", "Williamson County", 103L),
                        row("TN", "Shelby County", 104L))),
                new int[] {0, 1});

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        Vector leftStates = batch.output(0).borrow(Stream.VALUES);
        Vector leftCounties = batch.output(1).borrow(Stream.VALUES);
        Vector leftPayload = batch.output(2).borrow(Stream.VALUES);
        Vector rightPayload = batch.output(5).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(2);
        assertThat(utf8(leftStates, 0)).isEqualTo("AL");
        assertThat(utf8(leftCounties, 0)).isEqualTo("Walker County");
        assertThat(longValue(leftPayload, 0)).isEqualTo(11L);
        assertThat(longValue(rightPayload, 0)).isEqualTo(101L);
        assertThat(utf8(leftStates, 1)).isEqualTo("TN");
        assertThat(utf8(leftCounties, 1)).isEqualTo("Williamson County");
        assertThat(longValue(leftPayload, 1)).isEqualTo(13L);
        assertThat(longValue(rightPayload, 1)).isEqualTo(103L);
    }

    @Test
    void testHashJoinOperatorCopiesDictionaryWrappedInnerPayloads()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        BinaryVector payloadValues = new BinaryVector(2, "alpha".length() + "beta".length());
        payloadValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        payloadValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        payloadValues.setBytes(0, "alpha".getBytes(UTF_8));
        payloadValues.setBytes(1, "beta".getBytes(UTF_8));
        Operator inner = new SingleBatchOperator(
                2,
                Mask.all(2),
                () -> new Output[] {
                        Output.of(Streams.ofValues(new I64Vector(new long[] {2L, 1L}))),
                        Output.of(Streams.ofValues(DictionaryVector.wrap(new int[] {1, 0}, payloadValues))),
                });

        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 10L),
                        row(2L, 20L))),
                new int[] {0},
                inner,
                new int[] {0});

        Batch batch = operator.next();
        int rowCount = batch.borrowMask().count();
        Vector leftPayload = batch.output(1).borrow(Stream.VALUES);
        Vector rightPayload = batch.output(3).borrow(Stream.VALUES);

        assertThat(rowCount).isEqualTo(2);
        assertThat(longValues(leftPayload, rowCount)).containsExactly(10L, 20L);
        assertThat(utf8(rightPayload, 0)).isEqualTo("alpha");
        assertThat(utf8(rightPayload, 1)).isEqualTo("beta");
    }

    @Test
    void testHashJoinOperatorPreservesSparseBinaryInnerPayloadsAfterConstrain()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L))),
                new int[] {0},
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, "alpha"),
                        row(2L, "beta"),
                        row(3L, "gamma"),
                        row(4L, "delta"))),
                new int[] {0});

        Batch batch = operator.next();
        batch.constrain(Mask.sparse(new int[] {0, 2}, 4));

        Vector rightPayload = batch.output(2).borrow(Stream.VALUES);
        assertThat(utf8(rightPayload, 0)).isEqualTo("alpha");
        assertThat(utf8(rightPayload, 2)).isEqualTo("gamma");
    }

    @Test
    void testHashJoinOperatorPreservesArrayPayloadColumn()
    {
        ArrayVector payload = new ArrayVector(3);
        payload.offsets()[0] = 0;
        payload.offsets()[1] = 2;
        payload.offsets()[2] = 3;
        payload.offsets()[3] = 3;
        payload.setElements(Streams.ofValues(new I64Vector(new long[] {100L, 101L, 200L})));

        Operator operator = new HashJoinOperator(
                new Allocator(EngineResources.createDefault()),
                new TableOperator(
                        1,
                        List.of(TableOperator.Page.values(
                                2,
                                new Vector[] {new I64Vector(new long[] {1L, 2L})},
                                org.weakref.nitro.data.Mask.all(2)))),
                0,
                new TableOperator(
                        2,
                        List.of(TableOperator.Page.values(
                                3,
                                new Vector[] {
                                        new I64Vector(new long[] {2L, 1L, 3L}),
                                        payload,
                                },
                                org.weakref.nitro.data.Mask.all(3)))),
                0);

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(
                        row(1L, 1L, List.of(200L)),
                        row(2L, 2L, List.of(100L, 101L)));
    }

    @Test
    void testHashJoinOperatorSupportsMultipleOuterPages()
    {
        BinaryVector firstPageKeys = new BinaryVector(2, 4);
        firstPageKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        firstPageKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        firstPageKeys.setBytes(0, "aa".getBytes(UTF_8));
        firstPageKeys.setBytes(1, "bb".getBytes(UTF_8));

        BinaryVector secondPageKeys = new BinaryVector(2, 4);
        secondPageKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        secondPageKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        secondPageKeys.setBytes(0, "aa".getBytes(UTF_8));
        secondPageKeys.setBytes(1, "bb".getBytes(UTF_8));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new TableOperator(
                        2,
                        List.of(
                                TableOperator.Page.values(
                                        2,
                                        new Vector[] {
                                                firstPageKeys,
                                                new I64Vector(new long[] {10L, 20L}),
                                        },
                                        org.weakref.nitro.data.Mask.all(2)),
                                TableOperator.Page.values(
                                        2,
                                        new Vector[] {
                                                secondPageKeys,
                                                new I64Vector(new long[] {30L, 40L}),
                                        },
                                        org.weakref.nitro.data.Mask.all(2)))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(
                        row("aa", 100L),
                        row("bb", 200L))),
                0);

        assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                .containsExactly(
                        row("aa", 10L, "aa", 100L),
                        row("bb", 20L, "bb", 200L),
                        row("aa", 30L, "aa", 100L),
                        row("bb", 40L, "bb", 200L));
    }

    @Test
    void testHashJoinOperatorPreservesLazyNonRetainedOuterPayloadAcrossOutputBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        List<org.weakref.nitro.data.Row> innerRows = new ArrayList<>();
        for (int index = 0; index < 4_500; index++) {
            innerRows.add(row(1L, 1000L + index));
        }
        for (int index = 0; index < 6_000; index++) {
            innerRows.add(row(2L, 2000L + index));
        }
        innerRows.add(row(3L, 3000L));

        try (Operator join = new HashJoinOperator(
                allocator,
                lazyNonRetainedOuterOperator(
                        new long[] {1L, 2L, 3L},
                        new long[] {10L, 20L, 30L}),
                0,
                new ConstantTableOperator(allocator, 2, innerRows),
                0)) {
            try (Batch first = join.next()) {
                assertThat(first.borrowMask().count()).isEqualTo(10_000);
                assertThat(((I64Vector) first.output(1).borrow(Stream.VALUES)).values()[9_999]).isEqualTo(20L);
            }

            try (Batch second = join.next()) {
                assertThat(second.borrowMask().count()).isEqualTo(501);
                I64Vector outerPayloads = (I64Vector) second.output(1).borrow(Stream.VALUES);
                I64Vector innerPayloads = (I64Vector) second.output(3).borrow(Stream.VALUES);

                for (int index = 0; index < 500; index++) {
                    assertThat(outerPayloads.values()[index]).isEqualTo(20L);
                }
                assertThat(outerPayloads.values()[500]).isEqualTo(30L);
                assertThat(innerPayloads.values()[500]).isEqualTo(3000L);
            }
        }
    }

    @Test
    void testHashJoinOperatorPreservesObservedSchemaOnEmptyResult()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"))),
                0,
                new ConstantTableOperator(allocator, 1, List.of()),
                0);

        Batch batch = operator.next();
        assertThat(batch.borrowMask().count()).isZero();
        assertThat(batch.output(0).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(1).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
        assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
    }

    @Test
    void testNestedLoopJoinOperatorPreservesObservedSchemaOnEmptyResult()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"))),
                0,
                new ConstantTableOperator(allocator, 1, List.of()),
                0);

        Batch batch = operator.next();
        assertThat(batch.borrowMask().count()).isZero();
        assertThat(batch.output(0).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(1).streams()).containsExactly(Stream.VALUES, Stream.NULLS);
        assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
        assertThat(batch.output(1).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
    }

    @Test
    void testTableOperatorProducesBatch()
    {
        Operator operator = new TableOperator(
                1,
                List.of(TableOperator.Page.values(
                        2,
                        new org.weakref.nitro.data.Vector[] {new I64Vector(new long[] {7L, 8L})},
                        org.weakref.nitro.data.Mask.all(2))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(7L, 8L);
    }

    @Test
    void testTableOperatorProducesExplicitStreams()
    {
        Operator operator = new TableOperator(
                1,
                List.of(new TableOperator.Page(
                        2,
                        new Streams[] {Streams.ofValuesAndNulls(
                                new I64Vector(new long[] {7L, 8L}),
                                new BooleanVector(new boolean[] {false, true}))},
                        org.weakref.nitro.data.Mask.all(2))));

        Batch batch = operator.next();
        assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(7L, 8L);
        assertThat(((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values()).containsExactly(false, true);
    }

    private static String utf8(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector binary -> new String(binary.copyBytes(position), UTF_8);
            case DictionaryVector dictionary -> utf8(dictionary.values(), dictionary.ids()[position]);
            case org.weakref.nitro.data.RleVector rle -> utf8(rle.values(), 0);
            default -> throw new IllegalArgumentException("Unsupported utf8 vector: " + vector.getClass().getSimpleName());
        };
    }

    private static long[] longValues(Vector vector, int count)
    {
        long[] values = new long[count];
        for (int index = 0; index < count; index++) {
            values[index] = longValue(vector, index);
        }
        return values;
    }

    private static long longValue(Vector vector, int position)
    {
        return VectorAccess.longValues(vector).value(position);
    }

    private static boolean[] booleanValues(Vector vector, int count)
    {
        boolean[] values = new boolean[count];
        VectorAccess.BooleanValues booleanValues = VectorAccess.booleanValues(vector);
        for (int index = 0; index < count; index++) {
            values[index] = booleanValues.value(index);
        }
        return values;
    }

    private static Operator lazyNonRetainedOuterOperator(long[] keys, long[] payloadValues)
    {
        I64Vector keyVector = new I64Vector(keys);
        long[] payloadCopy = Arrays.copyOf(payloadValues, payloadValues.length);
        SingleBatchOperator[] operatorHolder = new SingleBatchOperator[1];
        SingleBatchOperator operator = new SingleBatchOperator(
                2,
                Mask.all(keys.length),
                () -> new Output[] {
                        new Output(Set.of(Stream.VALUES), ignored -> keyVector),
                        new Output(Set.of(Stream.VALUES), ignored -> lazyPayload(payloadCopy, operatorHolder[0].currentMask())),
                });
        operatorHolder[0] = operator;
        return operator;
    }

    private static I64Vector lazyPayload(long[] payloadValues, Mask mask)
    {
        I64Vector payload = new I64Vector(payloadValues.length);
        if (mask.all()) {
            System.arraycopy(payloadValues, 0, payload.values(), 0, payloadValues.length);
            return payload;
        }
        for (int index = 0; index < mask.count(); index++) {
            int position = mask.position(index);
            payload.values()[position] = payloadValues[position];
        }
        return payload;
    }
}
