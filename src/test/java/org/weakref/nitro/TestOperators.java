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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.SelectedPositions;
import org.weakref.nitro.data.SelectionVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.DistinctCount;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.MarkDistinctOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.First;
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
import org.weakref.nitro.operator.evaluator.ir.MaterializationPolicy;
import org.weakref.nitro.operator.evaluator.ir.MemoizationPolicy;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestOperators
{
    private final Allocator allocator = new Allocator();

    @AfterAll
    void tearDown()
    {
        System.out.println(allocator);
    }

    @Test
    void testComplex()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable twenty = new Variable(0);
        Variable forty = new Variable(1);
        Variable lessThanTwenty = new Variable(2);
        Variable greaterThanForty = new Variable(3);
        Variable predicate = new Variable(4);
        EvaluationPlan filterPlan = plan(
                List.of(
                        literal(twenty, 20),
                        literal(forty, 40),
                        call(lessThanTwenty, "lt", values(new Input(0)), values(twenty)),
                        call(greaterThanForty, "lt", values(forty), values(new Input(0))),
                        call(predicate, "or", values(lessThanTwenty), values(greaterThanForty))),
                values(predicate));

        Variable two = new Variable(0);
        Variable doubled = new Variable(1);
        EvaluationPlan projectPlan = plan(
                List.of(
                        literal(two, 2),
                        call(doubled, "multiply", values(new Input(1)), values(two))),
                values(doubled));

        /*
          SELECT min(v), max(v), sum(v), count(*)
          FROM (
            SELECT y * 2 AS v
            FROM TABLE(sequences([0, 100], 50) t(x, y)
            WHERE x < 20 OR x > 40;
            LIMIT 5)
         */
        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(
                                new Min(0),
                                new Max(0),
                                new Sum(0),
                                new CountAll()),
                        new LimitOperator(
                                allocator,
                                5,
                                new ProjectOperator(
                                        allocator,
                                        projectPlan,
                                        primitiveRegistry,
                                        new FilterOperator(
                                                new GeneratorOperator(
                                                        allocator,
                                                        50,
                                                        10,
                                                        List.of(
                                                                new SequenceGenerator(0),
                                                                new SequenceGenerator(100))),
                                                filterPlan,
                                                primitiveRegistry,
                                                values(predicate),
                                                allocator))))))
                .matchesExactly(List.of(row(200L, 208L, 1020L, 5L)));
    }

    @Test
    void testProjectOperatorUsesPlanEvaluator()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        sum,
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(sum, Stream.VALUES)),
                Map.of(new Reference(sum, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        assertThat(operator(
                new ProjectOperator(
                        allocator,
                        evaluationPlan,
                        primitiveRegistry,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 10L),
                                        row(2L, 20L),
                                        row(3L, 30L))))))
                .matchesExactly(List.of(
                        row(11L),
                        row(22L),
                        row(33L)));
    }

    @Test
    void testProjectOperatorPreservesDictionaryInput()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable projected = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        projected,
                        new org.weakref.nitro.operator.evaluator.ir.Copy(new Reference(new Input(0), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(projected, Stream.VALUES)));

        DictionaryVector dictionary = new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}));
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
                return new Batch(Mask.all(4), Output.of(Streams.ofValues(dictionary)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        assertThat(operator(new ProjectOperator(allocator, evaluationPlan, primitiveRegistry, source)))
                .matchesExactly(List.of(row(30L), row(10L), row(20L), row(30L)));
    }

    @Test
    void testProjectOperatorSupportsNestedDictionaryIntegerDispatch()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable projected = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        projected,
                        new Call("subtract", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(projected, Stream.VALUES)));

        DictionaryVector left = DictionaryVector.wrap(
                new int[] {3, 0, 2, 1},
                DictionaryVector.wrap(new int[] {2, 1, 0, 1}, new I64Vector(new long[] {10, 20, 30})));
        DictionaryVector right = DictionaryVector.wrap(
                new int[] {2, 1, 0, 3},
                DictionaryVector.wrap(new int[] {1, 0, 1, 2}, new I64Vector(new long[] {1, 2, 3})));

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
                return new Batch(Mask.all(4),
                        Output.of(Streams.ofValues(left)),
                        Output.of(Streams.ofValues(right)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        assertThat(operator(new ProjectOperator(allocator, evaluationPlan, primitiveRegistry, source)))
                .matchesExactly(List.of(
                        row(18L),
                        row(29L),
                        row(8L),
                        row(17L)));
    }

    @Test
    void testTopNOperatorOrdersNestedDictionaryValues()
    {
        DictionaryVector ordering = DictionaryVector.wrap(
                new int[] {3, 0, 2, 1},
                DictionaryVector.wrap(new int[] {2, 1, 0, 1}, new I64Vector(new long[] {10, 20, 30})));
        DictionaryVector payload = DictionaryVector.wrap(
                new int[] {0, 1, 2, 3},
                new I64Vector(new long[] {100, 200, 300, 400}));

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
                return new Batch(Mask.all(4),
                        Output.of(Streams.ofValues(ordering)),
                        Output.of(Streams.ofValues(payload)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        assertThat(operator(new TopNOperator(allocator, 4, 0, false, source)))
                .matchesExactly(List.of(
                        row(10L, 300L),
                        row(20L, 100L),
                        row(20L, 400L),
                        row(30L, 200L)));
    }

    @Test
    void testDictionaryWrapComposesNestedIds()
    {
        DictionaryVector nested = DictionaryVector.wrap(
                new int[] {3, 0, 2, 1},
                DictionaryVector.wrap(new int[] {2, 1, 0, 1}, new I64Vector(new long[] {10, 20, 30})));

        assertThat(nested.values()).isInstanceOf(I64Vector.class);
        assertThat(nested.ids()).containsExactly(1, 2, 0, 1);
        assertThat(((I64Vector) nested.values()).values()[nested.ids()[0]]).isEqualTo(20L);
        assertThat(((I64Vector) nested.values()).values()[nested.ids()[1]]).isEqualTo(30L);
        assertThat(((I64Vector) nested.values()).values()[nested.ids()[2]]).isEqualTo(10L);
        assertThat(((I64Vector) nested.values()).values()[nested.ids()[3]]).isEqualTo(20L);
    }

    @Test
    void testIfI64TreatsNullConditionAsFalseBranch()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable selected = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        selected,
                        new Call("if_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        3,
                        List.of(
                                row(true, 11L, 21L),
                                row((Object) null, 12L, 22L),
                                row(false, 13L, 23L))))) {
            try (Batch batch = operator.next()) {
                assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(11L, 22L, 23L);
            }
        }
    }

    @Test
    void testIfI64KeepsConstantBranchesEncoded()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable selected = new Variable(2);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(
                                selected,
                                new Call("if_i64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(one, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(
                                row(true),
                                row((Object) null),
                                row(false),
                                row(true))))) {
            try (Batch batch = operator.next()) {
                Vector values = batch.output(0).borrow(Stream.VALUES);
                assertThat(values).isInstanceOf(DictionaryVector.class);
                DictionaryVector dictionary = (DictionaryVector) values;
                assertThat(dictionary.values()).isInstanceOf(I64Vector.class);
                long[] dictionaryValues = ((I64Vector) dictionary.values()).values();
                assertThat(dictionaryValues[dictionary.ids()[0]]).isEqualTo(1L);
                assertThat(dictionaryValues[dictionary.ids()[1]]).isEqualTo(0L);
                assertThat(dictionaryValues[dictionary.ids()[2]]).isEqualTo(0L);
                assertThat(dictionaryValues[dictionary.ids()[3]]).isEqualTo(1L);
            }
        }
    }

    @Test
    void testIfI64KeepsConstantBranchesEncodedWithSelectionNulls()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable selected = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        selected,
                        new Call("if_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES)));

        Vector noNulls = SelectionVector.wrap(
                SelectedPositions.positions(new int[] {0, 1, 2, 3}),
                new BooleanVector(new boolean[] {false, false, false, false}));
        Operator source = new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return 3;
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
                        Mask.all(4),
                        Output.of(Streams.ofValues(new BooleanVector(new boolean[] {true, false, true, false}))),
                        Output.of(Streams.builder()
                                .put(Stream.VALUES, new RleVector(new int[] {4}, new I64Vector(new long[] {1})))
                                .put(Stream.NULLS, noNulls)
                                .build()),
                        Output.of(Streams.builder()
                                .put(Stream.VALUES, new RleVector(new int[] {4}, new I64Vector(new long[] {0})))
                                .put(Stream.NULLS, noNulls)
                                .build()));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (ProjectOperator operator = new ProjectOperator(allocator, evaluationPlan, primitiveRegistry, source)) {
            try (Batch batch = operator.next()) {
                Vector values = batch.output(0).borrow(Stream.VALUES);
                assertThat(values).isInstanceOf(DictionaryVector.class);
                DictionaryVector dictionary = (DictionaryVector) values;
                assertThat(((I64Vector) dictionary.values()).values()[dictionary.ids()[0]]).isEqualTo(1L);
                assertThat(((I64Vector) dictionary.values()).values()[dictionary.ids()[1]]).isEqualTo(0L);
                assertThat(((I64Vector) dictionary.values()).values()[dictionary.ids()[2]]).isEqualTo(1L);
                assertThat(((I64Vector) dictionary.values()).values()[dictionary.ids()[3]]).isEqualTo(0L);
            }
        }
    }

    @Test
    void testIfUtf8TreatsNullConditionAsFalseBranch()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable selected = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        selected,
                        new Call("if_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        3,
                        List.of(
                                row(true, "left", "right"),
                                row((Object) null, "wrong", "fallback"),
                                row(false, "wrong-again", "false-branch"))))) {
            try (Batch batch = operator.next()) {
                BinaryVector values = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
                assertThat(new String(values.copyBytes(0), UTF_8)).isEqualTo("left");
                assertThat(new String(values.copyBytes(1), UTF_8)).isEqualTo("fallback");
                assertThat(new String(values.copyBytes(2), UTF_8)).isEqualTo("false-branch");
            }
        }
    }

    @Test
    void testLessThanPropagatesNulls()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("lt", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES), new Reference(result, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(10L, 11L),
                                row(20L, (Object) null),
                                row(30L, 2L))))) {
            try (Batch batch = operator.next()) {
                assertThat(((BooleanVector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(true, false, false);
                assertThat(((BooleanVector) batch.output(1).borrow(Stream.NULLS)).values()).containsExactly(false, true, false);
            }
        }
    }

    @Test
    void testAndPropagatesNullsUnlessFalseBranchDecidesResult()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("and", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES), new Reference(result, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(true, true),
                                row(true, (Object) null),
                                row(false, (Object) null),
                                row((Object) null, false))))) {
            try (Batch batch = operator.next()) {
                assertThat(((BooleanVector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(true, false, false, false);
                assertThat(((BooleanVector) batch.output(1).borrow(Stream.NULLS)).values()).containsExactly(false, true, false, false);
            }
        }
    }

    @Test
    void testOrPropagatesNullsUnlessTrueBranchDecidesResult()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("or", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES), new Reference(result, Stream.NULLS)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(false, false),
                                row(false, (Object) null),
                                row(true, (Object) null),
                                row((Object) null, true))))) {
            try (Batch batch = operator.next()) {
                assertThat(((BooleanVector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(false, false, true, true);
                assertThat(((BooleanVector) batch.output(1).borrow(Stream.NULLS)).values()).containsExactly(false, true, false, false);
            }
        }
    }

    @Test
    void testConstantTableOperatorSupportsTypedScalarColumns()
    {
        assertThat(operator(new ConstantTableOperator(
                allocator,
                3,
                List.of(
                        row("alice", 1.5, true),
                        row(null, 2.5, false)))))
                .matchesExactly(List.of(
                        row("alice", 1.5, 1L),
                        row(null, 2.5, 0L)));
    }

    @Test
    void testGroupOperatorGroupsUtf8Values()
    {
        assertThat(operator(new GroupOperator(
                allocator,
                0,
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(
                                row("alpha"),
                                row("beta"),
                                row("alpha"),
                                row((Object) null),
                                row("beta"))))))
                .matchesExactly(List.of(
                        row(0L, "alpha"),
                        row(1L, "beta"),
                        row(0L, "alpha"),
                        row(2L, null),
                        row(1L, "beta")));
    }

    @Test
    void testGroupOperatorGroupsBooleans()
    {
        assertThat(operator(new GroupOperator(
                allocator,
                0,
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(
                                row(true),
                                row(false),
                                row(true),
                                row((Object) null))))))
                .matchesExactly(List.of(
                        row(0L, 1L),
                        row(1L, 0L),
                        row(0L, 1L),
                        row(2L, null)));
    }

    @Test
    void testGroupOperatorGroupsDoubles()
    {
        assertThat(operator(new GroupOperator(
                allocator,
                0,
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(
                                row(1.5),
                                row(2.5),
                                row(1.5))))))
                .matchesExactly(List.of(
                        row(0L, 1.5),
                        row(1L, 2.5),
                        row(0L, 1.5)));
    }

    @Test
    void testGroupOperatorDefersPayloadBorrowsAndHonorsBatchConstraint()
    {
        AtomicInteger keyBorrows = new AtomicInteger();
        AtomicInteger payloadBorrows = new AtomicInteger();
        AtomicReference<Mask> constrainedMask = new AtomicReference<>();

        Operator source = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }

            @Override
            public Batch next()
            {
                done = true;
                I64Vector keys = new I64Vector(new long[] {10L, 10L, 20L, 10L});
                I64Vector payload = new I64Vector(new long[] {1L, 2L, 3L, 4L});
                return new Batch(
                        Mask.all(4),
                        constrainedMask::set,
                        Function.identity(),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            keyBorrows.incrementAndGet();
                            return keys;
                        }),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }));
            }

            @Override
            public void constrain(Mask mask)
            {
                constrainedMask.set(mask);
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

        try (Operator operator = new GroupOperator(allocator, 0, source)) {
            Batch batch = operator.next();
            assertThat(keyBorrows).hasValue(0);
            assertThat(payloadBorrows).hasValue(0);

            batch.constrain(Mask.sparse(new int[] {0, 2}, 4));
            assertThat(batch.borrowMask()).containsExactly(0, 2);
            assertThat(constrainedMask.get()).containsExactly(0, 2);
            assertThat(keyBorrows).hasValue(0);
            assertThat(payloadBorrows).hasValue(0);

            I64Vector payload = (I64Vector) batch.output(2).borrow(Stream.VALUES);
            assertThat(payload.values()[0]).isEqualTo(1L);
            assertThat(payload.values()[2]).isEqualTo(3L);
            assertThat(keyBorrows).hasValue(0);
            assertThat(payloadBorrows).hasValue(1);

            I64Vector groups = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            assertThat(groups.values()[0]).isEqualTo(0L);
            assertThat(groups.values()[2]).isEqualTo(1L);
            assertThat(keyBorrows).hasValue(1);
            assertThat(payloadBorrows).hasValue(1);
        }
    }

    @Test
    void testGroupedAggregationOperatorLeavesUnusedPayloadsCold()
    {
        AtomicInteger payloadBorrows = new AtomicInteger();

        Operator source = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 3;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }

            @Override
            public Batch next()
            {
                done = true;
                I64Vector keys = new I64Vector(new long[] {10L, 10L, 20L});
                I64Vector values = new I64Vector(new long[] {1L, 2L, 3L});
                I64Vector payload = new I64Vector(new long[] {100L, 200L, 300L});
                return new Batch(
                        Mask.all(3),
                        Output.of(Streams.ofValues(keys)),
                        Output.of(Streams.ofValues(values)),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }));
            }

            @Override
            public void constrain(Mask mask)
            {
            }

            @Override
            public void close()
            {
            }
        };

        try (Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(new Sum(2)),
                new GroupOperator(allocator, 0, source))) {
            Batch batch = operator.next();
            assertThat(payloadBorrows).hasValue(0);

            I64Vector sums = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            assertThat(sums.values()[0]).isEqualTo(3L);
            assertThat(sums.values()[1]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);
        }
    }

    @Test
    void testAggregationOperatorDefersWorkUntilBorrowAndSkipsWhenConstrainedEmpty()
    {
        AtomicInteger valueBorrows = new AtomicInteger();
        AtomicInteger payloadBorrows = new AtomicInteger();

        Operator source = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }

            @Override
            public Batch next()
            {
                done = true;
                I64Vector values = new I64Vector(new long[] {1L, 2L, 3L});
                I64Vector payload = new I64Vector(new long[] {100L, 200L, 300L});
                return new Batch(
                        Mask.all(3),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            valueBorrows.incrementAndGet();
                            return values;
                        }),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }));
            }

            @Override
            public void constrain(Mask mask)
            {
            }

            @Override
            public void close()
            {
            }
        };

        try (Operator operator = new AggregationOperator(
                allocator,
                List.of(new Sum(0)),
                source)) {
            Batch batch = operator.next();
            assertThat(valueBorrows).hasValue(0);
            assertThat(payloadBorrows).hasValue(0);

            batch.constrain(Mask.sparse(new int[0], 1));
            assertThat(batch.borrowMask().count()).isEqualTo(0);

            assertThat(((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values()[0]).isTrue();
            assertThat(valueBorrows).hasValue(0);
            assertThat(payloadBorrows).hasValue(0);
        }
    }

    @Test
    void testAggregationOperatorLeavesUnusedPayloadsCold()
    {
        AtomicInteger valueBorrows = new AtomicInteger();
        AtomicInteger payloadBorrows = new AtomicInteger();

        Operator source = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }

            @Override
            public Batch next()
            {
                done = true;
                I64Vector values = new I64Vector(new long[] {1L, 2L, 3L});
                I64Vector payload = new I64Vector(new long[] {100L, 200L, 300L});
                return new Batch(
                        Mask.all(3),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            valueBorrows.incrementAndGet();
                            return values;
                        }),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }));
            }

            @Override
            public void constrain(Mask mask)
            {
            }

            @Override
            public void close()
            {
            }
        };

        try (Operator operator = new AggregationOperator(
                allocator,
                List.of(new Sum(0)),
                source)) {
            Batch batch = operator.next();
            assertThat(valueBorrows).hasValue(0);
            assertThat(payloadBorrows).hasValue(0);

            I64Vector sums = (I64Vector) batch.output(0).borrow(Stream.VALUES);
            assertThat(sums.values()[0]).isEqualTo(6L);
            assertThat(valueBorrows).hasValue(1);
            assertThat(payloadBorrows).hasValue(0);
        }
    }

    @Test
    void testLimitOperatorDefersPayloadBorrowsAndHonorsBatchConstraint()
    {
        AtomicInteger payloadBorrows = new AtomicInteger();
        AtomicReference<Mask> constrainedMask = new AtomicReference<>();

        Operator source = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }

            @Override
            public Batch next()
            {
                done = true;
                I64Vector keys = new I64Vector(new long[] {10L, 20L, 30L, 40L});
                I64Vector payload = new I64Vector(new long[] {1L, 2L, 3L, 4L});
                return new Batch(
                        Mask.all(4),
                        constrainedMask::set,
                        Function.identity(),
                        Output.of(Streams.ofValues(keys)),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }));
            }

            @Override
            public void constrain(Mask mask)
            {
                constrainedMask.set(mask);
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

        try (Operator operator = new LimitOperator(allocator, 3, source)) {
            Batch batch = operator.next();
            assertThat(batch.borrowMask()).containsExactly(0, 1, 2);
            assertThat(constrainedMask.get()).containsExactly(0, 1, 2);
            assertThat(payloadBorrows).hasValue(0);

            batch.constrain(Mask.sparse(new int[] {0, 2}, 4));
            assertThat(batch.borrowMask()).containsExactly(0, 2);
            assertThat(constrainedMask.get()).containsExactly(0, 2);
            assertThat(payloadBorrows).hasValue(0);

            I64Vector payload = (I64Vector) batch.output(1).borrow(Stream.VALUES);
            assertThat(payload.values()[0]).isEqualTo(1L);
            assertThat(payload.values()[2]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(1);
        }
    }

    @Test
    void testOperatorAssertionsDecodeNestedArrays()
    {
        ArrayVector arrays = new ArrayVector(1);
        arrays.offsets()[0] = 0;
        arrays.offsets()[1] = 2;
        arrays.setElements(Streams.ofValues(new I64Vector(new long[] {10L, 20L})));

        LinkedHashMap<String, Object> expected = new LinkedHashMap<>();
        expected.put("name", "alpha");
        expected.put("score", 7L);

        BinaryVector names = new BinaryVector(1, 5);
        names.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        names.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        names.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        org.weakref.nitro.data.StructVector struct = new org.weakref.nitro.data.StructVector(1);
        struct.setField("name", Streams.ofValues(names));
        struct.setField("score", Streams.ofValues(new I64Vector(new long[] {7L})));

        assertThat(operator(new TableOperator(
                2,
                List.of(new TableOperator.Page(
                        1,
                        new Streams[] {
                                Streams.ofValues(arrays),
                                Streams.ofValues(struct),
                        },
                        Mask.all(1))))))
                .matchesExactly(List.of(
                        row(List.of(10L, 20L), expected)));
    }

    @Test
    void testBinaryVectorTraitsRemainImmutableAndDetachedFromCaller()
    {
        BinaryVector values = new BinaryVector(1, 5);
        Set<BinaryVector.Trait> callerTraits = new java.util.LinkedHashSet<>();
        callerTraits.add(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);

        values.addTraits(callerTraits);
        callerTraits.add(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);

        assertThat(values.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)).isTrue();
        assertThat(values.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isFalse();
        assertThatThrownBy(() -> values.traits().add(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testProjectOperatorCanProjectErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable quotient = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        quotient,
                        new Call("divide", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(quotient, Stream.ERRORS)),
                Map.of(new Reference(quotient, Stream.ERRORS), StreamPlan.MATERIALIZED));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(20L, 5L),
                                row(21L, 0L),
                                row(22L, 2L))))) {
            Batch batch = operator.next();
            BooleanVector errors = (BooleanVector) batch.output(0).borrow(Stream.ERRORS);
            assertThat(errors.values()).containsExactly(false, true, false);
        }
    }

    @Test
    void testProjectOperatorSynthesizesAbsentErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        sum,
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(sum, Stream.ERRORS)),
                Map.of(new Reference(sum, Stream.ERRORS), StreamPlan.MATERIALIZED));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(20L, 5L),
                                row(21L, 1L),
                                row(22L, 2L))))) {
            Batch batch = operator.next();
            BooleanVector errors = (BooleanVector) batch.output(0).borrow(Stream.ERRORS);
            assertThat(Arrays.copyOf(errors.values(), batch.borrowMask().count())).containsExactly(false, false, false);
        }
    }

    @Test
    void testProjectOperatorCanProjectNullsStream()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable_copy", (inputs, mask, requestedStreams, output, context) -> {
            long[] inputValues = ((org.weakref.nitro.data.I64Vector) inputs.getFirst().values()).values();
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new org.weakref.nitro.data.I64Vector(inputValues.clone()));
            }
            if (requestedStreams.contains(Stream.NULLS)) {
                result = result.with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("nullable_copy", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.NULLS)),
                Map.of(new Reference(result, Stream.NULLS), StreamPlan.MATERIALIZED));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(
                                row(10L),
                                row(20L),
                                row(30L))))) {
            Batch batch = operator.next();
            BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
            assertThat(nulls.values()).containsExactly(false, true, false);
        }
    }

    @Test
    void testProjectOperatorExposesCompanionNullsForProjectedValues()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable_copy", (inputs, mask, requestedStreams, output, context) -> {
            long[] inputValues = ((org.weakref.nitro.data.I64Vector) inputs.getFirst().values()).values();
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new org.weakref.nitro.data.I64Vector(inputValues.clone()));
            }
            if (requestedStreams.contains(Stream.NULLS)) {
                result = result.with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("nullable_copy", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(
                                row(10L),
                                row(20L),
                                row(30L))))) {
            Batch batch = operator.next();
            assertThat(((org.weakref.nitro.data.I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(10L, 20L, 30L);
            BooleanVector nulls = (BooleanVector) batch.output(0).borrowOrNull(Stream.NULLS);
            assertThat(nulls).isNotNull();
            assertThat(nulls.values()).containsExactly(false, true, false);
        }
    }

    @Test
    void testProjectOperatorSynthesizesAbsentNullsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        sum,
                        new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(sum, Stream.NULLS)),
                Map.of(new Reference(sum, Stream.NULLS), StreamPlan.MATERIALIZED));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(20L, 5L),
                                row(21L, 1L),
                                row(22L, 2L))))) {
            Batch batch = operator.next();
            BooleanVector nulls = (BooleanVector) batch.output(0).borrow(Stream.NULLS);
            assertThat(Arrays.copyOf(nulls.values(), batch.borrowMask().count())).containsExactly(false, false, false);
        }
    }

    @Test
    void testProjectOperatorSharesProjectedSiblingStreamsWithoutValues()
    {
        AtomicInteger evaluations = new AtomicInteger();
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable_error", (inputs, mask, requested, output, context) -> {
            evaluations.incrementAndGet();
            requestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.NULLS)) {
                result = result.with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {true, false, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference nulls = new Reference(result, Stream.NULLS);
        Reference errors = new Reference(result, Stream.ERRORS);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call("nullable_error", List.of()),
                        AllMask.ALL)),
                List.of(nulls, errors));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                evaluationPlan,
                primitiveRegistry,
                new ConstantTableOperator(allocator, 0, List.of(row(), row(), row())))) {
            Batch batch = operator.next();
            assertThat(((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values()).containsExactly(false, true, false);
            assertThat(((BooleanVector) batch.output(1).borrow(Stream.ERRORS)).values()).containsExactly(true, false, false);
            assertThat(evaluations).hasValue(1);
            assertThat(requestedStreams.get()).containsExactlyInAnyOrder(Stream.NULLS, Stream.ERRORS);
        }
    }

    @Test
    void testFilterOperatorUsesPlanEvaluator()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable literalThreshold = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(
                        new Assignment(literalThreshold, new Literal(3L), AllMask.ALL),
                        new Assignment(
                                predicate,
                                new Call("lt", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literalThreshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(),
                Map.of(new Reference(predicate, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        assertThat(operator(
                new FilterOperator(
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 10L),
                                        row(2L, 20L),
                                        row(3L, 30L),
                                        row(4L, 40L))),
                        evaluationPlan,
                        primitiveRegistry,
                        new Reference(predicate, Stream.VALUES),
                        allocator)))
                .matchesExactly(List.of(
                        row(1L, 10L),
                        row(2L, 20L)));
    }

    @Test
    void testFilterOperatorUsesPlannedMaskExpressionForPredicateReference()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable literalThreshold = new Variable(0);
        Variable predicate = new Variable(1);
        Reference predicateValues = new Reference(predicate, Stream.VALUES);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(
                        new Assignment(literalThreshold, new Literal(3L), AllMask.ALL),
                        new Assignment(
                                predicate,
                                new Call("lt", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literalThreshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(),
                Map.of(),
                Map.of(predicateValues, new NotMask(new ReferenceMask(predicateValues))));

        assertThat(operator(
                new FilterOperator(
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 10L),
                                        row(2L, 20L),
                                        row(3L, 30L),
                                        row(4L, 40L))),
                        evaluationPlan,
                primitiveRegistry,
                        predicateValues,
                        allocator)))
                .matchesExactly(List.of(
                        row(3L, 30L),
                        row(4L, 40L)));
    }

    @Test
    void testFilterOverLimit()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        assertThat(operator(
                filterLessThanOrGreaterThan(
                        new LimitOperator(
                                allocator,
                                15,
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(
                                                new SequenceGenerator(0),
                                                new SequenceGenerator(100)))),
                        0,
                        10,
                        40,
                        primitiveRegistry)))
                .matchesExactly(List.of(
                        row(0L, 100L),
                        row(1L, 101L),
                        row(2L, 102L),
                        row(3L, 103L),
                        row(4L, 104L),
                        row(5L, 105L),
                        row(6L, 106L),
                        row(7L, 107L),
                        row(8L, 108L),
                        row(9L, 109L)));

        assertThat(operator(
                filterDivisibleBy(
                        new LimitOperator(
                                allocator,
                                15,
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(
                                                new SequenceGenerator(0),
                                                new SequenceGenerator(100)))),
                        0,
                        2,
                        primitiveRegistry)))
                .matchesExactly(List.of(
                        row(0L, 100L),
                        row(2L, 102L),
                        row(4L, 104L),
                        row(6L, 106L),
                        row(8L, 108L),
                        row(10L, 110L),
                        row(12L, 112L),
                        row(14L, 114L)));
    }

    private PrimitiveRegistry primitiveRegistry()
    {
        return TestPrimitiveFunctions.primitiveRegistry();
    }

    @Test
    void testAggregationOverLimit()
    {
        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(new CountAll()),
                        new LimitOperator(
                                allocator,
                                15,
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(new SequenceGenerator(0)))))))
                .matchesExactly(List.of(row(15L)));
    }

    @Test
    void testFilterOverFilter()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        assertThat(operator(
                filterDivisibleBy(
                        filterDivisibleBy(
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(new SequenceGenerator(0))),
                                0,
                                3,
                                primitiveRegistry),
                        0,
                        2,
                        primitiveRegistry)))
                .matchesExactly(List.of(
                        row(0L),
                        row(6L),
                        row(12L),
                        row(18L),
                        row(24L),
                        row(30L),
                        row(36L),
                        row(42L),
                        row(48L)));
    }

    @Test
    void testGroup()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable three = new Variable(0);
        Variable quotient = new Variable(1);
        EvaluationPlan evaluationPlan = plan(
                List.of(
                        literal(three, 3),
                        call(quotient, "divide", values(new Input(0)), values(three))),
                values(quotient));

        assertThat(operator(
                new GroupOperator(
                        allocator,
                        0,
                        new ProjectOperator(
                                allocator,
                                evaluationPlan,
                                primitiveRegistry,
                                new GeneratorOperator(
                                        allocator,
                                        10,
                                        10,
                                        List.of(new SequenceGenerator(100)))))))
                .matchesExactly(List.of(
                        row(0L, 33L),
                        row(0L, 33L),
                        row(1L, 34L),
                        row(1L, 34L),
                        row(1L, 34L),
                        row(2L, 35L),
                        row(2L, 35L),
                        row(2L, 35L),
                        row(3L, 36L),
                        row(3L, 36L)));
    }

    @Test
    void testProjectAddSupportsI32Inputs()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable three = new Variable(0);
        Variable sum = new Variable(1);
        EvaluationPlan evaluationPlan = plan(
                List.of(
                        literal(three, 3),
                        call(sum, "add", values(new Input(0)), values(three))),
                values(sum));

        assertThat(operator(
                new ProjectOperator(
                        allocator,
                        evaluationPlan,
                        primitiveRegistry,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row(1),
                                        row(2),
                                        row(3))))))
                .matchesExactly(List.of(
                        row(4L),
                        row(5L),
                        row(6L)));
    }

    @Test
    void testLimit()
    {
        assertThat(operator(
                new LimitOperator(
                        allocator,
                        5,
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(new SequenceGenerator(0))))))
                .describedAs("Within first batch")
                .matchesExactly(List.of(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L)));

        assertThat(operator(
                new LimitOperator(
                        allocator,
                        15,
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(new SequenceGenerator(0))))))
                .describedAs("Middle of second batch")
                .matchesExactly(List.of(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L),
                        row(5L),
                        row(6L),
                        row(7L),
                        row(8L),
                        row(9L),
                        row(10L),
                        row(11L),
                        row(12L),
                        row(13L),
                        row(14L)));

        assertThat(operator(
                new LimitOperator(
                        allocator,
                        15,
                        new GeneratorOperator(
                                allocator,
                                12,
                                10,
                                List.of(new SequenceGenerator(0))))))
                .describedAs("Beyond end of underlying sequence")
                .matchesExactly(List.of(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L),
                        row(5L),
                        row(6L),
                        row(7L),
                        row(8L),
                        row(9L),
                        row(10L),
                        row(11L)));
    }

    @Test
    void testTopN()
    {
        assertThat(operator(
                new TopNOperator(
                        allocator,
                        5,
                        0,
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(
                                        new SequenceGenerator(0),
                                        new SequenceGenerator(100))))))
                .matchesExactly(List.of(
                        row(49L, 149L),
                        row(48L, 148L),
                        row(47L, 147L),
                        row(46L, 146L),
                        row(45L, 145L)));
    }

    @Test
    void testAggregation()
    {
        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(
                                new First(0),
                                new Min(0),
                                new Max(0),
                                new Sum(0),
                                new CountAll()),
                        new GeneratorOperator(
                                allocator,
                                50,
                                10,
                                List.of(new SequenceGenerator(100))))))
                .matchesExactly(List.of(row(100L, 100L, 149L, 6225L, 50L)));

        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(
                                new First(0),
                                new Min(0),
                                new Max(0),
                                new Sum(0),
                                new CountAll()),
                        filterDivisibleBy(
                                new GeneratorOperator(
                                        allocator,
                                        50,
                                        10,
                                        List.of(new SequenceGenerator(100))),
                                0,
                                2,
                                primitiveRegistry()))))
                .matchesExactly(List.of(row(100L, 100L, 148L, 3100L, 25L)));
    }

    @Test
    void testCountColumn()
    {
        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(
                                new CountColumn(0),
                                new CountColumn(1),
                                new CountColumn(2)),
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, null, 10L),
                                        row(2L, null, 20L),
                                        row(null, null, 30L),
                                        row(4L, null, 40L),
                                row(5L, null, 50L))))))
                .matchesExactly(List.of(row(4L, 0L, 5L)));
    }

    @Test
    void testDistinctCountForIntegers()
    {
        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(new DistinctCount(0)),
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row((Object) 10L),
                                        row((Object) 10L),
                                        row((Object) 20L),
                                        row((Object) null),
                                        row((Object) 30L))))))
                .matchesExactly(List.of(row(3L)));
    }

    @Test
    void testDistinctCountForUtf8()
    {
        assertThat(operator(
                new AggregationOperator(
                        allocator,
                        List.of(new DistinctCount(0)),
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row((Object) "alpha"),
                                        row((Object) "alpha"),
                                        row((Object) "beta"),
                                        row((Object) null),
                                        row((Object) "gamma"))))))
                .matchesExactly(List.of(row(3L)));
    }

    @Test
    void testMarkDistinctOperatorKeepsFirstOccurrenceRows()
    {
        assertThat(operator(
                new MarkDistinctOperator(
                        allocator,
                        new int[] {0, 1},
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, "alpha"),
                                        row(1L, "alpha"),
                                        row(1L, "beta"),
                                        row(2L, "alpha"),
                                        row((Object) null, "alpha"),
                                        row(2L, (Object) null))))))
                .matchesExactly(List.of(
                        row(1L, "alpha"),
                        row(1L, "beta"),
                        row(2L, "alpha")));
    }

    @Test
    void testMarkDistinctOperatorPreservesSourceBatchWhenAllRowsAreDistinct()
    {
        DictionaryVector dictionary = DictionaryVector.wrap(
                new int[] {2, 0, 1, 3},
                new I64Vector(new long[] {10, 20, 30, 40}));

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
                return new Batch(Mask.all(4), Output.of(Streams.ofValues(dictionary)));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (MarkDistinctOperator operator = new MarkDistinctOperator(allocator, 0, source);
                Batch batch = operator.next()) {
            assertThat(batch.borrowMask().all()).isTrue();
            assertThat(batch.output(0).borrow(Stream.VALUES)).isSameAs(dictionary);
        }
    }

    @Test
    void testGroupedAggregationWithMixedDistinctAccumulator()
    {
        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        0,
                        List.of(1),
                        List.of(new Sum(2), new CountAll(), new Avg(3), new DistinctCount(4)),
                        new GroupOperator(
                                allocator,
                                0,
                                new ConstantTableOperator(
                                        allocator,
                                        4,
                                        List.of(
                                                row(1L, 10L, 100L, 1000L),
                                                row(1L, 20L, 200L, 1000L),
                                                row(1L, 30L, 300L, 2000L),
                                                row(2L, 40L, 100L, 3000L),
                                                row(2L, 50L, 200L, 3000L),
                                                row(2L, 60L, 300L, null)))))))
                .matchesExactly(List.of(
                        row(1L, 60L, 3L, 200.0, 2L),
                        row(2L, 150L, 3L, 200.0, 1L)));
    }

    @Test
    void testGroupedAggregation()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable ten = new Variable(0);
        Variable thirteen = new Variable(1);
        Variable modulo = new Variable(2);
        Variable groupingKey = new Variable(3);
        EvaluationPlan evaluationPlan = plan(
                List.of(
                        literal(ten, 10),
                        literal(thirteen, 13),
                        call(modulo, "modulo", values(new Input(0)), values(ten)),
                        call(groupingKey, "add", values(modulo), values(thirteen))),
                values(groupingKey),
                values(new Input(0)));

        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        0,
                        List.of(
                                new First(1), // key
                                new Min(2),
                                new Max(2),
                                new Sum(2),
                                new CountAll()),
                        new GroupOperator(
                                allocator,
                                0,
                                new ProjectOperator(
                                        allocator,
                                        evaluationPlan,
                                        primitiveRegistry,
                                        new GeneratorOperator(
                                                allocator,
                                                50,
                                                10,
                                                List.of(new SequenceGenerator(100))))))))
                .matchesExactly(List.of(
                        row(13L, 100L, 140L, 600L, 5L),
                        row(14L, 101L, 141L, 605L, 5L),
                        row(15L, 102L, 142L, 610L, 5L),
                        row(16L, 103L, 143L, 615L, 5L),
                        row(17L, 104L, 144L, 620L, 5L),
                        row(18L, 105L, 145L, 625L, 5L),
                        row(19L, 106L, 146L, 630L, 5L),
                        row(20L, 107L, 147L, 635L, 5L),
                        row(21L, 108L, 148L, 640L, 5L),
                        row(22L, 109L, 149L, 645L, 5L)));
    }

    @Test
    void testGroupedAggregationCanExposeGroupingKeyWithoutFirstAccumulator()
    {
        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        0,
                        List.of(1),
                        List.of(new CountAll()),
                        new GroupOperator(
                                allocator,
                                0,
                                new ConstantTableOperator(
                                        allocator,
                                        1,
                                        List.of(
                                                row("alpha"),
                                                row("alpha"),
                                                row("beta")))))))
                .matchesExactly(List.of(
                        row("alpha", 2L),
                        row("beta", 1L)));
    }

    @Test
    void testConstantTable()
    {
        assertThat(operator(new ConstantTableOperator(
                allocator,
                3,
                List.of(
                        row(1L, 10L, 100L),
                        row(2L, 20L, 200L),
                        row(null, 30L, 300L),
                        row(4L, null, 400L),
                        row(5L, 50L, null)))))
                .matchesExactly(List.of(
                        row(1L, 10L, 100L),
                        row(2L, 20L, 200L),
                        row(null, 30L, 300L),
                        row(4L, null, 400L),
                        row(5L, 50L, null)));
    }

    @Test
    void testNestedLoop()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row(1L),
                                        row(2L),
                                        row(3L))),
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row(10L),
                                        row(20L),
                                        row(30L))))))
                .matches(List.of(
                        row(1L, 10L),
                        row(1L, 20L),
                        row(1L, 30L),
                        row(2L, 10L),
                        row(2L, 20L),
                        row(2L, 30L),
                        row(3L, 10L),
                        row(3L, 20L),
                        row(3L, 30L)));
    }

    @Test
    void testNestedLoop1()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(1L))),
                        new GeneratorOperator(
                                allocator,
                                10,
                                5,
                                List.of(new SequenceGenerator(0))))))
                .matches(List.of(
                        row(1L, 0L),
                        row(1L, 1L),
                        row(1L, 2L),
                        row(1L, 3L),
                        row(1L, 4L),
                        row(1L, 5L),
                        row(1L, 6L),
                        row(1L, 7L),
                        row(1L, 8L),
                        row(1L, 9L)));
    }

    @Test
    void testNestedLoop2()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new GeneratorOperator(allocator, 6, 2, List.of(new SequenceGenerator(0))),
                        new GeneratorOperator(allocator, 3, 1, List.of(new SequenceGenerator(10))))))
                .matches(List.of(
                        row(0L, 10L),
                        row(0L, 11L),
                        row(0L, 12L),
                        row(1L, 10L),
                        row(1L, 11L),
                        row(1L, 12L),
                        row(2L, 10L),
                        row(2L, 11L),
                        row(2L, 12L),
                        row(3L, 10L),
                        row(3L, 11L),
                        row(3L, 12L),
                        row(4L, 10L),
                        row(4L, 11L),
                        row(4L, 12L),
                        row(5L, 10L),
                        row(5L, 11L),
                        row(5L, 12L)));
    }

    @Test
    void testNestedLoop3()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new GeneratorOperator(allocator, 3, 1, List.of(new SequenceGenerator(0))),
                        new GeneratorOperator(allocator, 6, 2, List.of(new SequenceGenerator(10))))))
                .matches(List.of(
                        row(0L, 10L),
                        row(0L, 11L),
                        row(0L, 12L),
                        row(0L, 13L),
                        row(0L, 14L),
                        row(0L, 15L),
                        row(1L, 10L),
                        row(1L, 11L),
                        row(1L, 12L),
                        row(1L, 13L),
                        row(1L, 14L),
                        row(1L, 15L),
                        row(2L, 10L),
                        row(2L, 11L),
                        row(2L, 12L),
                        row(2L, 13L),
                        row(2L, 14L),
                        row(2L, 15L)));
    }

    @Test
    void testNestedLoopEmptyBuild()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new GeneratorOperator(allocator, 10, 2, List.of(new SequenceGenerator(0))),
                        new ConstantTableOperator(allocator, 1, List.of()))))
                .matches(List.of());
    }

    @Test
    void testNestedLoopEmptyProbe()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of()),
                        new GeneratorOperator(allocator, 10, 2, List.of(new SequenceGenerator(0))))))
                .matches(List.of());
    }

    @Test
    void testNestedLoopEquiJoin()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 10L),
                                        row(2L, 20L),
                                        row(null, 30L),
                                        row(3L, 40L))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(2L, 200L),
                                        row(null, 300L),
                                        row(3L, 400L),
                                        row(4L, 500L))),
                        0)))
                .matchesExactly(List.of(
                        row(2L, 20L, 2L, 200L),
                        row(3L, 40L, 3L, 400L)));
    }

    @Test
    void testNestedLoopMultiKeyEquiJoin()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, "alpha", 10L),
                                        row(1L, "beta", 20L),
                                        row(2L, "alpha", 30L),
                                        row(2L, null, 40L),
                                        row(null, "alpha", 50L))),
                        new int[] {0, 1},
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, "alpha", 100L),
                                        row(1L, "beta", 200L),
                                        row(2L, "beta", 300L),
                                        row(2L, null, 400L),
                                        row(null, "alpha", 500L))),
                        new int[] {0, 1})))
                .matchesExactly(List.of(
                        row(1L, "alpha", 10L, 1L, "alpha", 100L),
                        row(1L, "beta", 20L, 1L, "beta", 200L)));
    }

    @Test
    void testHashJoin()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 10L),
                                        row(2L, 20L),
                                        row(null, 30L),
                                        row(3L, 40L))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(2L, 200L),
                                        row(null, 300L),
                                        row(3L, 400L),
                                        row(4L, 500L))),
                        0)))
                .matchesExactly(List.of(
                        row(2L, 20L, 2L, 200L),
                        row(3L, 40L, 3L, 400L)));
    }

    @Test
    void testHashJoinMultiKey()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, "alpha", 10L),
                                        row(1L, "beta", 20L),
                                        row(2L, "alpha", 30L),
                                        row(2L, null, 40L),
                                        row(null, "alpha", 50L))),
                        new int[] {0, 1},
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, "alpha", 100L),
                                        row(1L, "beta", 200L),
                                        row(2L, "beta", 300L),
                                        row(2L, null, 400L),
                                        row(null, "alpha", 500L))),
                        new int[] {0, 1})))
                .matchesExactly(List.of(
                        row(1L, "alpha", 10L, 1L, "alpha", 100L),
                        row(1L, "beta", 20L, 1L, "beta", 200L)));
    }

    @Test
    void testHashJoinLateMaterializesProjectedOuterPayloads()
    {
        AtomicInteger payloadBorrows = new AtomicInteger();
        AtomicReference<Mask> constrainedMask = new AtomicReference<>();
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Operator baseSource = new Operator()
        {
            private boolean hasNext = true;
            private final I64Vector keys = new I64Vector(new long[] {1L, 2L, 3L});
            private final BooleanVector flags = new BooleanVector(new boolean[] {true, false, true});
            private final I64Vector payload = new I64Vector(new long[] {10L, 20L, 30L});

            @Override
            public int outputCount()
            {
                return 3;
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
                        Mask.all(3),
                        constrainedMask::set,
                        Function.identity(),
                        new Output(Set.of(Stream.VALUES), ignored -> keys),
                        new Output(Set.of(Stream.VALUES), ignored -> flags),
                        new Output(Set.of(Stream.VALUES), ignored -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }));
            }

            @Override
            public void constrain(Mask mask)
            {
                constrainedMask.set(mask);
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

        Variable projectedPayload = new Variable(0);
        EvaluationPlan projectPlan = plan(
                List.of(call(projectedPayload, "multiply", values(new Input(2)), values(new Input(2)))),
                values(new Input(0)),
                values(projectedPayload));

        Operator outer = new ProjectOperator(
                allocator,
                projectPlan,
                primitiveRegistry,
                new FilterOperator(
                        baseSource,
                        new EvaluationPlan(List.of(), List.of()),
                        primitiveRegistry,
                        new Reference(new Input(1), Stream.VALUES),
                        allocator));

        try (Operator join = new HashJoinOperator(
                allocator,
                outer,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(3L))),
                0)) {
            Batch batch = join.next();

            assertThat(batch.borrowMask()).containsExactly(0);
            assertThat(payloadBorrows).hasValue(0);
            assertThat(constrainedMask.get()).isNotNull();
            assertThat(constrainedMask.get()).containsExactly(0, 2);

            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()[0]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            assertThat(((I64Vector) batch.output(2).borrow(Stream.VALUES)).values()[0]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            I64Vector projectedPayloads = (I64Vector) batch.output(1).borrow(Stream.VALUES);
            assertThat(projectedPayloads.values()[0]).isEqualTo(900L);
            assertThat(payloadBorrows).hasValue(1);
            assertThat(constrainedMask.get()).containsExactly(2);
        }
    }

    @Test
    void testHashJoinLateMaterializesProjectedInnerPayloads()
    {
        AtomicInteger payloadBorrows = new AtomicInteger();
        AtomicReference<Mask> constrainedMask = new AtomicReference<>();
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Operator baseSource = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 3;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }

            @Override
            public Batch next()
            {
                done = true;
                I64Vector keys = new I64Vector(new long[] {2L, 2L, 3L, 4L});
                BooleanVector keep = new BooleanVector(new boolean[] {true, false, true, true});
                I64Vector payload = new I64Vector(new long[] {10L, 20L, 30L, 40L});
                Output[] outputs = new Output[] {
                        new Output(Set.of(Stream.VALUES), stream -> keys),
                        new Output(Set.of(Stream.VALUES), stream -> keep),
                        new Output(Set.of(Stream.VALUES), stream -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }),
                };
                return new Batch(Mask.all(4), constrainedMask::set, Function.identity(), outputs);
            }

            @Override
            public void constrain(Mask mask)
            {
                constrainedMask.set(mask);
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

        Variable projectedPayload = new Variable(0);
        EvaluationPlan projectPlan = plan(
                List.of(call(projectedPayload, "multiply", values(new Input(2)), values(new Input(2)))),
                values(new Input(0)),
                values(projectedPayload));

        Operator inner = new ProjectOperator(
                allocator,
                projectPlan,
                primitiveRegistry,
                new FilterOperator(
                        baseSource,
                        new EvaluationPlan(List.of(), List.of()),
                        primitiveRegistry,
                        new Reference(new Input(1), Stream.VALUES),
                        allocator));

        try (Operator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(3L))),
                0,
                inner,
                0)) {
            Batch batch = join.next();

            assertThat(batch.borrowMask()).containsExactly(0);
            assertThat(payloadBorrows).hasValue(0);
            assertThat(constrainedMask.get()).isNotNull();
            assertThat(constrainedMask.get()).containsExactly(0, 2, 3);

            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()[0]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            assertThat(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values()[0]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            I64Vector projectedPayloads = (I64Vector) batch.output(2).borrow(Stream.VALUES);
            assertThat(projectedPayloads.values()[0]).isEqualTo(900L);
            assertThat(payloadBorrows).hasValue(1);
            assertThat(constrainedMask.get()).containsExactly(2);
        }
    }

    @Test
    void testNestedLoopJoinLateMaterializesProjectedInnerPayloads()
    {
        AtomicInteger payloadBorrows = new AtomicInteger();
        AtomicReference<Mask> constrainedMask = new AtomicReference<>();
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Operator baseSource = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 3;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }

            @Override
            public Batch next()
            {
                done = true;
                I64Vector keys = new I64Vector(new long[] {2L, 2L, 3L, 4L});
                BooleanVector keep = new BooleanVector(new boolean[] {true, false, true, true});
                I64Vector payload = new I64Vector(new long[] {10L, 20L, 30L, 40L});
                Output[] outputs = new Output[] {
                        new Output(Set.of(Stream.VALUES), stream -> keys),
                        new Output(Set.of(Stream.VALUES), stream -> keep),
                        new Output(Set.of(Stream.VALUES), stream -> {
                            payloadBorrows.incrementAndGet();
                            return payload;
                        }),
                };
                return new Batch(Mask.all(4), constrainedMask::set, Function.identity(), outputs);
            }

            @Override
            public void constrain(Mask mask)
            {
                constrainedMask.set(mask);
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

        Variable projectedPayload = new Variable(0);
        EvaluationPlan projectPlan = plan(
                List.of(call(projectedPayload, "multiply", values(new Input(2)), values(new Input(2)))),
                values(new Input(0)),
                values(projectedPayload));

        Operator inner = new ProjectOperator(
                allocator,
                projectPlan,
                primitiveRegistry,
                new FilterOperator(
                        baseSource,
                        new EvaluationPlan(List.of(), List.of()),
                        primitiveRegistry,
                        new Reference(new Input(1), Stream.VALUES),
                        allocator));

        try (Operator join = new NestedLoopJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(3L))),
                0,
                inner,
                0)) {
            Batch batch = join.next();

            assertThat(batch.borrowMask()).containsExactly(0);
            assertThat(payloadBorrows).hasValue(0);
            assertThat(constrainedMask.get()).isNotNull();
            assertThat(constrainedMask.get()).containsExactly(0, 2, 3);

            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()[0]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            assertThat(((I64Vector) batch.output(1).borrow(Stream.VALUES)).values()[0]).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            I64Vector projectedPayloads = (I64Vector) batch.output(2).borrow(Stream.VALUES);
            assertThat(projectedPayloads.values()[0]).isEqualTo(900L);
            assertThat(payloadBorrows).hasValue(1);
            assertThat(constrainedMask.get()).containsExactly(2);
        }
    }

    @Test
    void testProject()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable negativeOne = new Variable(0);
        Variable squared = new Variable(1);
        Variable doubled = new Variable(2);
        Variable negative = new Variable(3);
        EvaluationPlan evaluationPlan = plan(
                List.of(
                        literal(negativeOne, -1),
                        call(squared, "multiply", values(new Input(0)), values(new Input(0))),
                        call(doubled, "add", values(squared), values(squared)),
                        call(negative, "multiply", values(squared), values(negativeOne))),
                values(new Input(0)),
                values(doubled),
                values(negative));

        /*
           %0 = %input * %input
           %1 = %0 + %0
           %2 = -%0
         */

        assertThat(operator(
                new ProjectOperator(
                        allocator,
                        evaluationPlan,
                        primitiveRegistry,
                        new GeneratorOperator(
                                allocator,
                                10,
                                5,
                                List.of(new SequenceGenerator(0), new SequenceGenerator(100))))))
                .matchesExactly(List.of(
                        row(0L, 0L, 0L),
                        row(1L, 2L, -1L),
                        row(2L, 8L, -4L),
                        row(3L, 18L, -9L),
                        row(4L, 32L, -16L),
                        row(5L, 50L, -25L),
                        row(6L, 72L, -36L),
                        row(7L, 98L, -49L),
                        row(8L, 128L, -64L),
                        row(9L, 162L, -81L)));
    }

    private FilterOperator filterDivisibleBy(Operator source, int inputColumn, long divisor, PrimitiveRegistry primitiveRegistry)
    {
        Variable divisorLiteral = new Variable(0);
        Variable remainder = new Variable(1);
        Variable one = new Variable(2);
        Variable predicate = new Variable(3);
        EvaluationPlan evaluationPlan = plan(
                List.of(
                        literal(divisorLiteral, divisor),
                        call(remainder, "modulo", values(new Input(inputColumn)), values(divisorLiteral)),
                        literal(one, 1),
                        call(predicate, "lt", values(remainder), values(one))),
                values(predicate));

        return new FilterOperator(source, evaluationPlan, primitiveRegistry, values(predicate), allocator);
    }

    private FilterOperator filterLessThanOrGreaterThan(Operator source, int inputColumn, long lowerBound, long upperBound, PrimitiveRegistry primitiveRegistry)
    {
        Variable lowerLiteral = new Variable(0);
        Variable upperLiteral = new Variable(1);
        Variable lessThanLower = new Variable(2);
        Variable greaterThanUpper = new Variable(3);
        Variable predicate = new Variable(4);
        EvaluationPlan evaluationPlan = plan(
                List.of(
                        literal(lowerLiteral, lowerBound),
                        literal(upperLiteral, upperBound),
                        call(lessThanLower, "lt", values(new Input(inputColumn)), values(lowerLiteral)),
                        call(greaterThanUpper, "lt", values(upperLiteral), values(new Input(inputColumn))),
                        call(predicate, "or", values(lessThanLower), values(greaterThanUpper))),
                values(predicate));

        return new FilterOperator(source, evaluationPlan, primitiveRegistry, values(predicate), allocator);
    }

    private static EvaluationPlan plan(List<Assignment> assignments, Reference... outputs)
    {
        return new EvaluationPlan(assignments, List.of(outputs));
    }

    private static Assignment literal(Variable output, long value)
    {
        return new Assignment(output, new Literal(value), AllMask.ALL);
    }

    private static Assignment call(Variable output, String function, Reference... arguments)
    {
        return new Assignment(output, new Call(function, List.of(arguments)), AllMask.ALL);
    }

    private static Reference values(Producer producer)
    {
        return new Reference(producer, Stream.VALUES);
    }
}
