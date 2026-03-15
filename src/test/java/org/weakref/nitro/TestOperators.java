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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.TopNOperator;
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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
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
    void testProjectOperatorCanProjectNullsStream()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable_copy", (inputs, mask, output, context) -> {
            long[] inputValues = ((org.weakref.nitro.data.I64Vector) inputs.getFirst().values()).values();
            return Streams.ofValues(new org.weakref.nitro.data.I64Vector(inputValues.clone()))
                    .with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
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
