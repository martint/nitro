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
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.LongStateUpdate;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionProgram;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.jit.FusedProjectionCompiler;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.BatchSliceOperator;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.CountingNextOperator;
import org.weakref.nitro.operator.DistinctCount;
import org.weakref.nitro.operator.EnforceSingleRowOperator;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.FullJoinOperator;
import org.weakref.nitro.operator.GeneratorOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.MarkDistinctMarkerOperator;
import org.weakref.nitro.operator.MarkDistinctOperator;
import org.weakref.nitro.operator.MaterializeOperator;
import org.weakref.nitro.operator.MultiStageOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.OffsetOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.RankWindowFunction;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.SortOperator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TopNRankingOperator;
import org.weakref.nitro.operator.UnionAllOperator;
import org.weakref.nitro.operator.WindowOperator;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.AvgF64;
import org.weakref.nitro.operator.aggregation.ConditionalSum;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.FilteredAccumulator;
import org.weakref.nitro.operator.aggregation.First;
import org.weakref.nitro.operator.aggregation.GeneratedGroupedAggregationUnit;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.StddevSamp;
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.aggregation.SumF64;
import org.weakref.nitro.operator.aggregation.SumProductIfEqual;
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
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Producer;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.generator.SequenceGenerator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestOperators
{
    private final Allocator allocator = new Allocator(EngineResources.createDefault());

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
                                                allocator,
                                                allocator.engineResources().operatorResources().filter()))))))
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
            public void constrain(Mask mask)
            {
            }

            @Override
            public void close()
            {
            }
        };

        assertThat(operator(new ProjectOperator(allocator, evaluationPlan, primitiveRegistry, source)))
                .matchesExactly(List.of(row(30L), row(10L), row(20L), row(30L)));
    }

    @Test
    void testDictionarySharedMappingIdentity()
    {
        int[] ids = {2, 0, 1, 2};
        I64Vector values = new I64Vector(new long[] {10, 20, 30});
        DictionaryVector dictionary = DictionaryVector.wrap(ids, values);
        DictionaryVector firstView = dictionary.sharedMappingView();
        DictionaryVector secondView = firstView.sharedMappingView();

        assertThat(dictionary.hasSameMapping(firstView)).isTrue();
        assertThat(firstView.hasSameMapping(dictionary)).isTrue();
        assertThat(firstView.hasSameMapping(secondView)).isTrue();
        assertThat(firstView.ids()).isSameAs(ids);
        assertThat(firstView.values()).isSameAs(values);

        assertThat(dictionary.hasSameMapping(DictionaryVector.wrap(ids, values))).isFalse();
        assertThat(dictionary.hasSameMapping(DictionaryVector.wrap(ids.clone(), values))).isFalse();
        assertThat(dictionary.hasSameMapping(null)).isFalse();
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
    void testProjectOperatorFusesUtf8CategoricalBucketsOverNestedDictionaries()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable zero = new Variable(0);
        Variable monday = new Variable(1);
        Variable tuesday = new Variable(2);
        Variable isMonday = new Variable(3);
        Variable isTuesday = new Variable(4);
        Variable mondayValue = new Variable(5);
        Variable tuesdayValue = new Variable(6);
        EvaluationPlan evaluationPlan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(monday, new Literal("Monday"), AllMask.ALL),
                        new Assignment(tuesday, new Literal("Tuesday"), AllMask.ALL),
                        new Assignment(isMonday, new Call("eq_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(monday, Stream.VALUES))), AllMask.ALL),
                        new Assignment(isTuesday, new Call("eq_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(tuesday, Stream.VALUES))), AllMask.ALL),
                        new Assignment(mondayValue, new Call("if_i64", List.of(
                                new Reference(isMonday, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(tuesdayValue, new Call("if_i64", List.of(
                                new Reference(isTuesday, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                List.of(
                        new Reference(mondayValue, Stream.VALUES),
                        new Reference(tuesdayValue, Stream.VALUES)));

        BinaryVector names = new BinaryVector(3, 19);
        names.setBytes(0, "Monday".getBytes(UTF_8));
        names.setBytes(1, "Tuesday".getBytes(UTF_8));
        names.setBytes(2, "Sunday".getBytes(UTF_8));
        int[] innerIds = {2, 0, 1};
        int[] outerIds = {1, 2, 0, 1};
        Vector encodedNames = DictionaryVector.wrap(outerIds, DictionaryVector.wrap(innerIds, names));
        Vector encodedNulls = DictionaryVector.wrap(
                outerIds,
                DictionaryVector.wrap(innerIds, new BooleanVector(new boolean[] {false, false, true})));

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
                        Mask.all(4),
                        Output.of(Streams.of(encodedNames, encodedNulls, null)),
                        Output.of(Streams.ofValues(new I64Vector(new long[] {10, 20, 30, 40}))));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        assertThat(operator(new ProjectOperator(allocator, evaluationPlan, primitiveRegistry, source)))
                .matchesExactly(List.of(
                        row(10L, 0L),
                        row(0L, 20L),
                        row(0L, 0L),
                        row(40L, 0L)));
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
    void testSortOperatorColumnarRadixIsStable()
    {
        assertThat(operator(new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {true},
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(2.0, 10L),
                                row(1.0, 20L),
                                row(2.0, 30L),
                                row(3.0, 40L))))))
                .matchesExactly(List.of(
                        row(3.0, 40L),
                        row(2.0, 10L),
                        row(2.0, 30L),
                        row(1.0, 20L)));
    }

    @Test
    void testSortOperatorColumnarMultiKeyNullOrdering()
    {
        assertThat(operator(new SortOperator(
                allocator,
                new int[] {0, 1},
                new boolean[] {false, false},
                new ConstantTableOperator(
                        allocator,
                        3,
                        List.of(
                                row(2L, "b", 10L),
                                row(1L, "z", 20L),
                                row(1L, "a", 30L),
                                row(null, "x", 40L),
                                row(1L, null, 50L))))))
                .matchesExactly(List.of(
                        row(1L, "a", 30L),
                        row(1L, "z", 20L),
                        row(1L, null, 50L),
                        row(2L, "b", 10L),
                        row(null, "x", 40L)));
    }

    @Test
    void testSortOperatorIgnoresTrailingEmptyBatchWithPartialStreams()
    {
        Operator source = new Operator()
        {
            private int batch;

            @Override
            public int outputCount()
            {
                return 1;
            }

            @Override
            public boolean hasNext()
            {
                return batch < 2;
            }

            @Override
            public Batch next()
            {
                if (batch++ == 0) {
                    return new Batch(Mask.all(2), Output.of(Streams.ofValues(new I64Vector(new long[] {2, 1}))));
                }
                return new Batch(
                        Mask.none(1),
                        Output.of(Streams.of(Stream.NULLS, new BooleanVector(new boolean[] {true}))));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        assertThat(operator(new SortOperator(allocator, new int[] {0}, new boolean[] {false}, source)))
                .matchesExactly(List.of(row(1L), row(2L)));
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
    void testDictionaryWrapNestedPreservesNestedIdsAndLogicalLength()
    {
        DictionaryVector base = DictionaryVector.wrap(new int[] {2, 1, 0, 1}, new I64Vector(new long[] {10, 20, 30}));
        DictionaryVector nested = DictionaryVector.wrapNested(new int[] {3, 0, 2, 1}, 3, base);

        assertThat(nested.length()).isEqualTo(3);
        assertThat(nested.values()).isSameAs(base);
        assertThat(nested.ids()).containsExactly(3, 0, 2, 1);
        assertThat(((I64Vector) base.values()).values()[base.ids()[nested.ids()[0]]]).isEqualTo(20L);
        assertThat(((I64Vector) base.values()).values()[base.ids()[nested.ids()[1]]]).isEqualTo(30L);
        assertThat(((I64Vector) base.values()).values()[base.ids()[nested.ids()[2]]]).isEqualTo(10L);
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
    void testDictionaryComposesBasePositionsIntoCallerScratch()
    {
        I64Vector base = new I64Vector(new long[] {10, 20, 30, 40});
        DictionaryVector inner = DictionaryVector.wrapNested(new int[] {3, 1, 0, 2}, 4, base);
        DictionaryVector middle = DictionaryVector.wrapNested(new int[] {2, 0, 3}, 3, inner);
        DictionaryVector outer = DictionaryVector.wrapNested(new int[] {1, 2, 0, 2}, 3, middle);
        int[] composed = {-1, -1, -1, -1, -1};

        Vector resolvedBase = outer.composeBasePositions(composed);

        assertThat(resolvedBase).isSameAs(base);
        assertThat(composed).containsExactly(3, 2, 0, -1, -1);
    }

    @Test
    void testFusedProjectionReadsNestedDoubleDictionaryWithoutFlatteningValues()
    {
        F64Vector base = new F64Vector(new double[] {10, 20, 30, 40});
        DictionaryVector inner = DictionaryVector.wrapNested(new int[] {3, 1, 0, 2}, 4, base);
        DictionaryVector middle = DictionaryVector.wrapNested(new int[] {2, 0, 3}, 3, inner);
        DictionaryVector outer = DictionaryVector.wrapNested(new int[] {1, 2, 0}, 3, middle);
        Operator source = singleBatchOperator(Streams.ofValues(outer));

        Variable one = new Variable(0);
        Variable two = new Variable(1);
        Variable incremented = new Variable(2);
        Variable doubled = new Variable(3);
        Reference result = new Reference(doubled, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1.0), AllMask.ALL),
                        new Assignment(two, new Literal(2.0), AllMask.ALL),
                        new Assignment(incremented, new Call("add_f64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(doubled, new Call("multiply_f64", List.of(
                                new Reference(incremented, Stream.VALUES),
                                new Reference(two, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, primitiveRegistry(), List.of(result))).isPresent();
        }
        try (ProjectOperator operator = new ProjectOperator(allocator, plan, primitiveRegistry(), source);
                Batch batch = operator.next()) {
            assertThat(((F64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(82.0, 62.0, 22.0);
        }
    }

    @Test
    void testFusedProjectionCompilesUtf8InList()
    {
        Variable apple = new Variable(0);
        Variable orange = new Variable(1);
        Variable inList = new Variable(2);
        Variable one = new Variable(3);
        Variable zero = new Variable(4);
        Variable selected = new Variable(5);
        Reference selectedReference = new Reference(selected, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(apple, new Literal("apple"), AllMask.ALL),
                        new Assignment(orange, new Literal("orange"), AllMask.ALL),
                        new Assignment(inList, new Call("in_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(apple, Stream.VALUES),
                                new Reference(orange, Stream.VALUES))), AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(selected, new Call("if_i64", List.of(
                                new Reference(inList, Stream.VALUES),
                                new Reference(one, Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                List.of(selectedReference));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, primitiveRegistry(), List.of(selectedReference))).isPresent();
        }
        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                primitiveRegistry(),
                new ConstantTableOperator(allocator, 1, List.of(row("apple"), row("pear"), row((Object) null))));
                Batch batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(1L, 0L, 0L);
        }
    }

    @Test
    void testFusedProjectionUsesDynamicallyRegisteredProviderWithoutFunctionVocabulary()
    {
        class DynamicallyNamedFunction
                implements PrimitiveFunction
        {
            @Override
            public Streams apply(
                    List<Streams> inputs,
                    Mask mask,
                    Set<Stream> requestedStreams,
                    Streams output,
                    PrimitiveExecutionContext context)
            {
                throw new UnsupportedOperationException();
            }
        }

        class DynamicallyNamedProjection
                implements ProjectionCodeProvider
        {
            @Override
            public Optional<ProjectionProgram> generate(
                    ProjectionCodeBuilder builder,
                    List<ProjectionArgument> arguments)
            {
                if (arguments.size() != 2) {
                    return Optional.empty();
                }
                var left = builder.argument(0, ProjectionCodeBuilder.ValueType.I64);
                var right = builder.argument(1, ProjectionCodeBuilder.ValueType.I64);
                return Optional.of(builder.program(
                        List.of(ProjectionCodeBuilder.ValueType.I64, ProjectionCodeBuilder.ValueType.I64),
                        builder.add(left, right),
                        builder.or(builder.isNull(0), builder.isNull(1))));
            }
        }

        String dynamicName = "provider_name_unknown_to_engine";
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register(dynamicName, new DynamicallyNamedFunction(), new DynamicallyNamedProjection());
        Variable one = new Variable(0);
        Variable first = new Variable(1);
        Variable second = new Variable(2);
        Reference result = new Reference(second, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(first, new Call(dynamicName, List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(second, new Call(dynamicName, List.of(
                                new Reference(first, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, registry, List.of(result))).isPresent();
        }
    }

    @Test
    void testFusedProjectionCompilesMixedWidthNullTests()
    {
        Variable zero = new Variable(0);
        Variable i32Null = new Variable(1);
        Variable i64Null = new Variable(2);
        Variable anyNull = new Variable(3);
        Variable product = new Variable(4);
        Variable selected = new Variable(5);
        Reference result = new Reference(selected, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(i32Null, new Call("is_null_i32", List.of(
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                        new Assignment(i64Null, new Call("is_null_i64", List.of(
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(anyNull, new Call("or", List.of(
                                new Reference(i32Null, Stream.VALUES),
                                new Reference(i64Null, Stream.VALUES))), AllMask.ALL),
                        new Assignment(product, new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(selected, new Call("if_i64", List.of(
                                new Reference(anyNull, Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(product, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        PrimitiveRegistry registry = primitiveRegistry();
        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, registry, List.of(result))).isPresent();
        }
        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                registry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(3, 7L),
                                row(null, 11L),
                                row(5, null),
                                row(null, null))));
                Batch batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values())
                    .containsExactly(21L, 0L, 0L, 0L);
        }
    }

    @Test
    void testFusedProjectionLeavesShortNullOnlySliceToInterpreter()
    {
        Variable zero = new Variable(0);
        Variable one = new Variable(1);
        Variable isNull = new Variable(2);
        Variable selected = new Variable(3);
        Reference result = new Reference(selected, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(isNull, new Call("is_null_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                        new Assignment(selected, new Call("if_i64", List.of(
                                new Reference(isNull, Stream.VALUES),
                                new Reference(zero, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, primitiveRegistry(), List.of(result))).isEmpty();
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

        Vector noNulls = DictionaryVector.wrap(
                new int[] {0, 1, 2, 3},
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
    void testFusedProjectionCompilesVariableWidthConditional()
    {
        Variable zero = new Variable(0);
        Variable leftEquals = new Variable(1);
        Variable rightEquals = new Variable(2);
        Variable bothEqual = new Variable(3);
        Variable empty = new Variable(4);
        Variable selected = new Variable(5);
        Reference result = new Reference(selected, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(leftEquals, new Call("eq", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(rightEquals, new Call("eq", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(bothEqual, new Call("and", List.of(
                                new Reference(leftEquals, Stream.VALUES),
                                new Reference(rightEquals, Stream.VALUES))), AllMask.ALL),
                        new Assignment(empty, new Literal(""), AllMask.ALL),
                        new Assignment(selected, new Call("if_utf8", List.of(
                                new Reference(bothEqual, Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(empty, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        PrimitiveRegistry registry = primitiveRegistry();
        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, registry, List.of(result))).isPresent();
        }
        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                registry,
                new ConstantTableOperator(
                        allocator,
                        3,
                        List.of(
                                row(0L, 0L, "selected"),
                                row(0L, 1L, "not-selected"),
                                row(null, 0L, "null-condition"))));
                Batch batch = operator.next()) {
            BinaryVector values = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            assertThat(new String(values.copyBytes(0), UTF_8)).isEqualTo("selected");
            assertThat(new String(values.copyBytes(1), UTF_8)).isEmpty();
            assertThat(new String(values.copyBytes(2), UTF_8)).isEmpty();
        }
    }

    @Test
    void testFusedProjectionWritesVariableWidthOutputForSparseMask()
    {
        Variable zero = new Variable(0);
        Variable leftEquals = new Variable(1);
        Variable rightEquals = new Variable(2);
        Variable bothEqual = new Variable(3);
        Variable empty = new Variable(4);
        Variable selected = new Variable(5);
        Reference result = new Reference(selected, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(leftEquals, new Call("eq", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(rightEquals, new Call("eq", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(bothEqual, new Call("and", List.of(
                                new Reference(leftEquals, Stream.VALUES),
                                new Reference(rightEquals, Stream.VALUES))), AllMask.ALL),
                        new Assignment(empty, new Literal(""), AllMask.ALL),
                        new Assignment(selected, new Call("if_utf8", List.of(
                                new Reference(bothEqual, Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(empty, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        BinaryVector input = new BinaryVector(4, 29);
        input.setBytes(0, "ignored".getBytes(UTF_8));
        input.setBytes(1, "alpha".getBytes(UTF_8));
        input.setBytes(2, "also-ignored".getBytes(UTF_8));
        input.setBytes(3, "omega".getBytes(UTF_8));
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
                        Mask.sparse(new int[] {1, 3}, 4),
                        Output.of(Streams.ofValues(new I64Vector(new long[] {9, 0, 9, 0}))),
                        Output.of(Streams.ofValues(new I64Vector(new long[] {9, 0, 9, 0}))),
                        Output.of(Streams.builder()
                                .put(Stream.VALUES, input)
                                .put(Stream.NULLS, new BooleanVector(new boolean[] {false, false, false, true}))
                                .build()));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (ProjectOperator operator = new ProjectOperator(allocator, plan, primitiveRegistry(), source);
                Batch batch = operator.next()) {
            assertThat(batch.borrowMask()).containsExactly(1, 3);
            BinaryVector values = (BinaryVector) batch.output(0).borrow(Stream.VALUES);
            assertThat(values.offsets()).containsExactly(0, 0, 5, 5, 5);
            assertThat(new String(values.copyBytes(0), UTF_8)).isEmpty();
            assertThat(new String(values.copyBytes(1), UTF_8)).isEqualTo("alpha");
            assertThat(new String(values.copyBytes(2), UTF_8)).isEmpty();
            assertThat(new String(values.copyBytes(3), UTF_8)).isEmpty();
            assertThat(((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values())
                    .containsExactly(false, false, false, true);
        }
    }

    @Test
    void testFusedProjectionLeavesShortVariableWidthSliceToInterpreter()
    {
        Variable zero = new Variable(0);
        Variable equals = new Variable(1);
        Variable sentinel = new Variable(2);
        Variable selected = new Variable(3);
        Reference result = new Reference(selected, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(equals, new Call("eq", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(sentinel, new Literal("sentinel"), AllMask.ALL),
                        new Assignment(selected, new Call("if_utf8", List.of(
                                new Reference(equals, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(sentinel, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, primitiveRegistry(), List.of(result))).isEmpty();
        }
    }

    @Test
    void testFusedProjectionCompilesUtf8PrefixIntoFixedWidthConditional()
    {
        Variable prefix = new Variable(0);
        Variable matches = new Variable(1);
        Variable zero = new Variable(2);
        Variable selected = new Variable(3);
        Reference result = new Reference(selected, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(prefix, new Literal("PROMO"), AllMask.ALL),
                        new Assignment(matches, new Call("starts_with_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(prefix, Stream.VALUES))), AllMask.ALL),
                        new Assignment(zero, new Literal(0.0), AllMask.ALL),
                        new Assignment(selected, new Call("if_f64", List.of(
                                new Reference(matches, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                List.of(result));

        PrimitiveRegistry registry = primitiveRegistry();
        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, registry, List.of(result))).isPresent();
        }
        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                registry,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row("PROMO LARGE", 11.5),
                                row("STANDARD", 12.5),
                                row(null, 13.5))));
                Batch batch = operator.next()) {
            assertThat(((F64Vector) batch.output(0).borrow(Stream.VALUES)).values())
                    .containsExactly(11.5, 0.0, 0.0);
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
                boolean[] values = ((BooleanVector) batch.output(0).borrow(Stream.VALUES)).values();
                // A value at a null position is deliberately unspecified and can contain pooled storage state.
                assertThat(values[0]).isTrue();
                assertThat(values[2]).isFalse();
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
    void testGroupOperatorPreservesSourceSchemaAfterGroupId()
    {
        TypeBinding i32Only = i32OnlyType();
        Field first = new Field("first", i32Only, false);
        Field second = new Field("second", i32Only, true);
        Schema sourceSchema = new Schema(List.of(first, second));

        try (GroupOperator group = new GroupOperator(
                allocator,
                0,
                typedTable(sourceSchema))) {
            assertThat(group.outputSchema().field(0).type().isSpecified()).isFalse();
            assertThat(group.outputSchema().field(1)).isSameAs(first);
            assertThat(group.outputSchema().field(2)).isSameAs(second);
        }
    }

    @Test
    void testRowShapingOperatorsPreserveSourceSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema sourceSchema = new Schema(List.of(
                new Field("first", i32Only, false),
                new Field("second", i32Only, true)));

        try (Operator limit = new LimitOperator(allocator, 1, typedTable(sourceSchema));
                Operator sort = new SortOperator(allocator, new int[] {0}, new boolean[] {false}, typedTable(sourceSchema));
                Operator topN = new TopNOperator(allocator, 1, 0, typedTable(sourceSchema))) {
            assertThat(limit.outputSchema()).isSameAs(sourceSchema);
            assertThat(sort.outputSchema()).isSameAs(sourceSchema);
            assertThat(topN.outputSchema()).isSameAs(sourceSchema);
        }
    }

    @Test
    void testTransparentOperatorsPreserveSourceSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema sourceSchema = new Schema(List.of(
                new Field("first", i32Only, false),
                new Field("second", i32Only, true)));

        try (Operator batchSlice = new BatchSliceOperator(allocator, 10, typedTable(sourceSchema));
                Operator counting = new CountingNextOperator(typedTable(sourceSchema));
                Operator singleRow = new EnforceSingleRowOperator(allocator, typedTable(sourceSchema));
                Operator materialize = new MaterializeOperator(allocator, typedTable(sourceSchema));
                Operator offset = new OffsetOperator(allocator, 1, typedTable(sourceSchema))) {
            assertThat(batchSlice.outputSchema()).isSameAs(sourceSchema);
            assertThat(counting.outputSchema()).isSameAs(sourceSchema);
            assertThat(singleRow.outputSchema()).isSameAs(sourceSchema);
            assertThat(materialize.outputSchema()).isSameAs(sourceSchema);
            assertThat(offset.outputSchema()).isSameAs(sourceSchema);
        }
    }

    @Test
    void testFullJoinPreservesFieldsAndWidensNullability()
    {
        TypeBinding i32Only = i32OnlyType();
        Field outerKey = new Field("outer_key", i32Only, false);
        Field outerValue = new Field("outer_value", i32Only, true);
        Field innerKey = new Field("inner_key", i32Only, false);
        Schema outerSchema = new Schema(List.of(outerKey, outerValue));
        Schema innerSchema = new Schema(List.of(innerKey));

        try (Operator join = new FullJoinOperator(
                allocator,
                typedTable(outerSchema),
                new int[] {0},
                typedTable(innerSchema),
                new int[] {0},
                allocator.engineResources().operatorResources().fullJoinPolicy())) {
            assertThat(join.outputSchema().fields()).extracting(Field::name)
                    .containsExactly(outerKey.name(), outerValue.name(), innerKey.name());
            assertThat(join.outputSchema().fields()).extracting(Field::type)
                    .containsExactly(i32Only, i32Only, i32Only);
            assertThat(join.outputSchema().fields()).extracting(Field::nullable)
                    .containsExactly(true, true, true);
            assertThat(join.outputSchema().field(1)).isSameAs(outerValue);
        }
    }

    @Test
    void testNestedLoopJoinConcatenatesInputSchemas()
    {
        TypeBinding i32Only = i32OnlyType();
        Field outerKey = new Field("outer_key", i32Only, false);
        Field outerValue = new Field("outer_value", i32Only, true);
        Field innerKey = new Field("inner_key", i32Only, false);
        Schema outerSchema = new Schema(List.of(outerKey, outerValue));
        Schema innerSchema = new Schema(List.of(innerKey));

        try (Operator join = new NestedLoopJoinOperator(
                allocator,
                typedTable(outerSchema),
                typedTable(innerSchema))) {
            assertThat(join.outputSchema().fields())
                    .containsExactly(outerKey, outerValue, innerKey);
            assertThat(join.outputSchema().field(0)).isSameAs(outerKey);
            assertThat(join.outputSchema().field(1)).isSameAs(outerValue);
            assertThat(join.outputSchema().field(2)).isSameAs(innerKey);
        }
    }

    @Test
    void testRankingAndWindowOperatorsUseDeclaredResultSchemas()
    {
        TypeBinding i32Only = i32OnlyType();
        Field sourceField = new Field("source", i32Only, false);
        Field rankField = new Field("rank", i32Only, false);
        Field runningField = new Field("running", i32Only, true);
        Schema sourceSchema = new Schema(List.of(sourceField));
        Schema rankSchema = new Schema(List.of(rankField));
        Schema windowSchema = new Schema(List.of(rankField, runningField));

        try (Operator ranking = new TopNRankingOperator(
                allocator,
                10,
                new int[] {0},
                new boolean[] {false},
                typedTable(sourceSchema),
                rankSchema,
                allocator.engineResources().operatorResources().topNRankingPolicy());
                Operator window = new WindowOperator(
                        allocator,
                        typedTable(sourceSchema),
                        new int[0],
                        new int[] {0},
                        new boolean[] {false},
                        List.of(
                                new RankWindowFunction(new int[] {0}, new boolean[] {false}),
                                new RankWindowFunction(new int[] {0}, new boolean[] {false})),
                        windowSchema)) {
            assertThat(ranking.outputSchema().fields()).containsExactly(sourceField, rankField);
            assertThat(ranking.outputSchema().field(0)).isSameAs(sourceField);
            assertThat(ranking.outputSchema().field(1)).isSameAs(rankField);
            assertThat(window.outputSchema().fields()).containsExactly(sourceField, rankField, runningField);
            assertThat(window.outputSchema().field(0)).isSameAs(sourceField);
            assertThat(window.outputSchema().field(1)).isSameAs(rankField);
            assertThat(window.outputSchema().field(2)).isSameAs(runningField);
        }
    }

    @Test
    void testMarkerOperatorsPreserveSourceSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Field key = new Field("key", i32Only, false);
        Field payload = new Field("payload", i32Only, true);
        Schema sourceSchema = new Schema(List.of(key, payload));
        Schema keySchema = new Schema(List.of(key));

        try (Operator distinct = new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0},
                typedTable(sourceSchema),
                false,
                allocator.engineResources().operatorResources());
                Operator filteringSemiJoin = new SemiJoinOperator(
                        allocator,
                        typedTable(sourceSchema),
                        0,
                        typedTable(keySchema),
                        0,
                        true,
                        false);
                Operator markingSemiJoin = new SemiJoinOperator(
                        allocator,
                        typedTable(sourceSchema),
                        0,
                        typedTable(keySchema),
                        0,
                        true,
                        true)) {
            assertThat(filteringSemiJoin.outputSchema()).isSameAs(sourceSchema);
            assertMarkerSchema(distinct.outputSchema(), key, payload);
            assertMarkerSchema(markingSemiJoin.outputSchema(), key, payload);
        }
    }

    private static void assertMarkerSchema(Schema schema, Field key, Field payload)
    {
        assertThat(schema.field(0)).isSameAs(key);
        assertThat(schema.field(1)).isSameAs(payload);
        assertThat(schema.field(2).type().isSpecified()).isFalse();
        assertThat(schema.field(2).nullable()).isFalse();
    }

    @Test
    void testUnionAllUsesExplicitOutputSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema outputSchema = new Schema(List.of(new Field("value", i32Only, true)));

        try (Operator union = new UnionAllOperator(
                outputSchema,
                List.of(typedTable(outputSchema), typedTable(outputSchema)))) {
            assertThat(union.outputSchema()).isSameAs(outputSchema);
            assertThat(union.outputCount()).isEqualTo(outputSchema.size());
        }
    }

    @Test
    void testTableAndMultiStageOperatorsUseExplicitOutputSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema outputSchema = new Schema(List.of(new Field("value", i32Only, true)));
        AtomicInteger factoryCalls = new AtomicInteger();

        try (Operator table = new TableOperator(outputSchema, List.of());
                Operator stages = new MultiStageOperator(
                        outputSchema,
                        List.of(1),
                        ignored -> {
                            factoryCalls.incrementAndGet();
                            return new TableOperator(outputSchema, List.of());
                        })) {
            assertThat(table.outputSchema()).isSameAs(outputSchema);
            assertThat(table.outputCount()).isEqualTo(outputSchema.size());
            assertThat(stages.outputSchema()).isSameAs(outputSchema);
            assertThat(stages.outputCount()).isEqualTo(outputSchema.size());
            assertThat(factoryCalls).hasValue(0);
        }
    }

    @Test
    void testGroupedConditionalProductSumPreservesSqlNullAndZeroSemantics()
    {
        assertThat(operator(new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new SumProductIfEqual(1, 1, 2, 3)),
                new ConstantTableOperator(
                        allocator,
                        4,
                        List.of(
                                row(10L, 1L, 2L, 3L),
                                row(10L, 2L, 100L, 100L),
                                row(10L, 1L, null, 5L),
                                row(20L, 2L, 7L, 8L),
                                row(30L, 1L, null, 9L))))))
                .matchesExactly(List.of(row(10L, 6L), row(20L, 0L), row(30L, null)));
    }

    @Test
    void testPhysicalAggregationProgramRoutesMultipleResultsFromOneUnit()
    {
        SumAndCountUnit unit = new SumAndCountUnit(0);
        PhysicalAggregationProgram program = new PhysicalAggregationProgram(
                List.of(unit),
                List.of(
                        new PhysicalAggregationProgram.Output(0, 1),
                        new PhysicalAggregationProgram.Output(0, 0)));

        assertThat(operator(new AggregationOperator(
                allocator,
                program,
                new ConstantTableOperator(allocator, 1, List.of(row(3L), row(5L), row(7L))))))
                .matchesExactly(List.of(row(3L, 15L)));
        assertThat(unit.accumulationCalls).isEqualTo(1);
    }

    @Test
    void testGroupedPhysicalAggregationProgramRoutesMultipleResultsFromOneUnit()
    {
        SumAndCountUnit unit = new SumAndCountUnit(1);
        PhysicalAggregationProgram program = new PhysicalAggregationProgram(
                List.of(unit),
                List.of(
                        new PhysicalAggregationProgram.Output(0, 1),
                        new PhysicalAggregationProgram.Output(0, 0)));

        assertThat(operator(new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(0),
                program,
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(row(10L, 3L), row(20L, 11L), row(10L, 5L))),
                allocator.engineResources().operatorResources())))
                .matchesExactly(List.of(row(10L, 2L, 8L), row(20L, 1L, 11L)));
        assertThat(unit.accumulationCalls).isZero();
    }

    @Test
    void testPhysicalAggregationProgramDeclaresOperatorSchemas()
    {
        TypeBinding i32Only = i32OnlyType();
        Field key = new Field("key", i32Only, false);
        Field count = new Field("count", i32Only, false);
        Field sum = new Field("sum", i32Only, true);
        Schema sourceSchema = new Schema(List.of(key));
        Schema resultSchema = new Schema(List.of(count, sum));
        PhysicalAggregationProgram program = new PhysicalAggregationProgram(
                List.of(new SumAndCountUnit(0)),
                List.of(
                        new PhysicalAggregationProgram.Output(0, 1),
                        new PhysicalAggregationProgram.Output(0, 0)),
                resultSchema);

        try (Operator global = new AggregationOperator(allocator, program, typedTable(sourceSchema));
                Operator grouped = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        List.of(0),
                        program,
                        typedTable(sourceSchema))) {
            assertThat(global.outputSchema()).isSameAs(resultSchema);
            assertThat(grouped.outputSchema().field(0)).isSameAs(key);
            assertThat(grouped.outputSchema().field(1)).isSameAs(count);
            assertThat(grouped.outputSchema().field(2)).isSameAs(sum);
        }
    }

    @Test
    void testConditionalSumsPreserveSqlNullAndZeroSemanticsForBinaryDiscriminator()
    {
        assertThat(operator(new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(
                        ConditionalSum.equalUtf8(1, "Monday", 2),
                        ConditionalSum.equalUtf8(1, "Tuesday", 2)),
                new ConstantTableOperator(
                        allocator,
                        3,
                        List.of(
                                row(10L, "Monday", 2L),
                                row(10L, "Tuesday", 3L),
                                row(10L, "Monday", null),
                                row(20L, "Monday", null),
                                row(30L, "Sunday", 7L),
                                row(40L, null, 100L))))))
                .matchesExactly(List.of(
                        row(10L, 2L, 3L),
                        row(20L, null, 0L),
                        row(30L, 0L, 0L),
                        row(40L, 0L, 0L)));
    }

    @Test
    void testConditionalSumsSupportLongDiscriminator()
    {
        assertThat(operator(new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(
                        ConditionalSum.equalLong(1, 1, 2),
                        ConditionalSum.equalLong(1, 2, 2)),
                new ConstantTableOperator(
                        allocator,
                        3,
                        List.of(
                                row(10L, 1L, 4L),
                                row(10L, 2L, 5L),
                                row(20L, 3L, 6L))))))
                .matchesExactly(List.of(
                        row(10L, 4L, 5L),
                        row(20L, 0L, 0L)));
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
    void testGroupedAggregationOperatorClosesEveryConsumedBatch()
    {
        int batchCount = 5;
        AtomicInteger nextBatch = new AtomicInteger();
        AtomicInteger closedBatches = new AtomicInteger();

        Operator source = new Operator()
        {
            @Override
            public int outputCount()
            {
                return 1;
            }

            @Override
            public boolean hasNext()
            {
                return nextBatch.get() < batchCount;
            }

            @Override
            public Batch next()
            {
                long value = nextBatch.getAndIncrement();
                return new Batch(
                        Mask.all(2),
                        _ -> {},
                        Function.identity(),
                        _ -> {},
                        closedBatches::incrementAndGet,
                        Output.of(Streams.ofValues(new I64Vector(new long[] {value, value}))));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (Operator operator = new GroupedAggregationOperator(
                allocator,
                0,
                List.of(1),
                List.of(new CountAll()),
                new GroupOperator(allocator, 0, source))) {
            try (Batch result = operator.next()) {
                assertThat(result.output(1).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
            }
            assertThat(closedBatches).hasValue(batchCount);
        }
    }

    @Test
    void testHashJoinOperatorClosesEveryConsumedProbeBatch()
    {
        int batchCount = 5;
        AtomicInteger nextBatch = new AtomicInteger();
        AtomicInteger closedBatches = new AtomicInteger();

        Operator probe = new Operator()
        {
            @Override
            public int outputCount()
            {
                return 1;
            }

            @Override
            public boolean hasNext()
            {
                return nextBatch.get() < batchCount;
            }

            @Override
            public Batch next()
            {
                long value = nextBatch.getAndIncrement();
                return new Batch(
                        Mask.all(1),
                        _ -> {},
                        Function.identity(),
                        _ -> {},
                        closedBatches::incrementAndGet,
                        Output.of(Streams.ofValues(new I64Vector(new long[] {value}))));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (Operator operator = new HashJoinOperator(
                allocator,
                probe,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(0L), row(1L), row(2L), row(3L), row(4L))),
                0)) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    result.output(0).borrow(Stream.VALUES);
                }
            }
            assertThat(closedBatches).hasValue(batchCount);
        }
    }

    @Test
    void testHashJoinReleasesMaterializedResultBuffersWhenBatchCloses()
    {
        Allocator.Context profileContext = new Allocator.Context("HashJoinOperator");
        try (Operator operator = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(1L))),
                0,
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 11L))),
                0)) {
            Batch result = operator.next();
            long bytesBeforeBorrow = allocator.currentBytes(profileContext);
            result.output(2).borrow(Stream.VALUES);
            long bytesAfterBorrow = allocator.currentBytes(profileContext);
            assertThat(bytesAfterBorrow).isGreaterThan(bytesBeforeBorrow);

            result.close();
            assertThat(allocator.currentBytes(profileContext)).isLessThan(bytesAfterBorrow);
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
    void testAggregationOperatorMaterializesResultOnlyAfterAllInputBatches()
    {
        AtomicInteger resultMaterializations = new AtomicInteger();
        Sum sum = new Sum(0)
        {
            @Override
            public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
            {
                resultMaterializations.incrementAndGet();
                return super.result(maxGroup, state, output, allocator, allocationContext);
            }
        };

        try (Operator operator = new AggregationOperator(
                allocator,
                List.of(sum),
                new GeneratorOperator(allocator, 25_000, 10_000, List.of(new SequenceGenerator(1))))) {
            Batch batch = operator.next();
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()[0]).isEqualTo(312_512_500L);
            assertThat(resultMaterializations).hasValue(1);
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
            Vector errors = batch.output(0).borrow(Stream.ERRORS);
            VectorAccess.BooleanValues errorValues = VectorAccess.booleanValues(errors);
            int count = batch.borrowMask().count();
            boolean[] decoded = new boolean[count];
            int cursor = 0;
            for (int position : batch.borrowMask()) {
                decoded[cursor++] = errorValues.value(position);
            }
            assertThat(decoded).containsExactly(false, false, false);
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
            Vector nulls = batch.output(0).borrow(Stream.NULLS);
            VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
            int count = batch.borrowMask().count();
            boolean[] decoded = new boolean[count];
            int cursor = 0;
            for (int position : batch.borrowMask()) {
                decoded[cursor++] = nullValues.value(position);
            }
            assertThat(decoded).containsExactly(false, false, false);
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
                        allocator,
                        allocator.engineResources().operatorResources().filter())))
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
                        allocator,
                        allocator.engineResources().operatorResources().filter())))
                .matchesExactly(List.of(
                        row(3L, 30L),
                        row(4L, 40L)));
    }

    @Test
    void testFilterOperatorPassesCurrentPredicateMaskToLazyInputs()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        AtomicReference<int[]> lazyInputMask = new AtomicReference<>();
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
                BooleanVector first = new BooleanVector(new boolean[] {true, false, true, false});
                BooleanVector second = new BooleanVector(new boolean[] {false, true, false, true});
                return new Batch(
                        Mask.all(4),
                        new Output(Set.of(Stream.VALUES), _ -> first),
                        new Output(
                                Set.of(Stream.VALUES),
                                _ -> second,
                                (_, mask) -> {
                                    int[] positions = new int[mask.selectedCount()];
                                    for (int index = 0; index < positions.length; index++) {
                                        positions[index] = mask.position(index);
                                    }
                                    lazyInputMask.set(positions);
                                    return second;
                                },
                                (_, vector) -> vector,
                                (_, _) -> {},
                                null,
                                null));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (FilterOperator filter = new FilterOperator(
                source,
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry,
                new OrMask(List.of(
                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                allocator,
                allocator.engineResources().operatorResources().filter())) {
            try (Batch batch = filter.next()) {
                assertThat(batch.borrowMask()).containsExactly(0, 1, 2, 3);
            }
        }

        assertThat(lazyInputMask.get()).containsExactly(1, 3);
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
                                2,
                                primitiveRegistry),
                        0,
                        3,
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
    void testNestedAggregationsDoNotReleaseOuterState()
    {
        Operator firstEmptySemiJoin = new SemiJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of()),
                0,
                new AggregationOperator(
                        allocator,
                        List.of(new Sum(0)),
                        new ConstantTableOperator(allocator, 1, List.of(row(7L)))),
                0);
        Operator secondEmptySemiJoin = new SemiJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of()),
                0,
                new AggregationOperator(
                        allocator,
                        List.of(new Sum(0)),
                        new ConstantTableOperator(allocator, 1, List.of(row(11L)))),
                0);

        assertThat(operator(new AggregationOperator(
                allocator,
                List.of(new Sum(0)),
                new UnionAllOperator(1, List.of(firstEmptySemiJoin, secondEmptySemiJoin)))))
                .matchesExactly(List.of(row((Object) null)));
    }

    @Test
    void testF64SumAvgPreserveIndependentResultsAndNulls()
    {
        Operator global = new AggregationOperator(
                allocator,
                List.of(new SumF64(0), new AvgF64(0)),
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(row(1.5), row((Object) null), row(2.5), row(4.0))));
        assertThat(operator(global))
                .matchesExactly(List.of(row(8.0, 8.0 / 3)));

        // Reverse accumulator order verifies that output order does not affect independent state.
        Operator grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new AvgF64(1), new SumF64(1)),
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(1L, 1.0),
                                row(1L, null),
                                row(1L, 3.0),
                                row(2L, null),
                                row(2L, 5.0))));
        assertThat(operator(grouped))
                .matchesExactly(List.of(
                        row(1L, 2.0, 4.0),
                        row(2L, 5.0, 5.0)));
    }

    @Test
    void testCountAvgStddevPreserveIndependentResultsAndNulls()
    {
        // Put STDDEV first to verify that independent aggregate state is insensitive to output order.
        Operator grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new StddevSamp(1), new Avg(1), new CountColumn(1)),
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(1L, 1L),
                                row(1L, null),
                                row(1L, 3L),
                                row(2L, null),
                                row(2L, 5L))));
        assertThat(operator(grouped))
                .matchesExactly(List.of(
                        row(1L, Math.sqrt(2), 2.0, 2L),
                        row(2L, null, 5.0, 1L)));
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
    void testGroupedAccumulatorStateSurvivesFilterConstraint()
    {
        Operator grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountColumn(1)),
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(
                                row(4L, 10L),
                                row(4L, null),
                                row(4L, 20L))));

        // Resolving COUNT for the predicate and then constraining the grouped batch must not recycle the live
        // accumulator state before downstream consumers borrow it again.
        assertThat(operator(filterDivisibleBy(grouped, 2, 1, primitiveRegistry())))
                .matchesExactly(List.of(row(4L, 30L, 2L)));
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
    void testDistinctCountRejectsLaterVectorOutsidePlanTimeTypeBinding()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(new Field(i32Only, false)));
        Operator source = typedTable(
                schema,
                TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1)),
                TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)));

        assertThatThrownBy(() -> {
            try (Operator aggregation = new AggregationOperator(allocator, List.of(new DistinctCount(0)), source);
                    Batch result = aggregation.next()) {
                result.output(0).borrow(Stream.VALUES);
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Distinct key vector at index 0")
                .hasMessageContaining("testing:i32-only");
    }

    @Test
    void testGroupedDistinctRejectsLaterVectorOutsidePlanTimeTypeBinding()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(
                new Field(i32Only, false),
                new Field(i32Only, false)));
        Operator source = typedTable(
                schema,
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I32Vector(new int[] {1}), new I32Vector(new int[] {10})},
                        Mask.all(1)),
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I32Vector(new int[] {1}), new I64Vector(new long[] {20})},
                        Mask.all(1)));

        assertThatThrownBy(() -> {
            try (Operator aggregation = new GroupedAggregationOperator(
                    allocator,
                    List.of(0),
                    List.of(0),
                    List.of(new DistinctCount(1)),
                    source);
                    Batch result = aggregation.next()) {
                result.output(1).borrow(Stream.VALUES);
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Distinct key vector at index 1")
                .hasMessageContaining("testing:i32-only");
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
                                        row(2L, (Object) null))),
                        allocator.engineResources().operatorResources())))
                .matchesExactly(List.of(
                        row(1L, "alpha"),
                        row(1L, "beta"),
                        row(2L, "alpha")));
    }

    @Test
    void testMarkDistinctRejectsLaterVectorOutsidePlanTimeTypeBinding()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(new Field(i32Only, false)));
        Operator source = typedTable(
                schema,
                TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1)),
                TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)));

        try (MarkDistinctOperator distinct = new MarkDistinctOperator(
                allocator,
                0,
                source,
                allocator.engineResources().operatorResources())) {
            assertThat(distinct.outputSchema()).isEqualTo(schema);
            distinct.next().close();
            assertThatThrownBy(distinct::next)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Distinct key vector at index 0")
                    .hasMessageContaining("testing:i32-only");
        }
    }

    @Test
    void testMarkDistinctOperatorPreservesSingleBinarySentinelOrder()
    {
        assertThat(operator(
                new MarkDistinctOperator(
                        allocator,
                        0,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(
                                        row((Object) "alpha"),
                                        row((Object) ""),
                                        row((Object) "beta"),
                                        row((Object) ""),
                                        row((Object) null),
                                        row((Object) "alpha"))),
                        allocator.engineResources().operatorResources())))
                .matchesExactly(List.of(
                        row((Object) "alpha"),
                        row((Object) ""),
                        row((Object) "beta")));
    }

    @Test
    void testMarkDistinctOperatorRetainsNullKeysWithSqlDistinctSemantics()
    {
        // With retainNulls, a NULL key is a distinguishable value: equal nulls collapse to one survivor and a
        // null stays distinct from every concrete value, matching SQL DISTINCT/UNION (TPC-DS q75).
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
                                        row((Object) null, "alpha"),
                                        row(2L, (Object) null),
                                        row(2L, (Object) null),
                                        row((Object) null, (Object) null))),
                        true,
                        allocator.engineResources().operatorResources())))
                .matchesExactly(List.of(
                        row(1L, "alpha"),
                        row(1L, "beta"),
                        row(2L, "alpha"),
                        row((Object) null, "alpha"),
                        row(2L, (Object) null),
                        row((Object) null, (Object) null)));
    }

    @Test
    void testMarkDistinctOperatorWithWideIntegerKeysAndEncodedNullStreams()
    {
        long[][] columns = {
                {1, 1, 1, 2, 2, 0, 0},
                {2, 2, 2, 3, 3, 8, 8},
                {3, 3, 3, 4, 4, 9, 9},
                {4, 4, 4, 5, 5, 10, 10},
                {5, 5, 5, 6, 6, 11, 11},
                {6, 6, 6, 7, 7, 12, 12},
                {7, 7, 8, 8, 8, 13, 1L << 40},
        };
        Vector noNulls = new RleVector(new int[] {7}, new BooleanVector(new boolean[] {false}));
        Vector firstColumnNulls = new BooleanVector(new boolean[] {false, false, false, false, false, true, true});

        Operator source = new Operator()
        {
            private boolean hasNext = true;

            @Override
            public int outputCount()
            {
                return columns.length;
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
                Output[] outputs = new Output[columns.length];
                for (int column = 0; column < columns.length; column++) {
                    outputs[column] = Output.of(Streams.builder()
                            .put(Stream.VALUES, new I64Vector(columns[column]))
                            .put(Stream.NULLS, column == 0 ? firstColumnNulls : noNulls)
                            .build());
                }
                return new Batch(Mask.all(7), outputs);
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        assertThat(operator(new MarkDistinctOperator(
                allocator,
                new int[] {0, 1, 2, 3, 4, 5, 6},
                source,
                true,
                allocator.engineResources().operatorResources())))
                .matchesExactly(List.of(
                        row(1L, 2L, 3L, 4L, 5L, 6L, 7L),
                        row(1L, 2L, 3L, 4L, 5L, 6L, 8L),
                        row(2L, 3L, 4L, 5L, 6L, 7L, 8L),
                        row(null, 8L, 9L, 10L, 11L, 12L, 13L),
                        row(null, 8L, 9L, 10L, 11L, 12L, 1L << 40)));
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

        try (MarkDistinctOperator operator = new MarkDistinctOperator(allocator, 0, source, allocator.engineResources().operatorResources());
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
    void testMarkDistinctMarkerPreservesRowsAndAddsMarker()
    {
        assertThat(operator(
                new MarkDistinctMarkerOperator(
                        allocator,
                        new int[] {0, 1},
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 7L),
                                        row(1L, 7L),
                                        row(1L, 8L),
                                        row(2L, 9L),
                                        row(2L, 9L))),
                        true,
                        allocator.engineResources().operatorResources())))
                .matchesExactly(List.of(
                        row(1L, 7L, 1L),
                        row(1L, 7L, 0L),
                        row(1L, 8L, 1L),
                        row(2L, 9L, 1L),
                        row(2L, 9L, 0L)));
    }

    @Test
    void testMarkDistinctMarkerRejectsLaterVectorOutsidePlanTimeTypeBinding()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(new Field(i32Only, false)));
        Operator source = typedTable(
                schema,
                TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1)),
                TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)));

        try (MarkDistinctMarkerOperator distinct = new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0},
                source,
                false,
                allocator.engineResources().operatorResources())) {
            try (Batch first = distinct.next()) {
                first.output(1).borrow(Stream.VALUES);
            }
            try (Batch second = distinct.next()) {
                assertThatThrownBy(() -> second.output(1).borrow(Stream.VALUES))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Distinct key vector at index 0")
                        .hasMessageContaining("testing:i32-only");
            }
        }
    }

    @Test
    void testGroupedAggregationConsumesSeparateDistinctMarkerThroughGenericFilter()
    {
        Operator marked = new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0, 3},
                new ConstantTableOperator(
                        allocator,
                        4,
                        List.of(
                                row(1L, 10L, 100L, 1000L),
                                row(1L, 20L, 200L, 1000L),
                                row(1L, 30L, 300L, 2000L),
                                row(2L, 40L, 100L, 3000L),
                                row(2L, 50L, 200L, 3000L),
                                row(2L, 60L, 300L, 4000L))),
                true,
                allocator.engineResources().operatorResources());

        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        List.of(0),
                        List.of(new Sum(1), new CountAll(), new Avg(2), new FilteredAccumulator(new CountAll(), 4)),
                        marked)))
                .matchesExactly(List.of(
                        row(1L, 60L, 3L, 200.0, 2L),
                        row(2L, 150L, 3L, 200.0, 2L)));
    }

    @Test
    void testInlineGroupedLongDistinctHandlesZeroNullAndPerGroupDuplicates()
    {
        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        List.of(0),
                        List.of(new DistinctCount(1)),
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 0L),
                                        row(1L, 0L),
                                        row(1L, 10L),
                                        row(1L, (Object) null),
                                        row(2L, 0L),
                                        row(2L, 10L),
                                        row(2L, 10L),
                                        row(2L, 20L),
                                        row(3L, 0L),
                                        row(3L, 1L),
                                        row(3L, 2L),
                                        row(3L, 3L),
                                        row(3L, 4L),
                                        row(3L, 5L),
                                        row(3L, 5L),
                                        row(3L, (Object) null))))))
                .matchesExactly(List.of(
                        row(1L, 2L),
                        row(2L, 3L),
                        row(3L, 6L)));
    }

    @Test
    void testInlineGroupedAggregationPartiallyFusesPlainAccumulatorsWithDistinct()
    {
        assertThat(operator(
                new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        List.of(0),
                        List.of(new Sum(1), new CountAll(), new Avg(2), new DistinctCount(3)),
                        new ConstantTableOperator(
                                allocator,
                                4,
                                List.of(
                                        row(1L, 10L, 100L, 7L),
                                        row(1L, 20L, 200L, 7L),
                                        row(1L, 30L, 300L, 8L),
                                        row(2L, 40L, 400L, 9L),
                                        row(2L, 50L, 500L, 9L),
                                        row(2L, 60L, 600L, null))))))
                .matchesExactly(List.of(
                        row(1L, 60L, 3L, 200.0, 2L),
                        row(2L, 150L, 3L, 500.0, 1L)));
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
    void testMinMaxLargeGroupedNullableResults()
    {
        // Some groups remain SQL NULL while the others receive multiple values.
        List<org.weakref.nitro.data.Row> input = new ArrayList<>();
        List<org.weakref.nitro.data.Row> expected = new ArrayList<>();
        for (long group = 0; group < 100; group++) {
            if (group % 10 == 0) {
                input.add(row(group, null));
                expected.add(row(group, null, null));
            }
            else {
                input.add(row(group, group + 10));
                input.add(row(group, group - 10));
                expected.add(row(group, group - 10, group + 10));
            }
        }

        assertThat(operator(new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Min(1), new Max(1)),
                new ConstantTableOperator(allocator, 2, input))))
                .matchesExactly(expected);
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
    void testHashJoinRejectsLaterBuildVectorOutsidePlanTimeTypeBinding()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(new Field(i32Only, false)));
        Operator probe = typedTable(
                schema,
                TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1)));
        Operator build = typedTable(
                schema,
                TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1)),
                TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)));

        assertThatThrownBy(() -> {
            try (Operator join = new HashJoinOperator(allocator, probe, 0, build, 0)) {
                join.next();
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Hash-join build key vector at index 0")
                .hasMessageContaining("testing:i32-only");
    }

    @Test
    void testHashJoinRejectsLaterProbeVectorOutsidePlanTimeTypeBinding()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(new Field(i32Only, false)));
        Operator probe = typedTable(
                schema,
                TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1)),
                TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)));
        Operator build = typedTable(
                schema,
                TableOperator.Page.values(2, new Vector[] {new I32Vector(new int[] {1, 2})}, Mask.all(2)));

        assertThatThrownBy(() -> {
            try (Operator join = new HashJoinOperator(allocator, probe, 0, build, 0)) {
                while (join.hasNext()) {
                    join.next().close();
                }
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Hash-join probe key vector at index 0")
                .hasMessageContaining("testing:i32-only");
    }

    @Test
    void testHashJoinProjectsPlanTimeSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(new Field(i32Only, false)));
        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                typedTable(schema, TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1))),
                0,
                typedTable(schema, TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1))),
                0)
                .withOutputs(1)) {
            assertThat(join.outputSchema().fields())
                    .singleElement()
                    .extracting(Field::type)
                    .isSameAs(i32Only);
        }
    }

    @Test
    void testHashJoinValidatesPromotedEqualityKeyBinding()
    {
        TypeBinding i32Only = i32OnlyType();
        Schema schema = new Schema(List.of(
                new Field(i32Only, false),
                new Field(i32Only, false)));
        Operator probe = typedTable(
                schema,
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I32Vector(new int[] {1}), new I32Vector(new int[] {10})},
                        Mask.all(1)));
        Operator build = typedTable(
                schema,
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I32Vector(new int[] {1}), new I32Vector(new int[] {10})},
                        Mask.all(1)),
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I32Vector(new int[] {2}), new I64Vector(new long[] {20})},
                        Mask.all(1)));

        assertThatThrownBy(() -> {
            try (Operator join = new HashJoinOperator(
                    allocator,
                    probe,
                    new int[] {0},
                    build,
                    new int[] {0},
                    HashJoinOperator.JoinFilter.binaryEquals(1, 1))) {
                join.next();
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Hash-join build key vector at index 1")
                .hasMessageContaining("testing:i32-only");
    }

    @Test
    void testDefaultProjectionPreservesDirectInputSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Field first = new Field("first", i32Only, false);
        Field second = new Field("second", i32Only, true);
        Schema sourceSchema = new Schema(List.of(first, second));
        Variable computed = new Variable(1000);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(computed, new Literal(7L), AllMask.ALL)),
                List.of(
                        new Reference(new Input(1), Stream.VALUES),
                        new Reference(computed, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(new Input(0), Stream.NULLS)));

        try (ProjectOperator projection = new ProjectOperator(
                allocator,
                plan,
                primitiveRegistry(),
                typedTable(sourceSchema))) {
            assertThat(projection.outputSchema().field(0)).isSameAs(second);
            assertThat(projection.outputSchema().field(1).type().isSpecified()).isFalse();
            assertThat(projection.outputSchema().field(2)).isSameAs(first);
            assertThat(projection.outputSchema().field(3).type().isSpecified()).isFalse();
        }
    }

    private static Operator typedTable(Schema schema, TableOperator.Page... pages)
    {
        return new TableOperator(
                schema.size(),
                List.of(pages))
        {
            @Override
            public Schema outputSchema()
            {
                return schema;
            }
        };
    }

    private static TypeBinding i32OnlyType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:i32-only");
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
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I32Vector.class);
            }
        };
    }

    @Test
    void testHashJoinBinaryDictionaryProbeCacheTracksBaseIdentityAndNulls()
    {
        BinaryVector buildKeys = new BinaryVector(3, 14);
        buildKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        buildKeys.setBytes(0, "alpha".getBytes(UTF_8));
        buildKeys.setBytes(1, "beta".getBytes(UTF_8));
        buildKeys.setBytes(2, "gamma".getBytes(UTF_8));

        BinaryVector firstBase = new BinaryVector(3, 17);
        firstBase.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        firstBase.setBytes(0, "gamma".getBytes(UTF_8));
        firstBase.setBytes(1, "alpha".getBytes(UTF_8));
        firstBase.setBytes(2, "missing".getBytes(UTF_8));
        DictionaryVector firstProbe = DictionaryVector.wrap(new int[] {1, 0, 2, 1, 0, 2, 1, 0}, firstBase);

        BinaryVector secondBase = new BinaryVector(3, 16);
        secondBase.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        secondBase.setBytes(0, "beta".getBytes(UTF_8));
        secondBase.setBytes(1, "absent".getBytes(UTF_8));
        secondBase.setBytes(2, "alpha".getBytes(UTF_8));
        DictionaryVector secondProbe = DictionaryVector.wrap(new int[] {0, 1, 2, 0, 1, 2}, secondBase);

        Operator probe = new TableOperator(
                1,
                List.of(
                        new TableOperator.Page(
                                firstProbe.length(),
                                new Streams[] {Streams.ofValuesAndNulls(
                                        firstProbe,
                                        new BooleanVector(new boolean[] {false, false, false, true, false, false, false, false}))},
                                Mask.all(firstProbe.length())),
                        TableOperator.Page.values(
                                secondProbe.length(),
                                new Vector[] {secondProbe},
                                Mask.all(secondProbe.length()))));
        Operator build = new TableOperator(
                1,
                List.of(TableOperator.Page.values(3, new Vector[] {buildKeys}, Mask.all(3))));

        assertThat(operator(new HashJoinOperator(allocator, probe, 0, build, 0)))
                .matchesExactly(List.of(
                        row("alpha", "alpha"),
                        row("gamma", "gamma"),
                        row("gamma", "gamma"),
                        row("alpha", "alpha"),
                        row("gamma", "gamma"),
                        row("beta", "beta"),
                        row("alpha", "alpha"),
                        row("beta", "beta"),
                        row("alpha", "alpha")));
    }

    @Test
    void testHashJoinPromotesImplicitBuildRowReferencesOnNullGap()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 100L),
                                        row(null, 999L),
                                        row(2L, 200L),
                                        row(2L, 201L),
                                        row(3L, 300L))),
                        0)
                        .withLazyDuplicateSlotState()
                        .withDirectExactBuildCoalescing()))
                .matchesExactly(List.of(
                        row(1L, 1L, 100L),
                        row(2L, 2L, 200L),
                        row(2L, 2L, 201L),
                        row(3L, 3L, 300L)));
    }

    @Test
    void testHashJoinDuplicateChainsSurvivePooledArrayReuse()
    {
        var expected = List.of(
                row(7L, 10L, 7L, 100L),
                row(7L, 10L, 7L, 200L));

        // The first join returns its direct-range duplicate metadata to the primitive-array pool.  The second
        // join must initialize that recycled sparse state rather than treating the old tail/count as its own.
        for (int iteration = 0; iteration < 2; iteration++) {
            assertThat(operator(
                    new HashJoinOperator(
                            allocator,
                            new ConstantTableOperator(allocator, 2, List.of(row(7L, 10L))),
                            0,
                            new ConstantTableOperator(allocator, 2, List.of(row(7L, 100L), row(7L, 200L))),
                            0)))
                    .matchesExactly(expected);
        }
    }

    @Test
    void testHashJoinKeyOnlyDirectRangeBuildPreservesDuplicateMultiplicity()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of(row(7L), row(8L))),
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(row(7L), row(7L), row(7L), row(9L))),
                        0)
                        .withOutputs(0)))
                .matchesExactly(List.of(row(7L), row(7L), row(7L)));
    }

    @Test
    void testHashJoinKeyOnlyDirectRangeBuildPreservesDuplicateMultiplicityAcrossBatches()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of(row(7L), row(8L), row(9L))),
                        0,
                        new GeneratorOperator(allocator, 8, 4, List.of(new SequenceGenerator(7, 9))),
                        0)
                        .withOutputs(0)))
                .matchesExactly(List.of(
                        row(7L), row(7L), row(7L), row(7L),
                        row(8L), row(8L), row(8L), row(8L)));
    }

    @Test
    void testHashJoinDirectRangeBuildPreservesMultipleSparseDuplicateGroups()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of(row(7L), row(70_000L), row(900_000L))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(7L, 70L),
                                        row(70_000L, 700_001L),
                                        row(7L, 71L),
                                        row(900_000L, 9_000_001L),
                                        row(70_000L, 700_002L),
                                        row(900_000L, 9_000_002L),
                                        row(900_000L, 9_000_003L))),
                        0)))
                .matchesExactly(List.of(
                        row(7L, 7L, 70L),
                        row(7L, 7L, 71L),
                        row(70_000L, 70_000L, 700_001L),
                        row(70_000L, 70_000L, 700_002L),
                        row(900_000L, 900_000L, 9_000_001L),
                        row(900_000L, 900_000L, 9_000_002L),
                        row(900_000L, 900_000L, 9_000_003L)));
    }

    @Test
    void testHashJoinStreamsUnusedBuildPayloadAndPreservesMultiplicity()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of(row(7L), row(8L))),
                        0,
                        new ConstantTableOperator(allocator, 2, List.of(row(7L, 100L), row(7L, 200L), row(null, 300L), row(9L, 400L))),
                        0)
                        .withOutputs(0)))
                .matchesExactly(List.of(row(7L), row(7L)));
    }

    @Test
    void testHashJoinCompactsCompletedHighKeyStreamingRange()
    {
        long firstKey = 2_451_545;
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(firstKey - 1), row(firstKey), row(firstKey + 1), row(firstKey + 2), row(firstKey + 3))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(firstKey), row(firstKey + 1), row(firstKey + 2))),
                        0)
                        .withOutputs(0)))
                .matchesExactly(List.of(row(firstKey), row(firstKey + 1), row(firstKey + 2)));
    }

    @Test
    void testHashJoinCompactsCompletedHighKeyStreamingRangeWithNonSequentialReferences()
    {
        long firstKey = 2_451_545;
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(firstKey), row(firstKey + 1), row(firstKey + 2))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(firstKey + 1), row(firstKey), row(firstKey + 2))),
                        0)
                        .withOutputs(0)))
                .matchesExactly(List.of(row(firstKey), row(firstKey + 1), row(firstKey + 2)));
    }

    @Test
    void testHashJoinOutputProjection()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L), row(2L, 20L))),
                        0,
                        new ConstantTableOperator(allocator, 2, List.of(row(2L, 200L), row(1L, 100L))),
                        0)
                        .withOutputs(1, 3)))
                .matchesExactly(List.of(row(10L, 100L), row(20L, 200L)));
    }

    @Test
    void testHashJoinBinaryResidualFilter()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, "USA", 10L),
                                        row(1L, "CAN", 20L),
                                        row(1L, null, 30L))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, "USA", 100L),
                                        row(1L, "MEX", 200L),
                                        row(1L, null, 300L))),
                        0,
                        HashJoinOperator.JoinFilter.binaryEquals(1, 1))))
                .matchesExactly(List.of(row(1L, "USA", 10L, 1L, "USA", 100L)));
    }

    @Test
    void testHashJoinLongNotEqualResidualFilter()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, 10L, 100L),
                                        row(1L, 20L, 200L),
                                        row(1L, null, 300L))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, 10L, 1_000L),
                                        row(1L, 30L, 2_000L),
                                        row(1L, null, 3_000L))),
                        0,
                        HashJoinOperator.JoinFilter.longNotEqual(1, 1))))
                .matchesExactly(List.of(
                        row(1L, 10L, 100L, 1L, 30L, 2_000L),
                        row(1L, 20L, 200L, 1L, 10L, 1_000L),
                        row(1L, 20L, 200L, 1L, 30L, 2_000L)));
    }

    @Test
    void testHashJoinLongBitwiseOverlapResidualFilter()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(row(1L, 0b001L), row(1L, 0b110L), row(1L, 0L), row(1L, null))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(row(1L, 0b010L), row(1L, 0b100L), row(1L, 0L), row(1L, null))),
                        0,
                        HashJoinOperator.JoinFilter.longBitwiseOverlap(1, 1))))
                .matchesExactly(List.of(
                        row(1L, 0b110L, 1L, 0b010L),
                        row(1L, 0b110L, 1L, 0b100L)));
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
    void testHashJoinCompactLongPairDoesNotTruncateProbeKeys()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, 2L, 10L),
                                        row(0x1_0000_0001L, 2L, 20L))),
                        new int[] {0, 1},
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(row(1L, 2L, 100L))),
                        new int[] {0, 1})))
                .matchesExactly(List.of(row(1L, 2L, 10L, 1L, 2L, 100L)));
    }

    @Test
    void testHashJoinCompactLongPairPromotesForWideBuildKey()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, 2L, 10L),
                                        row(0x1_0000_0001L, 2L, 20L))),
                        new int[] {0, 1},
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(
                                        row(1L, 2L, 100L),
                                        row(0x1_0000_0001L, 2L, 200L))),
                        new int[] {0, 1})))
                .matchesExactly(List.of(
                        row(1L, 2L, 10L, 1L, 2L, 100L),
                        row(0x1_0000_0001L, 2L, 20L, 0x1_0000_0001L, 2L, 200L)));
    }

    @Test
    void testHashJoinLongPairAllocatesDuplicateRowsLazily()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 3, List.of(row(1L, 2L, 10L))),
                        new int[] {0, 1},
                        new ConstantTableOperator(allocator, 3, List.of(
                                row(1L, 2L, 100L),
                                row(1L, 2L, 200L))),
                        new int[] {0, 1})))
                .matchesExactly(List.of(
                        row(1L, 2L, 10L, 1L, 2L, 100L),
                        row(1L, 2L, 10L, 1L, 2L, 200L)));
    }

    @Test
    void testHashJoinDenseLongPairPromotesSingleBatchPositionsAcrossBatches()
    {
        Operator build = new TableOperator(
                3,
                List.of(
                        TableOperator.Page.values(
                                1,
                                new Vector[] {new I64Vector(new long[] {1}), new I64Vector(new long[] {2}), new I64Vector(new long[] {100})},
                                Mask.all(1)),
                        TableOperator.Page.values(
                                1,
                                new Vector[] {new I64Vector(new long[] {1}), new I64Vector(new long[] {2}), new I64Vector(new long[] {200})},
                                Mask.all(1))));
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 3, List.of(row(1L, 2L, 10L))),
                        new int[] {0, 1},
                        build,
                        new int[] {0, 1})))
                .matchesExactly(List.of(
                        row(1L, 2L, 10L, 1L, 2L, 100L),
                        row(1L, 2L, 10L, 1L, 2L, 200L)));
    }

    @Test
    void testHashJoinKeyOnlyLongPairPreservesWideKeyDuplicateMultiplicity()
    {
        long wideKey = 0x1_0000_0001L;
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 2, List.of(row(wideKey, 2L), row(7L, 8L))),
                        new int[] {0, 1},
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(1L, 2L),
                                row(wideKey, 2L),
                                row(wideKey, 2L))),
                        new int[] {0, 1})
                        .withOutputs(0, 1)))
                .matchesExactly(List.of(row(wideKey, 2L), row(wideKey, 2L)));
    }

    @Test
    void testHashJoinDeduplicatesNonRetainedBinaryBuildOutput()
    {
        Operator inner = new ConstantTableOperator(
                allocator,
                2,
                List.of(
                        row(1L, "alpha"),
                        row(2L, "beta"),
                        row(3L, "alpha"),
                        row(4L, "beta")))
        {
            @Override
            public boolean supportsConstrainedReborrow()
            {
                return false;
            }
        };

        try (Operator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(
                        allocator,
                        1,
                        List.of(
                                row(1L),
                                row(3L),
                                row(4L))),
                0,
                inner,
                0)) {
            Batch batch = join.next();
            try {
                assertThat(batch.borrowMask()).containsExactly(0, 1, 2);

                Vector values = batch.output(2).borrow(Stream.VALUES);
                assertThat(values).isInstanceOf(DictionaryVector.class);
                DictionaryVector dictionary = (DictionaryVector) values;
                assertThat(dictionary.ids()).containsExactly(0, 0, 1);
                assertThat(dictionary.values()).isInstanceOf(BinaryVector.class);

                BinaryVector base = (BinaryVector) dictionary.values();
                assertThat(base.length()).isEqualTo(2);
                assertThat(new String(base.copyBytes(0), UTF_8)).isEqualTo("alpha");
                assertThat(new String(base.copyBytes(1), UTF_8)).isEqualTo("beta");
            }
            finally {
                batch.close();
            }
        }
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
                        allocator,
                        allocator.engineResources().operatorResources().filter()));

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
    void testHashJoinForwardsFullyConsumedIdentityOuterPayload()
    {
        AtomicReference<Mask> constrainedMask = new AtomicReference<>();
        I64Vector keys = new I64Vector(new long[] {1L, 2L, 3L});
        I64Vector payload = new I64Vector(new long[] {10L, 20L, 30L});
        Operator outer = new Operator()
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
                return new Batch(
                        Mask.all(3),
                        constrainedMask::set,
                        Function.identity(),
                        new Output(Set.of(Stream.VALUES), ignored -> keys),
                        new Output(Set.of(Stream.VALUES), ignored -> payload));
            }

            @Override
            public void constrain(Mask mask)
            {
                constrainedMask.set(mask);
            }

            @Override
            public boolean supportsConstrainedReborrow()
            {
                return true;
            }

            @Override
            public void close()
            {
            }
        };

        try (Operator join = new HashJoinOperator(
                allocator,
                outer,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))),
                0)) {
            try (Batch batch = join.next()) {
                assertThat(batch.borrowMask()).containsExactly(0, 1, 2);
                assertThat(batch.output(1).borrow(Stream.VALUES)).isSameAs(payload);
                assertThat(constrainedMask.get()).isNull();
            }
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
                        allocator,
                        allocator.engineResources().operatorResources().filter()));

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
                        allocator,
                        allocator.engineResources().operatorResources().filter()));

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

    private static Operator singleBatchOperator(Streams streams)
    {
        return new Operator()
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
                return new Batch(Mask.all(streams.values().length()), Output.of(streams));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };
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

        return new FilterOperator(
                source,
                evaluationPlan,
                primitiveRegistry,
                values(predicate),
                allocator,
                allocator.engineResources().operatorResources().filter());
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

        return new FilterOperator(
                source,
                evaluationPlan,
                primitiveRegistry,
                values(predicate),
                allocator,
                allocator.engineResources().operatorResources().filter());
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

    /**
     * Test-only physical unit with shared state and two results. Its identity is deliberately
     * opaque to the operators; only the program's unit/result bindings describe the output shape.
     */
    private static final class SumAndCountUnit
            implements GeneratedGroupedAggregationUnit
    {
        private final int inputColumn;
        private int accumulationCalls;

        private SumAndCountUnit(int inputColumn)
        {
            this.inputColumn = inputColumn;
        }

        @Override
        public int outputCount()
        {
            return 2;
        }

        @Override
        public List<GroupedAggregationUpdate> generatedGroupedUpdates()
        {
            return List.of(
                    GroupedAggregationUpdate.inputValue(inputColumn),
                    GroupedAggregationUpdate.constant(1));
        }

        @Override
        public void bindGeneratedGroupedState(Object state, LongStateUpdate[] targets, int offset)
        {
            State current = (State) state;
            targets[offset] = (group, value) -> current.sums[group] += value;
            targets[offset + 1] = (group, value) -> current.counts[group] += value;
        }

        @Override
        public Object allocate(AggregationExecutionContext context, int size)
        {
            return new State(new long[size], new long[size]);
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int size)
        {
            State current = (State) state;
            return new State(
                    java.util.Arrays.copyOf(current.sums, size),
                    java.util.Arrays.copyOf(current.counts, size));
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            State current = (State) state;
            java.util.Arrays.fill(current.sums, offset, offset + length, 0);
            java.util.Arrays.fill(current.counts, offset, offset + length, 0);
        }

        @Override
        public void accumulate(Object state, int group, Mask mask, StreamAccessor streams)
        {
            accumulationCalls++;
            State current = (State) state;
            VectorAccess.LongValues values = VectorAccess.longValues(streams.values(inputColumn));
            for (int position : mask) {
                current.sums[group] += values.value(position);
                current.counts[group]++;
            }
        }

        @Override
        public void accumulate(Object state, Vector groups, Mask mask, StreamAccessor streams)
        {
            accumulationCalls++;
            State current = (State) state;
            I64Vector groupIds = (I64Vector) groups;
            VectorAccess.LongValues values = VectorAccess.longValues(streams.values(inputColumn));
            for (int position : mask) {
                int group = toIntExact(groupIds.values()[position]);
                current.sums[group] += values.value(position);
                current.counts[group]++;
            }
        }

        @Override
        public Streams result(int output, int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context allocationContext)
        {
            State current = (State) state;
            long[] source = output == 0 ? current.sums : current.counts;
            I64Vector values = allocator.allocateOrGrow(
                    allocationContext,
                    existing == null ? null : (I64Vector) existing.values(),
                    I64Vector.class,
                    maxGroup + 1,
                    I64Vector::new);
            System.arraycopy(source, 0, values.values(), 0, maxGroup + 1);
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    allocator,
                    allocationContext,
                    existing == null ? null : existing.getOrNull(Stream.NULLS),
                    maxGroup + 1);
            java.util.Arrays.fill(nulls.values(), 0, maxGroup + 1, false);
            return allocator.reuseValuesAndNulls(existing, values, nulls);
        }

        private record State(long[] sums, long[] counts) {}
    }
}
