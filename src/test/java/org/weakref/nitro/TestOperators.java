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
import org.weakref.nitro.core.execution.ExecutionSuspension;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTarget;
import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionProgram;
import org.weakref.nitro.core.source.RuntimeFilterAcceptance;
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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.builtin.Cardinality;
import org.weakref.nitro.function.scalar.builtin.MaterializeLongCarrier;
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
import org.weakref.nitro.operator.GroupIdOperator;
import org.weakref.nitro.operator.GroupOperator;
import org.weakref.nitro.operator.GroupedAggregationOperator;
import org.weakref.nitro.operator.HashJoinExecutionPolicy;
import org.weakref.nitro.operator.HashJoinOperator;
import org.weakref.nitro.operator.JoinBufferPolicy;
import org.weakref.nitro.operator.JoinSessionOperator;
import org.weakref.nitro.operator.LimitOperator;
import org.weakref.nitro.operator.MarkDistinctMarkerOperator;
import org.weakref.nitro.operator.MarkDistinctOperator;
import org.weakref.nitro.operator.MaterializeOperator;
import org.weakref.nitro.operator.MultiStageOperator;
import org.weakref.nitro.operator.NestedLoopJoinOperator;
import org.weakref.nitro.operator.NestedLoopJoinSession;
import org.weakref.nitro.operator.OffsetOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.OperatorResources;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.PartitionBucketWindowFunction;
import org.weakref.nitro.operator.PartitionSumI64WindowFunction;
import org.weakref.nitro.operator.PeerDistributionWindowFunction;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.RankWindowFunction;
import org.weakref.nitro.operator.RankingWindowFunction;
import org.weakref.nitro.operator.SelectedPositionWindowFunction;
import org.weakref.nitro.operator.SemiJoinOperator;
import org.weakref.nitro.operator.SortOperator;
import org.weakref.nitro.operator.SortOperatorPolicy;
import org.weakref.nitro.operator.StaticFilterEnforcement;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.TopNOperator;
import org.weakref.nitro.operator.TopNRankingOperator;
import org.weakref.nitro.operator.UnionAllOperator;
import org.weakref.nitro.operator.WindowOperator;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.Avg;
import org.weakref.nitro.operator.aggregation.AvgF64;
import org.weakref.nitro.operator.aggregation.ConditionalSumsAggregationUnit;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountAvgStddevI64AggregationUnit;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.DiscriminatedAggregationUnit;
import org.weakref.nitro.operator.aggregation.FilteredAccumulator;
import org.weakref.nitro.operator.aggregation.First;
import org.weakref.nitro.operator.aggregation.GeneratedGroupedAggregationUnit;
import org.weakref.nitro.operator.aggregation.Max;
import org.weakref.nitro.operator.aggregation.Min;
import org.weakref.nitro.operator.aggregation.MinMaxI64AggregationUnit;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.aggregation.SumF64;
import org.weakref.nitro.operator.aggregation.SumProductIfEqual;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.Conditional;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.IrNormalizer;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.MaskExpressionResolver;
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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;
import static org.weakref.nitro.function.scalar.builtin.JoinFilterFunctions.longBitwiseOverlap;
import static org.weakref.nitro.function.scalar.builtin.JoinFilterFunctions.longGreaterThan;
import static org.weakref.nitro.function.scalar.builtin.JoinFilterFunctions.longLessThan;
import static org.weakref.nitro.function.scalar.builtin.JoinFilterFunctions.longNotEqual;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestOperators
{
    @Test
    void testTableConsumersHaveIndependentMasks()
    {
        TableOperator.Page page = TableOperator.Page.values(
                3,
                new Vector[] {new I64Vector(new long[] {1, 2, 3})},
                Mask.all(3));
        TableOperator first = TableOperator.retained(Schema.unspecified(1), List.of(page));
        TableOperator second = TableOperator.retained(Schema.unspecified(1), List.of(page));

        Mask firstMask = first.next().borrowMask();
        firstMask.retainIf(position -> position == 0);
        Mask secondMask = second.next().borrowMask();

        assertThat(firstMask).containsExactly(0);
        assertThat(secondMask).containsExactly(0, 1, 2);
        assertThat(page.mask()).containsExactly(0, 1, 2);
    }

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
                                                EngineResources.from(allocator).operatorResources().filter()))))))
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
    void testProjectOperatorExposesComputedDictionaryMask()
    {
        int[] ids = {0, 1, 2, 0};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {2, 8, 4}),
                new int[] {2, 1, 1});
        Variable threshold = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan plan = plan(
                List.of(
                        literal(threshold, 5),
                        call(predicate, "lt", values(new Input(0)), values(threshold))),
                values(predicate));
        Allocator.Context resultContext = new Allocator.Context("project-mask-test");

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                primitiveRegistry(),
                new TableOperator(1, List.of(TableOperator.Page.values(
                        ids.length,
                        new Vector[] {dictionary},
                        Mask.all(ids.length)))));
                Batch batch = operator.next()) {
            Mask result = batch.output(0).tryBorrowMask(
                    Stream.VALUES,
                    batch.borrowMask(),
                    true,
                    allocator,
                    resultContext);

            assertThat(result).isNotNull();
            assertThat(result.dictionaryDomainSelection(dictionary)).isNotNull();
            assertThat(result).containsExactly(0, 2, 3);
        }
        finally {
            allocator.release(resultContext);
        }
    }

    @Test
    void testProjectOperatorUsesOutputTypeForEmptyComputedValues()
    {
        TypeBinding binaryType = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:binary");
            }

            @Override
            public Class<?> carrierType()
            {
                return String.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(new TypeVectorFactory()
                {
                    @Override
                    public Vector constant(VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(VectorAllocator allocator, int length)
                    {
                        return allocator.allocate(BinaryVector.class, length, size -> new BinaryVector(size, 0));
                    }
                });
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(BinaryVector.class);
            }
        };
        Variable computed = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(computed, new Literal("unused"), AllMask.ALL)),
                List.of(new Reference(computed, Stream.VALUES)));
        Schema outputSchema = new Schema(List.of(new Field(binaryType, false)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                primitiveRegistry(),
                new TableOperator(0, List.of(TableOperator.Page.values(0, new Vector[0], Mask.all(0)))),
                outputSchema);
                Batch batch = operator.next()) {
            assertThat(batch.borrowMask().none()).isTrue();
            assertThat(batch.output(0).borrow(Stream.VALUES))
                    .isInstanceOf(BinaryVector.class)
                    .extracting(Vector::length)
                    .isEqualTo(0);
        }
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
    void testDictionaryDomainFrequencyContract()
    {
        int[] frequencies = {2, 3, 3};
        DictionaryVector dictionary = DictionaryVector.wrapWithDomainFrequencies(
                new int[] {0, 1, 2, 1, 0, 2, 2, 1},
                8,
                new I64Vector(new long[] {10, 20, 30}),
                frequencies);

        assertThat(dictionary.hasDomainFrequencies()).isTrue();
        assertThat(dictionary.domainFrequency(0)).isEqualTo(2);
        assertThat(dictionary.domainFrequency(1)).isEqualTo(3);
        assertThat(dictionary.domainFrequency(2)).isEqualTo(3);
        assertThat(dictionary.retainedBytes()).isEqualTo(3L * Integer.BYTES);

        DictionaryVector view = dictionary.sharedMappingView();
        assertThat(view.hasSameMapping(dictionary)).isTrue();
        assertThat(view.hasDomainFrequencies()).isTrue();
        assertThat(view.domainFrequency(2)).isEqualTo(3);
        assertThat(view.retainedBytes()).isZero();

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context allocationContext = new Allocator.Context("dictionary-frequency-copy");
        DictionaryVector copy = (DictionaryVector) dictionary.copy(allocator, allocationContext);
        assertThat(copy.ids()).isNotSameAs(dictionary.ids());
        assertThat(copy.hasDomainFrequencies()).isTrue();
        assertThat(copy.domainFrequency(0)).isEqualTo(2);
        assertThat(copy.domainFrequency(1)).isEqualTo(3);
        assertThat(copy.domainFrequency(2)).isEqualTo(3);
        allocator.release(allocationContext, copy);

        assertThatThrownBy(() -> DictionaryVector.wrapWithDomainFrequencies(
                        new int[] {0},
                        1,
                        new I64Vector(new long[] {10}),
                        new int[] {-1}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DictionaryVector.wrapWithDomainFrequencies(
                        new int[] {0},
                        1,
                        new I64Vector(new long[] {10}),
                        new int[] {1, 0}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DictionaryVector.wrapWithDomainFrequencies(
                        new int[] {0},
                        1,
                        new I64Vector(new long[] {10}),
                        new int[] {0}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDictionaryDomainPresenceContract()
    {
        DictionaryVector dictionary = DictionaryVector.wrapOwnedIdsWithDomainPresence(
                new I32Vector(new int[] {2, 0, 2, 0}),
                4,
                new I64Vector(new long[] {10, 20, 30}),
                0b101);
        List<Integer> visited = new ArrayList<>();

        assertThat(dictionary.hasDomainPresence()).isTrue();
        assertThat(dictionary.hasDomainFrequencies()).isFalse();
        assertThat(dictionary.visitSelectedDomain(Mask.all(4), dictionaryId -> {
            visited.add(dictionaryId);
            return true;
        })).isTrue();
        assertThat(visited).containsExactly(0, 2);

        DictionaryVector view = dictionary.sharedMappingView();
        assertThat(view.hasDomainPresence()).isTrue();
        assertThat(view.visitSelectedDomain(Mask.all(4), _ -> true)).isTrue();

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context allocationContext = new Allocator.Context("dictionary-presence-copy");
        DictionaryVector copy = (DictionaryVector) dictionary.copy(allocator, allocationContext);
        visited.clear();
        assertThat(copy.ids()).isNotSameAs(dictionary.ids());
        assertThat(copy.hasDomainPresence()).isTrue();
        assertThat(copy.hasDomainFrequencies()).isFalse();
        assertThat(copy.visitSelectedDomain(Mask.all(4), dictionaryId -> {
            visited.add(dictionaryId);
            return true;
        })).isTrue();
        assertThat(visited).containsExactly(0, 2);
        allocator.release(allocationContext, copy);

        assertThatThrownBy(() -> DictionaryVector.wrapOwnedIdsWithDomainPresence(
                        new I32Vector(new int[] {0}),
                        1,
                        new I64Vector(new long[] {10}),
                        0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DictionaryVector.wrapOwnedIdsWithDomainPresence(
                        new I32Vector(new int[] {0}),
                        1,
                        new I64Vector(new long[] {10}),
                        0b10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDictionaryDomainPresenceProvesReferencedNullDomainIsEmpty()
    {
        BooleanVector nullDomain = new BooleanVector(new boolean[] {false, true});
        DictionaryVector nulls = DictionaryVector.wrapOwnedIdsWithDomainPresence(
                new I32Vector(new int[] {0, 0, 0}),
                3,
                nullDomain,
                0b01);
        assertThat(VectorAccess.isAllFalseNulls(nulls)).isTrue();

        DictionaryVector nullable = DictionaryVector.wrapOwnedIdsWithDomainPresence(
                new I32Vector(new int[] {0, 1, 0}),
                3,
                nullDomain,
                0b11);
        assertThat(VectorAccess.isAllFalseNulls(nullable)).isFalse();

        DictionaryVector unknownMembership = DictionaryVector.wrap(new int[] {0, 0, 0}, nullDomain);
        assertThat(VectorAccess.isAllFalseNulls(unknownMembership)).isFalse();
    }

    @Test
    void testRleNullProofUsesTheCompletePhysicalDomain()
    {
        assertThat(VectorAccess.isAllFalseNulls(new RleVector(
                new int[] {3, 4},
                new BooleanVector(new boolean[] {false, false})))).isTrue();
        assertThat(VectorAccess.isAllFalseNulls(new RleVector(
                new int[] {3, 4},
                new BooleanVector(new boolean[] {false, true})))).isFalse();
        assertThat(VectorAccess.isAllTrueNulls(new RleVector(
                new int[] {3, 4},
                new BooleanVector(new boolean[] {true, true})))).isTrue();
        assertThat(VectorAccess.isAllTrueNulls(new RleVector(
                new int[] {3, 4},
                new BooleanVector(new boolean[] {true, false})))).isFalse();
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
    void testSortOperatorUsesLessWorkspaceForNarrowKeyRange()
    {
        long countingSortBytes = sortWorkspaceBytes(new SortOperatorPolicy(true, 8, 3));
        long radixSortBytes = sortWorkspaceBytes(new SortOperatorPolicy(true, 8, 0));

        assertThat(countingSortBytes).isLessThan(radixSortBytes);
    }

    private static long sortWorkspaceBytes(SortOperatorPolicy policy)
    {
        try (EngineResources resources = EngineResources.createDefault(0);
                Allocator allocator = new Allocator(resources)) {
            long initialBytes = resources.primitiveArrays().allocatedBytes();
            assertThat(operator(new SortOperator(
                    allocator,
                    new int[] {0},
                    new boolean[] {true},
                    new ConstantTableOperator(
                            allocator,
                            2,
                            List.of(row(0L, 10L), row(-1L, 20L), row(0L, 30L), row(1L, 40L))),
                    policy,
                    JoinBufferPolicy.defaults())))
                    .matchesExactly(List.of(
                            row(1L, 40L),
                            row(0L, 10L),
                            row(0L, 30L),
                            row(-1L, 20L)));
            return resources.primitiveArrays().allocatedBytes() - initialBytes;
        }
    }

    @Test
    void testSortOperatorResumesLoadingAfterExecutionSuspension()
    {
        Operator delegate = new TableOperator(
                1,
                List.of(
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)),
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {1})}, Mask.all(1))));
        Operator suspendingSource = new Operator()
        {
            private int hasNextCalls;

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
                if (++hasNextCalls == 2) {
                    throw ExecutionSuspension.yield();
                }
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
            public void close()
            {
                delegate.close();
            }
        };

        try (Operator sort = new SortOperator(allocator, new int[] {0}, new boolean[] {false}, suspendingSource)) {
            assertThatThrownBy(sort::next).isSameAs(ExecutionSuspension.yield());
            assertThat(operator(sort)).matchesExactly(List.of(row(1L), row(2L)));
        }
    }

    @Test
    void testSortOperatorProducesTypedEmptyOutputWithoutInputBatches()
    {
        Schema schema = new Schema(List.of(new Field("value", i64ValueType(), false)));
        assertThat(operator(new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {false},
                typedTable(schema))))
                .matchesExactly(List.of());
    }

    @Test
    void testTopNRankingOperatorResumesLoadingAfterExecutionSuspension()
    {
        Operator delegate = new TableOperator(
                1,
                List.of(
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)),
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {1})}, Mask.all(1))));
        Operator suspendingSource = new Operator()
        {
            private int hasNextCalls;

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
                if (++hasNextCalls == 2) {
                    throw ExecutionSuspension.yield();
                }
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
            public void close()
            {
                delegate.close();
            }
        };

        try (Operator ranking = new TopNRankingOperator(
                allocator,
                2,
                new int[0],
                new int[] {0},
                new boolean[] {false},
                suspendingSource,
                EngineResources.from(allocator).operatorResources().topNRankingPolicy())) {
            assertThatThrownBy(ranking::next).isSameAs(ExecutionSuspension.yield());
            assertThat(operator(ranking)).matchesExactly(List.of(row(1L, 1L), row(2L, 2L)));
        }
    }

    @Test
    void testTopNOperatorResumesLoadingAfterExecutionSuspension()
    {
        Operator delegate = new TableOperator(
                1,
                List.of(
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {1})}, Mask.all(1)),
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1))));
        Operator suspendingSource = new Operator()
        {
            private int hasNextCalls;

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
                if (++hasNextCalls == 2) {
                    throw ExecutionSuspension.yield();
                }
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
            public void close()
            {
                delegate.close();
            }
        };

        try (Operator topN = new TopNOperator(allocator, 1, 0, true, suspendingSource)) {
            assertThatThrownBy(topN::next).isSameAs(ExecutionSuspension.yield());
            assertThat(operator(topN)).matchesExactly(List.of(row(2L)));
        }
    }

    @Test
    void testTopNOperatorPreservesCurrentBatchWhenAvailabilityCheckSuspends()
    {
        Operator delegate = new TableOperator(
                2,
                List.of(
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2}), new I64Vector(new long[] {20})}, Mask.all(1)),
                        TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {1}), new I64Vector(new long[] {10})}, Mask.all(1))));
        Operator suspendingSource = new Operator()
        {
            private int hasNextCalls;
            private Batch current;

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
                if (++hasNextCalls == 2) {
                    throw ExecutionSuspension.yield();
                }
                return delegate.hasNext();
            }

            @Override
            public Batch next()
            {
                if (current != null) {
                    current.close();
                }
                current = delegate.next();
                return current;
            }

            @Override
            public void constrain(Mask mask)
            {
                delegate.constrain(mask);
            }

            @Override
            public boolean supportsConstrainedReborrow()
            {
                return true;
            }

            @Override
            public void close()
            {
                if (current != null) {
                    current.close();
                }
                delegate.close();
            }
        };

        try (Operator topN = new TopNOperator(allocator, 1, 0, true, suspendingSource)) {
            assertThatThrownBy(topN::next).isSameAs(ExecutionSuspension.yield());
            assertThat(operator(topN)).matchesExactly(List.of(row(2L, 20L)));
        }
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
    void testSortOperatorHonorsExplicitNullPlacementIndependentlyOfDirection()
    {
        OperatorResources resources = EngineResources.from(allocator).operatorResources();

        assertThat(operator(new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {false},
                new boolean[] {true},
                new ConstantTableOperator(allocator, 1, List.of(row(2L), row((Object) null), row(1L))),
                resources)))
                .matchesExactly(List.of(row((Object) null), row(1L), row(2L)));

        assertThat(operator(new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {true},
                new boolean[] {true},
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row((Object) null), row(2L))),
                resources)))
                .matchesExactly(List.of(row((Object) null), row(2L), row(1L)));
    }

    @Test
    void testTopNOperatorHonorsExplicitNullPlacement()
    {
        assertThat(operator(new TopNOperator(
                allocator,
                2,
                new int[] {0},
                new boolean[] {false},
                new boolean[] {true},
                new ConstantTableOperator(allocator, 1, List.of(row(2L), row((Object) null), row(1L))),
                EngineResources.from(allocator).operatorResources())))
                .matchesExactly(List.of(row((Object) null), row(1L)));
    }

    @Test
    void testSortOperatorOrdersBooleans()
    {
        assertThat(operator(new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {false},
                new boolean[] {false},
                new ConstantTableOperator(allocator, 1, List.of(row(true), row((Object) null), row(false))),
                EngineResources.from(allocator).operatorResources())))
                .matchesExactly(List.of(row(0L), row(1L), row((Object) null)));
    }

    @Test
    void testWindowOperatorHonorsExplicitNullPlacement()
    {
        try (Operator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(2L), row((Object) null), row(1L))),
                new int[0],
                new int[] {0},
                new boolean[] {false},
                new boolean[] {true},
                List.of(new RankWindowFunction(new int[] {0}, new boolean[] {false})),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(null, 1L),
                            row(1L, 2L),
                            row(2L, 3L)));
        }
    }

    @Test
    void testWindowOperatorSupportsDenseRank()
    {
        try (Operator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(4L), row((Object) null), row(1L), row((Object) null), row(4L))),
                new int[0],
                new int[] {0},
                new boolean[] {false},
                List.of(new RankingWindowFunction(
                        Schema.unspecified(1),
                        new int[] {0},
                        new boolean[] {false},
                        RankingWindowFunction.RankingType.DENSE_RANK)),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(1L, 1L),
                            row(4L, 2L),
                            row(4L, 2L),
                            row(null, 3L),
                            row(null, 3L)));
        }
    }

    @Test
    void testWindowOperatorSupportsRowNumberFunction()
    {
        try (Operator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(4L), row(1L), row(4L))),
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RankingWindowFunction(
                        Schema.unspecified(1),
                        new int[0],
                        new boolean[0],
                        RankingWindowFunction.RankingType.ROW_NUMBER)),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(4L, 1L),
                            row(1L, 2L),
                            row(4L, 3L)));
        }
    }

    @Test
    void testWindowOperatorSupportsPeerDistributions()
    {
        List<Row> input = List.of(row(4L), row((Object) null), row(1L), row((Object) null), row(4L));
        try (Operator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, input),
                new int[0],
                new int[] {0},
                new boolean[] {false},
                List.of(
                        new PeerDistributionWindowFunction(
                                Schema.unspecified(1),
                                new int[] {0},
                                PeerDistributionWindowFunction.Distribution.PERCENT_RANK),
                        new PeerDistributionWindowFunction(
                                Schema.unspecified(1),
                                new int[] {0},
                                PeerDistributionWindowFunction.Distribution.CUMULATIVE)),
                Schema.unspecified(2),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(1L, 0.0, 0.2),
                            row(4L, 0.25, 0.6),
                            row(4L, 0.25, 0.6),
                            row(null, 0.75, 1.0),
                            row(null, 0.75, 1.0)));
        }
    }

    @Test
    void testWindowOperatorSupportsNTile()
    {
        try (Operator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(
                        row(1L, 10L, 4L),
                        row(1L, 20L, 4L),
                        row(1L, 30L, 4L),
                        row(1L, 40L, 4L),
                        row(1L, 50L, 4L),
                        row(1L, 60L, 4L),
                        row(2L, 10L, 20L),
                        row(2L, 20L, (Object) null),
                        row(2L, 30L, 20L))),
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(new PartitionBucketWindowFunction(2)),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(1L, 10L, 4L, 1L),
                            row(1L, 20L, 4L, 1L),
                            row(1L, 30L, 4L, 2L),
                            row(1L, 40L, 4L, 2L),
                            row(1L, 50L, 4L, 3L),
                            row(1L, 60L, 4L, 4L),
                            row(2L, 10L, 20L, 1L),
                            row(2L, 20L, null, null),
                            row(2L, 30L, 20L, 3L)));
        }
    }

    @Test
    void testWindowOperatorProvidesIndexedPartitionsAcrossPages()
    {
        Operator source = new TableOperator(
                3,
                List.of(
                        TableOperator.Page.values(
                                3,
                                new Vector[] {
                                        new I64Vector(new long[] {2, 1, 1}),
                                        new I64Vector(new long[] {2, 3, 1}),
                                        new I64Vector(new long[] {200, 130, 110})},
                                Mask.all(3)),
                        TableOperator.Page.values(
                                2,
                                new Vector[] {
                                        new I64Vector(new long[] {2, 1}),
                                        new I64Vector(new long[] {1, 2}),
                                        new I64Vector(new long[] {190, 120})},
                                Mask.all(2))));
        SelectedPositionWindowFunction reversePartition = new SelectedPositionWindowFunction(
                i64ValueType(),
                (partition, outputPosition, selection) -> selection.set(2, partition.size() - outputPosition - 1));

        try (Operator window = new WindowOperator(
                allocator,
                source,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(reversePartition))) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(1L, 1L, 110L, 130L),
                            row(1L, 2L, 120L, 120L),
                            row(1L, 3L, 130L, 110L),
                            row(2L, 1L, 190L, 200L),
                            row(2L, 2L, 200L, 190L)));
        }
    }

    @Test
    void testSelectedPositionWindowUsesPerRowFrames()
    {
        SelectedPositionWindowFunction frameStart = new SelectedPositionWindowFunction(
                i64ValueType(),
                (partition, outputPosition, bounds) -> bounds.set(
                        Math.max(0, outputPosition - 1),
                        Math.min(partition.size(), outputPosition + 2)),
                (partition, frame, _, selection) -> selection.set(1, frame.start()));

        try (Operator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(
                        row(1L, 30L),
                        row(1L, 10L),
                        row(1L, 20L),
                        row(2L, 50L))),
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(frameStart))) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(1L, 10L, 10L),
                            row(1L, 20L, 10L),
                            row(1L, 30L, 20L),
                            row(2L, 50L, 50L)));
        }
    }

    @Test
    void testPeerDistributionsResetAtPartitionBoundaries()
    {
        try (Operator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(row(2L, 9L), row(1L, 5L), row(1L, 2L), row(1L, 2L))),
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(
                        new PeerDistributionWindowFunction(
                                Schema.unspecified(2),
                                new int[] {1},
                                PeerDistributionWindowFunction.Distribution.PERCENT_RANK),
                        new PeerDistributionWindowFunction(
                                Schema.unspecified(2),
                                new int[] {1},
                                PeerDistributionWindowFunction.Distribution.CUMULATIVE)),
                Schema.unspecified(2),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(1L, 2L, 0.0, 2.0 / 3.0),
                            row(1L, 2L, 0.0, 2.0 / 3.0),
                            row(1L, 5L, 1.0, 1.0),
                            row(2L, 9L, 0.0, 1.0)));
        }
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
    void testProjectOperatorPassesBranchMaskToLazyInputs()
    {
        List<int[]> lazyInputMasks = new ArrayList<>();
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
                BooleanVector condition = new BooleanVector(new boolean[] {true, false, true, false});
                I64Vector values = new I64Vector(new long[] {11, 22, 33, 44});
                return new Batch(
                        Mask.all(4),
                        new Output(Set.of(Stream.VALUES), _ -> condition),
                        new Output(
                                Set.of(Stream.VALUES),
                                _ -> values,
                                (_, mask) -> {
                                    int[] positions = new int[mask.selectedCount()];
                                    for (int index = 0; index < positions.length; index++) {
                                        positions[index] = mask.position(index);
                                    }
                                    lazyInputMasks.add(positions);
                                    return values;
                                },
                                (_, vector) -> vector,
                                (_, _) -> {},
                                null,
                                null),
                        new Output(Set.of(Stream.VALUES), _ -> new I64Vector(new long[] {55, 66, 77, 88})));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };
        Variable selected = new Variable(0);
        Reference condition = new Reference(new Input(0), Stream.VALUES);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(new Assignment(
                        selected,
                        new Conditional(
                                condition,
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES))));

        try (ProjectOperator project = new ProjectOperator(allocator, plan, primitiveRegistry(), source);
                Batch batch = project.next()) {
            batch.output(0).borrow(Stream.VALUES);
        }

        assertThat(lazyInputMasks)
                .anySatisfy(mask -> assertThat(mask).containsExactly(0, 2));
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
    void testFusedProjectionReportsPhysicalExecutionShape()
    {
        F64Vector base = new F64Vector(new double[] {10, 20, 30, 40});
        DictionaryVector values = DictionaryVector.wrapNested(new int[] {3, 1, 0}, 3, base);
        Operator source = singleBatchOperator(Streams.ofValues(values));
        Variable one = new Variable(0);
        Variable incremented = new Variable(1);
        Variable doubled = new Variable(2);
        Reference result = new Reference(doubled, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1.0), AllMask.ALL),
                        new Assignment(incremented, new Call("add_f64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(doubled, new Call("multiply_f64", List.of(
                                new Reference(incremented, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of(result));
        Map<String, Long> diagnostics = new LinkedHashMap<>();

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                primitiveRegistry(),
                source,
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources(),
                (event, value) -> diagnostics.merge(event, value, Long::sum));
                Batch batch = operator.next()) {
            assertThat(((F64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(41.0, 21.0, 11.0);
        }

        assertThat(diagnostics)
                .containsEntry(ProjectOperator.PLANNED_ASSIGNMENTS, 3L)
                .containsEntry(ProjectOperator.GENERATED_KERNELS, 1L)
                .containsEntry(ProjectOperator.GENERATED_PHYSICAL_PROGRAM_KERNELS, 1L)
                .containsEntry(ProjectOperator.GENERATED_SCALAR_TARGET_KERNELS, 0L)
                .containsEntry(ProjectOperator.GENERATED_ATTEMPTS, 1L)
                .containsEntry(ProjectOperator.GENERATED_SUCCESSES, 1L)
                .containsEntry(ProjectOperator.GENERATED_SELECTED_POSITIONS, 3L)
                .containsEntry(ProjectOperator.DICTIONARY_INPUT_POSITIONS, 3L)
                .containsEntry(ProjectOperator.DICTIONARY_FLATTENING_POSITIONS, 0L);
    }

    @Test
    void testFusedProjectionExecutesOverSharedDictionaryDomain()
    {
        int[] ids = {0, 1, 0, 1, 1, 0, 1, 0};
        DictionaryVector values = DictionaryVector.wrap(ids, new I64Vector(new long[] {10, 20}));
        Operator source = singleBatchOperator(Streams.ofValues(values));
        Variable one = new Variable(0);
        Variable incremented = new Variable(1);
        Variable doubled = new Variable(2);
        Reference result = new Reference(doubled, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(incremented, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(doubled, new Call("multiply", List.of(
                                new Reference(incremented, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of(result));
        Map<String, Long> diagnostics = new LinkedHashMap<>();
        Vector transferred;

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                primitiveRegistry(),
                source,
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources(),
                (event, value) -> diagnostics.merge(event, value, Long::sum))) {
            operator.sourceOutputDemand(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS));
            try (Batch batch = operator.next()) {
                Vector projected = batch.output(0).borrow(Stream.VALUES);
                assertThat(projected).isInstanceOf(DictionaryVector.class);
                assertThat(((DictionaryVector) projected).ids()).isSameAs(ids);
                VectorAccess.LongValues longs = VectorAccess.longValues(projected);
                assertThat(java.util.stream.IntStream.range(0, projected.length())
                        .mapToLong(longs::value)
                        .toArray())
                        .containsExactly(11, 21, 11, 21, 21, 11, 21, 11);
                transferred = batch.output(0).take(Stream.VALUES);
            }
        }

        try {
            assertThat(transferred).isInstanceOf(DictionaryVector.class);
            VectorAccess.LongValues longs = VectorAccess.longValues(transferred);
            assertThat(java.util.stream.IntStream.range(0, transferred.length())
                    .mapToLong(longs::value)
                    .toArray())
                    .containsExactly(11, 21, 11, 21, 21, 11, 21, 11);
        }
        finally {
            transferred.releaseTransferredBuffers();
        }

        assertThat(diagnostics)
                .containsEntry(ProjectOperator.GENERATED_ATTEMPTS, 1L)
                .containsEntry(ProjectOperator.GENERATED_SUCCESSES, 1L)
                .containsEntry(ProjectOperator.GENERATED_SELECTED_POSITIONS, 8L)
                .containsEntry(ProjectOperator.GENERATED_DICTIONARY_DOMAIN_POSITIONS, 2L)
                .containsEntry(ProjectOperator.DICTIONARY_INPUT_POSITIONS, 8L)
                .containsEntry(ProjectOperator.DICTIONARY_FLATTENING_POSITIONS, 0L);
    }

    @Test
    void testFusedProjectionExecutesOverRowAlignedStructuralDictionaryDomain()
    {
        class StructuralIdentity
                implements PrimitiveFunction
        {
            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                throw new UnsupportedOperationException("generated projection was not used");
            }
        }

        class StructuralIdentityProjection
                implements ProjectionCodeProvider
        {
            @Override
            public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
            {
                if (arguments.size() != 1) {
                    return Optional.empty();
                }
                var value = builder.argument(0, ProjectionCodeBuilder.ValueType.STRUCT);
                return Optional.of(builder.program(
                        List.of(ProjectionCodeBuilder.ValueType.STRUCT),
                        builder.structure(
                                List.of("high", "low", "score", "accepted"),
                                List.of(
                                        builder.field(value, "high", ProjectionCodeBuilder.ValueType.I64),
                                        builder.field(value, "low", ProjectionCodeBuilder.ValueType.I64),
                                        builder.field(value, "score", ProjectionCodeBuilder.ValueType.F64),
                                        builder.field(value, "accepted", ProjectionCodeBuilder.ValueType.BOOLEAN))),
                        builder.isNull(0)));
            }
        }

        int[] ids = {0, 1, 0, 1, 1, 0, 1, 0};
        StructVector values = new StructVector(ids.length);
        values.setField("high", Streams.ofValues(DictionaryVector.wrap(ids, new I64Vector(new long[] {0, -1}))));
        values.setField("low", Streams.ofValues(DictionaryVector.wrap(ids, new I64Vector(new long[] {10, -20}))));
        values.setField("score", Streams.ofValues(DictionaryVector.wrap(ids, new F64Vector(new double[] {1.5, 2.5}))));
        values.setField("accepted", Streams.ofValues(DictionaryVector.wrap(ids, new BooleanVector(new boolean[] {true, false}))));
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("structural_identity", new StructuralIdentity(), new StructuralIdentityProjection());
        Variable first = new Variable(0);
        Variable result = new Variable(1);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(first, new Call("structural_identity", List.of(
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new Call("structural_identity", List.of(
                                new Reference(first, Stream.VALUES))), AllMask.ALL)),
                List.of(output));
        Map<String, Long> diagnostics = new LinkedHashMap<>();

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                registry,
                singleBatchOperator(Streams.ofValues(values)),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources(),
                (event, count) -> diagnostics.merge(event, count, Long::sum))) {
            operator.sourceOutputDemand(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS));
            try (Batch batch = operator.next()) {
                DictionaryVector projected = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
                assertThat(projected.ids()).isSameAs(ids);
                assertThat(projected.values()).isInstanceOfSatisfying(StructVector.class, domain -> {
                    assertThat(((I64Vector) domain.fieldValues("high")).values()).containsExactly(0, -1);
                    assertThat(((I64Vector) domain.fieldValues("low")).values()).containsExactly(10, -20);
                    assertThat(((F64Vector) domain.fieldValues("score")).values()).containsExactly(1.5, 2.5);
                    assertThat(((BooleanVector) domain.fieldValues("accepted")).values()).containsExactly(true, false);
                });
            }
        }

        assertThat(diagnostics)
                .containsEntry(ProjectOperator.GENERATED_SUCCESSES, 1L)
                .containsEntry(ProjectOperator.GENERATED_SELECTED_POSITIONS, 8L)
                .containsEntry(ProjectOperator.GENERATED_DICTIONARY_DOMAIN_POSITIONS, 2L)
                .containsEntry(ProjectOperator.DICTIONARY_FLATTENING_POSITIONS, 0L);
    }

    @Test
    void testFusedProjectionFlattensDictionaryForOrdinaryFullDemand()
    {
        DictionaryVector values = DictionaryVector.wrap(
                new int[] {0, 1, 0, 1, 1, 0, 1, 0},
                new I64Vector(new long[] {10, 20}));
        Operator source = singleBatchOperator(Streams.ofValues(values));
        Variable one = new Variable(0);
        Variable incremented = new Variable(1);
        Variable doubled = new Variable(2);
        Reference result = new Reference(doubled, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(incremented, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(doubled, new Call("multiply", List.of(
                                new Reference(incremented, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of(result));
        Map<String, Long> diagnostics = new LinkedHashMap<>();

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                primitiveRegistry(),
                source,
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources(),
                (event, value) -> diagnostics.merge(event, value, Long::sum))) {
            operator.sourceOutputDemand(Map.of(0, ValueDemand.FULL));
            try (Batch batch = operator.next()) {
                assertThat(batch.output(0).borrow(Stream.VALUES)).isInstanceOf(I64Vector.class);
            }
        }

        assertThat(diagnostics)
                .containsEntry(ProjectOperator.GENERATED_DICTIONARY_DOMAIN_POSITIONS, 0L)
                .containsEntry(ProjectOperator.DICTIONARY_FLATTENING_POSITIONS, 8L);
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
    void testFusedProjectionFallsBackForProviderGuard()
    {
        class GuardedProjection
                implements ProjectionCodeProvider
        {
            @Override
            public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
            {
                if (arguments.size() != 2) {
                    return Optional.empty();
                }
                var left = builder.argument(0, ProjectionCodeBuilder.ValueType.I64);
                var right = builder.argument(1, ProjectionCodeBuilder.ValueType.I64);
                return Optional.of(builder.guardedProgram(
                        List.of(ProjectionCodeBuilder.ValueType.I64, ProjectionCodeBuilder.ValueType.I64),
                        builder.add(left, right),
                        builder.or(builder.isNull(0), builder.isNull(1)),
                        builder.equal(left, builder.constant(7L))));
            }
        }

        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("guarded_add", new org.weakref.nitro.function.scalar.builtin.HandwrittenBigintAdd(), new GuardedProjection());
        Variable one = new Variable(0);
        Variable first = new Variable(1);
        Variable second = new Variable(2);
        Reference result = new Reference(second, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(first, new Call("guarded_add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(second, new Call("guarded_add", List.of(
                                new Reference(first, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of(result));
        Map<String, Long> diagnostics = new LinkedHashMap<>();

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                registry,
                new ConstantTableOperator(allocator, 1, List.of(row(2L), row(7L))),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources(),
                (event, value) -> diagnostics.merge(event, value, Long::sum));
                Batch batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(4L, 9L);
        }

        assertThat(diagnostics)
                .containsEntry(ProjectOperator.GENERATED_ATTEMPTS, 1L)
                .containsEntry(ProjectOperator.GENERATED_SUCCESSES, 0L)
                .containsEntry(ProjectOperator.GENERATED_LAYOUT_FALLBACKS, 1L);
        assertThat(allocator.currentBytes(new Allocator.Context("FusedProjection"))).isZero();
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
    void testFusedProjectionMaterializesUnsupportedSharedInputOnce()
    {
        PrimitiveRegistry registry = primitiveRegistry();
        registry.register("boundary", new MaterializeLongCarrier());
        Variable one = new Variable(0);
        Variable boundary = new Variable(1);
        Variable incremented = new Variable(2);
        Variable result = new Variable(3);
        Reference boundaryReference = new Reference(boundary, Stream.VALUES);
        Reference resultReference = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(boundary, new Call("boundary", List.of(
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                        new Assignment(incremented, new Call("add", List.of(
                                boundaryReference,
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new Call("multiply", List.of(
                                new Reference(incremented, Stream.VALUES),
                                boundaryReference)), AllMask.ALL)),
                List.of(resultReference));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            FusedProjectionCompiler.CompiledMultiProjection compiled = compiler.tryCompile(plan, registry, List.of(resultReference))
                    .orElseThrow();
            assertThat(compiled.inputs()).containsExactly(boundaryReference);
        }
        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                registry,
                new ConstantTableOperator(allocator, 1, List.of(row(2L), row(3L))));
                Batch batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values())
                    .containsExactly(6L, 12L);
        }
    }

    @Test
    void testFusedProjectionFallsBackWhenStagedInputHasErrors()
    {
        PrimitiveRegistry registry = primitiveRegistry();
        Variable quotient = new Variable(0);
        Variable one = new Variable(1);
        Variable incremented = new Variable(2);
        Variable result = new Variable(3);
        Reference quotientReference = new Reference(quotient, Stream.VALUES);
        Reference resultReference = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(quotient, new Call("bigint_divide", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(incremented, new Call("add", List.of(
                                quotientReference,
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new Call("multiply", List.of(
                                new Reference(incremented, Stream.VALUES),
                                quotientReference)), AllMask.ALL)),
                List.of(resultReference));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            FusedProjectionCompiler.CompiledMultiProjection compiled = compiler.tryCompile(plan, registry, List.of(resultReference))
                    .orElseThrow();
            assertThat(compiled.inputs()).containsExactly(quotientReference);
        }
        try (Allocator testAllocator = new Allocator(EngineResources.createDefault());
                ProjectOperator operator = new ProjectOperator(
                        testAllocator,
                        plan,
                        registry,
                        new ConstantTableOperator(
                                testAllocator,
                                2,
                                List.of(
                                        row(20L, 5L),
                                        row(21L, 0L),
                                        row(22L, 2L))));
                Batch batch = operator.next()) {
            batch.output(0).borrow(Stream.VALUES);
            assertThat(testAllocator.peakBytes(new Allocator.Context("FusedProjection"))).isZero();
            Vector errors = batch.output(0).borrow(Stream.ERRORS);
            assertThat(VectorAccess.booleanValues(errors).value(0)).isFalse();
            assertThat(VectorAccess.booleanValues(errors).value(1)).isTrue();
            assertThat(VectorAccess.booleanValues(errors).value(2)).isFalse();
        }
    }

    @Test
    void testFusedProjectionPreservesBoundaryComparison()
    {
        PrimitiveRegistry registry = primitiveRegistry();
        registry.register("boundary", new MaterializeLongCarrier());
        Variable boundary = new Variable(0);
        Variable limit = new Variable(1);
        Variable condition = new Variable(2);
        Variable one = new Variable(3);
        Variable zero = new Variable(4);
        Variable result = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(boundary, new Call("boundary", List.of(
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                        new Assignment(limit, new Literal(30L), AllMask.ALL),
                        new Assignment(condition, new Call("lte_i64", List.of(
                                new Reference(boundary, Stream.VALUES),
                                new Reference(limit, Stream.VALUES))), AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(result, new Call("if_i64", List.of(
                                new Reference(condition, Stream.VALUES),
                                new Reference(one, Stream.VALUES),
                                new Reference(zero, Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        try (ProjectOperator operator = new ProjectOperator(
                allocator,
                plan,
                registry,
                new ConstantTableOperator(allocator, 1, List.of(row(29L), row(30L), row(31L), row((Object) null))));
                Batch batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values())
                    .containsExactly(1L, 1L, 0L, 0L);
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
        Field groupIdField = new Field("group_id", i32Only, false);
        Field first = new Field("first", i32Only, false);
        Field second = new Field("second", i32Only, true);
        Schema sourceSchema = new Schema(List.of(first, second));

        try (GroupOperator group = new GroupOperator(
                allocator,
                new int[] {0},
                typedTable(sourceSchema),
                groupIdField,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(group.outputSchema().field(0)).isSameAs(groupIdField);
            assertThat(group.outputSchema().field(1)).isSameAs(first);
            assertThat(group.outputSchema().field(2)).isSameAs(second);
        }
    }

    @Test
    void testGroupIdOperatorPreservesMappedFieldsAndWidensNullability()
    {
        TypeBinding i32Only = i32OnlyType();
        Field rollup = new Field("rollup", i32Only, false);
        Field payload = new Field("payload", i32Only, true);
        Field groupIdField = new Field("group_id", i32Only, false);
        Schema sourceSchema = new Schema(List.of(rollup, payload));

        try (Operator groupId = new GroupIdOperator(
                allocator,
                typedTable(sourceSchema),
                new int[][] {
                        {-1, 1},
                        {0, 1}},
                groupIdField,
                EngineResources.from(allocator).operatorResources().groupIdPolicy())) {
            assertThat(groupId.outputSchema().field(0).name()).isEqualTo(rollup.name());
            assertThat(groupId.outputSchema().field(0).type()).isSameAs(i32Only);
            assertThat(groupId.outputSchema().field(0).nullable()).isTrue();
            assertThat(groupId.outputSchema().field(1)).isSameAs(payload);
            assertThat(groupId.outputSchema().field(2)).isSameAs(groupIdField);
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
                EngineResources.from(allocator).operatorResources().fullJoinPolicy())) {
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
    void testFullJoinUsesTypeFactoryForStructuralNullPlaceholders()
    {
        AtomicInteger nullValueConstructions = new AtomicInteger();
        TypeBinding structuralType = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:structural");
            }

            @Override
            public Class<?> carrierType()
            {
                return Object.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(new TypeVectorFactory()
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
                        StructVector values = allocator.allocate(StructVector.class, length, StructVector::new);
                        values.setField("0", Streams.ofValues(allocator.allocate(I64Vector.class, length, I64Vector::new)));
                        return values;
                    }
                });
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class);
            }
        };
        TypeBinding keyType = Schema.unspecified(1).field(0).type();
        Schema outerSchema = new Schema(List.of(
                new Field(keyType, false),
                new Field(structuralType, false)));
        Schema innerSchema = new Schema(List.of(new Field(keyType, false)));
        StructVector payload = new StructVector(0);
        payload.setField("0", Streams.ofValues(new I64Vector(0)));

        try (Operator join = new FullJoinOperator(
                allocator,
                typedTable(
                        outerSchema,
                        TableOperator.Page.values(
                                0,
                                new Vector[] {new I64Vector(0), payload},
                                Mask.all(0))),
                new int[] {0},
                typedTable(
                        innerSchema,
                        TableOperator.Page.values(
                                1,
                                new Vector[] {new I64Vector(new long[] {2})},
                                Mask.all(1))),
                new int[] {0},
                EngineResources.from(allocator).operatorResources().fullJoinPolicy());
                Batch batch = join.next()) {
            assertThat(batch.borrowMask().selectedCount()).isEqualTo(1);
            assertThat(batch.output(1).borrow(Stream.VALUES))
                    .isInstanceOfSatisfying(StructVector.class, values ->
                            assertThat(((I64Vector) values.fieldValues("0")).values()).containsExactly(0));
            assertThat(VectorAccess.booleanValues(batch.output(1).borrow(Stream.NULLS)).value(0)).isTrue();
            assertThat(nullValueConstructions).hasValue(1);
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
                EngineResources.from(allocator).operatorResources().topNRankingPolicy());
                Operator rankingWithoutOutput = new TopNRankingOperator(
                        allocator,
                        10,
                        new int[0],
                        new int[] {0},
                        new boolean[] {false},
                        new boolean[] {false},
                        TopNRankingOperator.RankingType.ROW_NUMBER,
                        false,
                        typedTable(sourceSchema),
                        rankSchema,
                        EngineResources.from(allocator).operatorResources());
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
            assertThat(rankingWithoutOutput.outputSchema()).isSameAs(sourceSchema);
            assertThat(rankingWithoutOutput.outputCount()).isEqualTo(1);
            assertThat(window.outputSchema().fields()).containsExactly(sourceField, rankField, runningField);
            assertThat(window.outputSchema().field(0)).isSameAs(sourceField);
            assertThat(window.outputSchema().field(1)).isSameAs(rankField);
            assertThat(window.outputSchema().field(2)).isSameAs(runningField);
        }
    }

    @Test
    void testOperatorsUseRegistrySuppliedStructuralSemantics()
            throws ReflectiveOperationException
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        TypeOperators operators = new TypeOperators(
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "absoluteIdentical",
                        MethodType.methodType(boolean.class, long.class, long.class))),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "absoluteHash",
                        MethodType.methodType(long.class, long.class))),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "compareAbsolute",
                        MethodType.methodType(int.class, long.class, long.class))),
                Optional.empty(),
                Optional.empty(),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "readI64",
                        MethodType.methodType(long.class, Vector.class, int.class))));
        TypeBinding absoluteLong = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:absolute-long");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return operators;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class);
            }
        };
        Schema sourceSchema = new Schema(List.of(new Field("value", absoluteLong, false)));
        Operator source = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        4,
                        new Vector[] {new I64Vector(new long[] {-2, 1, 2, -1})},
                        Mask.all(4)));

        try (Operator ranking = new TopNRankingOperator(
                allocator,
                10,
                new int[0],
                new int[] {0},
                new boolean[] {false},
                source,
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(ranking))
                    .matchesExactly(List.of(
                            row(1L, 1L),
                            row(-1L, 1L),
                            row(-2L, 3L),
                            row(2L, 3L)));
        }

        Operator sortSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        4,
                        new Vector[] {new I64Vector(new long[] {-2, 1, 2, -1})},
                        Mask.all(4)));
        try (Operator sort = new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {false},
                sortSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(sort))
                    .matchesExactly(List.of(
                            row(1L),
                            row(-1L),
                            row(-2L),
                            row(2L)));
        }

        Operator windowSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        4,
                        new Vector[] {new I64Vector(new long[] {-1, 1, -2, 2})},
                        Mask.all(4)));
        try (Operator window = new WindowOperator(
                allocator,
                windowSource,
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new PartitionSumI64WindowFunction(0)),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(-1L, 0L),
                            row(1L, 0L),
                            row(-2L, 0L),
                            row(2L, 0L)));
        }

        Operator rankSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        4,
                        new Vector[] {new I64Vector(new long[] {-1, 1, -2, 2})},
                        Mask.all(4)));
        try (Operator window = new WindowOperator(
                allocator,
                rankSource,
                new int[0],
                new int[] {0},
                new boolean[] {false},
                List.of(new RankWindowFunction(sourceSchema, new int[] {0}, new boolean[] {false})),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(window))
                    .matchesExactly(List.of(
                            row(-1L, 1L),
                            row(1L, 1L),
                            row(-2L, 3L),
                            row(2L, 3L)));
        }

        Operator hashOuter = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {1, 2})},
                        Mask.all(2)));
        Operator hashInner = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {-1, -2})},
                        Mask.all(2)));
        try (Operator join = new FullJoinOperator(
                allocator,
                hashOuter,
                new int[] {0},
                hashInner,
                new int[] {0},
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(join))
                    .matchesExactly(List.of(
                            row(1L, -1L),
                            row(2L, -2L)));
        }

        Operator sortedOuter = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {1, 2})},
                        Mask.all(2)));
        Operator sortedInner = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {-1, -2})},
                        Mask.all(2)));
        try (Operator join = FullJoinOperator.sorted(
                allocator,
                sortedOuter,
                new int[] {0},
                sortedInner,
                new int[] {0},
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(join))
                    .matchesExactly(List.of(
                            row(1L, -1L),
                            row(2L, -2L)));
        }

        Operator nestedOuter = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {1, 2})},
                        Mask.all(2)));
        Operator nestedInner = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {-1, -2})},
                        Mask.all(2)));
        try (Operator join = new NestedLoopJoinOperator(
                EngineResources.from(allocator).operatorResources(),
                allocator,
                nestedOuter,
                0,
                nestedInner,
                0)) {
            assertThat(operator(join))
                    .matchesExactly(List.of(
                            row(1L, -1L),
                            row(2L, -2L)));
        }

        Operator hashJoinOuter = rejectDynamicFilters(typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I64Vector(new long[] {1})},
                        Mask.all(1)),
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I64Vector(new long[] {2})},
                        Mask.all(1))));
        Operator hashJoinInner = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I64Vector(new long[] {-1})},
                        Mask.all(1)),
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I64Vector(new long[] {-2})},
                        Mask.all(1)));
        try (Operator join = new HashJoinOperator(
                EngineResources.from(allocator).operatorResources(),
                allocator,
                hashJoinOuter,
                0,
                hashJoinInner,
                0)) {
            assertThat(operator(join))
                    .matchesExactly(List.of(
                            row(1L, -1L),
                            row(2L, -2L)));
        }

        Operator semiJoinOuter = rejectDynamicFilters(typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        3,
                        new Vector[] {new I64Vector(new long[] {1, 2, 3})},
                        Mask.all(3))));
        Operator semiJoinInner = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I64Vector(new long[] {-1})},
                        Mask.all(1)),
                TableOperator.Page.values(
                        1,
                        new Vector[] {new I64Vector(new long[] {-2})},
                        Mask.all(1)));
        try (Operator join = new SemiJoinOperator(
                allocator,
                semiJoinOuter,
                0,
                semiJoinInner,
                0,
                true,
                false,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(join))
                    .matchesExactly(List.of(
                            row(1L),
                            row(2L)));
        }

        Operator distinctSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {1, 2})},
                        Mask.all(2)),
                TableOperator.Page.values(
                        3,
                        new Vector[] {new I64Vector(new long[] {-1, -2, 3})},
                        Mask.all(3)));
        try (Operator distinct = new MarkDistinctOperator(
                allocator,
                0,
                distinctSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(distinct))
                    .matchesExactly(List.of(
                            row(1L),
                            row(2L),
                            row(3L)));
        }

        Schema nullableAbsoluteSchema = new Schema(List.of(new Field("value", absoluteLong, true)));
        Operator nullableDistinctSource = typedTable(
                nullableAbsoluteSchema,
                new TableOperator.Page(
                        4,
                        new Streams[] {Streams.ofValuesAndNulls(
                                new I64Vector(new long[] {0, 0, 1, -1}),
                                new BooleanVector(new boolean[] {true, true, false, false}))},
                        Mask.all(4)));
        try (Operator distinct = new MarkDistinctOperator(
                allocator,
                new int[] {0},
                nullableDistinctSource,
                true,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(distinct))
                    .matchesExactly(List.of(
                            row((Object) null),
                            row(1L)));
        }

        Operator distinctCountSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {1, -1})},
                        Mask.all(2)),
                TableOperator.Page.values(
                        3,
                        new Vector[] {new I64Vector(new long[] {2, -2, 3})},
                        Mask.all(3)));
        try (Operator aggregation = new AggregationOperator(
                allocator,
                List.of(new DistinctCount(0)),
                distinctCountSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(aggregation))
                    .matchesExactly(List.of(row(3L)));
        }

        Schema groupedDistinctSchema = new Schema(List.of(
                new Field(Schema.unspecified(1).field(0).type(), false),
                new Field("value", absoluteLong, false)));
        Operator groupedDistinctSource = typedTable(
                groupedDistinctSchema,
                TableOperator.Page.values(
                        3,
                        new Vector[] {
                                new I64Vector(new long[] {7, 7, 7}),
                                new I64Vector(new long[] {1, -1, 2})},
                        Mask.all(3)));
        try (Operator aggregation = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new DistinctCount(1)),
                groupedDistinctSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(aggregation))
                    .matchesExactly(List.of(row(7L, 2L)));
        }

        Operator groupingSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {1, 2})},
                        Mask.all(2)),
                TableOperator.Page.values(
                        3,
                        new Vector[] {new I64Vector(new long[] {-1, -2, 3})},
                        Mask.all(3)));
        try (Operator group = new GroupOperator(
                allocator,
                0,
                groupingSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(group))
                    .matchesExactly(List.of(
                            row(0L, 1L),
                            row(1L, 2L),
                            row(0L, -1L),
                            row(1L, -2L),
                            row(2L, 3L)));
        }

        Operator groupedAggregationSource = typedTable(
                nullableAbsoluteSchema,
                new TableOperator.Page(
                        2,
                        new Streams[] {Streams.ofValuesAndNulls(
                                new I64Vector(new long[] {1, 0}),
                                new BooleanVector(new boolean[] {false, true}))},
                        Mask.all(2)),
                new TableOperator.Page(
                        3,
                        new Streams[] {Streams.ofValuesAndNulls(
                                new I64Vector(new long[] {-1, 0, 2}),
                                new BooleanVector(new boolean[] {false, true, false}))},
                        Mask.all(3)));
        try (Operator aggregation = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                groupedAggregationSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(aggregation))
                    .matchesExactly(List.of(
                            row(1L, 2L),
                            row(null, 2L),
                            row(2L, 1L)));
        }

        Schema compositeGroupingSchema = new Schema(List.of(
                new Field("absolute", absoluteLong, false),
                new Field("ordinary", Schema.unspecified(1).field(0).type(), false)));
        Operator compositeGroupingSource = typedTable(
                compositeGroupingSchema,
                TableOperator.Page.values(
                        3,
                        new Vector[] {
                                new I64Vector(new long[] {1, -1, 1}),
                                new I64Vector(new long[] {10, 10, 20})},
                        Mask.all(3)));
        try (Operator aggregation = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(new CountAll()),
                compositeGroupingSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(aggregation))
                    .matchesExactly(List.of(
                            row(1L, 10L, 2L),
                            row(1L, 20L, 1L)));
        }
    }

    @Test
    void testOrderingOperatorsUseDirectVectorStructuralSemantics()
            throws ReflectiveOperationException
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        TypeOperators operators = new TypeOperators(
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "absoluteIdentical",
                        MethodType.methodType(boolean.class, long.class, long.class))),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "absoluteHash",
                        MethodType.methodType(long.class, long.class))),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "compareAbsolute",
                        MethodType.methodType(int.class, long.class, long.class))),
                Optional.empty(),
                Optional.empty(),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "failValueRead",
                        MethodType.methodType(long.class, Vector.class, int.class))),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "vectorAbsoluteIdentical",
                        MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class))),
                Optional.empty(),
                Optional.of(lookup.findStatic(
                        TestOperators.class,
                        "compareVectorAbsolute",
                        MethodType.methodType(int.class, Vector.class, int.class, Vector.class, int.class))));
        TypeBinding absoluteLong = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:direct-absolute-long");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return operators;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class);
            }
        };
        Schema sourceSchema = new Schema(List.of(new Field("value", absoluteLong, false)));

        Operator rankingSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        4,
                        new Vector[] {new I64Vector(new long[] {-2, 1, 2, -1})},
                        Mask.all(4)));
        try (Operator ranking = new TopNRankingOperator(
                allocator,
                10,
                new int[0],
                new int[] {0},
                new boolean[] {false},
                rankingSource,
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(ranking))
                    .matchesExactly(List.of(
                            row(1L, 1L),
                            row(-1L, 1L),
                            row(-2L, 3L),
                            row(2L, 3L)));
        }

        Operator sortSource = typedTable(
                sourceSchema,
                TableOperator.Page.values(
                        4,
                        new Vector[] {new I64Vector(new long[] {-2, 1, 2, -1})},
                        Mask.all(4)));
        try (Operator sort = new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {false},
                sortSource,
                EngineResources.from(allocator).operatorResources())) {
            assertThat(operator(sort))
                    .matchesExactly(List.of(
                            row(1L),
                            row(-1L),
                            row(-2L),
                            row(2L)));
        }
    }

    private static Operator rejectDynamicFilters(Operator delegate)
    {
        return new Operator()
        {
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
            public void pushDynamicFilter(org.weakref.nitro.operator.DynamicFilter filter)
            {
                throw new AssertionError("Raw-representation dynamic filter must not be pushed for registry-bound keys");
            }

            @Override
            public void close()
            {
                delegate.close();
            }
        };
    }

    private static long readI64(Vector vector, int position)
    {
        return VectorAccess.longValues(vector).value(position);
    }

    private static long failValueRead(Vector vector, int position)
    {
        throw new AssertionError("Direct vector structural operations must not materialize a carrier");
    }

    private static int compareVectorAbsolute(Vector left, int leftPosition, Vector right, int rightPosition)
    {
        return compareAbsolute(readI64(left, leftPosition), readI64(right, rightPosition));
    }

    private static boolean vectorAbsoluteIdentical(Vector left, int leftPosition, Vector right, int rightPosition)
    {
        return absoluteIdentical(readI64(left, leftPosition), readI64(right, rightPosition));
    }

    private static int compareAbsolute(long left, long right)
    {
        return Long.compare(Math.abs(left), Math.abs(right));
    }

    private static boolean absoluteIdentical(long left, long right)
    {
        return Math.abs(left) == Math.abs(right);
    }

    private static long absoluteHash(long value)
    {
        return Long.hashCode(Math.abs(value));
    }

    @Test
    void testMarkerOperatorsPreserveSourceSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Field marker = new Field("marker", i32Only, false);
        Field key = new Field("key", i32Only, false);
        Field payload = new Field("payload", i32Only, true);
        Schema sourceSchema = new Schema(List.of(key, payload));
        Schema keySchema = new Schema(List.of(key));

        try (Operator distinct = new MarkDistinctMarkerOperator(
                allocator,
                new int[] {0},
                typedTable(sourceSchema),
                false,
                marker,
                EngineResources.from(allocator).operatorResources());
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
                        marker,
                        EngineResources.from(allocator).operatorResources())) {
            assertThat(filteringSemiJoin.outputSchema()).isSameAs(sourceSchema);
            assertMarkerSchema(distinct.outputSchema(), key, payload, marker);
            assertMarkerSchema(markingSemiJoin.outputSchema(), key, payload, marker);
        }
    }

    private static void assertMarkerSchema(Schema schema, Field key, Field payload, Field marker)
    {
        assertThat(schema.field(0)).isSameAs(key);
        assertThat(schema.field(1)).isSameAs(payload);
        assertThat(schema.field(2)).isSameAs(marker);
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
                EngineResources.from(allocator).operatorResources())))
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
                PhysicalAggregationProgram.singleUnit(ConditionalSumsAggregationUnit.equalUtf8(
                        1,
                        2,
                        List.of("Monday", "Tuesday"))),
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
                PhysicalAggregationProgram.singleUnit(ConditionalSumsAggregationUnit.equalLong(
                        1,
                        2,
                        List.of(1L, 2L))),
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
    void testDiscriminatedAggregationsRouteToIndependentDelegatesWithSqlNullSemantics()
    {
        assertThat(operator(new GroupedAggregationOperator(
                allocator,
                List.of(0),
                PhysicalAggregationProgram.singleUnit(DiscriminatedAggregationUnit.equalUtf8(
                        1,
                        List.of("Monday", "Tuesday"),
                        List.of(new Sum(2), new Sum(2)))),
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
                        row(20L, null, null),
                        row(30L, null, null),
                        row(40L, null, null)));
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
    void testGroupOperatorDoesNotProbeNonRetainedSourceWithOpenBatch()
    {
        AtomicBoolean batchOpen = new AtomicBoolean();
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
                if (batchOpen.get()) {
                    throw new AssertionError("hasNext called with an open non-retained batch");
                }
                return true;
            }

            @Override
            public Batch next()
            {
                batchOpen.set(true);
                return new Batch(
                        Mask.all(1),
                        _ -> {},
                        Function.identity(),
                        _ -> {},
                        () -> batchOpen.set(false),
                        new Output(Set.of(Stream.VALUES), _ -> new I64Vector(new long[] {11})));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (Operator group = new GroupOperator(allocator, 0, source);
                Batch batch = group.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values())
                    .containsExactly(0L);
        }
    }

    @Test
    void testGroupOperatorUsesSafeOpenBatchAvailability()
    {
        AtomicBoolean batchOpen = new AtomicBoolean();
        AtomicInteger availabilityChecks = new AtomicInteger();
        Operator source = new Operator()
        {
            private boolean produced;

            @Override
            public int outputCount()
            {
                return 1;
            }

            @Override
            public boolean hasNext()
            {
                availabilityChecks.incrementAndGet();
                return !produced;
            }

            @Override
            public Batch next()
            {
                produced = true;
                batchOpen.set(true);
                return new Batch(
                        Mask.all(1),
                        _ -> {},
                        Function.identity(),
                        _ -> {},
                        () -> batchOpen.set(false),
                        new Output(Set.of(Stream.VALUES), _ -> new I64Vector(new long[] {11})));
            }

            @Override
            public boolean supportsOpenBatchHasNext()
            {
                return true;
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (Operator group = new GroupOperator(allocator, 0, source);
                Batch batch = group.next()) {
            assertThat(batchOpen).isTrue();
            assertThat(availabilityChecks).hasValue(1);
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values())
                    .containsExactly(0L);
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
    void testJoinSessionOperatorPullsProbeBatchesLazily()
    {
        Schema schema = Schema.unspecified(1);
        Operator probe = typedTable(
                schema,
                TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {1})}, Mask.all(1)),
                TableOperator.Page.values(1, new Vector[] {new I64Vector(new long[] {2})}, Mask.all(1)));
        AtomicInteger sessions = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();

        Operator join = new JoinSessionOperator(
                probe,
                () -> {
                    sessions.incrementAndGet();
                    return new NestedLoopJoinSession(
                            EngineResources.from(allocator).operatorResources(),
                            allocator,
                            schema,
                            new ConstantTableOperator(allocator, 1, List.of(row(10L), row(20L))));
                },
                Schema.unspecified(2),
                closes::incrementAndGet);

        assertThat(sessions).hasValue(0);
        assertThat(operator(join))
                .matchesExactly(List.of(
                        row(1L, 10L),
                        row(1L, 20L),
                        row(2L, 10L),
                        row(2L, 20L)));
        assertThat(sessions).hasValue(1);
        assertThat(closes).hasValue(1);
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
    void testLimitOperatorStopsWithoutPollingUpstreamAgain()
    {
        AtomicInteger hasNextCalls = new AtomicInteger();
        Operator source = new Operator()
        {
            private boolean returned;

            @Override
            public int outputCount()
            {
                return 1;
            }

            @Override
            public Schema outputSchema()
            {
                return Schema.unspecified(1);
            }

            @Override
            public boolean hasNext()
            {
                hasNextCalls.incrementAndGet();
                return !returned;
            }

            @Override
            public Batch next()
            {
                returned = true;
                return new Batch(Mask.all(1), Output.of(Streams.ofValues(new I64Vector(new long[] {11}))));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        try (Operator limit = new LimitOperator(allocator, 1, source)) {
            assertThat(limit.hasNext()).isTrue();
            try (Batch ignored = limit.next()) {
                assertThat(limit.hasNext()).isFalse();
            }
        }
        assertThat(hasNextCalls).hasValue(1);
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
        names.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
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
        callerTraits.add(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);

        values.addTraits(callerTraits);
        callerTraits.add(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);

        assertThat(values.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID)).isTrue();
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
                        new Call("bigint_divide", List.of(
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
                        EngineResources.from(allocator).operatorResources().filter())))
                .matchesExactly(List.of(
                        row(1L, 10L),
                        row(2L, 20L)));
    }

    @Test
    void testFilterOperatorReportsCompiledMaskExecution()
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
                List.of());
        Map<String, Long> diagnostics = new LinkedHashMap<>();

        assertThat(operator(new FilterOperator(
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L), row(4L))),
                evaluationPlan,
                primitiveRegistry,
                MaskExpressionResolver.resolve(evaluationPlan, predicateValues),
                allocator,
                EngineResources.from(allocator).operatorResources().filter(),
                (event, value) -> diagnostics.merge(event, value, Long::sum))))
                .matchesExactly(List.of(row(1L), row(2L)));

        assertThat(diagnostics)
                .containsEntry(FilterOperator.PLANNED_ASSIGNMENTS, 2L)
                .containsEntry(FilterOperator.PLANNED_COMPILED_MASKS, 1L)
                .containsEntry(FilterOperator.INPUT_POSITIONS, 4L)
                .containsEntry(FilterOperator.OUTPUT_POSITIONS, 2L)
                .containsEntry(FilterOperator.COMPILED_MASK_SUCCESSES, 1L)
                .containsEntry(FilterOperator.COMPILED_MASK_FALLBACKS, 0L)
                .containsEntry(FilterOperator.MATERIALIZED_MASK_FALLBACKS, 0L);
    }

    @Test
    void testFilterOperatorRemovesPredicateExplicitlyEnforcedBySource()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable literal = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan evaluationPlan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(2L), AllMask.ALL),
                new Assignment(predicate, new Call("eq", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L)))
        {
            @Override
            public StaticFilterEnforcement pushStaticFilter(org.weakref.nitro.operator.DynamicFilter filter)
            {
                assertThat(filter.column()).isZero();
                assertThat(filter.accepts(2)).isTrue();
                StaticFilterEnforcement enforcement = StaticFilterEnforcement.pending();
                enforcement.complete(RuntimeFilterAcceptance.ENFORCED);
                return enforcement;
            }
        };

        // The mock source deliberately leaves its rows unchanged. Seeing all rows proves that FilterOperator removed
        // exactly the predicate whose source-enforcement negotiation completed successfully.
        assertThat(operator(new FilterOperator(
                source,
                evaluationPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter())))
                .matchesExactly(List.of(row(1L), row(2L), row(3L)));
    }

    @Test
    void testFilterOperatorRecomputesResidualAfterDeferredSourceEnforcement()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable literal = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan evaluationPlan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(2L), AllMask.ALL),
                new Assignment(predicate, new Call("eq", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        java.util.concurrent.atomic.AtomicReference<StaticFilterEnforcement> pending = new java.util.concurrent.atomic.AtomicReference<>();
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L)))
        {
            @Override
            public StaticFilterEnforcement pushStaticFilter(org.weakref.nitro.operator.DynamicFilter filter)
            {
                StaticFilterEnforcement enforcement = StaticFilterEnforcement.pending();
                pending.set(enforcement);
                return enforcement;
            }
        };
        FilterOperator filter = new FilterOperator(
                source,
                evaluationPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());

        // Island construction derives source demands before a host-fed source exists, so enforcement is unresolved
        // during this call. Completing it afterwards must still remove the now source-enforced residual.
        filter.sourceOutputDemand(Map.of());
        pending.get().complete(RuntimeFilterAcceptance.ENFORCED);

        assertThat(operator(filter)).matchesExactly(List.of(row(1L), row(2L), row(3L)));
    }

    @Test
    void testFilterOperatorRemovesPredicateEnforcedThroughProjection()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable literal = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan filterPlan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(2L), AllMask.ALL),
                new Assignment(predicate, new Call("eq", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                2,
                List.of(row(1L, 10L), row(2L, 20L), row(3L, 30L)))
        {
            @Override
            public StaticFilterEnforcement pushStaticFilter(org.weakref.nitro.operator.DynamicFilter filter)
            {
                assertThat(filter.column()).isEqualTo(1);
                StaticFilterEnforcement enforcement = StaticFilterEnforcement.pending();
                enforcement.complete(RuntimeFilterAcceptance.ENFORCED);
                return enforcement;
            }
        };
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(),
                List.of(new Reference(new Input(1), Stream.VALUES)));

        // The source intentionally does not apply the predicate. Seeing every projected row proves that the source's
        // enforcement acknowledgement crossed the projection and removed the residual from the filter.
        assertThat(operator(new FilterOperator(
                new ProjectOperator(allocator, projectionPlan, primitiveRegistry, source),
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter())))
                .matchesExactly(List.of(row(10L), row(20L), row(30L)));
    }

    @Test
    void testProjectionAndFilterDeriveSourceOutputDemandAfterEnforcement()
    {
        assertThat(filterProjectSourceDemand(true)).containsExactly(1);
        assertThat(filterProjectSourceDemand(false)).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    void testProjectionPropagatesFunctionStructuralValueDemand()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("cardinality", new Cardinality());
        Variable cardinality = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        cardinality,
                        new Call("cardinality", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(cardinality, Stream.VALUES)));
        ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of())
        {
            @Override
            public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
            {
                return Optional.of(Map.copyOf(demandedOutputs));
            }
        };

        try (ProjectOperator project = new ProjectOperator(allocator, projectionPlan, primitiveRegistry, source)) {
            assertThat(project.sourceOutputDemand(Map.of(0, ValueDemand.FULL)).orElseThrow())
                    .containsExactlyEntriesOf(Map.of(0, ValueDemand.STRUCTURE));
        }
    }

    @Test
    void testProjectionPropagatesFunctionDictionaryDomainCountDemand()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable divisor = new Variable(0);
        Variable remainder = new Variable(1);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(
                        new Assignment(divisor, new Literal(7L), AllMask.ALL),
                        new Assignment(
                                remainder,
                                new Call("bigint_modulus", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(divisor, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(remainder, Stream.VALUES)));
        ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of())
        {
            @Override
            public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
            {
                return Optional.of(Map.copyOf(demandedOutputs));
            }
        };

        try (ProjectOperator project = new ProjectOperator(allocator, projectionPlan, primitiveRegistry, source)) {
            assertThat(project.sourceOutputDemand(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS)).orElseThrow())
                    .containsExactlyEntriesOf(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS));
            assertThat(project.sourceOutputDemand(Map.of(0, ValueDemand.FULL)).orElseThrow())
                    .containsExactlyEntriesOf(Map.of(0, ValueDemand.FULL));
        }
    }

    @Test
    void testProjectionPropagatesDictionaryDomainCountDemandThroughGenericDeterministicFunction()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("identity", new PrimitiveFunction()
        {
            @Override
            public Streams apply(
                    List<Streams> inputs,
                    Mask mask,
                    Set<Stream> requestedStreams,
                    Streams output,
                    PrimitiveExecutionContext context)
            {
                return Streams.ofValues(inputs.getFirst().values());
            }
        });
        Variable incremented = new Variable(0);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(new Assignment(
                        incremented,
                        new Call("identity", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(incremented, Stream.VALUES)));
        DictionaryVector values = DictionaryVector.wrapWithDomainFrequencies(
                new int[] {0, 1, 0, 2},
                4,
                new I64Vector(new long[] {8, 9, 10}),
                new int[] {2, 1, 1});
        TableOperator source = new TableOperator(
                1,
                List.of(new TableOperator.Page(
                        4,
                        new Streams[] {Streams.ofValues(values)},
                        Mask.all(4))))
        {
            @Override
            public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
            {
                return Optional.of(Map.copyOf(demandedOutputs));
            }
        };

        try (ProjectOperator project = new ProjectOperator(allocator, projectionPlan, primitiveRegistry, source)) {
            assertThat(project.sourceOutputDemand(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS)).orElseThrow())
                    .containsExactlyEntriesOf(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS));
            try (Batch batch = project.next()) {
                assertThat(batch.output(0).borrow(Stream.VALUES))
                        .isInstanceOfSatisfying(DictionaryVector.class, dictionary -> {
                            assertThat(dictionary.hasDomainFrequencies()).isTrue();
                            assertThat(dictionary.domainFrequency(0)).isEqualTo(2);
                            assertThat(dictionary.domainFrequency(1)).isEqualTo(1);
                            assertThat(dictionary.domainFrequency(2)).isEqualTo(1);
                        });
            }
        }
    }

    @Test
    void testModuloPreservesDictionaryDomainFrequencies()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable divisor = new Variable(0);
        Variable remainder = new Variable(1);
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(
                        new Assignment(divisor, new Literal(7L), AllMask.ALL),
                        new Assignment(
                                remainder,
                                new Call("bigint_modulus", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(divisor, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(remainder, Stream.VALUES)));
        DictionaryVector values = DictionaryVector.wrapWithDomainFrequencies(
                new int[] {0, 1, 0, 2},
                4,
                new I64Vector(new long[] {8, 9, 10}),
                new int[] {2, 1, 1});

        try (ProjectOperator project = new ProjectOperator(
                allocator,
                projectionPlan,
                primitiveRegistry,
                new TableOperator(
                        1,
                        List.of(new TableOperator.Page(
                                4,
                                new Streams[] {Streams.ofValues(values)},
                                Mask.all(4)))));
                Batch batch = project.next()) {
            assertThat(batch.output(0).borrow(Stream.VALUES))
                    .isInstanceOfSatisfying(DictionaryVector.class, dictionary -> {
                        assertThat(dictionary.ids()).isSameAs(values.ids());
                        assertThat(dictionary.hasDomainFrequencies()).isTrue();
                        assertThat(dictionary.domainFrequency(0)).isEqualTo(2);
                        assertThat(dictionary.domainFrequency(1)).isEqualTo(1);
                        assertThat(dictionary.domainFrequency(2)).isEqualTo(1);
                        assertThat(((I64Vector) dictionary.values()).values()).containsExactly(1, 2, 3);
                    });
        }
    }

    private Set<Integer> filterProjectSourceDemand(boolean enforceFilter)
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable literal = new Variable(0);
        Variable predicate = new Variable(1);
        EvaluationPlan filterPlan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(2L), AllMask.ALL),
                new Assignment(predicate, new Call("eq", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        ConstantTableOperator source = new ConstantTableOperator(allocator, 2, List.of())
        {
            @Override
            public StaticFilterEnforcement pushStaticFilter(org.weakref.nitro.operator.DynamicFilter filter)
            {
                if (!enforceFilter) {
                    return StaticFilterEnforcement.residual();
                }
                StaticFilterEnforcement enforcement = StaticFilterEnforcement.pending();
                enforcement.complete(RuntimeFilterAcceptance.ENFORCED);
                return enforcement;
            }

            @Override
            public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
            {
                return Optional.of(Map.copyOf(demandedOutputs));
            }
        };
        FilterOperator filter = new FilterOperator(
                source,
                filterPlan,
                primitiveRegistry,
                new Reference(predicate, Stream.VALUES),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());
        EvaluationPlan projectionPlan = new EvaluationPlan(
                List.of(),
                List.of(new Reference(new Input(1), Stream.VALUES)));
        try (ProjectOperator project = new ProjectOperator(allocator, projectionPlan, primitiveRegistry, filter)) {
            return project.sourceOutputDemand(Set.of(0)).orElseThrow();
        }
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
                        EngineResources.from(allocator).operatorResources().filter())))
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
                EngineResources.from(allocator).operatorResources().filter())) {
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
                        call(quotient, "bigint_divide", values(new Input(0)), values(three))),
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
    void testF64SumConsumesSharedDictionaryDomainFrequencies()
    {
        DictionaryVector values = DictionaryVector.wrapWithDomainFrequencies(
                new int[] {0, 1, 2, 0},
                4,
                new F64Vector(new double[] {1, 3, 0}),
                new int[] {2, 1, 1});
        DictionaryVector nulls = values.sharedMappingWithValues(new BooleanVector(new boolean[] {false, false, true}));
        Operator aggregated = new AggregationOperator(
                allocator,
                List.of(new SumF64(0)),
                new TableOperator(
                        1,
                        List.of(new TableOperator.Page(
                                4,
                                new Streams[] {Streams.of(values, nulls, null)},
                                Mask.all(4)))));

        assertThat(operator(aggregated)).matchesExactly(List.of(row(5.0)));
    }

    @Test
    void testCountAvgStddevPhysicalUnitPreservesResultsAndNulls()
    {
        // Bind STDDEV first to verify that physical result slots are independent of output order.
        Operator grouped = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                new PhysicalAggregationProgram(
                        List.of(new CountAvgStddevI64AggregationUnit(1)),
                        List.of(
                                new PhysicalAggregationProgram.Output(0, 2),
                                new PhysicalAggregationProgram.Output(0, 1),
                                new PhysicalAggregationProgram.Output(0, 0))),
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
                        EngineResources.from(allocator).operatorResources())))
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
                EngineResources.from(allocator).operatorResources())) {
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
                        EngineResources.from(allocator).operatorResources())))
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
                        EngineResources.from(allocator).operatorResources())))
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
                EngineResources.from(allocator).operatorResources())))
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

        try (MarkDistinctOperator operator = new MarkDistinctOperator(allocator, 0, source, EngineResources.from(allocator).operatorResources());
                Batch batch = operator.next()) {
            assertThat(batch.borrowMask().all()).isTrue();
            assertThat(batch.output(0).borrow(Stream.VALUES)).isSameAs(dictionary);
        }
    }

    @Test
    void testMarkDistinctOperatorSizesInitialStateFromSelectedRows()
    {
        int addressablePositions = 1_000_000;
        int[] selected = {0, addressablePositions / 2, addressablePositions - 1};
        long[] values = new long[addressablePositions];
        values[selected[0]] = 11;
        values[selected[1]] = 22;
        values[selected[2]] = 33;

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
                return new Batch(Mask.sparse(selected, addressablePositions), Output.of(Streams.ofValues(new I64Vector(values))));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        Allocator.Context distinctContext = new Allocator.Context("MarkDistinctOperator");
        try (MarkDistinctOperator operator = new MarkDistinctOperator(
                allocator,
                0,
                source,
                EngineResources.from(allocator).operatorResources());
                Batch output = operator.next()) {
            assertThat(output.borrowMask().selectedPositions()).containsExactly(selected);
            assertThat(allocator.currentBytes(distinctContext)).isLessThan(1_000_000);
        }
        assertThat(allocator.currentBytes(distinctContext)).isZero();
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
                        EngineResources.from(allocator).operatorResources())))
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
                EngineResources.from(allocator).operatorResources())) {
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
                EngineResources.from(allocator).operatorResources());

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
                        call(modulo, "bigint_modulus", values(new Input(0)), values(ten)),
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
                PhysicalAggregationProgram.singleUnit(new MinMaxI64AggregationUnit(1)),
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
    void testNestedLoopDoesNotAdvanceNonRetainedOuterWithOpenBatch()
    {
        AtomicBoolean batchOpen = new AtomicBoolean();
        AtomicBoolean produced = new AtomicBoolean();
        Operator outer = new Operator()
        {
            @Override
            public int outputCount()
            {
                return 1;
            }

            @Override
            public boolean hasNext()
            {
                if (batchOpen.get()) {
                    throw new AssertionError("hasNext called with an open non-retained batch");
                }
                return !produced.get();
            }

            @Override
            public Batch next()
            {
                produced.set(true);
                batchOpen.set(true);
                return new Batch(
                        Mask.all(1),
                        _ -> {},
                        Function.identity(),
                        _ -> {},
                        () -> batchOpen.set(false),
                        new Output(Set.of(Stream.VALUES), _ -> new I64Vector(new long[] {7})));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        assertThat(operator(new NestedLoopJoinOperator(
                allocator,
                outer,
                new ConstantTableOperator(allocator, 1, List.of(row(11L))))))
                .matchesExactly(List.of(row(7L, 11L)));
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
    void testLeftNestedLoopEmptyBuild()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        EngineResources.from(allocator).operatorResources(),
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L))),
                        new ConstantTableOperator(allocator, 1, List.of()),
                        true)))
                .matchesExactly(List.of(row(1L, null), row(2L, null)));
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
    void testNestedLoopJoinFiltersCandidatesBeforeEmission()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        EngineResources.from(allocator).operatorResources(),
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(1L), row(2L), row((Object) null))),
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(2L), row(3L), row((Object) null))),
                        longLessThan(0, 0))))
                .matchesExactly(List.of(
                        row(1L, 2L),
                        row(1L, 3L),
                        row(2L, 3L)));
    }

    @Test
    void testNestedLoopJoinFiltersDictionaryDomainBeforeEmission()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        EngineResources.from(allocator).operatorResources(),
                        allocator,
                        new TableOperator(
                                1,
                                List.of(TableOperator.Page.values(
                                        4,
                                        new Vector[] {DictionaryVector.wrap(
                                                new int[] {0, 1, 0, 2},
                                                new I64Vector(new long[] {1, 2, 4}))},
                                        Mask.all(4)))),
                        new ConstantTableOperator(allocator, 1, List.of(row(2L), row(3L))),
                        longLessThan(0, 0))))
                .matchesExactly(List.of(
                        row(1L, 2L),
                        row(1L, 2L),
                        row(1L, 3L),
                        row(2L, 3L),
                        row(1L, 3L)));
    }

    @Test
    void testLeftNestedLoopJoinFiltersDictionaryDomainBeforeEmission()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        EngineResources.from(allocator).operatorResources(),
                        allocator,
                        new TableOperator(
                                1,
                                List.of(TableOperator.Page.values(
                                        4,
                                        new Vector[] {DictionaryVector.wrap(
                                                new int[] {0, 1, 0, 2},
                                                new I64Vector(new long[] {1, 2, 4}))},
                                        Mask.all(4)))),
                        new ConstantTableOperator(allocator, 1, List.of(row(2L), row(3L))),
                        true,
                        longLessThan(0, 0))))
                .matchesExactly(List.of(
                        row(1L, 2L),
                        row(1L, 2L),
                        row(1L, 3L),
                        row(2L, 3L),
                        row(1L, 3L),
                        row(4L, null)));
    }

    @Test
    void testNestedLoopUsesGenericLongPredicateOverDictionaryDomain()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        EngineResources.from(allocator).operatorResources(),
                        allocator,
                        new TableOperator(
                                1,
                                List.of(TableOperator.Page.values(
                                        4,
                                        new Vector[] {DictionaryVector.wrap(
                                                new int[] {0, 1, 0, 2},
                                                new I64Vector(new long[] {1, 2, 4}))},
                                        Mask.all(4)))),
                        new ConstantTableOperator(allocator, 1, List.of(row(2L), row(3L))),
                        longNotEqual(0, 0))))
                .matchesExactly(List.of(
                        row(1L, 2L),
                        row(1L, 2L),
                        row(4L, 2L),
                        row(1L, 3L),
                        row(2L, 3L),
                        row(1L, 3L),
                        row(4L, 3L)));
    }

    @Test
    void testNestedLoopUsesReversedLongPredicateOverDictionaryDomain()
    {
        assertThat(operator(
                new NestedLoopJoinOperator(
                        EngineResources.from(allocator).operatorResources(),
                        allocator,
                        new TableOperator(
                                1,
                                List.of(TableOperator.Page.values(
                                        4,
                                        new Vector[] {DictionaryVector.wrap(
                                                new int[] {0, 1, 0, 2},
                                                new I64Vector(new long[] {1, 2, 4}))},
                                        Mask.all(4)))),
                        new ConstantTableOperator(allocator, 1, List.of(row(2L), row(3L))),
                        longGreaterThan(0, 0))))
                .matchesExactly(List.of(
                        row(4L, 2L),
                        row(4L, 3L)));
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
    void testHashJoinSharesComposedMappingsAcrossEncodedBuildColumns()
    {
        int[] sharedIds = {0, 1, 0};
        DictionaryVector first = DictionaryVector.wrap(sharedIds, new I64Vector(new long[] {10, 20}));
        DictionaryVector second = DictionaryVector.wrap(sharedIds, new I64Vector(new long[] {100, 200}));
        DictionaryVector third = DictionaryVector.wrap(sharedIds, new I64Vector(new long[] {1000, 2000}));
        Operator build = new Operator()
        {
            private boolean done;

            @Override
            public int outputCount()
            {
                return 4;
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
                        ignored -> {},
                        Function.identity(),
                        new Output(Set.of(Stream.VALUES), ignored -> new I64Vector(new long[] {1, 2, 3})),
                        new Output(Set.of(Stream.VALUES), ignored -> first),
                        new Output(Set.of(Stream.VALUES), ignored -> second),
                        new Output(Set.of(Stream.VALUES), ignored -> third));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public boolean supportsRetainedBatches()
            {
                return true;
            }

            @Override
            public void close() {}
        };

        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))),
                0,
                build,
                0).withOutputs(2, 3, 4);
                Batch batch = join.next()) {
            DictionaryVector firstOutput = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
            DictionaryVector secondOutput = (DictionaryVector) batch.output(1).borrow(Stream.VALUES);
            DictionaryVector thirdOutput = (DictionaryVector) batch.output(2).borrow(Stream.VALUES);

            assertThat(secondOutput.ids()).isSameAs(firstOutput.ids());
            assertThat(thirdOutput.ids()).isSameAs(firstOutput.ids());
            assertThat(firstOutput.ids()).isNotSameAs(sharedIds);
            assertThat(new long[] {readI64(firstOutput, 0), readI64(firstOutput, 1), readI64(firstOutput, 2)})
                    .containsExactly(10, 20, 10);
            assertThat(new long[] {readI64(secondOutput, 0), readI64(secondOutput, 1), readI64(secondOutput, 2)})
                    .containsExactly(100, 200, 100);
            assertThat(new long[] {readI64(thirdOutput, 0), readI64(thirdOutput, 1), readI64(thirdOutput, 2)})
                    .containsExactly(1000, 2000, 1000);
        }
    }

    @Test
    void testTakenHashJoinDictionarySurvivesFollowingBatch()
    {
        int rowCount = HashJoinExecutionPolicy.defaults().maxBatchRows() + 1;
        long[] keys = new long[rowCount];
        java.util.Arrays.setAll(keys, index -> index);

        Vector firstValues;
        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                new TableOperator(1, List.of(TableOperator.Page.values(
                        rowCount,
                        new Vector[] {new I64Vector(keys.clone())},
                        Mask.all(rowCount)))),
                0,
                new TableOperator(1, List.of(TableOperator.Page.values(
                        rowCount,
                        new Vector[] {new I64Vector(keys.clone())},
                        Mask.all(rowCount)))),
                0).withOutputs(1)) {
            try (Batch first = join.next()) {
                firstValues = first.output(0).take(Stream.VALUES);
                assertThat(VectorAccess.longValues(firstValues).value(0)).isZero();
            }
            try (Batch second = join.next()) {
                second.output(0).borrow(Stream.VALUES);
            }

            assertThat(VectorAccess.longValues(firstValues).value(0)).isZero();
        }
    }

    @Test
    void testHashJoinAccountsScratchAndSingleLongIndexMemory()
    {
        Allocator.Context operatorContext = new Allocator.Context("HashJoinOperator");
        Allocator.Context indexContext = new Allocator.Context("HashJoinOperatorIndex");
        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))),
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(4L))),
                0)) {
            assertThat(allocator.currentBytes(operatorContext)).isPositive();
            try (Batch ignored = join.next()) {
                assertThat(allocator.peakBytes(indexContext)).isPositive();
            }
        }
        assertThat(allocator.currentBytes(operatorContext)).isZero();
        assertThat(allocator.currentBytes(indexContext)).isZero();
    }

    @Test
    void testHashJoinAccountsPrimitiveCompositeIndexMemory()
    {
        Allocator.Context indexContext = new Allocator.Context("HashJoinOperatorIndex");
        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L), row(2L, 20L), row(3L, 30L))),
                new int[] {0, 1},
                new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L), row(2L, 20L), row(4L, 40L))),
                new int[] {0, 1})) {
            try (Batch ignored = join.next()) {
                assertThat(allocator.peakBytes(indexContext)).isPositive();
            }
        }
        assertThat(allocator.currentBytes(indexContext)).isZero();

        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 3, List.of(row(1L, 10L, 100L), row(2L, 20L, 200L), row(3L, 30L, 300L))),
                new int[] {0, 1, 2},
                new ConstantTableOperator(allocator, 3, List.of(row(1L, 10L, 100L), row(2L, 20L, 200L), row(4L, 40L, 400L))),
                new int[] {0, 1, 2})) {
            try (Batch ignored = join.next()) {
                assertThat(allocator.peakBytes(indexContext)).isPositive();
            }
        }
        assertThat(allocator.currentBytes(indexContext)).isZero();
    }

    @Test
    void testHashJoinAccountsFlatIndexMemory()
    {
        Allocator.Context indexContext = new Allocator.Context("HashJoinOperatorIndex");
        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"), row("beta"), row("missing"))),
                0,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"), row("beta"), row("beta"))),
                0)) {
            try (Batch ignored = join.next()) {
                assertThat(allocator.peakBytes(indexContext)).isPositive();
            }
        }
        assertThat(allocator.currentBytes(indexContext)).isZero();
    }

    @Test
    void testGroupingAndDistinctAccountSharedFlatKeyState()
    {
        Allocator.Context groupContext = new Allocator.Context("GroupOperator");
        assertThat(operator(new GroupOperator(
                allocator,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"), row("beta"), row("alpha"))))))
                .matchesExactly(List.of(
                        row(0L, "alpha"),
                        row(1L, "beta"),
                        row(0L, "alpha")));
        assertThat(allocator.peakBytes(groupContext)).isGreaterThan(1_000_000);
        assertThat(allocator.currentBytes(groupContext)).isZero();

        Allocator.Context distinctContext = new Allocator.Context("MarkDistinctOperator");
        assertThat(operator(new MarkDistinctOperator(
                allocator,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row("alpha"), row("beta"), row("alpha"))),
                EngineResources.from(allocator).operatorResources())))
                .matchesExactly(List.of(row("alpha"), row("beta")));
        assertThat(allocator.peakBytes(distinctContext)).isGreaterThan(1_000_000);
        assertThat(allocator.currentBytes(distinctContext)).isZero();
    }

    @Test
    void testGroupingAccountsPrimitiveTableState()
    {
        Allocator.Context groupContext = new Allocator.Context("GroupOperator");
        assertThat(operator(new GroupOperator(
                allocator,
                new int[] {0, 1},
                new ConstantTableOperator(
                        allocator,
                        2,
                        List.of(row(1L, 10L), row(2L, 20L), row(1L, 10L))))))
                .matchesExactly(List.of(
                        row(0L, 1L, 10L),
                        row(1L, 2L, 20L),
                        row(0L, 1L, 10L)));
        assertThat(allocator.peakBytes(groupContext)).isGreaterThan(300);
        assertThat(allocator.currentBytes(groupContext)).isZero();
    }

    @Test
    void testSortAndWindowAccountPrimitiveRetainedState()
    {
        Allocator.Context sortContext = new Allocator.Context("SortOperator", SortOperator.class);
        try (SortOperator sort = new SortOperator(
                allocator,
                new int[] {0},
                new boolean[] {false},
                new ConstantTableOperator(allocator, 1, List.of(row(3L), row(1L), row(2L))))) {
            try (Batch ignored = sort.next()) {
                assertThat(allocator.currentBytes(sortContext)).isPositive();
                assertThat(allocator.currentBytes(sortContext)).isLessThan(allocator.peakBytes(sortContext));
            }
        }
        assertThat(allocator.currentBytes(sortContext)).isZero();

        Allocator.Context windowContext = new Allocator.Context("WindowOperator");
        try (WindowOperator window = new WindowOperator(
                allocator,
                new ConstantTableOperator(allocator, 1, List.of(row(3L), row(1L), row(2L))),
                new int[0],
                new int[] {0},
                new boolean[] {false},
                List.of(new PartitionSumI64WindowFunction(0)))) {
            try (Batch ignored = window.next()) {
                assertThat(allocator.currentBytes(windowContext)).isPositive();
            }
        }
        assertThat(allocator.currentBytes(windowContext)).isZero();
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
        Field field = new Field("key", i32Only, false);
        Schema schema = new Schema(List.of(field));
        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                typedTable(schema, TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1))),
                0,
                typedTable(schema, TableOperator.Page.values(1, new Vector[] {new I32Vector(new int[] {1})}, Mask.all(1))),
                0)
                .withOutputs(1)) {
            assertThat(join.outputSchema().fields())
                    .singleElement()
                    .isSameAs(field);
        }
    }

    @Test
    void testHashLeftJoinWidensAndProjectsPlanTimeSchema()
    {
        TypeBinding i32Only = i32OnlyType();
        Field outerKey = new Field("outer_key", i32Only, false);
        Field outerValue = new Field("outer_value", i32Only, true);
        Field innerValue = new Field("inner_value", i32Only, false);
        Schema outerSchema = new Schema(List.of(outerKey, outerValue));
        Schema innerSchema = new Schema(List.of(innerValue));

        try (HashJoinOperator join = new HashJoinOperator(
                allocator,
                typedTable(outerSchema),
                0,
                typedTable(innerSchema),
                0,
                true)
                .withOutputs(2, 0, 1)) {
            assertThat(join.outputSchema().fields()).extracting(Field::name)
                    .containsExactly(innerValue.name(), outerKey.name(), outerValue.name());
            assertThat(join.outputSchema().fields()).extracting(Field::type)
                    .containsExactly(i32Only, i32Only, i32Only);
            assertThat(join.outputSchema().fields()).extracting(Field::nullable)
                    .containsExactly(true, false, true);
            assertThat(join.outputSchema().field(1)).isSameAs(outerKey);
            assertThat(join.outputSchema().field(2)).isSameAs(outerValue);
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

    private static TypeBinding i64ValueType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:i64-value");
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
                return Optional.of(new TypeVectorFactory()
                {
                    @Override
                    public Vector constant(VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(VectorAllocator allocator, int length)
                    {
                        return allocator.allocate(I64Vector.class, length, I64Vector::new);
                    }
                });
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class);
            }
        };
    }

    @Test
    void testHashJoinBinaryDictionaryProbeCacheTracksBaseIdentityAndNulls()
    {
        BinaryVector buildKeys = new BinaryVector(3, 14);
        buildKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        buildKeys.setBytes(0, "alpha".getBytes(UTF_8));
        buildKeys.setBytes(1, "beta".getBytes(UTF_8));
        buildKeys.setBytes(2, "gamma".getBytes(UTF_8));

        BinaryVector firstBase = new BinaryVector(3, 17);
        firstBase.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        firstBase.setBytes(0, "gamma".getBytes(UTF_8));
        firstBase.setBytes(1, "alpha".getBytes(UTF_8));
        firstBase.setBytes(2, "missing".getBytes(UTF_8));
        DictionaryVector firstProbe = DictionaryVector.wrap(new int[] {1, 0, 2, 1, 0, 2, 1, 0}, firstBase);

        BinaryVector secondBase = new BinaryVector(3, 16);
        secondBase.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
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
    void testHashJoinRetainsExactReferencesWhenProjectingBuildKeysAcrossBatches()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, List.of(row(7L), row(8L), row(9L))),
                        0,
                        new GeneratorOperator(allocator, 8, 4, List.of(new SequenceGenerator(7, 9))),
                        0)
                        .withOutputs(0, 1)))
                .matchesExactly(List.of(
                        row(7L, 7L), row(7L, 7L), row(7L, 7L), row(7L, 7L),
                        row(8L, 8L), row(8L, 8L), row(8L, 8L), row(8L, 8L)));
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
    void testHashJoinCompactedRangesSkipSparseProbeMisses()
    {
        List<Row> probeRows = new ArrayList<>();
        probeRows.add(row(1_000_000L));
        for (int index = 0; index < 1_024; index++) {
            probeRows.add(row(10_000_000L + index));
        }
        probeRows.add(row(2_000_000L));

        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 1, probeRows),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                1,
                                List.of(row(1_000_000L), row(1_000_000L), row(2_000_000L), row(2_000_000L))),
                        0)
                        .withOutputs(0)))
                .matchesExactly(List.of(row(1_000_000L), row(1_000_000L), row(2_000_000L), row(2_000_000L)));
    }

    @Test
    void testHashJoinDirectSelectedSingleMatchesSkipSparseProbeMisses()
    {
        List<Row> probeRows = new ArrayList<>();
        for (int index = 0; index < 1_024; index++) {
            probeRows.add(row((long) index, index % 2 == 0));
        }

        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new FilterOperator(
                                new ConstantTableOperator(allocator, 2, probeRows),
                                new EvaluationPlan(List.of(), List.of()),
                                primitiveRegistry(),
                                new Reference(new Input(1), Stream.VALUES),
                                allocator,
                                EngineResources.from(allocator).operatorResources().filter()),
                        0,
                        new ConstantTableOperator(allocator, 1, List.of(row(2L), row(1_002L))),
                        0)
                        .withOutputs(0)))
                .matchesExactly(List.of(row(2L), row(1_002L)));
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
                        longNotEqual(1, 1))))
                .matchesExactly(List.of(
                        row(1L, 10L, 100L, 1L, 30L, 2_000L),
                        row(1L, 20L, 200L, 1L, 10L, 1_000L),
                        row(1L, 20L, 200L, 1L, 30L, 2_000L)));
    }

    @Test
    void testHashJoinLongNotEqualResidualFilterAcrossBuildBatches()
    {
        Operator build = new TableOperator(
                2,
                List.of(
                        TableOperator.Page.values(
                                1,
                                new Vector[] {new I64Vector(new long[] {1}), new I64Vector(new long[] {10})},
                                Mask.all(1)),
                        TableOperator.Page.values(
                                1,
                                new Vector[] {new I64Vector(new long[] {1}), new I64Vector(new long[] {30})},
                                Mask.all(1))));
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(row(1L, 10L), row(1L, 20L))),
                        0,
                        build,
                        0,
                        longNotEqual(1, 1))))
                .matchesExactly(List.of(
                        row(1L, 10L, 1L, 30L),
                        row(1L, 20L, 1L, 10L),
                        row(1L, 20L, 1L, 30L)));
    }

    @Test
    void testLeftHashJoinResidualFilterNullExtendsOnlyWhenEveryCandidateIsRejected()
    {
        assertThat(operator(
                new HashJoinOperator(
                        EngineResources.from(allocator).operatorResources(),
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 10L),
                                        row(1L, 20L),
                                        row(2L, 30L),
                                        row(3L, 40L))),
                        new int[] {0},
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(
                                        row(1L, 10L),
                                        row(1L, 30L),
                                        row(2L, 30L))),
                        new int[] {0},
                        true,
                        longNotEqual(1, 1))))
                .matchesExactly(List.of(
                        row(1L, 10L, 1L, 30L),
                        row(1L, 20L, 1L, 10L),
                        row(1L, 20L, 1L, 30L),
                        row(2L, 30L, null, null),
                        row(3L, 40L, null, null)));
    }

    @Test
    void testHashJoinOutputSingleAdmittedMatch()
    {
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(row(1L, 10L))),
                        0,
                        new ConstantTableOperator(
                                allocator,
                                2,
                                List.of(row(1L, 10L), row(1L, 30L))),
                        0,
                        longNotEqual(1, 1))
                        .withOutputSingleMatch()))
                .matchesExactly(List.of(row(1L, 10L, 1L, 30L)));
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
                        longBitwiseOverlap(1, 1))))
                .matchesExactly(List.of(
                        row(1L, 0b110L, 1L, 0b010L),
                        row(1L, 0b110L, 1L, 0b100L)));
    }

    @Test
    void testHashJoinRegistryLongResidualFilter()
    {
        HashJoinOperator.LongJoinFilterFunction function = new HashJoinOperator.LongJoinFilterFunction()
        {
            @Override
            public boolean testLong(long outerValue, long innerValue)
            {
                if (innerValue == 0) {
                    throw new AssertionError("rejected zero value was evaluated");
                }
                return (outerValue & innerValue) != 0;
            }

            @Override
            public boolean rejectsZeroInnerValue()
            {
                return true;
            }
        };
        assertThat(operator(
                new HashJoinOperator(
                        allocator,
                        new ConstantTableOperator(allocator, 2, List.of(row(1L, 0b110L))),
                        0,
                        new ConstantTableOperator(allocator, 2, List.of(row(1L, 0L), row(1L, 0b010L), row(1L, 0b100L))),
                        0,
                        new HashJoinOperator.JoinFilter(1, 1, function))))
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
                        EngineResources.from(allocator).operatorResources().filter()));

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

            assertThat(readI64(batch.output(0).borrow(Stream.VALUES), 0)).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            assertThat(readI64(batch.output(2).borrow(Stream.VALUES), 0)).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            assertThat(readI64(batch.output(1).borrow(Stream.VALUES), 0)).isEqualTo(900L);
            assertThat(payloadBorrows).hasValue(1);
            assertThat(constrainedMask.get()).containsExactly(2);
        }
    }

    @Test
    void testHashJoinMaterializesFullyConsumedUniqueOuterPayload()
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
                Vector output = batch.output(1).borrow(Stream.VALUES);
                assertThat(output).isInstanceOf(I64Vector.class);
                assertThat(output).isNotSameAs(payload);
                assertThat(readI64(output, 0)).isEqualTo(10L);
                assertThat(readI64(output, 1)).isEqualTo(20L);
                assertThat(readI64(output, 2)).isEqualTo(30L);
                assertThat(constrainedMask.get()).containsExactly(0, 1, 2);
            }
        }
    }

    @Test
    void testProbeOuterJoinRetainsFullyConsumedUniqueOuterPayload()
    {
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
                        ignored -> {},
                        Function.identity(),
                        new Output(Set.of(Stream.VALUES), ignored -> keys),
                        new Output(Set.of(Stream.VALUES), ignored -> payload));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public boolean supportsConstrainedReborrow()
            {
                return true;
            }

            @Override
            public void close() {}
        };

        try (Operator join = new HashJoinOperator(
                allocator,
                outer,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L))),
                0,
                true);
                Batch batch = join.next()) {
            Vector output = batch.output(1).borrow(Stream.VALUES);
            assertThat(output).isInstanceOf(DictionaryVector.class);
            assertThat(((DictionaryVector) output).values()).isSameAs(payload);
            assertThat(readI64(output, 0)).isEqualTo(10L);
            assertThat(readI64(output, 1)).isEqualTo(20L);
            assertThat(readI64(output, 2)).isEqualTo(30L);
        }
    }

    @Test
    void testHashJoinRetainsReusedEncodedOuterPayload()
    {
        I64Vector keys = new I64Vector(new long[] {1L, 2L, 3L});
        DictionaryVector payload = DictionaryVector.wrap(
                new int[] {0, 0, 0},
                new I64Vector(new long[] {10L}));
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
                        ignored -> {},
                        Function.identity(),
                        new Output(Set.of(Stream.VALUES), ignored -> keys),
                        new Output(Set.of(Stream.VALUES), ignored -> payload));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public boolean supportsConstrainedReborrow()
            {
                return true;
            }

            @Override
            public void close() {}
        };

        try (Operator join = new HashJoinOperator(
                allocator,
                outer,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))),
                0);
                Batch batch = join.next()) {
            Vector output = batch.output(1).borrow(Stream.VALUES);
            assertThat(output).isInstanceOf(DictionaryVector.class);
            assertThat(((DictionaryVector) output).values()).isSameAs(payload);
            assertThat(readI64(output, 0)).isEqualTo(10L);
            assertThat(readI64(output, 1)).isEqualTo(10L);
            assertThat(readI64(output, 2)).isEqualTo(10L);
        }
    }

    @Test
    void testHashJoinPreservesLowReuseEncodedBinaryPayload()
    {
        I64Vector keys = new I64Vector(new long[] {1L, 2L, 3L});
        BinaryVector binary = new BinaryVector(3, 14);
        binary.setBytes(0, "alpha".getBytes(UTF_8));
        binary.setBytes(1, "beta".getBytes(UTF_8));
        binary.setBytes(2, "gamma".getBytes(UTF_8));
        DictionaryVector payload = DictionaryVector.wrap(new int[] {0, 1, 2}, binary);
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
                        ignored -> {},
                        Function.identity(),
                        new Output(Set.of(Stream.VALUES), ignored -> keys),
                        new Output(Set.of(Stream.VALUES), ignored -> payload));
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public boolean supportsConstrainedReborrow()
            {
                return true;
            }

            @Override
            public void close() {}
        };

        try (Operator join = new HashJoinOperator(
                allocator,
                outer,
                0,
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L))),
                0);
                Batch batch = join.next()) {
            Vector result = batch.output(1).borrow(Stream.VALUES);
            assertThat(result).isInstanceOf(DictionaryVector.class);
            DictionaryVector output = (DictionaryVector) result;
            assertThat(output.values()).isSameAs(payload);
            assertThat(output.ids()).containsExactly(0, 1, 2);
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
                        EngineResources.from(allocator).operatorResources().filter()));

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

            assertThat(readI64(batch.output(0).borrow(Stream.VALUES), 0)).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            assertThat(readI64(batch.output(1).borrow(Stream.VALUES), 0)).isEqualTo(3L);
            assertThat(payloadBorrows).hasValue(0);

            assertThat(readI64(batch.output(2).borrow(Stream.VALUES), 0)).isEqualTo(900L);
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
                        EngineResources.from(allocator).operatorResources().filter()));

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
                        call(remainder, "bigint_modulus", values(new Input(inputColumn)), values(divisorLiteral)),
                        literal(one, 1),
                        call(predicate, "lt", values(remainder), values(one))),
                values(predicate));

        return new FilterOperator(
                source,
                evaluationPlan,
                primitiveRegistry,
                values(predicate),
                allocator,
                EngineResources.from(allocator).operatorResources().filter());
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
                EngineResources.from(allocator).operatorResources().filter());
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
        private static final GroupedAggregationUpdateTarget SUM_UPDATE = updateTarget("updateSum");
        private static final GroupedAggregationUpdateTarget COUNT_UPDATE = updateTarget("updateCount");

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
                    GroupedAggregationUpdate.inputValue(inputColumn, SUM_UPDATE),
                    GroupedAggregationUpdate.constant(1, COUNT_UPDATE));
        }

        @Override
        public void bindGeneratedGroupedState(Object state, Object[] targets, int offset)
        {
            targets[offset] = state;
            targets[offset + 1] = state;
        }

        private static GroupedAggregationUpdateTarget updateTarget(String name)
        {
            try {
                return new GroupedAggregationUpdateTarget(MethodHandles.lookup().findStatic(
                        SumAndCountUnit.class,
                        name,
                        MethodType.methodType(void.class, State.class, int.class, long.class)));
            }
            catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private static void updateSum(State state, int group, long value)
        {
            state.sums[group] += value;
        }

        private static void updateCount(State state, int group, long value)
        {
            state.counts[group] += value;
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
