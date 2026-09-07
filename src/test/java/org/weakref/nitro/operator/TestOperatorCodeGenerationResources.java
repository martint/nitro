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
package org.weakref.nitro.operator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.TestPrimitiveFunctions;
import org.weakref.nitro.core.function.aggregation.ContributionCarrier;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTarget;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.jit.FusedProjectionCompiler;
import org.weakref.nitro.jit.ProjectionCodeGenerationPolicy;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.Arrays;
import java.util.List;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.BINARY_REGION;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.BOOLEAN;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.DOUBLE;
import static org.weakref.nitro.core.function.aggregation.ContributionCarrier.LONG;

class TestOperatorCodeGenerationResources
{
    private static final GroupedAggregationUpdateTarget SCALED_LONG_TARGET = longTarget(ScaledState.class);
    private static final GroupedAggregationUpdateTarget WEIGHTED_LONG_TARGET = longTarget(WeightedState.class);
    private static final GroupedAggregationUpdateTarget ORDERED_LONG_TARGET = longTarget(OrderedState.class);
    private static final GroupedAggregationUpdateTarget DOUBLE_SUM_TARGET = doubleTarget(DoubleSumState.class);
    private static final GroupedAggregationUpdateTarget BOOLEAN_COUNT_TARGET = booleanTarget();
    private static final GroupedAggregationUpdateTarget BINARY_REGION_TARGET = binaryRegionTarget();
    private static final GroupedAggregationUpdateTarget MIXED_TARGET = mixedTarget();

    @Test
    void testGeneratedPhysicalHashSupportsBooleanAccessors()
    {
        long shape = 1L |
                (long) DictionaryHashBatchKernelGenerator.MIXED << 4 |
                (long) DictionaryHashBatchKernelGenerator.BOOLEAN_ACCESSOR_HASH <<
                        DictionaryHashBatchKernelGenerator.HASH_MODE_SHIFT;
        long[] hashes = new long[3];
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            DictionaryHashBatchKernel kernel = resources.dictionaryHash().create(shape, 32);
            kernel.hash(
                    3,
                    new int[1][],
                    new long[1][],
                    new VectorAccess.LongValues[1],
                    new VectorAccess.BooleanValues[] {position -> position != 1},
                    new DictionaryHashBatchKernel.BinaryHashes[1],
                    new VectorAccess.BooleanValues[] {position -> position == 2},
                    hashes);
        }

        assertThat(hashes).containsExactly(
                31L + Boolean.hashCode(true),
                31L + Boolean.hashCode(false),
                32L);
    }

    @Test
    void testProjectionCompilerIsOwnerScoped()
    {
        OperatorCodeGenerationResources first = new OperatorCodeGenerationResources();
        OperatorCodeGenerationResources second = new OperatorCodeGenerationResources();
        OperatorCodeGenerationResources configured = new OperatorCodeGenerationResources(
                new ProjectionCodeGenerationPolicy(false, false, false, 4, 4));
        PrimitiveRegistry registry = TestPrimitiveFunctions.primitiveRegistry();
        Variable one = new Variable(0);
        Variable incremented = new Variable(1);
        Variable doubled = new Variable(2);
        Reference output = new Reference(doubled, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1.0), AllMask.ALL),
                        new Assignment(incremented, new Call("add_f64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL),
                        new Assignment(doubled, new Call("multiply_f64", List.of(
                                new Reference(incremented, Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of(output));

        FusedProjectionCompiler firstCompiler = first.fusedProjection();
        Class<?> firstKernel = firstCompiler.tryCompile(plan, registry, List.of(output)).orElseThrow().kernel().getClass();
        Class<?> reusedKernel = firstCompiler.tryCompile(plan, registry, List.of(output)).orElseThrow().kernel().getClass();
        Class<?> isolatedKernel = second.fusedProjection().tryCompile(plan, registry, List.of(output)).orElseThrow().kernel().getClass();
        Class<?> configuredKernel = configured.fusedProjection().tryCompile(plan, registry, List.of(output)).orElseThrow().kernel().getClass();

        assertThat(reusedKernel).isSameAs(firstKernel);
        assertThat(isolatedKernel).isNotSameAs(firstKernel);
        assertThat(configuredKernel).isNotSameAs(firstKernel);

        first.close();
        assertThatThrownBy(first::fusedProjection)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operator code-generation resources are closed");
        assertThatThrownBy(first::projectionMask)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operator code-generation resources are closed");
        assertThatThrownBy(() -> firstCompiler.tryCompile(plan, registry, List.of(output)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Fused projection compiler is closed");
        second.close();
        configured.close();
    }

    @Test
    void testGroupingGeneratorsAreOwnerScoped()
    {
        OperatorCodeGenerationResources first = new OperatorCodeGenerationResources();
        OperatorCodeGenerationResources second = new OperatorCodeGenerationResources();
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1 << 20, 0);

        AbstractFixedWidthKeyTable firstTable =
                first.fixedWidthKeyTables().create(FixedWidthKeyTableLayout.rawI64(2), 16, arrayPool, AdaptiveLongGroupingPolicy.defaults());
        AbstractFixedWidthKeyTable reusedShape =
                first.fixedWidthKeyTables().create(FixedWidthKeyTableLayout.rawI64(2), 16, arrayPool, AdaptiveLongGroupingPolicy.defaults());
        AbstractFixedWidthKeyTable isolatedShape =
                second.fixedWidthKeyTables().create(FixedWidthKeyTableLayout.rawI64(2), 16, arrayPool, AdaptiveLongGroupingPolicy.defaults());
        assertThat(reusedShape.getClass()).isSameAs(firstTable.getClass());
        assertThat(isolatedShape.getClass()).isNotSameAs(firstTable.getClass());

        AdaptiveLongGroupingTable firstAdaptive =
                AdaptiveLongGroupingTable.create(2, 16, arrayPool, first, AdaptiveLongGroupingPolicy.defaults());
        AdaptiveLongGroupingTable reusedAdaptive =
                AdaptiveLongGroupingTable.create(2, 16, arrayPool, first, AdaptiveLongGroupingPolicy.defaults());
        AdaptiveLongGroupingTable isolatedAdaptive =
                AdaptiveLongGroupingTable.create(2, 16, arrayPool, second, AdaptiveLongGroupingPolicy.defaults());
        AdaptiveLongGroupingPolicy lowLoadFactorPolicy = new AdaptiveLongGroupingPolicy(
                0.25f,
                true,
                0.825f,
                1 << 24,
                true,
                1 << 22,
                true,
                1 << 20,
                false,
                false);
        AdaptiveLongGroupingTable differentlyConfiguredAdaptive =
                AdaptiveLongGroupingTable.create(2, 16, arrayPool, first, lowLoadFactorPolicy);
        assertThat(reusedAdaptive.getClass()).isSameAs(firstAdaptive.getClass());
        assertThat(differentlyConfiguredAdaptive.getClass()).isSameAs(firstAdaptive.getClass());
        assertThat(differentlyConfiguredAdaptive.slots).hasSize(64);
        assertThat(isolatedAdaptive.getClass()).isNotSameAs(firstAdaptive.getClass());

        DictionaryHashBatchKernel firstHash = first.dictionaryHash().create(1, 72);
        assertThat(first.dictionaryHash().create(1, 72)).isSameAs(firstHash);
        assertThat(first.dictionaryHash().create(1, 36)).isNotSameAs(firstHash);
        assertThat(second.dictionaryHash().create(1, 72)).isNotSameAs(firstHash);

        int mixedShape = MixedComposite3GroupingKernelGenerator.shape(0, 0, 0);
        MixedComposite3GroupingKernel firstMixed = first.mixedComposite3Grouping().create(mixedShape);
        assertThat(first.mixedComposite3Grouping().create(mixedShape)).isSameAs(firstMixed);
        assertThat(second.mixedComposite3Grouping().create(mixedShape)).isNotSameAs(firstMixed);

        long directShape = DirectCompositeGroupingKernelGenerator.fieldShape(0, 0, false, true);
        directShape = DirectCompositeGroupingKernelGenerator.fieldShape(directShape, 1, true, false);
        DirectCompositeGroupingKernel firstDirect = first.directCompositeGrouping().create(directShape, 2);
        assertThat(first.directCompositeGrouping().create(directShape, 2)).isSameAs(firstDirect);
        assertThat(second.directCompositeGrouping().create(directShape, 2)).isNotSameAs(firstDirect);

        DictionaryRecordEqualityKernelGenerator.Shape equalityShape =
                new DictionaryRecordEqualityKernelGenerator.Shape(
                        false,
                        List.of(new DictionaryRecordEqualityKernelGenerator.FieldShape(
                                0,
                                DictionaryRecordEqualityKernelGenerator.NULL_FREE,
                                DictionaryRecordEqualityKernelGenerator.LONG,
                                0,
                                Long.BYTES)));
        DictionaryRecordEqualityKernel firstEquality = first.dictionaryRecordEquality().create(equalityShape);
        assertThat(first.dictionaryRecordEquality().create(equalityShape)).isSameAs(firstEquality);
        assertThat(second.dictionaryRecordEquality().create(equalityShape)).isNotSameAs(firstEquality);

        FixedWidthKeyTableGenerator retainedGenerator = first.fixedWidthKeyTables();
        first.close();

        assertThat(firstTable.getClass()).isNotNull();
        assertThatThrownBy(() -> retainedGenerator.create(
                FixedWidthKeyTableLayout.rawI64(2),
                16,
                arrayPool,
                AdaptiveLongGroupingPolicy.defaults()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Fixed-width key table generator is closed");
        assertThatThrownBy(first::dictionaryHash)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operator code-generation resources are closed");
        assertThatThrownBy(first::directCompositeGrouping)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operator code-generation resources are closed");

        firstTable.releaseBuffers();
        reusedShape.releaseBuffers();
        isolatedShape.releaseBuffers();
        firstAdaptive.releaseBuffers();
        reusedAdaptive.releaseBuffers();
        isolatedAdaptive.releaseBuffers();
        second.close();
        arrayPool.close();
    }

    @Test
    void testCloseIsTerminal()
    {
        OperatorCodeGenerationResources first = new OperatorCodeGenerationResources();
        OperatorCodeGenerationResources second = new OperatorCodeGenerationResources();
        FusedGroupingAggregationKernelGenerator firstGenerator = first.fusedGrouping();
        FusedGroupingKernel firstKernel = createCountKernel(firstGenerator);

        assertThat(createCountKernel(firstGenerator)).isSameAs(firstKernel);
        assertThat(createCountKernel(second.fusedGrouping())).isNotSameAs(firstKernel);

        first.close();

        // Closing the cache owner does not invalidate an already constructed operator kernel.
        assertThat(firstKernel.getClass()).isNotNull();
        assertThatThrownBy(first::fusedGrouping)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operator code-generation resources are closed");
        assertThatThrownBy(() -> createCountKernel(firstGenerator))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Fused grouping kernel generator is closed");
    }

    @Test
    void testGeneratedGroupingUsesOpaqueProviderStateAndDeclaredConstant()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.constant(3, SCALED_LONG_TARGET)),
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    new boolean[] {false},
                    new ContributionCarrier[] {LONG},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false});
            int[] tableIds = new int[8];
            Arrays.fill(tableIds, -1);
            ScaledState state = new ScaledState(2);

            long nextGroup = kernel.accumulate(
                    null,
                    3,
                    new long[] {1, 1, 2},
                    null,
                    0,
                    new long[8],
                    tableIds,
                    7,
                    new long[2],
                    0,
                    null,
                    new Object[] {null},
                    new int[1][],
                    new int[1][],
                    new int[1],
                    new boolean[1][],
                    new int[1][],
                    new int[1],
                    new Object[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(6, 3);
        }
    }

    @Test
    void testGeneratedGroupingUsesTypedDoubleContribution()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.doubleInputValue(0, DOUBLE_SUM_TARGET)),
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    new boolean[] {false},
                    new ContributionCarrier[] {DOUBLE},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false});
            int[] tableIds = new int[8];
            Arrays.fill(tableIds, -1);
            DoubleSumState state = new DoubleSumState(2);

            long nextGroup = kernel.accumulate(
                    null,
                    3,
                    new long[] {1, 1, 2},
                    null,
                    0,
                    new long[8],
                    tableIds,
                    7,
                    new long[2],
                    0,
                    null,
                    new Object[] {new double[] {1.25, 2.75, -3.5}},
                    new int[1][],
                    new int[1][],
                    new int[1],
                    new boolean[1][],
                    new int[1][],
                    new int[1],
                    new Object[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(4, -3.5);
        }
    }

    @Test
    void testGeneratedGroupingUsesTypedBooleanContribution()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.booleanInputValue(0, BOOLEAN_COUNT_TARGET)),
                    false, false, false, false, false, false, false, false, false,
                    new boolean[] {false},
                    new ContributionCarrier[] {BOOLEAN},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false});
            int[] tableIds = new int[8];
            Arrays.fill(tableIds, -1);
            BooleanCountState state = new BooleanCountState(2);

            long nextGroup = kernel.accumulate(
                    null,
                    3,
                    new long[] {1, 1, 2},
                    null,
                    0,
                    new long[8],
                    tableIds,
                    7,
                    new long[2],
                    0,
                    null,
                    new Object[] {new boolean[] {true, false, true}},
                    new int[1][],
                    new int[1][],
                    new int[1],
                    new boolean[1][],
                    new int[1][],
                    new int[1],
                    new Object[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(1, 1);
        }
    }

    @Test
    void testGeneratedGroupingUsesAllocationFreeBinaryRegionContribution()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.inputValue(0, BINARY_REGION, BINARY_REGION_TARGET)),
                    false, false, false, false, false, false, false, false, false,
                    new boolean[] {false},
                    new ContributionCarrier[] {BINARY_REGION},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false});
            int[] tableIds = new int[8];
            Arrays.fill(tableIds, -1);
            BinaryRegionState state = new BinaryRegionState(2);

            long nextGroup = kernel.accumulate(
                    null,
                    3,
                    new long[] {1, 1, 2},
                    null,
                    0,
                    new long[8],
                    tableIds,
                    7,
                    new long[2],
                    0,
                    null,
                    new Object[] {new byte[] {9, 10, 11, 20, 21, 30}},
                    new int[][] {new int[] {0, 3, 5, 6}},
                    new int[1][],
                    new int[1],
                    new boolean[1][],
                    new int[1][],
                    new int[1],
                    new Object[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(529, 130);
        }
    }

    @Test
    void testGeneratedGroupingUsesMixedContributionsAndCombinedNullConvention()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.inputs(
                            List.of(
                                    new GroupedAggregationUpdate.InputValue(0),
                                    new GroupedAggregationUpdate.InputValue(1, DOUBLE)),
                            MIXED_TARGET)),
                    false, false, false, false, false, false, false, false, false,
                    new boolean[] {false, false},
                    new ContributionCarrier[] {LONG, DOUBLE},
                    new boolean[] {false, false},
                    new boolean[] {false, false},
                    new boolean[] {false, false},
                    new boolean[] {false, false},
                    new boolean[] {false, false},
                    new boolean[] {false, false});
            int[] tableIds = new int[8];
            Arrays.fill(tableIds, -1);
            MixedState state = new MixedState(2);

            long nextGroup = kernel.accumulate(
                    null,
                    3,
                    new long[] {1, 1, 2},
                    null,
                    0,
                    new long[8],
                    tableIds,
                    7,
                    new long[2],
                    0,
                    null,
                    new Object[] {new long[] {2, 4, 8}, new double[] {0.5, 1.5, 3.0}},
                    new int[2][],
                    new int[2][],
                    new int[2],
                    new boolean[][] {null, new boolean[] {false, true, false}},
                    new int[2][],
                    new int[2],
                    new Object[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(1.0, 24.0);
        }
    }

    @Test
    void testGeneratedGroupingCacheSeparatesFunctionTargetsWithTheSamePhysicalShape()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel unweighted = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.constant(2, SCALED_LONG_TARGET)),
                    false, false, false, false, false, false, false, false, false,
                    new boolean[] {false}, new ContributionCarrier[] {LONG}, new boolean[] {false}, new boolean[] {false},
                    new boolean[] {false}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false});
            FusedGroupingKernel weighted = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.constant(2, WEIGHTED_LONG_TARGET)),
                    false, false, false, false, false, false, false, false, false,
                    new boolean[] {false}, new ContributionCarrier[] {LONG}, new boolean[] {false}, new boolean[] {false},
                    new boolean[] {false}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false});

            assertThat(weighted).isNotSameAs(unweighted);

            ScaledState unweightedState = new ScaledState(1);
            WeightedState weightedState = new WeightedState(1);
            accumulateSingleGroup(unweighted, unweightedState);
            accumulateSingleGroup(weighted, weightedState);
            assertThat(unweightedState.values).containsExactly(4);
            assertThat(weightedState.values).containsExactly(20);
        }
    }

    @Test
    void testConstantRunsRequireProviderDeclaredRepeatedSemantics()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.constant(2, ORDERED_LONG_TARGET)),
                    false, false, false, false, true, true, false, false, false,
                    new boolean[] {false}, new ContributionCarrier[] {LONG}, new boolean[] {false}, new boolean[] {false},
                    new boolean[] {false}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false});
            OrderedState state = new OrderedState(1);

            accumulateSingleGroup(kernel, state);

            assertThat(state.values).containsExactly(22);
        }
    }

    @Test
    void testGeneratedGroupingConsumesPreResolvedDictionaryGroups()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.inputValue(0, SCALED_LONG_TARGET)),
                    false,
                    false,
                    true,
                    true,
                    false,
                    false,
                    false,
                    false,
                    false,
                    new boolean[] {false},
                    new ContributionCarrier[] {LONG},
                    new boolean[] {true},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false});
            ScaledState state = new ScaledState(2);

            long nextGroup = kernel.accumulate(
                    null,
                    4,
                    new long[] {90, 10},
                    new int[] {1, 0, 1, 0},
                    0,
                    new long[0],
                    new int[] {0, 1},
                    0,
                    new long[0],
                    2,
                    null,
                    new Object[] {new long[] {1, 2, 4, 8}},
                    new int[1][],
                    new int[][] {new int[] {0, 1, 2, 3}},
                    new int[1],
                    new boolean[1][],
                    new int[1][],
                    new int[1],
                    new Object[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(10, 5);
        }
    }

    @Test
    void testGeneratedGroupingConsumesContiguousRegions()
    {
        try (OperatorCodeGenerationResources resources = new OperatorCodeGenerationResources()) {
            FusedGroupingKernel kernel = resources.fusedGrouping().create(
                    List.of(GroupedAggregationUpdate.inputValue(0, SCALED_LONG_TARGET)),
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    true,
                    new boolean[] {false},
                    new ContributionCarrier[] {LONG},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {false},
                    new boolean[] {true},
                    new boolean[] {true});
            int[] tableIds = new int[8];
            Arrays.fill(tableIds, -1);
            ScaledState state = new ScaledState(2);

            long nextGroup = kernel.accumulate(
                    null,
                    3,
                    new long[] {99, 99, 1, 1, 2},
                    null,
                    2,
                    new long[8],
                    tableIds,
                    7,
                    new long[2],
                    0,
                    null,
                    new Object[] {new long[] {99, 10, 20, 30}},
                    new int[1][],
                    new int[1][],
                    new int[] {1},
                    new boolean[][] {new boolean[] {true, false, false, false}},
                    new int[1][],
                    new int[] {1},
                    new Object[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(30, 30);
        }
    }

    private static FusedGroupingKernel createCountKernel(FusedGroupingAggregationKernelGenerator generator)
    {
        return generator.create(
                List.of(GroupedAggregationUpdate.constant(1, SCALED_LONG_TARGET)),
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                new boolean[] {false},
                new ContributionCarrier[] {LONG},
                new boolean[] {false},
                new boolean[] {false},
                new boolean[] {false},
                new boolean[] {false},
                new boolean[] {false},
                new boolean[] {false});
    }

    private static void accumulateSingleGroup(FusedGroupingKernel kernel, Object state)
    {
        int[] tableIds = new int[8];
        Arrays.fill(tableIds, -1);
        kernel.accumulate(
                null,
                2,
                new long[] {7, 7},
                null,
                0,
                new long[8],
                tableIds,
                7,
                new long[1],
                0,
                null,
                new Object[] {null},
                new int[1][],
                new int[1][],
                new int[1],
                new boolean[1][],
                new int[1][],
                new int[1],
                new Object[] {state});
    }

    private static GroupedAggregationUpdateTarget longTarget(Class<?> stateType)
    {
        try {
            return new GroupedAggregationUpdateTarget(lookup().findVirtual(
                    stateType,
                    "update",
                    methodType(void.class, int.class, long.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static GroupedAggregationUpdateTarget doubleTarget(Class<?> stateType)
    {
        try {
            return new GroupedAggregationUpdateTarget(lookup().findVirtual(
                    stateType,
                    "update",
                    methodType(void.class, int.class, double.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static GroupedAggregationUpdateTarget mixedTarget()
    {
        try {
            return new GroupedAggregationUpdateTarget(lookup().findVirtual(
                    MixedState.class,
                    "update",
                    methodType(void.class, int.class, long.class, double.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static GroupedAggregationUpdateTarget booleanTarget()
    {
        try {
            return new GroupedAggregationUpdateTarget(lookup().findVirtual(
                    BooleanCountState.class,
                    "update",
                    methodType(void.class, int.class, boolean.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static GroupedAggregationUpdateTarget binaryRegionTarget()
    {
        try {
            return new GroupedAggregationUpdateTarget(lookup().findVirtual(
                    BinaryRegionState.class,
                    "update",
                    methodType(void.class, int.class, byte[].class, int.class, int.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final class BinaryRegionState
    {
        private final long[] values;

        private BinaryRegionState(int size)
        {
            values = new long[size];
        }

        public void update(int group, byte[] data, int offset, int length)
        {
            values[group] += length * 100L + data[offset];
        }
    }

    private static final class BooleanCountState
    {
        private final long[] values;

        private BooleanCountState(int size)
        {
            values = new long[size];
        }

        public void update(int group, boolean value)
        {
            if (value) {
                values[group]++;
            }
        }
    }

    private static final class MixedState
    {
        private final double[] values;

        private MixedState(int size)
        {
            values = new double[size];
        }

        public void update(int group, long left, double right)
        {
            values[group] += left * right;
        }
    }

    private static final class DoubleSumState
    {
        private final double[] values;

        private DoubleSumState(int size)
        {
            values = new double[size];
        }

        public void update(int group, double value)
        {
            values[group] += value;
        }
    }

    public static final class ScaledState
    {
        private final long[] values;

        public ScaledState(int size)
        {
            values = new long[size];
        }

        public void update(int group, long value)
        {
            values[group] += value;
        }
    }

    public static final class WeightedState
    {
        private final long[] values;

        public WeightedState(int size)
        {
            values = new long[size];
        }

        public void update(int group, long value)
        {
            values[group] += value * 5;
        }
    }

    public static final class OrderedState
    {
        private final long[] values;

        public OrderedState(int size)
        {
            values = new long[size];
        }

        public void update(int group, long value)
        {
            values[group] = values[group] * 10 + value;
        }
    }
}
