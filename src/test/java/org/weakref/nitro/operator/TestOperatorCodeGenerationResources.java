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
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.LongStateUpdate;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.jit.FusedProjectionCompiler;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOperatorCodeGenerationResources
{
    @Test
    void testProjectionCompilerIsOwnerScoped()
    {
        OperatorCodeGenerationResources first = new OperatorCodeGenerationResources();
        OperatorCodeGenerationResources second = new OperatorCodeGenerationResources();
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

        assertThat(reusedKernel).isSameAs(firstKernel);
        assertThat(isolatedKernel).isNotSameAs(firstKernel);

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
    }

    @Test
    void testGroupingGeneratorsAreOwnerScoped()
    {
        OperatorCodeGenerationResources first = new OperatorCodeGenerationResources();
        OperatorCodeGenerationResources second = new OperatorCodeGenerationResources();
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1 << 20, 0);

        AbstractMultiLongGroupingTable firstTable =
                first.multiLongGrouping().create(2, 16, arrayPool, AdaptiveLongGroupingPolicy.defaults());
        AbstractMultiLongGroupingTable reusedShape =
                first.multiLongGrouping().create(2, 16, arrayPool, AdaptiveLongGroupingPolicy.defaults());
        AbstractMultiLongGroupingTable isolatedShape =
                second.multiLongGrouping().create(2, 16, arrayPool, AdaptiveLongGroupingPolicy.defaults());
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

        DictionaryRecordEqualityKernelGenerator.Shape equalityShape =
                new DictionaryRecordEqualityKernelGenerator.Shape(1, false, 0, 0, 0, 0);
        DictionaryRecordEqualityKernel firstEquality = first.dictionaryRecordEquality().create(equalityShape);
        assertThat(first.dictionaryRecordEquality().create(equalityShape)).isSameAs(firstEquality);
        assertThat(second.dictionaryRecordEquality().create(equalityShape)).isNotSameAs(firstEquality);

        MultiLongGroupingTableGenerator retainedGenerator = first.multiLongGrouping();
        first.close();

        assertThat(firstTable.getClass()).isNotNull();
        assertThatThrownBy(() -> retainedGenerator.create(
                2,
                16,
                arrayPool,
                AdaptiveLongGroupingPolicy.defaults()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multi-long grouping table generator is closed");
        assertThatThrownBy(first::dictionaryHash)
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
                    List.of(GroupedAggregationUpdate.constant(3)),
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
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
                    new long[8],
                    tableIds,
                    7,
                    new long[2],
                    0,
                    null,
                    new Object[] {null},
                    new int[1][],
                    new boolean[1][],
                    new int[1][],
                    new LongStateUpdate[] {state});

            assertThat(nextGroup).isEqualTo(2);
            assertThat(state.values).containsExactly(6, 3);
        }
    }

    private static FusedGroupingKernel createCountKernel(FusedGroupingAggregationKernelGenerator generator)
    {
        return generator.create(
                List.of(GroupedAggregationUpdate.constant(1)),
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                new boolean[] {false},
                new boolean[] {false},
                new boolean[] {false},
                new boolean[] {false},
                new boolean[] {false});
    }

    public static final class ScaledState
            implements LongStateUpdate
    {
        private final long[] values;

        public ScaledState(int size)
        {
            values = new long[size];
        }

        @Override
        public void update(int group, long value)
        {
            values[group] += value;
        }
    }
}
