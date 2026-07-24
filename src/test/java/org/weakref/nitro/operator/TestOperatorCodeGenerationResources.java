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
import org.weakref.nitro.data.CountStateVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.operator.aggregation.FusedAccumulatorSpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOperatorCodeGenerationResources
{
    @Test
    void testGroupingGeneratorsAreOwnerScoped()
    {
        OperatorCodeGenerationResources first = new OperatorCodeGenerationResources();
        OperatorCodeGenerationResources second = new OperatorCodeGenerationResources();
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1 << 20, 0);

        AbstractMultiLongGroupingTable firstTable =
                first.multiLongGrouping().create(2, 16, arrayPool);
        AbstractMultiLongGroupingTable reusedShape =
                first.multiLongGrouping().create(2, 16, arrayPool);
        AbstractMultiLongGroupingTable isolatedShape =
                second.multiLongGrouping().create(2, 16, arrayPool);
        assertThat(reusedShape.getClass()).isSameAs(firstTable.getClass());
        assertThat(isolatedShape.getClass()).isNotSameAs(firstTable.getClass());

        AdaptiveLongGroupingTable firstAdaptive =
                AdaptiveLongGroupingTable.create(2, 16, arrayPool, first);
        AdaptiveLongGroupingTable reusedAdaptive =
                AdaptiveLongGroupingTable.create(2, 16, arrayPool, first);
        AdaptiveLongGroupingTable isolatedAdaptive =
                AdaptiveLongGroupingTable.create(2, 16, arrayPool, second);
        assertThat(reusedAdaptive.getClass()).isSameAs(firstAdaptive.getClass());
        assertThat(isolatedAdaptive.getClass()).isNotSameAs(firstAdaptive.getClass());

        DictionaryHashBatchKernel firstHash = first.dictionaryHash().create(1);
        assertThat(first.dictionaryHash().create(1)).isSameAs(firstHash);
        assertThat(second.dictionaryHash().create(1)).isNotSameAs(firstHash);

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
        assertThatThrownBy(() -> retainedGenerator.create(2, 16, arrayPool))
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

    private static FusedGroupingKernel createCountKernel(FusedGroupingAggregationKernelGenerator generator)
    {
        return generator.create(
                List.of(new FusedAccumulatorSpec(CountStateVector.class, -1)),
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
}
