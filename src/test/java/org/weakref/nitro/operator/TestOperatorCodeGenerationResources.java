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
import org.weakref.nitro.operator.aggregation.FusedAccumulatorSpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOperatorCodeGenerationResources
{
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
