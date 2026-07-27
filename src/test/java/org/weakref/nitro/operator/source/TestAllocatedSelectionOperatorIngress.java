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
package org.weakref.nitro.operator.source;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.execution.EngineResources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TestAllocatedSelectionOperatorIngress
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("test");

    @Test
    void testPreservesPositionDomainWithTrailingUnselectedRows()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            AllocatedSelectionOperatorIngress ingress = new AllocatedSelectionOperatorIngress(allocator, ALLOCATION_CONTEXT);
            Mask mask = ingress.toMask(new TestingSelection(5, new int[] {1, 2}));

            assertThat(mask.size()).isEqualTo(5);
            assertThat(mask.selectedPositions()).containsExactly(1, 2);

            Selection selection = ingress.toSelection(mask);
            assertThat(selection.positionCount()).isEqualTo(5);
            assertThat(selection.count()).isEqualTo(2);
            assertThat(selection.maxPosition()).isEqualTo(2);
            ingress.releaseMask(mask);
        }
    }

    @Test
    void testRejectsInvalidSelectionMetadata()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            AllocatedSelectionOperatorIngress ingress = new AllocatedSelectionOperatorIngress(allocator, ALLOCATION_CONTEXT);

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> ingress.toMask(new TestingSelection(5, new int[] {2, 1})))
                    .withMessageContaining("strictly increasing");
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> ingress.toMask(new TestingSelection(3, new int[] {0, 3})))
                    .withMessageContaining("in bounds");
        }
    }

    private record TestingSelection(int positionCount, int[] positions)
            implements Selection
    {
        @Override
        public int count()
        {
            return positions.length;
        }

        @Override
        public int maxPosition()
        {
            return positions.length == 0 ? -1 : positions[positions.length - 1];
        }

        @Override
        public boolean isDense()
        {
            return positions.length == positionCount;
        }

        @Override
        public int position(int index)
        {
            return positions[index];
        }
    }
}
