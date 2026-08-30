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
package org.weakref.nitro.operator.aggregation;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.execution.EngineResources;

import static org.assertj.core.api.Assertions.assertThat;

class TestSumStateVectorOwnership
{
    @Test
    void testOwnedGrowthCountsCopiedAndSharedStorageAccurately()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("sum-state");
            SumStateVector state = allocator.allocate(context, SumStateVector.class, 4, SumStateVector::new);
            state.increment(3, 11);
            long expectedAllocatedBytes = state.retainedBytes();

            SumStateVector smallGrowth = SumStateVector.growOwned(allocator, context, state, 8);
            expectedAllocatedBytes += smallGrowth.retainedBytes();
            assertThat(smallGrowth.sum(3)).isEqualTo(11);
            assertThat(allocator.allocatedBytes()).isEqualTo(expectedAllocatedBytes);
            assertThat(allocator.residentBytes()).isEqualTo(smallGrowth.retainedBytes());

            SumStateVector fullChunk = SumStateVector.growOwned(allocator, context, smallGrowth, 1_024);
            expectedAllocatedBytes += fullChunk.retainedBytes();
            assertThat(fullChunk.sum(3)).isEqualTo(11);
            assertThat(allocator.allocatedBytes()).isEqualTo(expectedAllocatedBytes);
            assertThat(allocator.residentBytes()).isEqualTo(fullChunk.retainedBytes());

            SumStateVector sharedGrowth = SumStateVector.growOwned(allocator, context, fullChunk, 1_025);
            expectedAllocatedBytes += sharedGrowth.retainedBytes() - fullChunk.retainedBytes();
            assertThat(sharedGrowth.sum(3)).isEqualTo(11);
            assertThat(allocator.allocatedBytes()).isEqualTo(expectedAllocatedBytes);
            assertThat(allocator.residentBytes()).isEqualTo(sharedGrowth.retainedBytes());

            allocator.discard(context, sharedGrowth);
            assertThat(allocator.residentBytes()).isZero();
        }
    }
}
