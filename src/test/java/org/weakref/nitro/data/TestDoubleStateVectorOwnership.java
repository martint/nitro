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
package org.weakref.nitro.data;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.execution.EngineResources;

import static org.assertj.core.api.Assertions.assertThat;

class TestDoubleStateVectorOwnership
{
    @Test
    void testOwnedGrowthCountsCopiedAndSharedStorageAccurately()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("double-state");
            DoubleStateVector state = allocator.allocate(context, DoubleStateVector.class, 4, DoubleStateVector::new);
            state.set(3, 11.5);
            long expectedAllocatedBytes = state.retainedBytes();

            DoubleStateVector smallGrowth = DoubleStateVector.growOwned(allocator, context, state, 8);
            expectedAllocatedBytes += smallGrowth.retainedBytes();
            assertThat(smallGrowth.get(3)).isEqualTo(11.5);
            assertThat(allocator.allocatedBytes()).isEqualTo(expectedAllocatedBytes);
            assertThat(allocator.residentBytes()).isEqualTo(smallGrowth.retainedBytes());

            DoubleStateVector fullChunk = DoubleStateVector.growOwned(allocator, context, smallGrowth, 1_024);
            expectedAllocatedBytes += fullChunk.retainedBytes();
            assertThat(fullChunk.get(3)).isEqualTo(11.5);
            assertThat(allocator.allocatedBytes()).isEqualTo(expectedAllocatedBytes);
            assertThat(allocator.residentBytes()).isEqualTo(fullChunk.retainedBytes());

            DoubleStateVector sharedGrowth = DoubleStateVector.growOwned(allocator, context, fullChunk, 1_025);
            expectedAllocatedBytes += sharedGrowth.retainedBytes() - fullChunk.retainedBytes();
            assertThat(sharedGrowth.get(3)).isEqualTo(11.5);
            assertThat(allocator.allocatedBytes()).isEqualTo(expectedAllocatedBytes);
            assertThat(allocator.residentBytes()).isEqualTo(sharedGrowth.retainedBytes());

            allocator.discard(context, sharedGrowth);
            assertThat(allocator.residentBytes()).isZero();
        }
    }
}
