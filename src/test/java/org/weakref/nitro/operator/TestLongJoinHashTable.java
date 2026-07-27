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
import org.weakref.nitro.data.PrimitiveArrayPool;

import static org.assertj.core.api.Assertions.assertThat;

class TestLongJoinHashTable
{
    private static final int EMPTY = -1;

    @Test
    void ownsGroupedSlotsLazyDuplicateStateGrowthAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        LongJoinHashTable table = new LongJoinHashTable(arrayPool, 16, true, true, EMPTY);

        for (int key = 0; key < 13; key++) {
            int slot = table.findSlot(key);
            assertThat(table.isOccupied(slot)).isFalse();
            table.initialize(slot, key, key + 100);
            table.growIfNeeded(key + 1);
        }

        assertThat(table.capacity()).isEqualTo(32);
        int slot = table.findSlot(7);
        assertThat(table.isOccupied(slot)).isTrue();
        assertThat(table.key(slot)).isEqualTo(7);
        assertThat(table.head(slot)).isEqualTo(107);
        assertThat(table.count(slot)).isEqualTo(1);

        table.ensureDuplicateState();
        assertThat(table.hasDuplicateState()).isTrue();
        assertThat(table.append(slot, 200)).isEqualTo(107);
        assertThat(table.tail(slot)).isEqualTo(200);
        assertThat(table.count(slot)).isEqualTo(2);
        assertThat(table.incrementCount(slot)).isEqualTo(3);

        table.release();
        assertThat(table.isAllocated()).isFalse();
        assertThat(arrayPool.retainedBytes()).isGreaterThan(0);
    }
}
