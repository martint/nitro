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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
        assertThat(table.hasDuplicates()).isFalse();

        table.ensureDuplicateState();
        assertThat(table.hasDuplicateState()).isTrue();
        assertThat(table.append(slot, 200)).isEqualTo(107);
        assertThat(table.hasDuplicates()).isTrue();
        assertThat(table.tail(slot)).isEqualTo(200);
        assertThat(table.count(slot)).isEqualTo(2);
        assertThat(table.incrementCount(slot)).isEqualTo(3);

        table.release();
        assertThat(table.isAllocated()).isFalse();
        assertThat(table.hasDuplicates()).isTrue();
        assertThat(arrayPool.retainedBytes()).isGreaterThan(0);
    }

    @Test
    void verifiesKeysAcrossWordHalvesAndWrappedGroups()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        LongJoinHashTable table = new LongJoinHashTable(arrayPool, 64, true, true, EMPTY);
        try {
            long[] keys = new long[33];
            int count = 0;
            for (long key = 0; count < keys.length; key++) {
                // Select an initially empty group at the end of the array before inserting any key.
                if (~table.findSlotForInsert(key) == 48) {
                    keys[count++] = key;
                }
            }
            for (int index = 0; index < keys.length - 1; index++) {
                int slot = ~table.findSlotForInsert(keys[index]);
                assertThat(slot).isEqualTo((48 + index) & 63);
                table.initialize(slot, keys[index], index);
            }
            for (int index = 0; index < keys.length - 1; index++) {
                int slot = table.findSlotForInsert(keys[index]);
                assertThat(slot).isNotNegative();
                assertThat(table.head(slot)).isEqualTo(index);
            }
            assertThat(table.findSlotForInsert(keys[32])).isEqualTo(~16);
        }
        finally {
            table.release();
        }
    }

    @Test
    void matchesReferenceAcrossGrowthAndDirtyPoolReuse()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        Random random = new Random(8191);
        long[] keys = new long[4096];
        keys[0] = 0;
        keys[1] = Long.MIN_VALUE;
        keys[2] = Long.MAX_VALUE;
        for (int index = 3; index < keys.length; index++) {
            keys[index] = random.nextLong();
        }
        for (boolean grouped : new boolean[] {false, true}) {
            for (int reuse = 0; reuse < 2; reuse++) {
                LongJoinHashTable table = new LongJoinHashTable(arrayPool, 16, grouped, true, EMPTY);
                Map<Long, Integer> expected = new HashMap<>();
                try {
                    for (int index = 0; index < keys.length; index++) {
                        long key = keys[index];
                        int slot = table.findSlotForInsert(key);
                        assertThat(slot).isNegative();
                        table.initialize(~slot, key, index);
                        expected.put(key, index);
                        table.growIfNeeded(expected.size());
                        assertThat(table.head(table.findSlot(key))).isEqualTo(index);
                    }
                    for (long key : keys) {
                        int slot = table.findSlotForInsert(key);
                        assertThat(slot).isNotNegative();
                        assertThat(table.key(slot)).isEqualTo(key);
                        assertThat(table.head(slot)).isEqualTo(expected.get(key));
                    }
                    for (int index = 0; index < keys.length; index++) {
                        long key = random.nextLong();
                        int slot = table.findSlotForInsert(key);
                        Integer head = expected.get(key);
                        if (head == null) {
                            assertThat(slot).isNegative();
                            assertThat(table.isOccupied(~slot)).isFalse();
                        }
                        else {
                            assertThat(table.head(slot)).isEqualTo(head);
                        }
                    }
                }
                finally {
                    table.release();
                }
            }
        }
    }

    @Test
    void insertionLookupReportsWhetherSlotAlreadyExists()
    {
        for (boolean grouped : new boolean[] {false, true}) {
            PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
            LongJoinHashTable table = new LongJoinHashTable(arrayPool, 16, grouped, true, EMPTY);

            int emptySlot = table.findSlotForInsert(37);
            assertThat(emptySlot).isNegative();
            int slot = ~emptySlot;
            table.initialize(slot, 37, 100);

            assertThat(table.findSlotForInsert(37)).isEqualTo(slot);
            assertThat(table.findSlot(37)).isEqualTo(slot);
            assertThat(table.head(slot)).isEqualTo(100);
        }
    }
}
