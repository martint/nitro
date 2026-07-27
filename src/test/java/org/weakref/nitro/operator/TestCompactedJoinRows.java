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

class TestCompactedJoinRows
{
    private static final int EMPTY = -1;

    @Test
    void ownsInsertionOrderedRangesAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        LongJoinHashTable hashTable = new LongJoinHashTable(arrayPool, 16, false, false, EMPTY);
        JoinRowStore rows = new JoinRowStore(arrayPool, 4, false, false, true, true, EMPTY);
        long first = JoinRowReference.pack(2, 7);
        long second = JoinRowReference.pack(2, 9);
        long third = JoinRowReference.pack(3, 11);
        rows.append(0, first);
        rows.append(1, second);
        rows.append(2, third);
        rows.link(0, 1);

        int firstSlot = hashTable.findSlot(10);
        hashTable.initialize(firstSlot, 10, 0, 1, 2);
        int secondSlot = hashTable.findSlot(20);
        hashTable.initialize(secondSlot, 20, 2, 2, 1);
        CompressedLongRangeIndex compressedRanges = new CompressedLongRangeIndex(arrayPool, false, 1, 16, 2, false);
        CompactedJoinRows compactedRows = new CompactedJoinRows(arrayPool, EMPTY);

        compactedRows.build(hashTable, rows, compressedRanges, 2, 3);

        assertThat(compactedRows.isBuilt()).isTrue();
        int firstStart = compactedRows.start(firstSlot);
        int secondStart = compactedRows.start(secondSlot);
        assertThat(firstStart).isIn(0, 1);
        assertThat(secondStart).isIn(0, 2);
        assertThat(firstStart).isNotEqualTo(secondStart);
        ChainLongList range = (ChainLongList) compactedRows.rows(firstStart, 2, new SingleLongList(), new ChainLongList());
        assertThat(range.getLong(0)).isEqualTo(first);
        assertThat(range.getLong(1)).isEqualTo(second);
        long[] copy = new long[2];
        compactedRows.copy(firstStart, copy, 0, 2);
        assertThat(copy).containsExactly(first, second);
        assertThat(compactedRows.rows(secondStart, 1, new SingleLongList(), new ChainLongList()).getLong(0)).isEqualTo(third);
        long[] values = new long[12];
        values[7] = 70;
        values[9] = 90;
        values[11] = 110;
        int[] payload = compactedRows.buildIntPayload(null, values, null, 3);
        assertThat(payload[firstStart]).isEqualTo(70);
        assertThat(payload[firstStart + 1]).isEqualTo(90);
        assertThat(payload[secondStart]).isEqualTo(110);
        arrayPool.release(payload);

        compactedRows.release();
        rows.release();
        hashTable.release();
        compressedRanges.release();
        assertThat(compactedRows.isBuilt()).isFalse();
        assertThat(arrayPool.retainedBytes()).isGreaterThan(0);
    }
}
