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

class TestCompressedLongRangeIndex
{
    @Test
    void ownsAdmissionPackedLookupAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        CompressedLongRangeIndex index = new CompressedLongRangeIndex(arrayPool, true, 2, 4, 2, false);
        LongJoinHashTable hashTable = hashTable(arrayPool);

        assertThat(index.prepare(hashTable, 2, 5, 3)).isTrue();
        index.build(
                hashTable,
                new int[] {hashTable.findSlot(0x10), hashTable.findSlot(0x12), 0, 2},
                2,
                5,
                0,
                1);

        int first = index.entry(0x10);
        assertThat(CompressedLongRangeIndex.start(first)).isEqualTo(0);
        assertThat(CompressedLongRangeIndex.count(first)).isEqualTo(2);
        int second = index.entry(0x12);
        assertThat(CompressedLongRangeIndex.start(second)).isEqualTo(2);
        assertThat(CompressedLongRangeIndex.count(second)).isEqualTo(3);
        assertThat(index.entry(0x11)).isZero();
        assertThat(index.entry(0x00)).isZero();

        index.release();
        assertThat(arrayPool.retainedBytes()).isEqualTo(2L * Integer.BYTES);
        hashTable.release();
    }

    @Test
    void rejectsBuildShapesOutsideItsImmutablePolicy()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        LongJoinHashTable hashTable = hashTable(arrayPool);

        assertThat(new CompressedLongRangeIndex(arrayPool, false, 2, 4, 2, false)
                .prepare(hashTable, 2, 5, 3)).isFalse();
        assertThat(new CompressedLongRangeIndex(arrayPool, true, 3, 4, 2, false)
                .prepare(hashTable, 2, 5, 3)).isFalse();
        assertThat(new CompressedLongRangeIndex(arrayPool, true, 2, 1, 2, false)
                .prepare(hashTable, 2, 5, 3)).isFalse();
        assertThat(new CompressedLongRangeIndex(arrayPool, true, 2, 4, 2, false)
                .prepare(hashTable, 2, 5, 256)).isFalse();
        hashTable.release();
    }

    private static LongJoinHashTable hashTable(PrimitiveArrayPool arrayPool)
    {
        LongJoinHashTable hashTable = new LongJoinHashTable(arrayPool, 4, false, false, -1);
        int first = hashTable.findSlot(0x10);
        hashTable.initialize(first, 0x10, 0, 0, 2);
        int second = hashTable.findSlot(0x12);
        hashTable.initialize(second, 0x12, 1, 1, 3);
        return hashTable;
    }
}
