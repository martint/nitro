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

class TestSparseLongRangeMembership
{
    @Test
    void rejectsOutsideWideBoundsWithoutAllocatingABitmap()
    {
        assertBoundsWithoutBitmap(-4_000_000_000L, 4_000_000_000L);
        assertBoundsWithoutBitmap(Long.MIN_VALUE, 0);
        assertBoundsWithoutBitmap(0, Long.MAX_VALUE);
        assertBoundsWithoutBitmap(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    private static void assertBoundsWithoutBitmap(long min, long max)
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        SparseLongRangeMembership membership = new SparseLongRangeMembership(HashJoinIndexPolicy.defaults(), arrayPool);
        LongJoinHashTable hashTable = new LongJoinHashTable(arrayPool, 4, false, true, -1);
        try {
            hashTable.initialize(~hashTable.findSlotForInsert(min), min, 0);
            hashTable.initialize(~hashTable.findSlotForInsert(max), max, 1);
            assertThat(membership.contains(Long.MIN_VALUE)).isTrue();
            assertThat(membership.contains(Long.MAX_VALUE)).isTrue();

            membership.build(hashTable, min, max, 2);

            assertThat(membership.contains(min)).isTrue();
            assertThat(membership.contains(max)).isTrue();
            // Interior holes remain candidates for exact lookup when no bitmap is admitted.
            assertThat(membership.contains(min + 1)).isTrue();
            assertThat(membership.contains(max - 1)).isTrue();
            if (min != Long.MIN_VALUE) {
                assertThat(membership.contains(min - 1)).isFalse();
            }
            if (max != Long.MAX_VALUE) {
                assertThat(membership.contains(max + 1)).isFalse();
            }
            assertThat(membership.retainedBytes()).isZero();
            assertThat(membership.dynamicFilter(3)).isNull();

            membership.release();
            assertThat(membership.contains(Long.MIN_VALUE)).isTrue();
            assertThat(membership.contains(Long.MAX_VALUE)).isTrue();
        }
        finally {
            membership.release();
            hashTable.release();
        }
    }

    @Test
    void buildsExactMembershipAndSharesItWithDynamicFilter()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        SparseLongRangeMembership membership =
                new SparseLongRangeMembership(HashJoinIndexPolicy.defaults(), arrayPool);
        LongJoinHashTable hashTable = new LongJoinHashTable(arrayPool, 4, false, true, -1);
        int first = hashTable.findSlot(10);
        hashTable.initialize(first, 10, 0);
        int second = hashTable.findSlot(17);
        hashTable.initialize(second, 17, 1);

        membership.build(hashTable, 10, 17, 2);

        assertThat(membership.contains(10)).isTrue();
        assertThat(membership.contains(11)).isFalse();
        assertThat(membership.contains(17)).isTrue();
        DynamicFilter filter = membership.dynamicFilter(3);
        assertThat(filter.column()).isEqualTo(3);
        assertThat(filter.size()).isEqualTo(2);
        assertThat(filter.accepts(10)).isTrue();
        assertThat(filter.accepts(11)).isFalse();
        assertThat(filter.accepts(17)).isTrue();

        membership.release();
        assertThat(arrayPool.retainedBytes()).isEqualTo(Long.BYTES);
        hashTable.release();
    }
}
