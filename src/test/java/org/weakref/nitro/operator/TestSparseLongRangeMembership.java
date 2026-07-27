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
