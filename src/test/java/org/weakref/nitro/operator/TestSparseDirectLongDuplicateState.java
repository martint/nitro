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

class TestSparseDirectLongDuplicateState
{
    @Test
    void ownsAdmissionGroupsGrowthAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        SparseDirectLongDuplicateState state =
                new SparseDirectLongDuplicateState(arrayPool, true, 4, 2, 1, -1);

        assertThat(state.admit(false, 7, 4)).isFalse();
        assertThat(state.admit(false, 8, 4)).isTrue();
        assertThat(state.admit(true, 8, 4)).isFalse();

        int first = state.groupEntry(11);
        assertThat(state.isGroupEntry(first)).isTrue();
        assertThat(state.head(first)).isEqualTo(11);
        state.increment(first);
        assertThat(state.count(first)).isEqualTo(2);
        assertThat(state.append(first, 17)).isEqualTo(11);
        assertThat(state.tail(first)).isEqualTo(17);
        assertThat(state.count(first)).isEqualTo(3);

        int second = state.groupEntry(23);
        assertThat(state.head(second)).isEqualTo(23);
        assertThat(state.groupCount()).isEqualTo(2);

        state.release();
        assertThat(state.groupCount()).isZero();
        assertThat(arrayPool.retainedBytes()).isGreaterThanOrEqualTo(3L * Integer.BYTES);
    }

    @Test
    void rejectsDisabledAdmissionAndLeavesOrdinaryEntriesUnchanged()
    {
        SparseDirectLongDuplicateState state = new SparseDirectLongDuplicateState(
                new PrimitiveArrayPool(1024, 0),
                false,
                1,
                1,
                1,
                -1);

        assertThat(state.admit(false, 1, 1)).isFalse();
        assertThat(state.isGroupEntry(3)).isFalse();
    }
}
