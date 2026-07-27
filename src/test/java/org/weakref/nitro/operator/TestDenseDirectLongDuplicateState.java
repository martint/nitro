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

class TestDenseDirectLongDuplicateState
{
    @Test
    void ownsDenseMetadataInitializationGrowthAndRelease()
    {
        PrimitiveArrayPool arrayPool = new PrimitiveArrayPool(1024, 0);
        DenseDirectLongDuplicateState state = new DenseDirectLongDuplicateState(arrayPool, -1);

        assertThat(state.tail(1, 7)).isEqualTo(7);
        assertThat(state.count(1)).isEqualTo(1);

        state.allocate(2);
        state.initializeKey(0, 5);
        state.increment(0);
        assertThat(state.tail(0, 5)).isEqualTo(5);
        assertThat(state.count(0)).isEqualTo(2);
        assertThat(state.append(0, 5, 9)).isEqualTo(5);
        assertThat(state.tail(0, 5)).isEqualTo(9);
        assertThat(state.count(0)).isEqualTo(3);

        state.resize(2, 4);
        assertThat(state.tail(0, 5)).isEqualTo(9);
        assertThat(state.count(0)).isEqualTo(3);
        assertThat(state.tail(3, 11)).isEqualTo(-1);
        assertThat(state.count(3)).isEqualTo(1);

        state.release();
        assertThat(state.isAllocated()).isFalse();
        assertThat(arrayPool.retainedBytes()).isGreaterThanOrEqualTo(2L * Integer.BYTES);
    }
}
