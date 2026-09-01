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

import static org.assertj.core.api.Assertions.assertThat;

class TestCountStateVector
{
    @Test
    void clearsCompactAndWideRanges()
    {
        CountStateVector state = new CountStateVector(8_200);
        state.increment(4_095, 7);
        state.increment(4_096, 300);
        state.increment(4_097, 11);
        state.increment(8_191, 300);
        state.increment(8_192, 5);

        state.clear(4_095, 4_097);

        assertThat(state.value(4_095)).isZero();
        assertThat(state.value(4_096)).isZero();
        assertThat(state.value(4_097)).isZero();
        assertThat(state.value(8_191)).isZero();
        assertThat(state.value(8_192)).isEqualTo(5);
    }
}
