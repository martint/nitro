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

import static org.assertj.core.api.Assertions.assertThat;

class TestJoinRowReference
{
    @Test
    void testFullWidthRoundTrip()
    {
        long reference = JoinRowReference.pack(123, Integer.MAX_VALUE);

        assertThat(JoinRowReference.batchIndex(reference)).isEqualTo(123);
        assertThat(JoinRowReference.position(reference)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testCompactExpansion()
    {
        int compactReference = (123 << Short.SIZE) | 0xFFFF;
        long reference = JoinRowReference.unpackCompact(compactReference);

        assertThat(JoinRowReference.batchIndex(reference)).isEqualTo(123);
        assertThat(JoinRowReference.position(reference)).isEqualTo(0xFFFF);
    }
}
