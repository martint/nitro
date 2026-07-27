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

class TestChainLongList
{
    @Test
    void testLinkedAndRangeViews()
    {
        ChainLongList list = new ChainLongList().reset(
                new long[] {11, 22, 33},
                new int[] {2, -1, 1},
                0,
                3);

        assertThat(list.size()).isEqualTo(3);
        assertThat(list.getLong(0)).isEqualTo(11);
        assertThat(list.getLong(1)).isEqualTo(33);
        assertThat(list.getLong(2)).isEqualTo(22);
        list.resetRange(new long[] {5, 6, 7, 8}, 1, 2);
        assertThat(list.getLong(0)).isEqualTo(6);
        assertThat(list.getLong(1)).isEqualTo(7);
    }

    @Test
    void testCompactAndRepeatedViews()
    {
        int compact = (3 << Short.SIZE) | 7;
        ChainLongList list = new ChainLongList().resetCompact(
                new int[] {compact},
                new int[] {-1},
                0,
                1);

        assertThat(list.getLong(0)).isEqualTo(JoinRowReference.pack(3, 7));
        assertThat(list.resetCompactSingleBatch(new int[] {9}, new int[] {-1}, 0, 1, 4).getLong(0))
                .isEqualTo(JoinRowReference.pack(4, 9));
        list.resetRepeated(12, 3);
        assertThat(list.size()).isEqualTo(3);
        assertThat(list.getLong(2)).isEqualTo(12);
        assertThat(list.storageIndex(0)).isEqualTo(-1);
    }
}
