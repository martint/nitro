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
package org.weakref.nitro.jit;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHashProbe
{
    @Test
    void pipelinedMatchesScalar()
    {
        int rows = 9001;
        int capacity = 16;
        while (capacity * 0.75f < rows) {
            capacity <<= 1;
        }
        int mask = capacity - 1;
        long[] jKey = new long[capacity];
        int[] jRow = new int[capacity];
        java.util.Arrays.fill(jRow, -1);
        for (int r = 0; r < rows; r++) {
            int slot = mix(r) & mask;
            while (jRow[slot] != -1) {
                slot = (slot + 1) & mask;
            }
            jKey[slot] = r;
            jRow[slot] = r;
        }

        // A count that is not a multiple of the group size, and a mix of hits and misses.
        int count = 4099;
        Random random = new Random(7);
        long[] keys = new long[count];
        for (int i = 0; i < count; i++) {
            keys[i] = random.nextDouble() < 0.9 ? random.nextInt(rows) : (long) rows + random.nextInt(rows);
        }

        int[] expected = new int[count];
        BenchmarkHashProbe.scalarProbe(jKey, jRow, mask, keys, count, expected);

        int[] actual = new int[count];
        BenchmarkHashProbe.pipelinedProbe(jKey, jRow, mask, keys, count, actual);

        assertThat(actual).containsExactly(expected);
    }

    private static int mix(long key)
    {
        long h = key;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return (int) h;
    }
}
