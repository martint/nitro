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

import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Completed-build bounds with optional exact, allocator-owned membership for a sparse long-key range.
 */
final class SparseLongRangeMembership
{
    private final HashJoinIndexPolicy policy;
    private final PrimitiveArrayPool arrayPool;

    private long[] words;
    private long min = Long.MIN_VALUE;
    private long max = Long.MAX_VALUE;
    private int distinctSize;
    private int range;

    SparseLongRangeMembership(HashJoinIndexPolicy policy, PrimitiveArrayPool arrayPool)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
    }

    void build(LongJoinHashTable hashTable, long min, long max, int distinctSize)
    {
        if (!hashTable.isAllocated() || words != null) {
            return;
        }
        // A domain too wide for the optional bitmap still proves every key outside these endpoints absent.
        // Compare endpoints directly when probing: subtracting them can overflow for full-width keys.
        this.min = min;
        this.max = max;
        if (!policy.sparseLongRangeMembership()) {
            return;
        }
        long range = max - min + 1;
        if (range <= 0 ||
                range > policy.maxArrayRange() ||
                range < (long) distinctSize * policy.sparseLongRangeMinRatio()) {
            return;
        }
        this.distinctSize = distinctSize;
        this.range = (int) range;
        words = arrayPool.borrowLongs((this.range + Long.SIZE - 1) / Long.SIZE);
        Arrays.fill(words, 0L);
        for (int slot = 0; slot < hashTable.capacity(); slot++) {
            if (!hashTable.isOccupied(slot)) {
                continue;
            }
            int ordinal = (int) (hashTable.key(slot) - min);
            words[ordinal >>> 6] |= 1L << ordinal;
        }
    }

    boolean contains(long key)
    {
        if (key < min || key > max) {
            return false;
        }
        if (words == null) {
            return true;
        }
        long ordinal = key - min;
        return ordinal >= 0 && ordinal < range &&
                (words[(int) ordinal >>> 6] & (1L << (int) ordinal)) != 0;
    }

    DynamicFilter dynamicFilter(int probeColumn)
    {
        if (words == null) {
            return null;
        }
        return DynamicFilter.fromExactBitset(probeColumn, min, max, words, distinctSize);
    }

    long retainedBytes()
    {
        return words == null ? 0 : (long) words.length * Long.BYTES;
    }

    void release()
    {
        arrayPool.release(words);
        words = null;
        min = Long.MIN_VALUE;
        max = Long.MAX_VALUE;
    }
}
