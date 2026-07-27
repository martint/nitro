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

import static java.util.Objects.requireNonNull;

/**
 * Allocator-owned duplicate groups for a sparse direct long-key build.
 */
final class SparseDirectLongDuplicateState
{
    private final PrimitiveArrayPool arrayPool;
    private final boolean enabled;
    private final int minimumExpectedRows;
    private final int minimumExpectedDomainRatio;
    private final int initialGroupCapacity;
    private final int empty;

    private int[] heads;
    private int[] tails;
    private int[] counts;
    private int groupCount;
    private boolean admitted;

    SparseDirectLongDuplicateState(
            PrimitiveArrayPool arrayPool,
            boolean enabled,
            int minimumExpectedRows,
            int minimumExpectedDomainRatio,
            int initialGroupCapacity,
            int empty)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.enabled = enabled;
        this.minimumExpectedRows = minimumExpectedRows;
        this.minimumExpectedDomainRatio = minimumExpectedDomainRatio;
        this.initialGroupCapacity = initialGroupCapacity;
        this.empty = empty;
    }

    boolean admit(boolean denseDuplicateStateAllocated, int directCapacity, int expectedRows)
    {
        if (denseDuplicateStateAllocated) {
            return false;
        }
        if (!admitted) {
            admitted = enabled &&
                    expectedRows >= minimumExpectedRows &&
                    (long) directCapacity >= (long) expectedRows * minimumExpectedDomainRatio;
        }
        return admitted;
    }

    boolean isGroupEntry(int entry)
    {
        return enabled && entry < empty;
    }

    int groupEntry(int entry)
    {
        if (isGroupEntry(entry)) {
            return entry;
        }
        ensureCapacity(groupCount + 1);
        int group = groupCount++;
        heads[group] = entry;
        tails[group] = entry;
        counts[group] = 1;
        return encode(group);
    }

    void increment(int entry)
    {
        counts[decode(entry)]++;
    }

    int append(int entry, int ordinal)
    {
        int group = decode(entry);
        int previousTail = tails[group];
        tails[group] = ordinal;
        counts[group]++;
        return previousTail;
    }

    int head(int entry)
    {
        return heads[decode(entry)];
    }

    int tail(int entry)
    {
        return tails[decode(entry)];
    }

    int count(int entry)
    {
        return counts[decode(entry)];
    }

    int groupCount()
    {
        return groupCount;
    }

    void release()
    {
        arrayPool.release(heads);
        heads = null;
        arrayPool.release(tails);
        tails = null;
        arrayPool.release(counts);
        counts = null;
        groupCount = 0;
        admitted = false;
    }

    private void ensureCapacity(int required)
    {
        if (heads != null && required <= heads.length) {
            return;
        }
        int oldLength = heads == null ? 0 : heads.length;
        int newLength = Math.max(initialGroupCapacity, oldLength * 2);
        while (newLength < required) {
            newLength *= 2;
        }
        int[] previousHeads = heads;
        int[] previousTails = tails;
        int[] previousCounts = counts;
        heads = arrayPool.borrowInts(newLength);
        tails = arrayPool.borrowInts(newLength);
        counts = arrayPool.borrowInts(newLength);
        if (oldLength > 0) {
            System.arraycopy(previousHeads, 0, heads, 0, groupCount);
            System.arraycopy(previousTails, 0, tails, 0, groupCount);
            System.arraycopy(previousCounts, 0, counts, 0, groupCount);
        }
        arrayPool.release(previousHeads);
        arrayPool.release(previousTails);
        arrayPool.release(previousCounts);
    }

    private static int encode(int group)
    {
        return -group - 2;
    }

    private static int decode(int entry)
    {
        return -entry - 2;
    }
}
