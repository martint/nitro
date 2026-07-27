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
 * Allocator-owned tail and count metadata for every key in a direct long-key build.
 */
final class DenseDirectLongDuplicateState
{
    private final PrimitiveArrayPool arrayPool;
    private final int empty;

    private int[] tails;
    private int[] counts;

    DenseDirectLongDuplicateState(PrimitiveArrayPool arrayPool, int empty)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.empty = empty;
    }

    boolean isAllocated()
    {
        return tails != null;
    }

    void allocate(int capacity)
    {
        if (isAllocated()) {
            return;
        }
        tails = arrayPool.borrowInts(capacity);
        counts = arrayPool.borrowInts(capacity);
        // PrimitiveArrayPool returns recycled storage. These arrays are sparse maps whose zero/empty defaults are
        // semantic state, so stale values from an earlier join must not be observed by the first duplicate.
        Arrays.fill(tails, empty);
        Arrays.fill(counts, 0);
    }

    void initializeKey(int key, int ordinal)
    {
        if (!isAllocated()) {
            return;
        }
        tails[key] = ordinal;
        counts[key] = 1;
    }

    void increment(int key)
    {
        if (counts[key] == 0) {
            counts[key] = 1;
        }
        counts[key]++;
    }

    int append(int key, int head, int ordinal)
    {
        if (counts[key] == 0) {
            tails[key] = head;
            counts[key] = 1;
        }
        int previousTail = tails[key];
        tails[key] = ordinal;
        counts[key]++;
        return previousTail;
    }

    int tail(int key, int head)
    {
        return isAllocated() ? tails[key] : head;
    }

    int count(int key)
    {
        return !isAllocated() || counts[key] == 0 ? 1 : counts[key];
    }

    void resize(int oldLength, int newLength)
    {
        if (!isAllocated()) {
            return;
        }
        int[] previousTails = tails;
        int[] previousCounts = counts;
        tails = arrayPool.borrowInts(newLength);
        System.arraycopy(previousTails, 0, tails, 0, oldLength);
        Arrays.fill(tails, oldLength, newLength, empty);
        counts = arrayPool.borrowInts(newLength);
        System.arraycopy(previousCounts, 0, counts, 0, oldLength);
        Arrays.fill(counts, oldLength, newLength, 0);
        arrayPool.release(previousTails);
        arrayPool.release(previousCounts);
    }

    void release()
    {
        arrayPool.release(tails);
        tails = null;
        arrayPool.release(counts);
        counts = null;
    }
}
