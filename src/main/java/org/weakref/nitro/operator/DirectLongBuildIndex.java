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
 * Allocator-owned direct-key build map and its dense or sparse duplicate metadata.
 */
final class DirectLongBuildIndex
{
    private final PrimitiveArrayPool arrayPool;
    private final int empty;
    private final DenseDirectLongDuplicateState denseDuplicates;
    private final SparseDirectLongDuplicateState sparseDuplicates;

    private int[] heads;
    private int rowCount;

    DirectLongBuildIndex(
            PrimitiveArrayPool arrayPool,
            boolean sparseDuplicateState,
            int sparseDuplicateMinimumExpectedRows,
            int sparseDuplicateMinimumExpectedDomainRatio,
            int duplicateGroupInitialCapacity,
            int empty)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.empty = empty;
        denseDuplicates = new DenseDirectLongDuplicateState(arrayPool, empty);
        sparseDuplicates = new SparseDirectLongDuplicateState(
                arrayPool,
                sparseDuplicateState,
                sparseDuplicateMinimumExpectedRows,
                sparseDuplicateMinimumExpectedDomainRatio,
                duplicateGroupInitialCapacity,
                empty);
    }

    void initialize(int capacity)
    {
        heads = arrayPool.borrowInts(capacity);
        Arrays.fill(heads, empty);
    }

    boolean isActive()
    {
        return heads != null;
    }

    int capacity()
    {
        return heads.length;
    }

    void recordRow()
    {
        rowCount++;
    }

    int rowCount()
    {
        return rowCount;
    }

    int entry(int key)
    {
        return heads[key];
    }

    void initializeKey(int key, int ordinal)
    {
        heads[key] = ordinal;
        denseDuplicates.initializeKey(key, ordinal);
    }

    boolean denseDuplicatesAllocated()
    {
        return denseDuplicates.isAllocated();
    }

    void incrementDenseDuplicate(int key)
    {
        denseDuplicates.allocate(heads.length);
        denseDuplicates.increment(key);
    }

    int appendDenseDuplicate(int key, int entry, int ordinal)
    {
        denseDuplicates.allocate(heads.length);
        return denseDuplicates.append(key, entry, ordinal);
    }

    boolean admitSparseDuplicates(int expectedRows)
    {
        return sparseDuplicates.admit(denseDuplicates.isAllocated(), heads.length, expectedRows);
    }

    int promoteSparseDuplicate(int key, int entry)
    {
        int groupEntry = sparseDuplicates.groupEntry(entry);
        heads[key] = groupEntry;
        return groupEntry;
    }

    void incrementSparseDuplicate(int entry)
    {
        sparseDuplicates.increment(entry);
    }

    int appendSparseDuplicate(int entry, int ordinal)
    {
        return sparseDuplicates.append(entry, ordinal);
    }

    int entryHead(int entry)
    {
        return sparseDuplicates.isGroupEntry(entry) ? sparseDuplicates.head(entry) : entry;
    }

    int entryTail(int key, int entry)
    {
        return sparseDuplicates.isGroupEntry(entry)
                ? sparseDuplicates.tail(entry)
                : denseDuplicates.tail(key, entryHead(entry));
    }

    int entryCount(int key, int entry)
    {
        return sparseDuplicates.isGroupEntry(entry)
                ? sparseDuplicates.count(entry)
                : denseDuplicates.count(key);
    }

    int sparseDuplicateGroupCount()
    {
        return sparseDuplicates.groupCount();
    }

    void ensureCapacity(int required)
    {
        if (required <= heads.length) {
            return;
        }
        int oldLength = heads.length;
        int newLength = oldLength;
        while (newLength < required) {
            newLength *= 2;
        }
        int[] previousHeads = heads;
        heads = arrayPool.borrowInts(newLength);
        System.arraycopy(previousHeads, 0, heads, 0, oldLength);
        Arrays.fill(heads, oldLength, newLength, empty);
        denseDuplicates.resize(oldLength, newLength);
        arrayPool.release(previousHeads);
    }

    void release()
    {
        arrayPool.release(heads);
        heads = null;
        rowCount = 0;
        denseDuplicates.release();
        sparseDuplicates.release();
    }
}
