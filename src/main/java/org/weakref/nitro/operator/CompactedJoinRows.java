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

import it.unimi.dsi.fastutil.longs.LongList;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.VectorAccess;

import static java.util.Objects.requireNonNull;

/**
 * Allocator-owned insertion-ordered row ranges for compacted duplicate join keys.
 */
final class CompactedJoinRows
{
    private final PrimitiveArrayPool arrayPool;
    private final int empty;

    private long[] orderedReferences;
    private int[] rangeStarts;

    CompactedJoinRows(PrimitiveArrayPool arrayPool, int empty)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.empty = empty;
    }

    void build(
            LongJoinHashTable hashTable,
            JoinRowStore rows,
            CompressedLongRangeIndex compressedRanges,
            int distinctSize,
            int rowCount,
            int maximumMatchCount,
            long keyAnd,
            long keyOr)
    {
        boolean compressedCandidate = compressedRanges.prepare(
                hashTable,
                distinctSize,
                rowCount,
                maximumMatchCount,
                keyAnd,
                keyOr);
        long[] ordered = arrayPool.borrowLongs(rowCount);
        int[] starts = arrayPool.borrowInts(hashTable.capacity());
        int cursor = 0;
        int group = 0;
        long compressedMin = Long.MAX_VALUE;
        long compressedMax = Long.MIN_VALUE;
        for (int slot = 0; slot < hashTable.capacity(); slot++) {
            int ordinal = hashTable.head(slot);
            if (ordinal == empty) {
                continue;
            }
            if (!compressedCandidate) {
                starts[slot] = cursor;
            }
            else {
                starts[group] = slot;
                starts[distinctSize + group] = cursor;
                long compressed = compressedRanges.compress(hashTable.key(slot));
                compressedMin = Math.min(compressedMin, compressed);
                compressedMax = Math.max(compressedMax, compressed);
                group++;
            }
            while (ordinal != empty) {
                ordered[cursor++] = rows.referenceAt(ordinal);
                ordinal = rows.next(ordinal);
            }
        }
        if (compressedCandidate) {
            compressedRanges.build(hashTable, starts, distinctSize, rowCount, compressedMin, compressedMax);
            arrayPool.release(starts);
            starts = null;
        }
        orderedReferences = ordered;
        rangeStarts = starts;
    }

    boolean isBuilt()
    {
        return orderedReferences != null;
    }

    int start(int slot)
    {
        return rangeStarts[slot];
    }

    LongList rows(int start, int count, SingleLongList single, ChainLongList chain)
    {
        return count == 1
                ? single.withValue(orderedReferences[start])
                : chain.resetRange(orderedReferences, start, count);
    }

    void copy(int start, long[] output, int outputOffset, int length)
    {
        System.arraycopy(orderedReferences, start, output, outputOffset, length);
    }

    int[] buildIntPayload(
            VectorAccess.LongValues values,
            long[] directValues,
            int[] sourcePositions,
            int rowCount)
    {
        if (!isBuilt()) {
            return null;
        }
        int[] payload = arrayPool.borrowInts(rowCount);
        for (int index = 0; index < rowCount; index++) {
            int logicalPosition = JoinRowReference.position(orderedReferences[index]);
            int sourcePosition = sourcePositions == null ? logicalPosition : sourcePositions[logicalPosition];
            long value = directValues == null ? values.value(sourcePosition) : directValues[sourcePosition];
            if (value != (int) value) {
                arrayPool.release(payload);
                return null;
            }
            payload[index] = (int) value;
        }
        return payload;
    }

    void release()
    {
        arrayPool.release(orderedReferences);
        orderedReferences = null;
        arrayPool.release(rangeStarts);
        rangeStarts = null;
    }
}
