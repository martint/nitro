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
 * Allocator-owned packed lookup for duplicate long keys whose varying bits form a compact domain.
 */
final class CompressedLongRangeIndex
{
    private static final int START_MASK = 0x00FF_FFFF;
    private static final int MAX_COUNT = 0xFF;

    private final PrimitiveArrayPool arrayPool;
    private final boolean enabled;
    private final int minimumKeys;
    private final int maximumEntries;
    private final int maximumRatio;
    private final boolean debug;

    private int[] entries;
    private long variableMask;
    private long invariantBits;
    private long minimumOrdinal;

    CompressedLongRangeIndex(
            PrimitiveArrayPool arrayPool,
            boolean enabled,
            int minimumKeys,
            int maximumEntries,
            int maximumRatio,
            boolean debug)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.enabled = enabled;
        this.minimumKeys = minimumKeys;
        this.maximumEntries = maximumEntries;
        this.maximumRatio = maximumRatio;
        this.debug = debug;
    }

    boolean prepare(
            long[] keys,
            int distinctSize,
            int rowCount,
            int maximumMatchCount,
            long keyAnd,
            long keyOr)
    {
        if (!enabled ||
                keys == null ||
                distinctSize < minimumKeys ||
                rowCount > START_MASK ||
                maximumMatchCount > MAX_COUNT ||
                (long) distinctSize * 2 > keys.length) {
            return false;
        }
        long candidateVariableMask = keyAnd ^ keyOr;
        int variableBits = Long.bitCount(candidateVariableMask);
        if (variableBits >= Long.SIZE - 1) {
            return false;
        }
        long range = 1L << variableBits;
        if (range <= 0 ||
                range > maximumEntries ||
                range > (long) distinctSize * maximumRatio) {
            return false;
        }
        variableMask = candidateVariableMask;
        invariantBits = keyAnd & ~candidateVariableMask;
        return true;
    }

    long compress(long key)
    {
        return Long.compress(key, variableMask);
    }

    void build(
            long[] keys,
            int[] matchCounts,
            int[] slotsAndStarts,
            int distinctSize,
            int rowCount,
            long minimumOrdinal,
            long maximumOrdinal)
    {
        int range = (int) (maximumOrdinal - minimumOrdinal + 1);
        int[] direct = arrayPool.borrowInts(range);
        Arrays.fill(direct, 0);
        for (int index = 0; index < distinctSize; index++) {
            int slot = slotsAndStarts[index];
            int ordinal = (int) (compress(keys[slot]) - minimumOrdinal);
            direct[ordinal] = matchCounts[slot] << 24 | (slotsAndStarts[distinctSize + index] + 1);
        }
        entries = direct;
        this.minimumOrdinal = minimumOrdinal;
        if (debug) {
            System.err.printf(
                    "[compressed-direct-range] admitted rows=%d keys=%d variableBits=%d range=%d ratio=%.3f bytes=%d%n",
                    rowCount,
                    distinctSize,
                    Long.bitCount(variableMask),
                    range,
                    (double) range / distinctSize,
                    (long) range * Integer.BYTES);
        }
    }

    boolean isBuilt()
    {
        return entries != null;
    }

    int entry(long key)
    {
        if (((key ^ invariantBits) & ~variableMask) != 0) {
            return 0;
        }
        long ordinal = compress(key) - minimumOrdinal;
        return ordinal >= 0 && ordinal < entries.length ? entries[(int) ordinal] : 0;
    }

    static int start(int entry)
    {
        return entry == 0 ? 0 : (entry & START_MASK) - 1;
    }

    static int count(int entry)
    {
        return entry >>> 24;
    }

    void release()
    {
        arrayPool.release(entries);
        entries = null;
    }
}
