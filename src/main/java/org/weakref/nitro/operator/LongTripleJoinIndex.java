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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

final class LongTripleJoinIndex
        extends JoinIndex
{
    private static final long NO_MATCH_ROW_REFERENCE = -1L;

    private static final float LOAD_FACTOR = 0.75f;
    // Swiss/F14-style SIMD-tag-bucket table (cf. LongPairJoinIndex): a probe scans a GROUP of 1-byte tags with
    // one vector load and touches the fat entry array only on a tag hit, so a collision step is a dense byte
    // read rather than a {first,second,third,row} cache-miss load.
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
    private static final int GROUP = SPECIES.length();
    private final HashJoinExecutionPolicy executionPolicy;
    private final PrimitiveArrayPool arrayPool;

    private byte[] tags;
    // Co-located entry table: for slot s, entries[4*s] = firstKey, entries[4*s+1] = secondKey, entries[4*s+2] =
    // thirdKey, entries[4*s+3] = rowReference. A matching probe's key verify and row read then hit one contiguous
    // 32-byte region instead of four separate long[] (firstKeys/secondKeys/thirdKeys/singleRows), which cost four
    // random line fetches per matched probe. Tags stay in their own byte[] (scanned a GROUP at a time).
    private long[] entries;
    private LongArrayList[] rowsBySlot;
    private int mask;
    private int maxFill;
    private int size;
    private boolean tripleHasDuplicates;
    private final SingleLongList singleMatch = new SingleLongList();

    LongTripleJoinIndex(HashJoinExecutionPolicy executionPolicy, PrimitiveArrayPool arrayPool, int expectedSize)
    {
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
        this.arrayPool = arrayPool;
        int capacity = GROUP;
        while (capacity < expectedSize / LOAD_FACTOR) {
            capacity <<= 1;
        }
        allocate(capacity);
    }

    private void allocate(int capacity)
    {
        tags = arrayPool.borrowBytes(capacity);
        Arrays.fill(tags, (byte) 0);
        entries = arrayPool.borrowLongs(capacity * 4);
        rowsBySlot = arrayPool.borrow(LongArrayList[].class, capacity, LongArrayList[].class);
        if (rowsBySlot == null) {
            rowsBySlot = new LongArrayList[capacity];
        }
        else {
            Arrays.fill(rowsBySlot, null);
        }
        mask = capacity - 1;
        maxFill = (int) (capacity * LOAD_FACTOR);
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    @Override
    public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
    {
        if (JoinIndex.hasNull(nulls, position)) {
            return;
        }
        addNoNulls(values, position, rowReference);
    }

    @Override
    public void addNoNulls(Vector[] values, int position, long rowReference)
    {
        addRow(
                OperatorVectorSupport.longValue(values[0], position),
                OperatorVectorSupport.longValue(values[1], position),
                OperatorVectorSupport.longValue(values[2], position),
                rowReference);
    }

    @Override
    public LongList matches(Vector[] values, Vector[] nulls, int position)
    {
        if (JoinIndex.hasNull(nulls, position)) {
            return LongLists.emptyList();
        }
        return matchesNoNulls(values, position);
    }

    @Override
    public LongList matchesNoNulls(Vector[] values, int position)
    {
        long first = OperatorVectorSupport.longValue(values[0], position);
        long second = OperatorVectorSupport.longValue(values[1], position);
        long third = OperatorVectorSupport.longValue(values[2], position);
        int slot = probe(first, second, third, hash64(first, second, third));
        if (slot < 0) {
            return LongLists.emptyList();
        }
        LongArrayList rows = rowsBySlot[slot];
        if (rows != null) {
            return rows;
        }
        return singleMatch.withValue(entries[slot * 4 + 3]);
    }

    @Override
    public void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
    {
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
        if (!hasNulls) {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                long first = firstValues.value(position);
                long second = secondValues.value(position);
                long third = thirdValues.value(position);
                int slot = probe(first, second, third, hash64(first, second, third));
                if (slot < 0) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                LongArrayList rows = rowsBySlot[slot];
                if (rows != null) {
                    matches[index] = rows;
                }
                else {
                    matches[index] = singleMatches[index].withValue(entries[slot * 4 + 3]);
                }
            }
            return;
        }
        VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
        VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
        VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)) {
                matches[index] = LongLists.emptyList();
                continue;
            }
            long first = firstValues.value(position);
            long second = secondValues.value(position);
            long third = thirdValues.value(position);
            int slot = probe(first, second, third, hash64(first, second, third));
            if (slot < 0) {
                matches[index] = LongLists.emptyList();
                continue;
            }
            LongArrayList rows = rowsBySlot[slot];
            if (rows != null) {
                matches[index] = rows;
            }
            else {
                matches[index] = singleMatches[index].withValue(entries[slot * 4 + 3]);
            }
        }
    }

    @Override
    public boolean supportsSingleMatchRefs()
    {
        return !tripleHasDuplicates;
    }

    @Override
    public void matchSingleRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, long[] refs)
    {
        VectorAccess.LongValues firstValues = VectorAccess.longValues(valuesArray[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(valuesArray[1]);
        VectorAccess.LongValues thirdValues = VectorAccess.longValues(valuesArray[2]);
        if (!hasNulls) {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = singleRef(firstValues.value(position), secondValues.value(position), thirdValues.value(position));
            }
            return;
        }
        VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nullsArray[0]);
        VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nullsArray[1]);
        VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nullsArray[2]);
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            refs[index] = firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)
                    ? NO_MATCH_ROW_REFERENCE
                    : singleRef(firstValues.value(position), secondValues.value(position), thirdValues.value(position));
        }
    }

    private long singleRef(long first, long second, long third)
    {
        int slot = probe(first, second, third, hash64(first, second, third));
        return slot < 0 ? NO_MATCH_ROW_REFERENCE : entries[slot * 4 + 3];
    }

    // Returns the slot holding (first, second, third), or -1 if absent. Scans GROUP tags per step: one vector
    // load plus one tag compare filters the whole bucket; a key is only read on a tag match. A bucket with any
    // empty slot ends the search (open-addressing invariant; no deletions).
    private int probe(long first, long second, long third, long hash)
    {
        byte[] tagTable = tags;
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, tagTable, group);
            long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                int base = slot * 4;
                if (entries[base] == first && entries[base + 1] == second && entries[base + 2] == third) {
                    return slot;
                }
                matchBits &= matchBits - 1;
            }
            if (groupTags.compare(VectorOperators.EQ, (byte) 0).toLong() != 0) {
                return -1;
            }
            group = (group + GROUP) & mask;
        }
    }

    private void rehash()
    {
        byte[] oldTags = tags;
        long[] oldEntries = entries;
        LongArrayList[] oldLists = rowsBySlot;
        allocate(oldTags.length * 2);
        size = 0;
        for (int oldSlot = 0; oldSlot < oldTags.length; oldSlot++) {
            if (oldTags[oldSlot] == 0) {
                continue;
            }
            int oldBase = oldSlot * 4;
            long first = oldEntries[oldBase];
            long second = oldEntries[oldBase + 1];
            long third = oldEntries[oldBase + 2];
            long hash = hash64(first, second, third);
            int slot = findEmpty(hash);
            int base = slot * 4;
            tags[slot] = (byte) ((hash >>> 56) | 0x80L);
            entries[base] = first;
            entries[base + 1] = second;
            entries[base + 2] = third;
            entries[base + 3] = oldEntries[oldBase + 3];
            rowsBySlot[slot] = oldLists[oldSlot];
            size++;
        }
        arrayPool.release(oldTags);
        arrayPool.release(oldEntries);
        releaseRowsBySlot(oldLists);
    }

    @Override
    public void releaseBuffers()
    {
        arrayPool.release(tags);
        tags = null;
        arrayPool.release(entries);
        entries = null;
        releaseRowsBySlot(rowsBySlot);
        rowsBySlot = null;
    }

    private void releaseRowsBySlot(LongArrayList[] rows)
    {
        if (rows == null) {
            return;
        }
        Arrays.fill(rows, null);
        arrayPool.retain(LongArrayList[].class, rows.length, (long) rows.length * Long.BYTES, rows);
    }

    // Distinct keys only (rehash): the first empty slot in the key's probe sequence.
    private int findEmpty(long hash)
    {
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            long emptyBits = ByteVector.fromArray(SPECIES, tags, group).compare(VectorOperators.EQ, (byte) 0).toLong();
            if (emptyBits != 0) {
                return group + Long.numberOfTrailingZeros(emptyBits);
            }
            group = (group + GROUP) & mask;
        }
    }

    private void addRow(long first, long second, long third, long rowReference)
    {
        long hash = hash64(first, second, third);
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, tags, group);
            long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                int base = slot * 4;
                if (entries[base] == first && entries[base + 1] == second && entries[base + 2] == third) {
                    tripleHasDuplicates = true;
                    if (rowsBySlot[slot] == null) {
                        LongArrayList rows = new LongArrayList(executionPolicy.duplicateListInitialCapacity());
                        rows.add(entries[base + 3]);
                        rows.add(rowReference);
                        rowsBySlot[slot] = rows;
                    }
                    else {
                        rowsBySlot[slot].add(rowReference);
                    }
                    return;
                }
                matchBits &= matchBits - 1;
            }
            long emptyBits = groupTags.compare(VectorOperators.EQ, (byte) 0).toLong();
            if (emptyBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(emptyBits);
                int base = slot * 4;
                tags[slot] = tag;
                entries[base] = first;
                entries[base + 1] = second;
                entries[base + 2] = third;
                entries[base + 3] = rowReference;
                size++;
                if (size >= maxFill) {
                    rehash();
                }
                return;
            }
            group = (group + GROUP) & mask;
        }
    }

    private static long hash64(long first, long second, long third)
    {
        long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L + third * 0x94D049BB133111EBL;
        hash ^= hash >>> 33;
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= hash >>> 33;
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;
        return hash;
    }
}
