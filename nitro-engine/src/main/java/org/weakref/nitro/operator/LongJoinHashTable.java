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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.Arrays;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

/**
 * Allocator-owned open-addressed slot storage for a long-key join index.
 */
final class LongJoinHashTable
{
    private static final float LOAD_FACTOR = 0.75f;
    private static final VectorSpecies<Byte> HASH_TAG_SPECIES = ByteVector.SPECIES_128;
    private static final int HASH_TAG_GROUP = HASH_TAG_SPECIES.length();

    private final PrimitiveArrayPool arrayPool;
    private final int initialCapacity;
    private final boolean grouped;
    private final boolean lazyDuplicateState;
    private final int empty;

    private long[] keys;
    private byte[] tags;
    private int[] heads;
    private int[] tails;
    private int[] counts;
    private int mask;
    private int maximumFill;
    private boolean duplicates;

    LongJoinHashTable(
            PrimitiveArrayPool arrayPool,
            int initialCapacity,
            boolean grouped,
            boolean lazyDuplicateState,
            int empty)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.initialCapacity = initialCapacity;
        this.grouped = grouped;
        this.lazyDuplicateState = lazyDuplicateState;
        this.empty = empty;
    }

    boolean isAllocated()
    {
        return keys != null;
    }

    boolean supportsBatchedProbes()
    {
        return grouped && keys != null;
    }

    int capacity()
    {
        return keys.length;
    }

    boolean isOccupied(int slot)
    {
        return heads[slot] != empty;
    }

    long key(int slot)
    {
        return keys[slot];
    }

    int head(int slot)
    {
        return heads[slot];
    }

    int tail(int slot)
    {
        return tails[slot];
    }

    int count(int slot)
    {
        return counts == null ? 1 : counts[slot];
    }

    boolean hasDuplicateState()
    {
        return tails != null;
    }

    boolean hasDuplicates()
    {
        return duplicates;
    }

    long retainedBytes()
    {
        long bytes = keys == null ? 0 : (long) keys.length * Long.BYTES;
        bytes += tags == null ? 0 : tags.length;
        bytes += heads == null ? 0 : (long) heads.length * Integer.BYTES;
        bytes += tails == null ? 0 : (long) tails.length * Integer.BYTES;
        bytes += counts == null ? 0 : (long) counts.length * Integer.BYTES;
        return bytes;
    }

    int findSlot(long key)
    {
        int slot = findSlotForInsert(key);
        return slot < 0 ? ~slot : slot;
    }

    /// Looks up a completed, immutable table. Outputs occupied slots or -1 for absent keys.
    /// Scratch is supplied by the probe owner and must not alias the input keys.
    void findSlots(long[] probeKeys, int count, int[] slots, long[] candidateKeys)
    {
        checkFromIndexSize(0, count, probeKeys.length);
        checkFromIndexSize(0, count, slots.length);
        checkFromIndexSize(0, count, candidateKeys.length);
        if (probeKeys == candidateKeys) {
            throw new IllegalArgumentException("Probe keys and candidate scratch must not alias");
        }
        if (keys == null) {
            Arrays.fill(slots, 0, count, -1);
            return;
        }
        if (!grouped) {
            for (int index = 0; index < count; index++) {
                int slot = findSlotForInsert(probeKeys[index]);
                slots[index] = slot < 0 ? -1 : slot;
            }
            return;
        }

        // Separate independent tag loads from the dependent key loads. A tag is only a candidate, never equality.
        for (int index = 0; index < count; index++) {
            long hash = hash64(probeKeys[index]);
            int group = ((int) hash) & mask & ~(HASH_TAG_GROUP - 1);
            ByteVector groupTags = ByteVector.fromArray(HASH_TAG_SPECIES, tags, group);
            long matches = groupTags.compare(VectorOperators.EQ, hashTag(hash)).toLong();
            if (matches != 0) {
                slots[index] = group + Long.numberOfTrailingZeros(matches);
            }
            else {
                // A full group may have displaced a matching key into a later group.
                slots[index] = groupTags.compare(VectorOperators.EQ, (byte) 0).anyTrue() ? -1 : -2;
            }
        }
        for (int index = 0; index < count; index++) {
            int slot = slots[index];
            if (slot >= 0) {
                candidateKeys[index] = keys[slot];
            }
        }
        for (int index = 0; index < count; index++) {
            int slot = slots[index];
            if (slot == -2 || (slot >= 0 && candidateKeys[index] != probeKeys[index])) {
                // Resolve additional tag matches and displaced groups with the authoritative exact algorithm.
                int exactSlot = findSlotForInsert(probeKeys[index]);
                slots[index] = exactSlot < 0 ? -1 : exactSlot;
            }
        }
    }

    /**
     * Returns an existing slot, or the bitwise complement of an empty insertion slot.
     */
    int findSlotForInsert(long key)
    {
        ensureAllocated();
        if (grouped) {
            long hash = hash64(key);
            byte tag = hashTag(hash);
            int group = ((int) hash) & mask & ~(HASH_TAG_GROUP - 1);
            while (true) {
                ByteVector groupTags = ByteVector.fromArray(HASH_TAG_SPECIES, tags, group);
                long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
                while (matchBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(matchBits);
                    if (keys[slot] == key) {
                        return slot;
                    }
                    matchBits &= matchBits - 1;
                }
                long emptyBits = groupTags.compare(VectorOperators.EQ, (byte) 0).toLong();
                if (emptyBits != 0) {
                    return ~(group + Long.numberOfTrailingZeros(emptyBits));
                }
                group = (group + HASH_TAG_GROUP) & mask;
            }
        }
        int index = mix(key) & mask;
        while (true) {
            if (heads[index] == empty) {
                return ~index;
            }
            if (keys[index] == key) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    void initialize(int slot, long key, int head)
    {
        keys[slot] = key;
        occupy(slot, key);
        heads[slot] = head;
        if (tails != null) {
            tails[slot] = head;
            counts[slot] = 1;
        }
    }

    void initialize(int slot, long key, int head, int tail, int count)
    {
        keys[slot] = key;
        occupy(slot, key);
        heads[slot] = head;
        if (tails != null) {
            tails[slot] = tail;
            counts[slot] = count;
        }
        duplicates |= count > 1;
    }

    void ensureDuplicateState()
    {
        ensureAllocated();
        if (tails != null) {
            return;
        }
        tails = arrayPool.borrowInts(heads.length);
        counts = arrayPool.borrowInts(heads.length);
        Arrays.fill(tails, empty);
        Arrays.fill(counts, 0);
        for (int slot = 0; slot < heads.length; slot++) {
            int head = heads[slot];
            if (head != empty) {
                tails[slot] = head;
                counts[slot] = 1;
            }
        }
    }

    int incrementCount(int slot)
    {
        duplicates = true;
        return ++counts[slot];
    }

    int append(int slot, int ordinal)
    {
        duplicates = true;
        int previousTail = tails[slot];
        tails[slot] = ordinal;
        counts[slot]++;
        return previousTail;
    }

    void growIfNeeded(int distinctSize)
    {
        if (distinctSize < maximumFill) {
            return;
        }
        rehash();
    }

    void release()
    {
        arrayPool.release(keys);
        keys = null;
        arrayPool.release(tags);
        tags = null;
        arrayPool.release(heads);
        heads = null;
        arrayPool.release(tails);
        tails = null;
        arrayPool.release(counts);
        counts = null;
    }

    private void ensureAllocated()
    {
        if (keys != null) {
            return;
        }
        keys = arrayPool.borrowLongs(initialCapacity);
        if (grouped) {
            tags = arrayPool.borrowBytes(initialCapacity);
            Arrays.fill(tags, (byte) 0);
        }
        heads = arrayPool.borrowInts(initialCapacity);
        Arrays.fill(heads, empty);
        if (!lazyDuplicateState) {
            tails = arrayPool.borrowInts(initialCapacity);
            counts = arrayPool.borrowInts(initialCapacity);
        }
        mask = initialCapacity - 1;
        maximumFill = (int) (initialCapacity * LOAD_FACTOR);
    }

    private void rehash()
    {
        long[] previousKeys = keys;
        byte[] previousTags = tags;
        int[] previousHeads = heads;
        int[] previousTails = tails;
        int[] previousCounts = counts;
        int capacity = previousKeys.length * 2;

        keys = arrayPool.borrowLongs(capacity);
        if (grouped) {
            tags = arrayPool.borrowBytes(capacity);
            Arrays.fill(tags, (byte) 0);
        }
        heads = arrayPool.borrowInts(capacity);
        Arrays.fill(heads, empty);
        if (previousTails != null) {
            tails = arrayPool.borrowInts(capacity);
            counts = arrayPool.borrowInts(capacity);
        }
        mask = capacity - 1;
        maximumFill = (int) (capacity * LOAD_FACTOR);
        for (int slot = 0; slot < previousKeys.length; slot++) {
            if (previousHeads[slot] == empty) {
                continue;
            }
            int newSlot = findSlot(previousKeys[slot]);
            initialize(
                    newSlot,
                    previousKeys[slot],
                    previousHeads[slot],
                    previousTails == null ? empty : previousTails[slot],
                    previousCounts == null ? 1 : previousCounts[slot]);
        }
        arrayPool.release(previousKeys);
        arrayPool.release(previousTags);
        arrayPool.release(previousHeads);
        arrayPool.release(previousTails);
        arrayPool.release(previousCounts);
    }

    private void occupy(int slot, long key)
    {
        if (grouped) {
            tags[slot] = hashTag(hash64(key));
        }
    }

    private static int mix(long key)
    {
        return (int) hash64(key);
    }

    private static long hash64(long key)
    {
        long hash = key ^ (key >>> 33);
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= (hash >>> 33);
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= (hash >>> 33);
        return hash;
    }

    private static byte hashTag(long hash)
    {
        return (byte) ((hash >>> 56) | 0x80L);
    }
}
