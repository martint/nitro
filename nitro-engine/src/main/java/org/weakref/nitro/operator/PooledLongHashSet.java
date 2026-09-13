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
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;
import java.util.function.LongConsumer;

/** Compact exact scalar-long set whose backing arrays participate in the shared primitive pool. */
final class PooledLongHashSet
{
    private static final int MIN_CAPACITY = 16;
    private static final int MAX_CAPACITY = 1 << 30;

    private final PrimitiveArrayPool arrayPool;
    private final PooledLongHashSetPolicy policy;
    private final VectorSpecies<Byte> tagSpecies;
    private final VectorSpecies<Long> keySpecies;
    private final int tagGroupSize;
    private final int keyGroupSize;
    private boolean vectorTags;
    private boolean vectorKeys;
    private long[] keys;
    private byte[] tags;
    private long[] batchKeys;
    private long[] initialKeys;
    private int[] batchSlots;
    private int mask;
    private int maxFill;
    private int size;
    private boolean containsZero;
    private long addCalls;
    private boolean admissionReported;

    PooledLongHashSet(int expectedSize, PrimitiveArrayPool arrayPool, PooledLongHashSetPolicy policy)
    {
        this(expectedSize, arrayPool, policy, policy.vectorTags());
    }

    PooledLongHashSet(int expectedSize, PrimitiveArrayPool arrayPool, PooledLongHashSetPolicy policy, boolean vectorTags)
    {
        this.arrayPool = arrayPool;
        this.policy = policy;
        this.tagSpecies = switch (policy.tagGroupBits()) {
            case 64 -> ByteVector.SPECIES_64;
            case 128 -> ByteVector.SPECIES_128;
            case 256 -> ByteVector.SPECIES_256;
            case 512 -> ByteVector.SPECIES_512;
            default -> throw new IllegalArgumentException("Unsupported tag group bits: " + policy.tagGroupBits());
        };
        this.keySpecies = switch (policy.keyGroupBits()) {
            case 64 -> LongVector.SPECIES_64;
            case 128 -> LongVector.SPECIES_128;
            case 256 -> LongVector.SPECIES_256;
            case 512 -> LongVector.SPECIES_512;
            default -> throw new IllegalArgumentException("Unsupported key group bits: " + policy.keyGroupBits());
        };
        this.tagGroupSize = tagSpecies.length();
        this.keyGroupSize = keySpecies.length();
        this.vectorTags = policy.vectorTags() && vectorTags;
        this.vectorKeys = this.vectorTags && policy.vectorKeys();
        allocate(capacity(expectedSize));
    }

    boolean add(long key)
    {
        addCalls++;
        if (key == 0) {
            if (containsZero) {
                return false;
            }
            containsZero = true;
            size++;
            return true;
        }

        if (vectorTags) {
            return addGrouped(key);
        }
        return addScalar(key);
    }

    /** Adds after the owning adaptive index has closed admission and enabled the final hash representation. */
    boolean addTaggedFinal(long key)
    {
        addCalls++;
        if (key == 0) {
            if (containsZero) {
                return false;
            }
            containsZero = true;
            size++;
            return true;
        }
        return addGrouped(key);
    }

    boolean addScalarFinal(long key)
    {
        addCalls++;
        if (key == 0) {
            if (containsZero) {
                return false;
            }
            containsZero = true;
            size++;
            return true;
        }
        return addScalar(key);
    }

    private boolean addScalar(long key)
    {
        return addScalarAtSlot(key, hash(key) & mask);
    }

    private boolean addScalarAtSlot(long key, int slot)
    {
        long current = keys[slot];
        while (current != 0 && current != key) {
            slot = (slot + 1) & mask;
            current = keys[slot];
        }
        if (current == key) {
            return false;
        }
        keys[slot] = key;
        size++;
        if (size >= maxFill) {
            if (keys.length == MAX_CAPACITY) {
                throw new IllegalStateException("Long hash set exceeds maximum capacity");
            }
            rehash(keys.length << 1);
        }
        return true;
    }

    int addScalarBatch(VectorAccess.LongValues values, int startPosition, int endPosition, int[] distinctPositions)
    {
        if (vectorTags) {
            throw new IllegalStateException("Scalar batch insertion requires the scalar table layout");
        }
        if (batchKeys == null) {
            batchKeys = arrayPool.borrowLongs(policy.scalarBatchSize());
            initialKeys = arrayPool.borrowLongs(policy.scalarBatchSize());
            batchSlots = arrayPool.borrowInts(policy.scalarBatchSize());
        }
        int distinctCount = 0;
        for (int start = startPosition; start < endPosition; ) {
            int count = Math.min(policy.scalarBatchSize(), endPosition - start);
            int initialMask = mask;
            for (int index = 0; index < count; index++) {
                long key = values.value(start + index);
                batchKeys[index] = key;
                batchSlots[index] = hash(key) & initialMask;
            }
            for (int index = 0; index < count; index++) {
                initialKeys[index] = keys[batchSlots[index]];
            }
            for (int index = 0; index < count; index++) {
                long key = batchKeys[index];
                addCalls++;
                if (key == 0) {
                    if (!containsZero) {
                        containsZero = true;
                        size++;
                        distinctPositions[distinctCount++] = start + index;
                    }
                    continue;
                }
                // Existing hits stay valid across insertion and growth. Misses must observe earlier inserts in
                // this batch, and growth invalidates their original slot coordinates.
                if (initialKeys[index] != key &&
                        addScalarAtSlot(key, initialMask == mask ? batchSlots[index] : hash(key) & mask)) {
                    distinctPositions[distinctCount++] = start + index;
                }
            }
            start += count;
        }
        return distinctCount;
    }

    boolean contains(long key)
    {
        if (key == 0) {
            return containsZero;
        }
        if (vectorTags) {
            return containsGrouped(key);
        }
        int slot = hash(key) & mask;
        long current = keys[slot];
        while (current != 0 && current != key) {
            slot = (slot + 1) & mask;
            current = keys[slot];
        }
        return current == key;
    }

    int size()
    {
        return size;
    }

    long retainedBytes()
    {
        return (keys == null ? 0 : (long) keys.length * Long.BYTES) +
                (tags == null ? 0 : tags.length) +
                (batchKeys == null ? 0 : (long) batchKeys.length * Long.BYTES) +
                (initialKeys == null ? 0 : (long) initialKeys.length * Long.BYTES) +
                (batchSlots == null ? 0 : (long) batchSlots.length * Integer.BYTES);
    }

    void enableVectorTags()
    {
        enableVectorTags(addCalls);
    }

    void enableVectorTags(long observedAddCalls)
    {
        if (policy.debug() && !admissionReported) {
            System.err.printf("[scalar-long-distinct-admission] size=%d addCalls=%d capacity=%d%n", size, observedAddCalls, keys.length);
            admissionReported = true;
        }
        if (!policy.vectorTags() || vectorTags || (long) size * 100 < observedAddCalls * policy.minimumVectorTagNewKeyPercent()) {
            return;
        }
        long[] previousKeys = keys;
        vectorTags = true;
        // Direct key comparison avoids a metadata stream when duplicates are common. In an insertion-dominated
        // stream it loads a full 64-byte key group merely to discover an empty lane; compact tags perform that
        // admission with one narrow control load. Select the immutable representation once, from the same observed
        // novelty window that decides whether grouped probing is worthwhile at all.
        vectorKeys = policy.vectorKeys() &&
                (long) size * 100 <= observedAddCalls * policy.maximumVectorKeyNewKeyPercent();
        allocate(previousKeys.length);
        for (long key : previousKeys) {
            if (key != 0) {
                insertGroupedRehash(key);
            }
        }
        arrayPool.release(previousKeys);
    }

    boolean vectorTagsEnabled()
    {
        return vectorTags;
    }

    boolean vectorKeysEnabled()
    {
        return vectorTags && vectorKeys;
    }

    void ensureCapacity(int expectedSize)
    {
        if (expectedSize < maxFill) {
            return;
        }
        int capacity = keys.length;
        while (expectedSize >= maxFill(capacity)) {
            if (capacity == MAX_CAPACITY) {
                throw new IllegalStateException("Long hash set exceeds maximum capacity");
            }
            capacity <<= 1;
        }
        rehash(capacity);
    }

    void forEach(LongConsumer consumer)
    {
        if (containsZero) {
            consumer.accept(0);
        }
        for (long key : keys) {
            if (key != 0) {
                consumer.accept(key);
            }
        }
    }

    void releaseBuffers()
    {
        if (policy.debug()) {
            System.err.printf("[scalar-long-distinct] tagged=%s vectorKeys=%s size=%d addCalls=%d capacity=%d%n", vectorTags, vectorKeys, size, addCalls, keys.length);
        }
        arrayPool.release(keys);
        arrayPool.release(tags);
        arrayPool.release(batchKeys);
        arrayPool.release(initialKeys);
        arrayPool.release(batchSlots);
        keys = null;
        tags = null;
        batchKeys = null;
        initialKeys = null;
        batchSlots = null;
        mask = 0;
        maxFill = 0;
        size = 0;
        containsZero = false;
        addCalls = 0;
        admissionReported = false;
        vectorKeys = false;
    }

    private int emptySlot(long key)
    {
        int slot = hash(key) & mask;
        while (keys[slot] != 0) {
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    private static int hash(long key)
    {
        return (int) hash64(key);
    }

    private static long hash64(long key)
    {
        long hash = key * -7046029254386353131L;
        hash ^= hash >>> 32;
        hash ^= hash >>> 16;
        return hash;
    }

    private void rehash(int capacity)
    {
        if (capacity == keys.length) {
            return;
        }
        long[] previousKeys = keys;
        byte[] previousTags = tags;
        allocate(capacity);
        for (long key : previousKeys) {
            if (key != 0) {
                if (vectorTags) {
                    insertGroupedRehash(key);
                }
                else {
                    keys[emptySlot(key)] = key;
                }
            }
        }
        arrayPool.release(previousKeys);
        arrayPool.release(previousTags);
    }

    private void allocate(int capacity)
    {
        keys = arrayPool.borrowLongs(capacity);
        Arrays.fill(keys, 0);
        if (vectorTags && !vectorKeys) {
            tags = arrayPool.borrowBytes(capacity);
            Arrays.fill(tags, (byte) 0);
        }
        mask = capacity - 1;
        maxFill = maxFill(capacity);
    }

    private boolean addTagged(long key)
    {
        long hash = hash64(key);
        byte tag = tag(hash);
        int group = ((int) hash) & mask & ~(tagGroupSize - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(tagSpecies, tags, group);
            long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                if (keys[slot] == key) {
                    return false;
                }
                matchBits &= matchBits - 1;
            }
            long emptyBits = groupTags.compare(VectorOperators.EQ, (byte) 0).toLong();
            if (emptyBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(emptyBits);
                tags[slot] = tag;
                keys[slot] = key;
                size++;
                if (size >= maxFill) {
                    if (keys.length == MAX_CAPACITY) {
                        throw new IllegalStateException("Long hash set exceeds maximum capacity");
                    }
                    rehash(keys.length << 1);
                }
                return true;
            }
            group = (group + tagGroupSize) & mask;
        }
    }

    private boolean addVectorKeys(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(keyGroupSize - 1);
        while (true) {
            LongVector groupKeys = LongVector.fromArray(keySpecies, keys, group);
            if (groupKeys.compare(VectorOperators.EQ, key).anyTrue()) {
                return false;
            }
            long emptyBits = groupKeys.compare(VectorOperators.EQ, 0L).toLong();
            if (emptyBits != 0) {
                keys[group + Long.numberOfTrailingZeros(emptyBits)] = key;
                size++;
                if (size >= maxFill) {
                    if (keys.length == MAX_CAPACITY) {
                        throw new IllegalStateException("Long hash set exceeds maximum capacity");
                    }
                    rehash(keys.length << 1);
                }
                return true;
            }
            group = (group + keyGroupSize) & mask;
        }
    }

    private boolean addGrouped(long key)
    {
        if (vectorKeys) {
            return addVectorKeys(key);
        }
        return addTagged(key);
    }

    private boolean containsTagged(long key)
    {
        long hash = hash64(key);
        byte tag = tag(hash);
        int group = ((int) hash) & mask & ~(tagGroupSize - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(tagSpecies, tags, group);
            long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                if (keys[slot] == key) {
                    return true;
                }
                matchBits &= matchBits - 1;
            }
            if (groupTags.compare(VectorOperators.EQ, (byte) 0).anyTrue()) {
                return false;
            }
            group = (group + tagGroupSize) & mask;
        }
    }

    private boolean containsVectorKeys(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(keyGroupSize - 1);
        while (true) {
            LongVector groupKeys = LongVector.fromArray(keySpecies, keys, group);
            if (groupKeys.compare(VectorOperators.EQ, key).anyTrue()) {
                return true;
            }
            if (groupKeys.compare(VectorOperators.EQ, 0L).anyTrue()) {
                return false;
            }
            group = (group + keyGroupSize) & mask;
        }
    }

    private boolean containsGrouped(long key)
    {
        if (vectorKeys) {
            return containsVectorKeys(key);
        }
        return containsTagged(key);
    }

    private void insertTaggedRehash(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(tagGroupSize - 1);
        while (true) {
            long emptyBits = ByteVector.fromArray(tagSpecies, tags, group)
                    .compare(VectorOperators.EQ, (byte) 0)
                    .toLong();
            if (emptyBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(emptyBits);
                tags[slot] = tag(hash);
                keys[slot] = key;
                return;
            }
            group = (group + tagGroupSize) & mask;
        }
    }

    private void insertVectorKeysRehash(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(keyGroupSize - 1);
        while (true) {
            long emptyBits = LongVector.fromArray(keySpecies, keys, group)
                    .compare(VectorOperators.EQ, 0L)
                    .toLong();
            if (emptyBits != 0) {
                keys[group + Long.numberOfTrailingZeros(emptyBits)] = key;
                return;
            }
            group = (group + keyGroupSize) & mask;
        }
    }

    private void insertGroupedRehash(long key)
    {
        if (vectorKeys) {
            insertVectorKeysRehash(key);
        }
        else {
            insertTaggedRehash(key);
        }
    }

    private static byte tag(long hash)
    {
        return (byte) ((hash >>> 56) | 0x80L);
    }

    private int capacity(int expectedSize)
    {
        if (expectedSize < 0) {
            throw new IllegalArgumentException("expectedSize is negative");
        }
        int capacity = MIN_CAPACITY;
        while (expectedSize >= maxFill(capacity)) {
            if (capacity == MAX_CAPACITY) {
                throw new IllegalArgumentException("expectedSize is too large: " + expectedSize);
            }
            capacity <<= 1;
        }
        return capacity;
    }

    private int maxFill(int capacity)
    {
        return (int) (capacity * policy.loadFactor());
    }
}
