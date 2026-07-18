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

import java.util.Arrays;
import java.util.function.LongConsumer;

/** Compact exact scalar-long set whose backing arrays participate in the shared primitive pool. */
final class PooledLongHashSet
{
    private static final float LOAD_FACTOR = Float.parseFloat(System.getProperty("nitro.distinct.scalarLongLoadFactor", "0.75"));
    private static final boolean VECTOR_TAGS = Boolean.parseBoolean(System.getProperty("nitro.distinct.scalarLongVectorTags", "true"));
    private static final boolean VECTOR_KEYS = Boolean.parseBoolean(System.getProperty("nitro.distinct.scalarLongVectorKeys", "true"));
    private static final int MIN_VECTOR_TAG_NEW_KEY_PERCENT = Integer.getInteger("nitro.distinct.scalarLongVectorTagMinNewKeyPercent", 5);
    private static final boolean DEBUG = Boolean.getBoolean("nitro.debug.scalarLongDistinct");
    private static final VectorSpecies<Byte> TAG_SPECIES = switch (Integer.getInteger("nitro.distinct.scalarLongTagGroupBits", 128)) {
        case 64 -> ByteVector.SPECIES_64;
        case 128 -> ByteVector.SPECIES_128;
        case 256 -> ByteVector.SPECIES_256;
        case 512 -> ByteVector.SPECIES_512;
        default -> throw new IllegalArgumentException("nitro.distinct.scalarLongTagGroupBits must be 64, 128, 256, or 512");
    };
    private static final int TAG_GROUP_SIZE = TAG_SPECIES.length();
    private static final VectorSpecies<Long> KEY_SPECIES = LongVector.SPECIES_512;
    private static final int KEY_GROUP_SIZE = KEY_SPECIES.length();
    private static final int MIN_CAPACITY = 16;
    private static final int MAX_CAPACITY = 1 << 30;

    private final PrimitiveArrayPool arrayPool;
    private boolean vectorTags;
    private long[] keys;
    private byte[] tags;
    private int mask;
    private int maxFill;
    private int size;
    private boolean containsZero;
    private long addCalls;
    private boolean admissionReported;

    PooledLongHashSet(int expectedSize)
    {
        this(expectedSize, PrimitiveArrayPool.shared(), VECTOR_TAGS);
    }

    PooledLongHashSet(int expectedSize, PrimitiveArrayPool arrayPool)
    {
        this(expectedSize, arrayPool, VECTOR_TAGS);
    }

    PooledLongHashSet(int expectedSize, PrimitiveArrayPool arrayPool, boolean vectorTags)
    {
        this.arrayPool = arrayPool;
        this.vectorTags = VECTOR_TAGS && vectorTags;
        allocate(capacity(expectedSize));
    }

    boolean add(long key)
    {
        if (DEBUG) {
            addCalls++;
        }
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
        if (DEBUG) {
            addCalls++;
        }
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
        if (DEBUG) {
            addCalls++;
        }
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
        int slot = hash(key) & mask;
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

    void enableVectorTags()
    {
        if (DEBUG && !admissionReported) {
            System.err.printf("[scalar-long-distinct-admission] size=%d addCalls=%d capacity=%d%n", size, addCalls, keys.length);
            admissionReported = true;
        }
        if (!VECTOR_TAGS || vectorTags || (long) size * 100 < addCalls * MIN_VECTOR_TAG_NEW_KEY_PERCENT) {
            return;
        }
        long[] previousKeys = keys;
        vectorTags = true;
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
        if (DEBUG) {
            System.err.printf("[scalar-long-distinct] tagged=%s size=%d addCalls=%d capacity=%d%n", vectorTags, size, addCalls, keys.length);
        }
        arrayPool.release(keys);
        arrayPool.release(tags);
        keys = null;
        tags = null;
        mask = 0;
        maxFill = 0;
        size = 0;
        containsZero = false;
        addCalls = 0;
        admissionReported = false;
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
        if (vectorTags && !VECTOR_KEYS) {
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
        int group = ((int) hash) & mask & ~(TAG_GROUP_SIZE - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(TAG_SPECIES, tags, group);
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
            group = (group + TAG_GROUP_SIZE) & mask;
        }
    }

    private boolean addVectorKeys(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(KEY_GROUP_SIZE - 1);
        while (true) {
            LongVector groupKeys = LongVector.fromArray(KEY_SPECIES, keys, group);
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
            group = (group + KEY_GROUP_SIZE) & mask;
        }
    }

    private boolean addGrouped(long key)
    {
        if (VECTOR_KEYS) {
            return addVectorKeys(key);
        }
        return addTagged(key);
    }

    private boolean containsTagged(long key)
    {
        long hash = hash64(key);
        byte tag = tag(hash);
        int group = ((int) hash) & mask & ~(TAG_GROUP_SIZE - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(TAG_SPECIES, tags, group);
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
            group = (group + TAG_GROUP_SIZE) & mask;
        }
    }

    private boolean containsVectorKeys(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(KEY_GROUP_SIZE - 1);
        while (true) {
            LongVector groupKeys = LongVector.fromArray(KEY_SPECIES, keys, group);
            if (groupKeys.compare(VectorOperators.EQ, key).anyTrue()) {
                return true;
            }
            if (groupKeys.compare(VectorOperators.EQ, 0L).anyTrue()) {
                return false;
            }
            group = (group + KEY_GROUP_SIZE) & mask;
        }
    }

    private boolean containsGrouped(long key)
    {
        if (VECTOR_KEYS) {
            return containsVectorKeys(key);
        }
        return containsTagged(key);
    }

    private void insertTaggedRehash(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(TAG_GROUP_SIZE - 1);
        while (true) {
            long emptyBits = ByteVector.fromArray(TAG_SPECIES, tags, group)
                    .compare(VectorOperators.EQ, (byte) 0)
                    .toLong();
            if (emptyBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(emptyBits);
                tags[slot] = tag(hash);
                keys[slot] = key;
                return;
            }
            group = (group + TAG_GROUP_SIZE) & mask;
        }
    }

    private void insertVectorKeysRehash(long key)
    {
        long hash = hash64(key);
        int group = ((int) hash) & mask & ~(KEY_GROUP_SIZE - 1);
        while (true) {
            long emptyBits = LongVector.fromArray(KEY_SPECIES, keys, group)
                    .compare(VectorOperators.EQ, 0L)
                    .toLong();
            if (emptyBits != 0) {
                keys[group + Long.numberOfTrailingZeros(emptyBits)] = key;
                return;
            }
            group = (group + KEY_GROUP_SIZE) & mask;
        }
    }

    private void insertGroupedRehash(long key)
    {
        if (VECTOR_KEYS) {
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

    private static int capacity(int expectedSize)
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

    private static int maxFill(int capacity)
    {
        if (!(LOAD_FACTOR > 0 && LOAD_FACTOR < 1)) {
            throw new IllegalArgumentException("nitro.distinct.scalarLongLoadFactor must be between 0 and 1: " + LOAD_FACTOR);
        }
        return (int) (capacity * LOAD_FACTOR);
    }
}
