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
import java.util.function.LongConsumer;

/** Compact exact scalar-long set whose backing arrays participate in the shared primitive pool. */
final class PooledLongHashSet
{
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final int MAX_CAPACITY = 1 << 30;

    private final PrimitiveArrayPool arrayPool;
    private long[] keys;
    private int mask;
    private int maxFill;
    private int size;
    private boolean containsZero;

    PooledLongHashSet(int expectedSize)
    {
        this(expectedSize, PrimitiveArrayPool.shared());
    }

    PooledLongHashSet(int expectedSize, PrimitiveArrayPool arrayPool)
    {
        this.arrayPool = arrayPool;
        allocate(capacity(expectedSize));
    }

    boolean add(long key)
    {
        if (key == 0) {
            if (containsZero) {
                return false;
            }
            containsZero = true;
            size++;
            return true;
        }

        int slot = hash(key) & mask;
        long current = keys[slot];
        if (current != 0) {
            if (current == key) {
                return false;
            }
            while (true) {
                slot = (slot + 1) & mask;
                current = keys[slot];
                if (current == 0) {
                    break;
                }
                if (current == key) {
                    return false;
                }
            }
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
        int slot = hash(key) & mask;
        long current = keys[slot];
        if (current == 0) {
            return false;
        }
        if (current == key) {
            return true;
        }
        while (true) {
            slot = (slot + 1) & mask;
            current = keys[slot];
            if (current == 0) {
                return false;
            }
            if (current == key) {
                return true;
            }
        }
    }

    int size()
    {
        return size;
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
        arrayPool.release(keys);
        keys = null;
        mask = 0;
        maxFill = 0;
        size = 0;
        containsZero = false;
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
        long hash = key * -7046029254386353131L;
        hash ^= hash >>> 32;
        hash ^= hash >>> 16;
        return (int) hash;
    }

    private void rehash(int capacity)
    {
        if (capacity == keys.length) {
            return;
        }
        long[] previousKeys = keys;
        allocate(capacity);
        for (long key : previousKeys) {
            if (key != 0) {
                keys[emptySlot(key)] = key;
            }
        }
        arrayPool.release(previousKeys);
    }

    private void allocate(int capacity)
    {
        keys = arrayPool.borrowLongs(capacity);
        Arrays.fill(keys, 0);
        mask = capacity - 1;
        maxFill = maxFill(capacity);
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
        return (int) (capacity * LOAD_FACTOR);
    }
}
