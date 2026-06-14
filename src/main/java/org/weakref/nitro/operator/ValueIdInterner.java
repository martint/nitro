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

import java.util.Arrays;

/**
 * Interns the distinct byte-string values of a key column to dense integer ids (0, 1, 2, …) that are stable
 * across batches — the Velox {@code VectorHasher} "value id" technique. The same value always maps to the same
 * id regardless of which batch or (per-page) dictionary it arrived in, so a low-cardinality string/binary
 * {@code GROUP BY} key can be reduced to a compact dense key, grouped with the integer-key tables, and the
 * value reconstructed at materialization from {@link #value(int)}.
 *
 * <p>Open-addressed hash set over a contiguous byte arena: each distinct value is appended to {@code data} once,
 * its {@code [offset, length)} kept in parallel arrays, and the slot table maps a value's hash to its id.
 * Equality is settled by a byte compare against the stored value, so distinct values never share an id.
 *
 * <p>Adaptive ceiling: once the distinct count would exceed {@code maxDistinct}, {@link #intern} returns
 * {@link #TOO_MANY} and the interner is marked {@link #overflowed()}. Callers use that to fall back to a general
 * (hash-of-value) grouping strategy rather than letting a high-cardinality key defeat the dense-id path.
 */
final class ValueIdInterner
{
    static final int TOO_MANY = -1;

    private final int maxDistinct;

    // Open-addressing slot table: slots[h] = id + 1, 0 means empty. Power-of-two capacity, mask = capacity - 1.
    private int[] slots;
    private int mask;

    // Per-id stored value: a slice [valueOffset[id], valueOffset[id] + valueLength[id]) into data.
    private byte[] data;
    private int dataSize;
    private int[] valueOffset;
    private int[] valueLength;
    private long[] valueHash;
    private int distinct;
    private boolean overflowed;

    ValueIdInterner(int maxDistinct)
    {
        this(maxDistinct, 1024);
    }

    ValueIdInterner(int maxDistinct, int initialDataCapacity)
    {
        this.maxDistinct = maxDistinct;
        int capacity = 16;
        this.slots = new int[capacity];
        this.mask = capacity - 1;
        this.data = new byte[Math.max(16, initialDataCapacity)];
        this.valueOffset = new int[16];
        this.valueLength = new int[16];
        this.valueHash = new long[16];
    }

    int distinctCount()
    {
        return distinct;
    }

    boolean overflowed()
    {
        return overflowed;
    }

    /**
     * The dense id for {@code value[offset, offset + length)}, assigning a new one (the next ordinal) on first
     * sight. Returns {@link #TOO_MANY} once assigning would exceed the configured ceiling; the interner then
     * stays {@link #overflowed()} and every later {@link #intern} also returns {@link #TOO_MANY}.
     */
    int intern(byte[] value, int offset, int length)
    {
        if (overflowed) {
            return TOO_MANY;
        }
        long hash = hash(value, offset, length);
        int slot = (int) (hash) & mask;
        while (true) {
            int entry = slots[slot];
            if (entry == 0) {
                if (distinct >= maxDistinct) {
                    overflowed = true;
                    return TOO_MANY;
                }
                int id = distinct;
                store(id, value, offset, length, hash);
                slots[slot] = id + 1;
                distinct++;
                if (distinct * 4 >= slots.length * 3) {
                    growSlots();
                }
                return id;
            }
            int id = entry - 1;
            if (valueHash[id] == hash && regionEquals(id, value, offset, length)) {
                return id;
            }
            slot = (slot + 1) & mask;
        }
    }

    /** Writes the interned value with the given id to {@code out}; returns its length. */
    int copyValue(int id, byte[] out, int outOffset)
    {
        int length = valueLength[id];
        System.arraycopy(data, valueOffset[id], out, outOffset, length);
        return length;
    }

    int valueLength(int id)
    {
        return valueLength[id];
    }

    /** A fresh copy of the interned value with the given id (for materialization / tests). */
    byte[] value(int id)
    {
        return Arrays.copyOfRange(data, valueOffset[id], valueOffset[id] + valueLength[id]);
    }

    private void store(int id, byte[] value, int offset, int length, long hash)
    {
        if (id >= valueOffset.length) {
            int newLength = valueOffset.length * 2;
            valueOffset = Arrays.copyOf(valueOffset, newLength);
            valueLength = Arrays.copyOf(valueLength, newLength);
            valueHash = Arrays.copyOf(valueHash, newLength);
        }
        if (dataSize + length > data.length) {
            int newLength = data.length * 2;
            while (newLength < dataSize + length) {
                newLength *= 2;
            }
            data = Arrays.copyOf(data, newLength);
        }
        System.arraycopy(value, offset, data, dataSize, length);
        valueOffset[id] = dataSize;
        valueLength[id] = length;
        valueHash[id] = hash;
        dataSize += length;
    }

    private boolean regionEquals(int id, byte[] value, int offset, int length)
    {
        if (valueLength[id] != length) {
            return false;
        }
        return Arrays.equals(data, valueOffset[id], valueOffset[id] + length, value, offset, offset + length);
    }

    private void growSlots()
    {
        int newCapacity = slots.length * 2;
        int[] newSlots = new int[newCapacity];
        int newMask = newCapacity - 1;
        for (int id = 0; id < distinct; id++) {
            int slot = (int) (valueHash[id]) & newMask;
            while (newSlots[slot] != 0) {
                slot = (slot + 1) & newMask;
            }
            newSlots[slot] = id + 1;
        }
        slots = newSlots;
        mask = newMask;
    }

    private static long hash(byte[] value, int offset, int length)
    {
        // 64-bit FNV-1a; cheap, decent spread for short keys (the low-cardinality case this targets).
        long hash = 0xcbf29ce484222325L;
        for (int index = 0; index < length; index++) {
            hash ^= value[offset + index] & 0xff;
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
