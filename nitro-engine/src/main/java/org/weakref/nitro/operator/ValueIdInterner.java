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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

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
 * (hash-of-value) grouping strategy rather than letting a high-cardinality key defeat the dense-id path. The
 * already-interned empty value remains addressable after overflow without probing the table; this preserves the
 * dominant sentinel's compact representation while every non-empty value takes the constant-time fallback.
 */
final class ValueIdInterner
{
    static final int TOO_MANY = -1;

    private final int maxDistinct;
    private final FlatKeyTablePolicy.ValueIds policy;

    // Swiss/F14-style open-addressing slot table: slots[h] = id + 1 (0 = empty), with a parallel byte tag per
    // slot (top hash bits | 0x80; 0 = empty). A probe scans GROUP slots with one vector tag-compare, so the
    // random valueHash[id] read happens only on a tag hit, not on every collision step. Power-of-two capacity
    // (multiple of GROUP), mask = capacity - 1.
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
    private static final int GROUP = SPECIES.length();
    private int[] slots;
    private byte[] slotTags;
    private int mask;

    // Per-id stored value: a slice [valueOffset[id], valueOffset[id] + valueLength[id]) into data.
    private byte[] data;
    private int dataSize;
    private int[] valueOffset;
    private int[] valueLength;
    private long[] valueHash;
    // Hash used by the surrounding operator hash tables. This deliberately uses
    // OperatorVectorSupport.binaryHash rather than the interner's private slot hash so a value
    // has the same hash whether it arrives flat, dictionary encoded, or after this interner has
    // overflowed. Dictionary consumers can read it by stable id without touching the bytes again.
    private int[] valueGroupingHash;
    private int distinct;
    private boolean overflowed;
    private int emptyId = TOO_MANY;

    ValueIdInterner(int maxDistinct, FlatKeyTablePolicy.ValueIds policy)
    {
        this(maxDistinct, 1024, policy);
    }

    private ValueIdInterner(int maxDistinct, int initialDataCapacity, FlatKeyTablePolicy.ValueIds policy)
    {
        this.maxDistinct = maxDistinct;
        this.policy = requireNonNull(policy, "policy is null");
        int capacity = 16;
        this.slots = new int[capacity];
        this.slotTags = new byte[capacity];
        this.mask = capacity - 1;
        this.data = new byte[Math.max(16, initialDataCapacity)];
        this.valueOffset = new int[16];
        this.valueLength = new int[16];
        this.valueHash = new long[16];
        this.valueGroupingHash = new int[16];
    }

    int distinctCount()
    {
        return distinct;
    }

    boolean overflowed()
    {
        return overflowed;
    }

    long retainedBytes()
    {
        long bytes = (long) slots.length * Integer.BYTES;
        bytes += slotTags.length;
        bytes += data.length;
        bytes += (long) valueOffset.length * Integer.BYTES;
        bytes += (long) valueLength.length * Integer.BYTES;
        bytes += (long) valueHash.length * Long.BYTES;
        bytes += (long) valueGroupingHash.length * Integer.BYTES;
        return bytes;
    }

    /**
     * The dense id for {@code value[offset, offset + length)}, assigning a new one (the next ordinal) on first
     * sight. Returns {@link #TOO_MANY} once assigning would exceed the configured ceiling; the interner then
     * stays {@link #overflowed()} and every later non-empty {@link #intern} also returns {@link #TOO_MANY}.
     */
    int intern(byte[] value, int offset, int length)
    {
        return intern(value, offset, length, OperatorVectorSupport.binaryHash(value, offset, length));
    }

    /**
     * Interns a value whose operator-compatible hash has already been computed by the batch layout. The supplied
     * hash is used both to place the interner slot and by the surrounding grouping table, avoiding a second scan of
     * the same bytes. Equality remains authoritative, so ordinary 32-bit hash collisions cannot merge values.
     */
    int intern(byte[] value, int offset, int length, int groupingHash)
    {
        return intern(value, offset, length, slotHash(groupingHash), groupingHash);
    }

    private int intern(byte[] value, int offset, int length, long hash, int groupingHash)
    {
        if (overflowed) {
            return policy.recognizeEmptyAfterOverflow() && length == 0 ? emptyId : TOO_MANY;
        }
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, slotTags, group);
            long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                int id = slots[slot] - 1;
                if (valueHash[id] == hash && regionEquals(id, value, offset, length)) {
                    return id;
                }
                matchBits &= matchBits - 1;
            }
            long emptyBits = groupTags.compare(VectorOperators.EQ, (byte) 0).toLong();
            if (emptyBits != 0) {
                if (distinct >= maxDistinct) {
                    overflowed = true;
                    return TOO_MANY;
                }
                int slot = group + Long.numberOfTrailingZeros(emptyBits);
                int id = distinct;
                store(id, value, offset, length, hash, groupingHash);
                slotTags[slot] = tag;
                slots[slot] = id + 1;
                distinct++;
                if (distinct * 4 >= slots.length * 3) {
                    growSlots();
                }
                return id;
            }
            group = (group + GROUP) & mask;
        }
    }

    /** Writes the interned value with the given id to {@code out}; returns its length. */
    int copyValue(int id, byte[] out, int outOffset)
    {
        int length = valueLength[id];
        System.arraycopy(data, valueOffset[id], out, outOffset, length);
        return length;
    }

    void copyValue(int id, org.weakref.nitro.data.BinaryVector output, int outputPosition)
    {
        output.setBytes(outputPosition, data, valueOffset[id], valueLength[id]);
    }

    int valueLength(int id)
    {
        return valueLength[id];
    }

    int groupingHash(int id)
    {
        return valueGroupingHash[id];
    }

    /**
     * Finds an already-interned value without assigning a new id. Unlike {@link #intern}, this remains available
     * after the adaptive ceiling has overflowed. Normalized-key consumers use it to keep a value that received an
     * id in an earlier batch in the same physical hash domain, while genuinely new high-cardinality values retain
     * the ordinary value-hash fallback.
     */
    int find(byte[] value, int offset, int length)
    {
        if (policy.recognizeEmptyAfterOverflow() && length == 0 && emptyId >= 0) {
            return emptyId;
        }
        long hash = slotHash(OperatorVectorSupport.binaryHash(value, offset, length));
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, slotTags, group);
            long matchBits = groupTags.compare(VectorOperators.EQ, tag).toLong();
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                int id = slots[slot] - 1;
                if (valueHash[id] == hash && regionEquals(id, value, offset, length)) {
                    return id;
                }
                matchBits &= matchBits - 1;
            }
            if (groupTags.compare(VectorOperators.EQ, (byte) 0).anyTrue()) {
                return TOO_MANY;
            }
            group = (group + GROUP) & mask;
        }
    }

    boolean valueEquals(int id, byte[] value, int offset, int length)
    {
        return id >= 0 && id < distinct && regionEquals(id, value, offset, length);
    }

    /** A fresh copy of the interned value with the given id (for materialization / tests). */
    byte[] value(int id)
    {
        return Arrays.copyOfRange(data, valueOffset[id], valueOffset[id] + valueLength[id]);
    }

    /**
     * The distinct interned values as a {@link BinaryVector} indexed by id (0..distinctCount-1), so a grouped
     * key column that stored these ids can be emitted as a {@link org.weakref.nitro.data.DictionaryVector} over
     * this base instead of a flat per-group copy -- letting a downstream re-group/join over the key see a
     * compact dictionary rather than one entry per row.
     */
    org.weakref.nitro.data.BinaryVector toBinaryVector(org.weakref.nitro.data.Allocator allocator, org.weakref.nitro.data.Allocator.Context allocationContext)
    {
        long totalBytes = 0;
        for (int id = 0; id < distinct; id++) {
            totalBytes += valueLength[id];
        }
        org.weakref.nitro.data.BinaryVector vector = org.weakref.nitro.data.BinaryVector.allocate(allocator, allocationContext, distinct, (int) totalBytes);
        Arrays.fill(vector.offsets(), 0);
        for (int id = 0; id < distinct; id++) {
            vector.setBytes(id, data, valueOffset[id], valueLength[id]);
        }
        return vector.freezeContent();
    }

    private void store(int id, byte[] value, int offset, int length, long hash, int groupingHash)
    {
        if (id >= valueOffset.length) {
            int newLength = valueOffset.length * 2;
            valueOffset = Arrays.copyOf(valueOffset, newLength);
            valueLength = Arrays.copyOf(valueLength, newLength);
            valueHash = Arrays.copyOf(valueHash, newLength);
            valueGroupingHash = Arrays.copyOf(valueGroupingHash, newLength);
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
        valueGroupingHash[id] = groupingHash;
        if (length == 0) {
            emptyId = id;
        }
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
        byte[] newTags = new byte[newCapacity];
        int newMask = newCapacity - 1;
        for (int id = 0; id < distinct; id++) {
            long hash = valueHash[id];
            int group = ((int) hash) & newMask & ~(GROUP - 1);
            while (true) {
                long emptyBits = ByteVector.fromArray(SPECIES, newTags, group).compare(VectorOperators.EQ, (byte) 0).toLong();
                if (emptyBits != 0) {
                    int slot = group + Long.numberOfTrailingZeros(emptyBits);
                    newTags[slot] = (byte) ((hash >>> 56) | 0x80L);
                    newSlots[slot] = id + 1;
                    break;
                }
                group = (group + GROUP) & newMask;
            }
        }
        slots = newSlots;
        slotTags = newTags;
        mask = newMask;
    }

    private static long slotHash(int groupingHash)
    {
        // binaryHash has already avalanched the value into 32 bits. Multiplication spreads those bits across the
        // 64-bit slot hash so both the low-bit group index and high-bit control tag remain well distributed.
        return Integer.toUnsignedLong(groupingHash) * 0x9E3779B97F4A7C15L;
    }
}
