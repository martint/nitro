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

import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.util.Arrays;

/**
 * Cold-path machinery shared by every generated multi-long grouping table. Holds the interleaved
 * {@code entries} array ({@code arity} keys + group id per slot, stride {@code arity + 1}), the parallel
 * {@code nullMasks}, the open-addressing bookkeeping, and the reverse map used to reconstruct group keys
 * at materialization ({@code keysByGroup[column][groupId]} + {@code nullMasksByGroup[groupId]}).
 *
 * <p>The arity-specialized hot work — {@link #assignBatch}, which reads the key columns into locals,
 * probes, and assigns groups for a whole batch, plus {@link #hashEntry} used by rehash — is generated
 * per arity by {@link MultiLongGroupingTableGenerator} so the emitted bytecode is structurally identical
 * to the former hand-written 2/3/4-key tables (keys in registers, no per-row dispatch or scratch array).
 * Fields are package-private so the generated same-package subclass can access them directly.
 */
abstract class AbstractMultiLongGroupingTable
{
    static final float LOAD_FACTOR = 0.75f;
    static final long EMPTY_GROUP_ID = -1L;

    // Distinct odd 64-bit multipliers, one per key column; the generated hash multiplies key i by
    // HASH_PRIMES[i], sums with nullMask, then applies a Murmur3 finalizer — matching the hand-written mixers.
    static final long[] HASH_PRIMES = {
            0x9E3779B97F4A7C15L,
            0xC4CEB9FE1A85EC53L,
            0xFF51AFD7ED558CCDL,
            0xD6E8FEB86659FD93L,
            0xA0761D6478BD642FL,
            0xE7037ED1A0B428DBL,
            0x8EBC6AF09C88C6E3L,
            0x589965CC75374CC3L,
    };

    static final int MAX_ARITY = HASH_PRIMES.length;

    final int arity;
    final int stride;
    long[] entries;
    byte[] nullMasks;
    int mask;
    int maxFill;
    int size;
    long[][] keysByGroup;
    byte[] nullMasksByGroup;

    AbstractMultiLongGroupingTable(int arity, int expectedSize)
    {
        this.arity = arity;
        this.stride = arity + 1;
        int capacity = 16;
        while (capacity < expectedSize / LOAD_FACTOR) {
            capacity <<= 1;
        }
        entries = allocateEntries(capacity);
        nullMasks = new byte[capacity];
        mask = capacity - 1;
        maxFill = (int) (capacity * LOAD_FACTOR);

        int reverse = Math.max(16, expectedSize);
        keysByGroup = new long[arity][];
        for (int column = 0; column < arity; column++) {
            keysByGroup[column] = new long[reverse];
        }
        nullMasksByGroup = new byte[reverse];
    }

    private long[] allocateEntries(int capacity)
    {
        long[] array = new long[capacity * stride];
        int groupIdOffset = arity;
        for (int slot = 0; slot < capacity; slot++) {
            array[slot * stride + groupIdOffset] = EMPTY_GROUP_ID;
        }
        return array;
    }

    /**
     * Assigns a group id to every active row in the batch: reads the {@code arity} key columns through the
     * supplied accessors, builds the null bitmask, probes/inserts, writes {@code result[position]}, and
     * records new group keys in the reverse map. Returns the next free group id. Generated per arity.
     */
    abstract long assignBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            long[] result,
            long startGroupId);

    /** Hash of the stored tuple at {@code table[base..base+arity-1]} + {@code nullMask}; used by rehash. */
    abstract int hashEntry(long[] table, int base, byte nullMask);

    void ensureCapacity(long expectedSize)
    {
        if (expectedSize < maxFill) {
            return;
        }
        int capacity = nullMasks.length;
        while (expectedSize >= (long) (capacity * LOAD_FACTOR)) {
            capacity <<= 1;
        }
        rehash(capacity);
    }

    final void rehash()
    {
        rehash(nullMasks.length * 2);
    }

    private void rehash(int capacity)
    {
        long[] previousEntries = entries;
        byte[] previousNullMasks = nullMasks;
        int previousCapacity = previousNullMasks.length;
        int groupIdOffset = arity;

        entries = allocateEntries(capacity);
        nullMasks = new byte[capacity];
        mask = capacity - 1;
        maxFill = (int) (capacity * LOAD_FACTOR);
        size = 0;

        for (int oldSlot = 0; oldSlot < previousCapacity; oldSlot++) {
            int previousBase = oldSlot * stride;
            long groupId = previousEntries[previousBase + groupIdOffset];
            if (groupId == EMPTY_GROUP_ID) {
                continue;
            }
            byte nullMask = previousNullMasks[oldSlot];
            int slot = hashEntry(previousEntries, previousBase, nullMask) & mask;
            while (entries[slot * stride + groupIdOffset] != EMPTY_GROUP_ID) {
                slot = (slot + 1) & mask;
            }
            int base = slot * stride;
            System.arraycopy(previousEntries, previousBase, entries, base, stride);
            nullMasks[slot] = nullMask;
            size++;
        }
    }

    /** Grows the reverse map to hold {@code groupId}. Called from generated code on each new group. */
    final void ensureReverseCapacity(int groupId)
    {
        if (groupId < nullMasksByGroup.length) {
            return;
        }
        int newSize = Math.max(16, nullMasksByGroup.length);
        while (groupId >= newSize) {
            newSize *= 2;
        }
        for (int column = 0; column < arity; column++) {
            keysByGroup[column] = Arrays.copyOf(keysByGroup[column], newSize);
        }
        nullMasksByGroup = Arrays.copyOf(nullMasksByGroup, newSize);
    }
}
