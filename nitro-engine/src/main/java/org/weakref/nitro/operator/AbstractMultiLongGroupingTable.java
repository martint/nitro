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
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

/**
 * Cold-path machinery shared by every generated multi-long grouping table. Ordinary and distinct tables hold an
 * interleaved {@code entries} key array. Grouping tables that retain keys for output use that reverse map as the
 * single canonical key copy; their hash slots contain only compact group ids and compare through the reverse map.
 *
 * <p>The arity-specialized hot work — {@link #assignBatch}, which reads the key columns into locals,
 * probes, and assigns groups for a whole batch, plus {@link #hashEntry} used by rehash — is generated
 * per arity by {@link MultiLongGroupingTableGenerator} so the emitted bytecode is structurally identical
 * to the former hand-written 2/3/4-key tables (keys in registers, no per-row dispatch or scratch array).
 * Fields are package-private so the generated same-package subclass can access them directly.
 */
abstract class AbstractMultiLongGroupingTable
        implements LongGroupingTable
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
    final boolean storesGroupIds;
    final boolean retainsGroupKeys;
    final boolean identityGroupIdSlots;
    private final boolean debugTableShapes;
    private final PrimitiveArrayPool arrayPool;
    long[] entries;
    byte[] nullMasks;
    int[] groupIds;
    // Swiss-table control byte per slot: 0 marks an empty slot, otherwise a 7-bit hash fragment with the high bit set
    // (always non-zero). The probe scans this dense array (one byte per slot, ~64 per cache line) and only reads the
    // fat key record on a fragment match, so a high-cardinality probe stays cache-resident where the records do not.
    byte[] control;
    int mask;
    int maxFill;
    int size;
    long[][] keysByGroup;
    int[][] compactKeysByGroup;
    byte[] nullMasksByGroup;

    /** The control fragment for {@code hash}: high byte with the top bit set so it is never the empty marker (0). */
    static byte controlFragment(int hash)
    {
        return (byte) ((hash >>> 24) | 0x80);
    }

    AbstractMultiLongGroupingTable(
            PrimitiveArrayPool arrayPool,
            int arity,
            int expectedSize,
            boolean storesGroupIds,
            boolean retainGroupKeys,
            int compactRetainedColumns,
            AdaptiveLongGroupingPolicy policy)
    {
        this.arrayPool = arrayPool;
        this.arity = arity;
        this.storesGroupIds = storesGroupIds;
        this.retainsGroupKeys = retainGroupKeys;
        this.identityGroupIdSlots = retainGroupKeys;
        this.debugTableShapes = policy.debugGeneratedTableShapes();
        this.stride = arity + (storesGroupIds ? 1 : 0);
        int capacity = 16;
        while (capacity < expectedSize / LOAD_FACTOR) {
            capacity <<= 1;
        }
        entries = identityGroupIdSlots ? new long[0] : allocateEntries(capacity);
        nullMasks = identityGroupIdSlots ? new byte[0] : arrayPool.borrowBytes(capacity);
        groupIds = identityGroupIdSlots ? arrayPool.borrowInts(capacity) : new int[0];
        control = arrayPool.borrowBytes(capacity);
        Arrays.fill(control, (byte) 0);
        mask = capacity - 1;
        maxFill = (int) (capacity * LOAD_FACTOR);

        if (retainsGroupKeys) {
            int reverse = Math.max(16, expectedSize);
            keysByGroup = new long[arity][];
            compactKeysByGroup = new int[arity][];
            for (int column = 0; column < arity; column++) {
                if ((compactRetainedColumns & (1 << column)) != 0) {
                    compactKeysByGroup[column] = arrayPool.borrowInts(reverse);
                }
                else {
                    keysByGroup[column] = arrayPool.borrowLongs(reverse);
                }
            }
            nullMasksByGroup = arrayPool.borrowBytes(reverse);
        }
    }

    private long[] allocateEntries(int capacity)
    {
        long[] array = arrayPool.borrowLongs(capacity * stride);
        if (storesGroupIds) {
            int groupIdOffset = stride - 1;
            for (int slot = 0; slot < capacity; slot++) {
                array[slot * stride + groupIdOffset] = EMPTY_GROUP_ID;
            }
        }
        return array;
    }

    /**
     * Assigns a group id to every active row in the batch: reads the {@code arity} key columns through the
     * supplied accessors, builds the null bitmask, probes/inserts, writes {@code result[position]}, and
     * records new group keys in the reverse map. Returns the next free group id. Generated per arity.
     */
    @Override
    public abstract long assignBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            long[] result,
            long startGroupId);

    @Override
    public abstract long assignBatchDiscardingResults(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            long startGroupId);

    /** Distinct-set variant of {@link #assignBatch}: writes only the first position for each newly inserted key. */
    abstract int assignDistinctBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            int[] distinctPositions,
            long startGroupId);

    /** Null-free variant that omits all null-accessor reads after the caller has proved every stream false. */
    abstract int assignDistinctBatchNullFree(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            int[] distinctPositions,
            long startGroupId);

    /** Hash of the stored tuple at {@code table[base..base+arity-1]} + {@code nullMask}; used by rehash. */
    abstract int hashEntry(long[] table, int base, byte nullMask);

    @Override
    public final int arity()
    {
        return arity;
    }

    @Override
    public final long groupedValue(int column, int groupId)
    {
        int[] compactKeys = compactKeysByGroup[column];
        return compactKeys == null ? keysByGroup[column][groupId] : compactKeys[groupId];
    }

    @Override
    public final boolean groupedValueIsNull(int column, int groupId)
    {
        return (nullMasksByGroup[groupId] & (1 << column)) != 0;
    }

    @Override
    public final void ensureCapacity(long expectedSize)
    {
        if (expectedSize < maxFill) {
            return;
        }
        int capacity = control.length;
        while (expectedSize >= (long) (capacity * LOAD_FACTOR)) {
            capacity <<= 1;
        }
        rehash(capacity);
    }

    final void rehash()
    {
        rehash(control.length * 2);
    }

    private void rehash(int capacity)
    {
        long[] previousEntries = entries;
        byte[] previousNullMasks = nullMasks;
        int[] previousGroupIds = groupIds;
        byte[] previousControl = control;
        int previousCapacity = previousControl.length;

        entries = identityGroupIdSlots ? new long[0] : allocateEntries(capacity);
        nullMasks = identityGroupIdSlots ? new byte[0] : arrayPool.borrowBytes(capacity);
        groupIds = identityGroupIdSlots ? arrayPool.borrowInts(capacity) : new int[0];
        control = arrayPool.borrowBytes(capacity);
        Arrays.fill(control, (byte) 0);
        mask = capacity - 1;
        maxFill = (int) (capacity * LOAD_FACTOR);
        size = 0;

        for (int oldSlot = 0; oldSlot < previousCapacity; oldSlot++) {
            if (previousControl[oldSlot] == 0) {
                continue;
            }
            int groupId = identityGroupIdSlots ? previousGroupIds[oldSlot] : -1;
            int previousBase = oldSlot * stride;
            byte nullMask = identityGroupIdSlots ? 0 : previousNullMasks[oldSlot];
            int hash = identityGroupIdSlots ? hashRetainedGroup(groupId) : hashEntry(previousEntries, previousBase, nullMask);
            int slot = hash & mask;
            while (control[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            if (identityGroupIdSlots) {
                groupIds[slot] = groupId;
            }
            else {
                int base = slot * stride;
                System.arraycopy(previousEntries, previousBase, entries, base, stride);
                nullMasks[slot] = nullMask;
            }
            control[slot] = controlFragment(hash);
            size++;
        }
        arrayPool.release(previousEntries);
        arrayPool.release(previousNullMasks);
        arrayPool.release(previousGroupIds);
        arrayPool.release(previousControl);
    }

    private int hashRetainedGroup(int groupId)
    {
        long hash = nullMasksByGroup[groupId];
        for (int column = 0; column < arity; column++) {
            hash += groupedValue(column, groupId) * HASH_PRIMES[column];
        }
        hash ^= hash >>> 33;
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= hash >>> 33;
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;
        return (int) hash;
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
            int[] previousCompact = compactKeysByGroup[column];
            if (previousCompact != null) {
                compactKeysByGroup[column] = arrayPool.borrowInts(newSize);
                System.arraycopy(previousCompact, 0, compactKeysByGroup[column], 0, previousCompact.length);
                arrayPool.release(previousCompact);
            }
            else {
                long[] previous = keysByGroup[column];
                keysByGroup[column] = arrayPool.borrowLongs(newSize);
                System.arraycopy(previous, 0, keysByGroup[column], 0, previous.length);
                arrayPool.release(previous);
            }
        }
        byte[] previousNullMasks = nullMasksByGroup;
        nullMasksByGroup = arrayPool.borrowBytes(newSize);
        System.arraycopy(previousNullMasks, 0, nullMasksByGroup, 0, previousNullMasks.length);
        arrayPool.release(previousNullMasks);
    }

    /** Stores an output key at its admitted width, widening exactly if a later batch requires it. */
    final void storeCompactRetainedKey(int column, int groupId, long value)
    {
        int[] compactKeys = compactKeysByGroup[column];
        if (compactKeys == null) {
            keysByGroup[column][groupId] = value;
            return;
        }
        if ((long) (int) value == value) {
            compactKeys[groupId] = (int) value;
            return;
        }

        long[] widened = arrayPool.borrowLongs(compactKeys.length);
        for (int index = 0; index < size; index++) {
            widened[index] = compactKeys[index];
        }
        arrayPool.release(compactKeys);
        compactKeysByGroup[column] = null;
        keysByGroup[column] = widened;
        widened[groupId] = value;
    }

    @Override
    public final long retainedBytes()
    {
        long bytes = entries == null ? 0 : (long) entries.length * Long.BYTES;
        bytes += nullMasks == null ? 0 : nullMasks.length;
        bytes += groupIds == null ? 0 : (long) groupIds.length * Integer.BYTES;
        bytes += control == null ? 0 : control.length;
        if (keysByGroup != null) {
            bytes += (long) keysByGroup.length * Long.BYTES;
            for (int column = 0; column < keysByGroup.length; column++) {
                long[] keys = keysByGroup[column];
                int[] compactKeys = compactKeysByGroup[column];
                bytes += keys == null ? (long) compactKeys.length * Integer.BYTES : (long) keys.length * Long.BYTES;
            }
        }
        bytes += nullMasksByGroup == null ? 0 : nullMasksByGroup.length;
        return bytes;
    }

    @Override
    public final void releaseBuffers()
    {
        if (debugTableShapes && retainsGroupKeys) {
            System.err.printf(
                    "[multi-long-table] arity=%d groups=%d capacity=%d stride=%d storesGroupIds=%s identitySlots=%s%n",
                    arity,
                    size,
                    control.length,
                    stride,
                    storesGroupIds,
                    identityGroupIdSlots);
        }
        arrayPool.release(entries);
        entries = null;
        arrayPool.release(nullMasks);
        nullMasks = null;
        arrayPool.release(groupIds);
        groupIds = null;
        arrayPool.release(control);
        control = null;
        if (keysByGroup != null) {
            for (int column = 0; column < arity; column++) {
                arrayPool.release(keysByGroup[column]);
                keysByGroup[column] = null;
                arrayPool.release(compactKeysByGroup[column]);
                compactKeysByGroup[column] = null;
            }
            keysByGroup = null;
            compactKeysByGroup = null;
        }
        if (nullMasksByGroup != null) {
            arrayPool.release(nullMasksByGroup);
        }
        nullMasksByGroup = null;
    }
}
