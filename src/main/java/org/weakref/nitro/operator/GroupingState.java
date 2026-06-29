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

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

final class GroupingState
{
    private final Object2LongMap<OperatorKeySemantics.Key> groups = new Object2LongOpenHashMap<>();
    // Single-long grouping key -> group id, as an open-addressed table probed with one fused
    // find-or-insert per row. A slot is empty iff its id is -1; ids are dense, assigned in first-seen
    // scan order (the hash only chooses the slot, never the id). longKeysByGroup is the reverse map.
    private static final float LONG_GROUP_LOAD_FACTOR = 0.75f;
    // Package-private so the fused grouped-aggregation kernel (operator package) can inline the probe over
    // this table directly instead of paying a per-row method call.
    long[] longGroupKeys;
    int[] longGroupIds;
    int longGroupMask;
    private int longGroupMaxFill;
    int longGroupCount;
    private final ArrayList<ArrayList<OperatorKeySemantics.Key>> keysByGroupColumns = new ArrayList<>();
    private OperatorKeySemantics.Key[] reusableProbeKeys;
    private OperatorKeySemantics.CompositeProbeKey reusableCompositeProbeKey;
    private FlatGroupingTable flatGroupingTable;
    private FlatTypeHandler[] keyHandlers;
    private Set<BinaryVector.Trait>[] binaryTraits;
    long[] longKeysByGroup = new long[0];
    private Vector cachedDictionaryValues;
    private long[] dictionaryGroupsById = new long[0];
    private int[] dictionaryGenerations = new int[0];
    private int dictionaryGeneration;
    long nextGroupId;
    private long nullGroup = -1;
    private boolean useLongGrouping;
    private boolean useMultiLongGrouping;
    private int multiLongArity;
    private int[] densePositionsCache = new int[0];
    private boolean useFlatGrouping;
    private boolean initialized;
    private AbstractMultiLongGroupingTable multiLongTable;

    GroupingState()
    {
        groups.defaultReturnValue(-1);
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    /** Number of distinct groups assigned so far; the max assigned group id is {@code groupCount() - 1}. */
    public long groupCount()
    {
        return nextGroupId;
    }

    /** True when grouping on a single long-packable key — the precondition for the fused kernel. */
    boolean usesSingleLongGrouping()
    {
        return useLongGrouping;
    }

    /**
     * Pre-grows the single-long table so it can absorb {@code additional} more keys without rehashing, so
     * the fused kernel's inlined probe needs no mid-loop rehash branch. Requires {@link #usesSingleLongGrouping}.
     */
    void reserveSingleLongTable(int additional)
    {
        while (longGroupCount + additional >= longGroupMaxFill) {
            rehashLongGroupTable();
        }
        // Pre-size the reverse map too, so the fused probe can write longKeysByGroup[gid] without a check.
        ensureLongGroupingCapacity(nextGroupId + additional);
    }

    public void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result)
    {
        assignGroups(new Vector[] {values}, new Vector[] {nulls}, mask, result);
    }

    /**
     * Registers the probe batch's key column before a run of {@link #contains} calls. Required for the flat
     * grouping path, whose per-batch dictionary hash/bound-dictionary cache is keyed to the batch's vectors;
     * probing without it would read the previous (membership-build) batch's stale cache. Must be paired with
     * {@link #endContainsBatch}. A no-op for the long/dictionary/hash-map paths.
     */
    public void beginContainsBatch(Vector values, Vector nulls)
    {
        initializeIfNecessary(new Vector[] {values}, new Vector[] {nulls});
        if (useFlatGrouping) {
            flatGroupingTable.beginBatch(new Vector[] {values}, new Vector[] {nulls});
        }
    }

    public void endContainsBatch()
    {
        if (useFlatGrouping) {
            flatGroupingTable.endBatch();
        }
    }

    public boolean contains(Vector values, Vector nulls, int position)
    {
        initializeIfNecessary(new Vector[] {values}, new Vector[] {nulls});
        if (OperatorVectorSupport.isNull(nulls, position)) {
            return false;
        }
        if (useLongGrouping) {
            return longGroupGet(OperatorVectorSupport.longValue(values, position)) != -1;
        }
        if (useFlatGrouping) {
            return flatGroupingTable.findGroup(new Vector[] {values}, new Vector[] {nulls}, position) != -1;
        }

        OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values, nulls, position, reusableProbeKeys[0]);
        return key != null && groups.getLong(key) != -1;
    }

    public void initializeSchema(Vector[] values, Vector[] nulls)
    {
        initializeIfNecessary(values, nulls);
    }

    @SuppressWarnings("unchecked")
    public void assignGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        initializeIfNecessary(values, nulls);
        reserveAdditionalGroups(mask.count() + 1L);
        if (useLongGrouping) {
            assignLongGroups(values[0], nulls[0], mask, result);
            return;
        }
        if (useMultiLongGrouping) {
            assignMultiLongGroups(values, nulls, mask, result);
            return;
        }
        if (useFlatGrouping) {
            assignFlatGroups(values, nulls, mask, result);
            return;
        }
        if (values.length == 1 && values[0] instanceof DictionaryVector dictionary) {
            assignDictionaryGroups(dictionary, nulls[0], mask, result);
            return;
        }

        OperatorKeySemantics.Key[] probeKeys = new OperatorKeySemantics.Key[values.length];
        for (int position : mask) {
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                probeKeys[keyIndex] = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, reusableProbeKeys[keyIndex]);
            }
            result.values()[position] = groupForKeys(probeKeys);
        }
    }

    private void reserveAdditionalGroups(long additionalGroups)
    {
        if (additionalGroups <= 0) {
            return;
        }

        long expectedSize = nextGroupId + additionalGroups;
        if (useMultiLongGrouping) {
            multiLongTable.ensureCapacity(expectedSize);
        }
    }

    private void initializeIfNecessary(Vector[] values, Vector[] nulls)
    {
        if (initialized) {
            return;
        }
        initialized = true;

        keyHandlers = new FlatTypeHandler[values.length];
        binaryTraits = (Set<BinaryVector.Trait>[]) new Set<?>[values.length];
        for (int index = 0; index < values.length; index++) {
            keyHandlers[index] = FlatTypeHandlers.forVector(values[index]);
            binaryTraits[index] = OperatorVectorSupport.binaryTraits(values[index]);
        }

        if (values.length == 1 && isSingleLongGroupingCandidate(values[0])) {
            useLongGrouping = true;
            initLongGroupTable(Math.max(16, values[0].length()));
            return;
        }
        boolean nullableCompositeKeys = values.length > 1 && hasNullableKeys(nulls);
        if (values.length >= 2 && values.length <= AbstractMultiLongGroupingTable.MAX_ARITY && allSingleLongGroupingCandidates(values)) {
            if (values.length == 2 && Boolean.getBoolean("nitro.experiment.useFlatBigintPairStrategy")) {
                useFlatGrouping = true;
                flatGroupingTable = new FlatGroupingTable(
                        BigintPairFlatKeyLayout.create(values, nullableCompositeKeys),
                        Math.max(16, values[0].length()));
                return;
            }
            // Generate (once per arity) a grouping table specialized to this many long keys — the row loop
            // is emitted as bytecode so the keys live in registers exactly like the former 2/3/4-key tables.
            useMultiLongGrouping = true;
            multiLongArity = values.length;
            multiLongTable = MultiLongGroupingTableGenerator.create(values.length, Math.max(16, values[0].length()));
            return;
        }

        FlatKeyLayout flatKeyLayout = FlatKeyLayout.tryCreate(values, nullableCompositeKeys);
        if (flatKeyLayout != null) {
            useFlatGrouping = true;
            flatGroupingTable = new FlatGroupingTable(flatKeyLayout, Math.max(16, values[0].length()));
            return;
        }

        reusableProbeKeys = new OperatorKeySemantics.Key[values.length];
        for (int index = 0; index < values.length; index++) {
            reusableProbeKeys[index] = OperatorKeySemantics.reusableProbeKey(values[index]);
            keysByGroupColumns.add(new ArrayList<>());
        }
        if (values.length > 1) {
            reusableCompositeProbeKey = OperatorKeySemantics.reusableCompositeProbeKey(values.length);
        }
    }

    private void assignFlatGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        flatGroupingTable.beginBatch(values, nulls);
        flatGroupingTable.prepareBatchHashes(values, nulls, mask);
        try {
            if (values.length == 1) {
                Vector nullVector = nulls[0];
                for (int position : mask) {
                    if (OperatorVectorSupport.isNull(nullVector, position)) {
                        result.values()[position] = nullGroup();
                    }
                    else {
                        long newGroupId = nextGroupId;
                        long groupId = flatGroupingTable.assignGroup(values, nulls, position, newGroupId);
                        if (groupId == newGroupId) {
                            nextGroupId++;
                        }
                        result.values()[position] = groupId;
                    }
                }
                return;
            }

            for (int position : mask) {
                long newGroupId = nextGroupId;
                long groupId = flatGroupingTable.assignGroup(values, nulls, position, newGroupId);
                if (groupId == newGroupId) {
                    nextGroupId++;
                }
                result.values()[position] = groupId;
            }
        }
        finally {
            flatGroupingTable.endBatch();
        }
    }

    private void assignLongGroups(Vector values, Vector nullVector, Mask mask, I64Vector result)
    {
        // Hoist Vector type dispatch once per batch so the per-position loop body reads
        // through monomorphic accessor lambdas instead of OperatorVectorSupport's switch.
        VectorAccess.LongValues keyValues = VectorAccess.longValues(values);
        VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nullVector);
        long[] out = result.values();
        // Hoist the table into locals; refresh after a rehash.
        long[] tableKeys = longGroupKeys;
        int[] tableIds = longGroupIds;
        int tableMask = longGroupMask;
        for (int position : mask) {
            if (nullValues.value(position)) {
                out[position] = nullGroup();
                continue;
            }

            long key = keyValues.value(position);
            int slot = hashLong(key) & tableMask;
            while (true) {
                int id = tableIds[slot];
                if (id == -1) {
                    int groupId = (int) nextGroupId++;
                    tableKeys[slot] = key;
                    tableIds[slot] = groupId;
                    ensureLongGroupingCapacity(groupId);
                    longKeysByGroup[groupId] = key;
                    out[position] = groupId;
                    if (++longGroupCount >= longGroupMaxFill) {
                        rehashLongGroupTable();
                        tableKeys = longGroupKeys;
                        tableIds = longGroupIds;
                        tableMask = longGroupMask;
                    }
                    break;
                }
                if (tableKeys[slot] == key) {
                    out[position] = id;
                    break;
                }
                slot = (slot + 1) & tableMask;
            }
        }
    }

    private void initLongGroupTable(int expectedSize)
    {
        int capacity = 16;
        while (capacity < expectedSize / LONG_GROUP_LOAD_FACTOR) {
            capacity <<= 1;
        }
        longGroupKeys = new long[capacity];
        longGroupIds = new int[capacity];
        Arrays.fill(longGroupIds, -1);
        longGroupMask = capacity - 1;
        longGroupMaxFill = (int) (capacity * LONG_GROUP_LOAD_FACTOR);
        longGroupCount = 0;
    }

    private void rehashLongGroupTable()
    {
        long[] previousKeys = longGroupKeys;
        int[] previousIds = longGroupIds;
        int capacity = previousKeys.length * 2;
        longGroupKeys = new long[capacity];
        longGroupIds = new int[capacity];
        Arrays.fill(longGroupIds, -1);
        longGroupMask = capacity - 1;
        longGroupMaxFill = (int) (capacity * LONG_GROUP_LOAD_FACTOR);
        for (int index = 0; index < previousKeys.length; index++) {
            int id = previousIds[index];
            if (id == -1) {
                continue;
            }
            long key = previousKeys[index];
            int slot = hashLong(key) & longGroupMask;
            while (longGroupIds[slot] != -1) {
                slot = (slot + 1) & longGroupMask;
            }
            longGroupKeys[slot] = key;
            longGroupIds[slot] = id;
        }
    }

    private int longGroupGet(long key)
    {
        int slot = hashLong(key) & longGroupMask;
        while (true) {
            int id = longGroupIds[slot];
            if (id == -1 || longGroupKeys[slot] == key) {
                return id;
            }
            slot = (slot + 1) & longGroupMask;
        }
    }

    static int hashLong(long key)
    {
        long hash = key ^ (key >>> 33);
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= hash >>> 33;
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;
        return (int) hash;
    }

    private static boolean allSingleLongGroupingCandidates(Vector[] values)
    {
        for (Vector value : values) {
            if (!isSingleLongGroupingCandidate(value)) {
                return false;
            }
        }
        return true;
    }

    private void assignMultiLongGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        int arity = multiLongArity;
        VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
        VectorAccess.BooleanValues[] nullAccessors = new VectorAccess.BooleanValues[arity];
        for (int key = 0; key < arity; key++) {
            keyAccessors[key] = VectorAccess.longValues(values[key]);
            nullAccessors[key] = VectorAccess.booleanValues(nulls[key]);
        }
        int[] positions = mask.all() ? densePositions(mask.size()) : mask.selectedPositions();
        nextGroupId = multiLongTable.assignBatch(keyAccessors, nullAccessors, positions, mask.count(), result.values(), nextGroupId);
    }

    // Reused 0..size-1 index array for the all-selected case (the generated batch kernel takes an int[]).
    private int[] densePositions(int size)
    {
        if (densePositionsCache.length < size) {
            densePositionsCache = new int[size];
            for (int index = 0; index < size; index++) {
                densePositionsCache[index] = index;
            }
        }
        return densePositionsCache;
    }

    private void assignDictionaryGroups(DictionaryVector dictionary, Vector nullVector, Mask mask, I64Vector result)
    {
        int[] ids = dictionary.ids();
        Vector dictionaryValues = dictionary.values();
        ensureDictionaryCacheCapacity(dictionaryValues.length());
        int generation = currentDictionaryGeneration(dictionaryValues);

        for (int position : mask) {
            if (OperatorVectorSupport.isNull(nullVector, position)) {
                result.values()[position] = nullGroup();
                continue;
            }

            int dictionaryId = ids[position];
            if (dictionaryGenerations[dictionaryId] != generation) {
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(dictionaryValues, null, dictionaryId, reusableProbeKeys[0]);
                dictionaryGroupsById[dictionaryId] = groupForSingleKey(key);
                dictionaryGenerations[dictionaryId] = generation;
            }
            result.values()[position] = dictionaryGroupsById[dictionaryId];
        }
    }

    private void ensureDictionaryCacheCapacity(int size)
    {
        if (dictionaryGroupsById.length >= size) {
            return;
        }
        int newSize = Math.max(size, Math.max(16, dictionaryGroupsById.length * 2));
        dictionaryGroupsById = Arrays.copyOf(dictionaryGroupsById, newSize);
        dictionaryGenerations = Arrays.copyOf(dictionaryGenerations, newSize);
    }

    private int currentDictionaryGeneration(Vector dictionaryValues)
    {
        if (cachedDictionaryValues != dictionaryValues) {
            cachedDictionaryValues = dictionaryValues;
            if (dictionaryGeneration == Integer.MAX_VALUE) {
                Arrays.fill(dictionaryGenerations, 0);
                dictionaryGeneration = 0;
            }
            return ++dictionaryGeneration;
        }
        return dictionaryGeneration;
    }

    public Streams groupedValues(int groupedColumnIndex, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        if (useLongGrouping) {
            return Streams.ofValuesAndNulls(
                    materializeLongGroupedValues(mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeLongNulls(mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        }
        if (useMultiLongGrouping) {
            return Streams.ofValuesAndNulls(
                    materializeMultiLongGroupedValues(groupedColumnIndex, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeCompositeLongNulls(multiLongTable.nullMasksByGroup, groupedColumnIndex, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        }
        if (useFlatGrouping) {
            return flatGroupingTable.groupedValues(groupedColumnIndex, mask, output, allocator, allocationContext);
        }
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        List<OperatorKeySemantics.Key> keysByGroup = keysByGroupColumns.get(groupedColumnIndex);
        Streams values = OperatorKeySemantics.materializeGroupedValues(
                keyHandlers[groupedColumnIndex],
                size,
                mask,
                keysByGroup,
                output == null ? null : output.values(),
                allocator,
                allocationContext,
                binaryTraits[groupedColumnIndex]);
        return Streams.ofValuesAndNulls(values.values(), materializeNulls(size, mask, keysByGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
    }

    private I64Vector materializeLongGroupedValues(Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index != nullGroup && index < longKeysByGroup.length) {
                result.values()[index] = longKeysByGroup[index];
            }
        }
        return result;
    }

    private BooleanVector materializeLongNulls(Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            result.values()[index] = index == nullGroup;
        }
        return result;
    }

    private BooleanVector materializeCompositeLongNulls(byte[] nullMasksByGroup, int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            result.values()[index] = index < nullMasksByGroup.length && isNull(nullMasksByGroup, index, groupedColumnIndex);
        }
        return result;
    }

    private I64Vector materializeMultiLongGroupedValues(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        long[] keysByGroup = multiLongTable.keysByGroup[groupedColumnIndex];
        byte[] nullMasksByGroup = multiLongTable.nullMasksByGroup;
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index < keysByGroup.length && !isNull(nullMasksByGroup, index, groupedColumnIndex)) {
                result.values()[index] = keysByGroup[index];
            }
        }
        return result;
    }

    private long groupForKeys(OperatorKeySemantics.Key[] probeKeys)
    {
        if (probeKeys.length == 1) {
            return groupForSingleKey(probeKeys[0]);
        }

        if (containsNullKey(probeKeys)) {
            OperatorKeySemantics.Key[] ownedKeys = new OperatorKeySemantics.Key[probeKeys.length];
            for (int index = 0; index < probeKeys.length; index++) {
                ownedKeys[index] = OperatorKeySemantics.ownedKey(probeKeys[index]);
            }
            OperatorKeySemantics.CompositeKey compositeKey = new OperatorKeySemantics.CompositeKey(ownedKeys);
            long group = groups.getLong(compositeKey);
            if (group != -1) {
                return group;
            }
            for (int index = 0; index < probeKeys.length; index++) {
                keysByGroupColumns.get(index).add(ownedKeys[index]);
            }
            groups.put(compositeKey, nextGroupId);
            return nextGroupId++;
        }

        OperatorKeySemantics.Key compositeKey = OperatorKeySemantics.probeCompositeKey(probeKeys, reusableCompositeProbeKey);
        long group = groups.getLong(compositeKey);
        if (group != -1) {
            return group;
        }

        OperatorKeySemantics.Key[] ownedKeys = new OperatorKeySemantics.Key[probeKeys.length];
        for (int index = 0; index < probeKeys.length; index++) {
            ownedKeys[index] = OperatorKeySemantics.ownedKey(probeKeys[index]);
            keysByGroupColumns.get(index).add(ownedKeys[index]);
        }
        groups.put(new OperatorKeySemantics.CompositeKey(ownedKeys), nextGroupId);
        return nextGroupId++;
    }

    private long groupForSingleKey(OperatorKeySemantics.Key key)
    {
        long group = groups.getLong(key);
        if (group != -1) {
            return group;
        }

        OperatorKeySemantics.Key ownedKey = OperatorKeySemantics.ownedKey(key);
        groups.put(ownedKey, nextGroupId);
        keysByGroupColumns.get(0).add(ownedKey);
        return nextGroupId++;
    }

    private long nullGroup()
    {
        if (nullGroup == -1) {
            if (!useFlatGrouping && !useLongGrouping && !useMultiLongGrouping) {
                for (ArrayList<OperatorKeySemantics.Key> keysByGroup : keysByGroupColumns) {
                    keysByGroup.add(null);
                }
            }
            nullGroup = nextGroupId++;
        }
        return nullGroup;
    }

    private static boolean hasNull(Vector[] nulls, int position)
    {
        for (Vector nullVector : nulls) {
            if (OperatorVectorSupport.isNull(nullVector, position)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNull(Vector nulls, int position)
    {
        return nulls != null && OperatorVectorSupport.isNull(nulls, position);
    }

    private static byte nullMask(Vector[] nulls, int position)
    {
        byte mask = 0;
        for (int index = 0; index < nulls.length; index++) {
            if (isNull(nulls[index], position)) {
                mask |= (byte) (1 << index);
            }
        }
        return mask;
    }

    private static boolean isNull(byte[] nullMasksByGroup, int groupId, int groupedColumnIndex)
    {
        return (nullMasksByGroup[groupId] & (1 << groupedColumnIndex)) != 0;
    }

    private static boolean hasNullableKeys(Vector[] nulls)
    {
        for (Vector nullVector : nulls) {
            if (nullVector != null) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsNullKey(OperatorKeySemantics.Key[] keys)
    {
        for (OperatorKeySemantics.Key key : keys) {
            if (key == null) {
                return true;
            }
        }
        return false;
    }

    private BooleanVector materializeNulls(int size, Mask mask, List<OperatorKeySemantics.Key> keysByGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            result.values()[index] = keysByGroup.get(index) == null;
        }
        return result;
    }

    private void ensureLongGroupingCapacity(long groupId)
    {
        if (groupId < longKeysByGroup.length) {
            return;
        }
        int newSize = Math.max(16, longKeysByGroup.length);
        while (groupId >= newSize) {
            newSize *= 2;
        }
        longKeysByGroup = Arrays.copyOf(longKeysByGroup, newSize);
    }

    private static boolean isSingleLongGroupingCandidate(Vector values)
    {
        FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
        return handler != null && handler.kind() == FlatTypeHandler.Kind.LONG;
    }
}
