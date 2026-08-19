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
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class GroupingState
{
    private static final VectorAccess.BooleanValues NEVER_NULL = _ -> false;
    private static final int PACKED_GROUP_ID_MASK = 0x00FF_FFFF;
    private static final int PACKED_INT_TRIPLE_MIN_THIRD = -(1 << 28);
    private static final int PACKED_INT_TRIPLE_MAX_THIRD = (1 << 28) - 1;
    private final Object2LongMap<OperatorKeySemantics.Key> groups = new Object2LongOpenHashMap<>();
    private final PrimitiveArrayPool arrayPool;
    private final OperatorCodeGenerationResources codeGeneration;
    private final GroupingStateResources resources;
    private final AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy;
    private final FlatKeyTablePolicy flatKeyTablePolicy;
    private final List<TypeBinding> keyTypes;
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final StructuralKeyKernel[] structuralKeyKernels;
    private final boolean allowsLegacyKeyShortcuts;
    private final LongGroupingPolicy longPolicy;
    private final CompositeGroupingPolicy compositePolicy;
    // Single-long grouping key -> group id, as an open-addressed table probed with one fused find-or-insert per
    // row. Ordinary slots keep parallel keys and use -1 ids as empty. A proven high-cardinality shape may instead
    // pack six hash bits plus group+1 into the id slot (zero is empty) and resolve exact equality via
    // longKeysByGroup. Ids remain dense first-seen order in both layouts; the hash only chooses the slot.
    private static final float LONG_GROUP_LOAD_FACTOR = 0.75f;
    private static final int ID_INDEXED_LONG_GROUP_MASK = 0x03FF_FFFF;
    private static final int ID_INDEXED_LONG_HASH_SHIFT = 26;
    // Staged aggregation can amortize migration over a much larger high-cardinality stream without coupling a
    // direct lookup to every accumulator update. Keep this admission separate from the fused kernel: broadening
    // the fused range reduced wall time on some shapes but substantially increased retired work and allocation.
    // Recycling a direct table through the dedicated zeroed family clears only occupied keys. That removes the
    // capacity-sized fill which made a somewhat wider sparse domain unprofitable, so admit up to 13 slots/group
    // with that lifecycle. The ordinary generic pool retains the established 11.5 slots/group threshold.
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
    private FlatKeyLayout flatGroupingLayout;
    private FlatTypeHandler[] keyHandlers;
    private Set<BinaryVector.Trait>[] binaryTraits;
    long[] longKeysByGroup = new long[0];
    private Vector cachedDictionaryValues;
    private long cachedDictionaryContentGeneration = -1;
    private long[] dictionaryGroupsById = new long[0];
    private int[] dictionaryGenerations = new int[0];
    private int dictionaryGeneration;
    private Vector[] cachedSharedDictionaryValues = new Vector[0];
    private long[] cachedSharedDictionaryGenerations = new long[0];
    // Packed (generation, group id) entries avoid two independent random cache-array probes by dictionary id.
    private long[] sharedDictionaryEntriesById = new long[0];
    private int sharedDictionaryGeneration;
    long nextGroupId;
    private long nullGroup = -1;
    private boolean useLongGrouping;
    private boolean useIdIndexedLongGrouping;
    private int[] longGroupHashes = new int[0];
    private boolean longRunCacheValid;
    private long longRunCacheKey;
    private int longRunCacheGroupId;
    private boolean useLongDirectGrouping;
    private boolean useCompressedLongDirectGrouping;
    private long longDirectCompressionMask = -1;
    private long longDirectConstantBits;
    private boolean longDirectGroupingDisabled;
    private boolean stagedLongDirectGroupingDisabled;
    private int longDirectNextCheck;
    private boolean usePackedIntPairGrouping;
    private int packedIntGroupingArity;
    private byte[] packedIntPairControl;
    private int[] packedIntTripleThirdByGroup = new int[0];
    private byte[] packedIntTripleNullMasksByGroup = new byte[0];
    private int[] packedIntTripleTailByGroup = new int[0];
    private boolean useMultiLongGrouping;
    private boolean useFullWidthPairPackedIdentity;
    private int multiLongArity;
    private int[] densePositionsCache = new int[0];
    private boolean useFlatGrouping;
    private boolean flatSingleIdentityAdmissionDecided;
    private boolean flatPackedIdentityAdmissionDecided;
    private boolean moreInputExpectedForCurrentBatch;
    private boolean blockingAggregationForCurrentBatch;
    private boolean flatSingleNullInTable;
    private int flatGroupingBatchCount;
    private boolean useSharedDictionaryGrouping;
    private boolean sharedDictionaryFlatBacking;
    private boolean initialized;
    private LongGroupingTable multiLongTable;
    private boolean discardMultiLongResults;
    private StructuralGroupingIndex structuralGrouping;

    GroupingState(
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            GroupingStateResources resources,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        this(arrayPool, codeGeneration, resources, adaptiveLongGroupingPolicy, flatKeyTablePolicy, List.of(), null, null);
    }

    GroupingState(
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            GroupingStateResources resources,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        this.arrayPool = arrayPool;
        this.codeGeneration = codeGeneration;
        this.resources = resources;
        this.adaptiveLongGroupingPolicy = adaptiveLongGroupingPolicy;
        this.flatKeyTablePolicy = flatKeyTablePolicy;
        this.keyTypes = List.copyOf(keyTypes);
        this.allocator = allocator;
        this.allocationContext = allocationContext;
        this.structuralKeyKernels = structuralKeyKernels(this.keyTypes, codeGeneration);
        this.allowsLegacyKeyShortcuts = allowsLegacyPhysicalShortcuts(structuralKeyKernels);
        this.longPolicy = resources.longGroupingPolicy();
        this.compositePolicy = resources.compositeGroupingPolicy();
        this.longDirectNextCheck = longPolicy.directMinGroups();
        groups.defaultReturnValue(-1);
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    boolean usesPackedFlatIdentitySlots()
    {
        return useFlatGrouping && flatGroupingTable.usesPackedHashRecordSlots();
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

    boolean prepareSingleLongDirectGrouping(
            Mask mask,
            Object keyValues,
            boolean intKey,
            int[] keyIds,
            boolean useStagedRange)
    {
        if (!longPolicy.direct() || longDirectGroupingDisabled) {
            return false;
        }
        // The generated fused kernel indexes raw keys directly. Compressed direct tables are admitted only after
        // that fused phase has ended; if an encoded shape later re-enters the fused path, restore the exact hash
        // representation before it can observe the table.
        if (useCompressedLongDirectGrouping) {
            disableLongDirectGrouping();
            longDirectGroupingDisabled = true;
            return false;
        }
        if (!useLongDirectGrouping && nextGroupId < longDirectNextCheck) {
            return false;
        }
        // A large first batch can cross the ordinary latest-admission threshold before there has been any
        // opportunity to inspect the domain. Always allow that first check; only a later failed check closes
        // admission. This remains bounded to one historical-key pass for a sparse/high-key workload.
        int maxRange = useStagedRange ? longPolicy.stagedDirectMaxRange() : longPolicy.directMaxRange();
        int latestAdmissionGroups = useStagedRange
                ? longPolicy.stagedDirectLatestAdmissionGroups()
                : longPolicy.directLatestAdmissionGroups();
        if (!useLongDirectGrouping && nextGroupId > latestAdmissionGroups &&
                longDirectNextCheck != longPolicy.directMinGroups()) {
            longDirectGroupingDisabled = true;
            return false;
        }

        long batchMax = -1;
        int[] positions = mask.selectedPositions();
        int count = mask.count();
        for (int index = 0; index < count; index++) {
            int position = positions == null ? index : positions[index];
            int keyPosition = keyIds == null ? position : keyIds[position];
            long key = intKey ? ((int[]) keyValues)[keyPosition] : ((long[]) keyValues)[keyPosition];
            if (key < 0 || key >= maxRange) {
                disableLongDirectGrouping();
                longDirectGroupingDisabled = true;
                return false;
            }
            batchMax = Math.max(batchMax, key);
        }

        return prepareSingleLongDirectGrouping(mask.count(), batchMax, maxRange);
    }

    private boolean prepareSingleLongDirectGrouping(
            Mask mask,
            VectorAccess.LongValues keyValues,
            VectorAccess.BooleanValues nullValues)
    {
        if (!longPolicy.direct() || longDirectGroupingDisabled ||
                (stagedLongDirectGroupingDisabled && !longPolicy.stagedCompressedDirect())) {
            return false;
        }
        if (!useLongDirectGrouping && nextGroupId < longDirectNextCheck) {
            return false;
        }
        int latestAdmissionGroups = longPolicy.stagedCompressedDirect()
                ? longPolicy.stagedCompressedDirectLatestAdmissionGroups()
                : longPolicy.stagedDirectLatestAdmissionGroups();
        if (!useLongDirectGrouping && nextGroupId > latestAdmissionGroups &&
                longDirectNextCheck != longPolicy.directMinGroups()) {
            longDirectGroupingDisabled = true;
            return false;
        }

        long batchMax = -1;
        boolean rawDomain = true;
        int[] positions = mask.selectedPositions();
        int count = mask.count();
        for (int index = 0; index < count; index++) {
            int position = positions == null ? index : positions[index];
            if (nullValues.value(position)) {
                continue;
            }
            long key = keyValues.value(position);
            if (key < 0 || key >= longPolicy.stagedDirectMaxRange()) {
                rawDomain = false;
            }
            batchMax = Math.max(batchMax, key);
        }
        if (useCompressedLongDirectGrouping) {
            return validateCompressedLongDirectBatch(mask, keyValues, nullValues);
        }
        if (!stagedLongDirectGroupingDisabled &&
                rawDomain &&
                prepareSingleLongDirectGrouping(mask.count(), batchMax, longPolicy.stagedDirectMaxRange())) {
            return true;
        }
        if (longPolicy.stagedCompressedDirect() &&
                nextGroupId < longPolicy.stagedCompressedDirectMinGroups() &&
                (!rawDomain || stagedLongDirectGroupingDisabled)) {
            longDirectNextCheck = longPolicy.stagedCompressedDirectMinGroups();
            return false;
        }
        if (longPolicy.stagedCompressedDirect() && nextGroupId >= longPolicy.stagedCompressedDirectMinGroups()) {
            return prepareCompressedLongDirectGrouping(mask, keyValues, nullValues);
        }
        if (!rawDomain && !longPolicy.stagedCompressedDirect()) {
            disableLongDirectGrouping();
            longDirectGroupingDisabled = true;
        }
        return false;
    }

    private boolean prepareCompressedLongDirectGrouping(
            Mask mask,
            VectorAccess.LongValues keyValues,
            VectorAccess.BooleanValues nullValues)
    {
        long andBits = -1;
        long orBits = 0;
        boolean observed = false;
        for (int group = 0; group < nextGroupId; group++) {
            if (group == nullGroup) {
                continue;
            }
            long key = longKeysByGroup[group];
            if (key < 0) {
                longDirectGroupingDisabled = true;
                return false;
            }
            andBits &= key;
            orBits |= key;
            observed = true;
        }
        int[] positions = mask.selectedPositions();
        int count = mask.count();
        for (int index = 0; index < count; index++) {
            int position = positions == null ? index : positions[index];
            if (nullValues.value(position)) {
                continue;
            }
            long key = keyValues.value(position);
            if (key < 0) {
                longDirectGroupingDisabled = true;
                return false;
            }
            andBits &= key;
            orBits |= key;
            observed = true;
        }
        if (!observed) {
            return false;
        }

        long compressionMask = andBits ^ orBits;
        if (Long.bitCount(compressionMask) >= Integer.SIZE) {
            return false;
        }
        long compressedMax = 0;
        for (int group = 0; group < nextGroupId; group++) {
            if (group != nullGroup) {
                compressedMax = Math.max(compressedMax, Long.compress(longKeysByGroup[group], compressionMask));
            }
        }
        for (int index = 0; index < count; index++) {
            int position = positions == null ? index : positions[index];
            if (!nullValues.value(position)) {
                compressedMax = Math.max(compressedMax, Long.compress(keyValues.value(position), compressionMask));
            }
        }
        if (compressedMax >= longPolicy.stagedCompressedDirectMaxRange() ||
                (compressedMax + 1) * longPolicy.directMaxRangePerGroupDenominator()
                        > Math.max(1, nextGroupId) * longPolicy.directMaxRangePerGroupNumerator()) {
            longDirectNextCheck = toIntExact(Math.min(
                    (long) longPolicy.stagedCompressedDirectLatestAdmissionGroups(),
                    Math.max(nextGroupId + 1, nextGroupId * 2)));
            return false;
        }

        longDirectCompressionMask = compressionMask;
        longDirectConstantBits = andBits & ~compressionMask;
        useCompressedLongDirectGrouping = true;
        rebuildLongDirectTable(toPowerOfTwoCapacity(toIntExact(compressedMax + 1)), compressedMax);
        return true;
    }

    private boolean validateCompressedLongDirectBatch(
            Mask mask,
            VectorAccess.LongValues keyValues,
            VectorAccess.BooleanValues nullValues)
    {
        long constantMask = ~longDirectCompressionMask;
        int[] positions = mask.selectedPositions();
        int count = mask.count();
        for (int index = 0; index < count; index++) {
            int position = positions == null ? index : positions[index];
            if (nullValues.value(position)) {
                continue;
            }
            long key = keyValues.value(position);
            long compressed = Long.compress(key, longDirectCompressionMask);
            if (key < 0 || (key & constantMask) != longDirectConstantBits || compressed >= longGroupIds.length) {
                disableLongDirectGrouping();
                longDirectGroupingDisabled = true;
                return false;
            }
        }
        return true;
    }

    private boolean prepareSingleLongDirectGrouping(int batchCount, long batchMax, int maxRange)
    {
        if (useLongDirectGrouping) {
            if (batchMax < longGroupIds.length) {
                return true;
            }
            long possibleGroups = Math.max(1, nextGroupId + batchCount);
            if ((batchMax + 1) * longPolicy.directMaxRangePerGroupDenominator()
                    > possibleGroups * longPolicy.directMaxRangePerGroupNumerator()) {
                disableLongDirectGrouping();
                longDirectGroupingDisabled = true;
                return false;
            }
            rebuildLongDirectTable(toPowerOfTwoCapacity(toIntExact(batchMax + 1)), batchMax);
            return true;
        }

        longDirectNextCheck = toIntExact(Math.min((long) maxRange, Math.max(nextGroupId + 1, nextGroupId * 2)));

        long max = batchMax;
        for (int group = 0; group < nextGroupId; group++) {
            if (group == nullGroup) {
                continue;
            }
            long key = longKeysByGroup[group];
            if (key < 0 || key >= maxRange) {
                if (!longPolicy.stagedCompressedDirect()) {
                    longDirectGroupingDisabled = true;
                }
                return false;
            }
            max = Math.max(max, key);
        }
        if (max < 0 || (max + 1) * longPolicy.directMaxRangePerGroupDenominator()
                > Math.max(1, nextGroupId) * longPolicy.directMaxRangePerGroupNumerator()) {
            return false;
        }
        rebuildLongDirectTable(toPowerOfTwoCapacity(toIntExact(max + 1)), max);
        return true;
    }

    boolean usesLongDirectGrouping()
    {
        return useLongDirectGrouping;
    }

    /**
     * Drops the duplicate key-by-slot array once a generated aggregation has established enough cardinality for
     * the saved key array to pay for the additional equality indirection. Occupied slots remain exact: their dense
     * group id indexes the canonical key in {@link #longKeysByGroup}. Activation may reserve a bounded geometric
     * capacity horizon and rebuild from the dense canonical map, avoiding repeated sparse-table rehashes while the
     * high-cardinality stream grows.
     */
    boolean prepareSingleLongIdIndexedGrouping(boolean runHeavyInput, long maximumNextGroupId)
    {
        if (useIdIndexedLongGrouping) {
            if (maximumNextGroupId >= longPolicy.idIndexedMaxGroups()) {
                rebuildOrdinaryLongGroupingTable();
                return false;
            }
            return true;
        }
        if (!longPolicy.idIndexed() || useLongDirectGrouping || nextGroupId < longPolicy.idIndexedMinGroups() || maximumNextGroupId >= longPolicy.idIndexedMaxGroups()) {
            return false;
        }
        long[] previousKeys = longGroupKeys;
        int[] previousIds = longGroupIds;
        int targetCapacity = previousIds.length;
        int capacityMultiplier = runHeavyInput
                ? longPolicy.idIndexedActivationCapacityMultiplier()
                : longPolicy.idIndexedUnclusteredActivationCapacityMultiplier();
        for (int multiplier = 1; multiplier < capacityMultiplier; multiplier <<= 1) {
            if (targetCapacity >= ID_INDEXED_LONG_GROUP_MASK / 2) {
                break;
            }
            targetCapacity <<= 1;
        }
        if (targetCapacity == previousIds.length) {
            for (int slot = 0; slot < previousIds.length; slot++) {
                int id = previousIds[slot];
                if (id != -1) {
                    previousIds[slot] = encodeIdIndexedLongGroup(hashLong(previousKeys[slot]), id);
                }
                else {
                    previousIds[slot] = 0;
                }
            }
        }
        else {
            longGroupIds = arrayPool.borrowInts(targetCapacity);
            Arrays.fill(longGroupIds, 0);
            longGroupMask = targetCapacity - 1;
            longGroupMaxFill = (int) (targetCapacity * LONG_GROUP_LOAD_FACTOR);
            for (int id = 0; id < longGroupCount; id++) {
                if (id == nullGroup) {
                    continue;
                }
                long key = longKeysByGroup[id];
                int hash = hashLong(key);
                int slot = hash & longGroupMask;
                while (longGroupIds[slot] != 0) {
                    slot = (slot + 1) & longGroupMask;
                }
                longGroupIds[slot] = encodeIdIndexedLongGroup(hash, id);
            }
            arrayPool.release(previousIds);
        }
        longGroupKeys = new long[0];
        useIdIndexedLongGrouping = true;
        arrayPool.release(previousKeys);
        return true;
    }

    boolean usesIdIndexedLongGrouping()
    {
        return useIdIndexedLongGrouping;
    }

    private void rebuildOrdinaryLongGroupingTable()
    {
        int[] previousIds = longGroupIds;
        longGroupKeys = arrayPool.borrowLongs(previousIds.length);
        Arrays.fill(previousIds, -1);
        for (int id = 0; id < nextGroupId; id++) {
            if (id == nullGroup) {
                continue;
            }
            long key = longKeysByGroup[id];
            int slot = hashLong(key) & longGroupMask;
            while (previousIds[slot] != -1) {
                slot = (slot + 1) & longGroupMask;
            }
            longGroupKeys[slot] = key;
            previousIds[slot] = id;
        }
        useIdIndexedLongGrouping = false;
    }

    private static int encodeIdIndexedLongGroup(int hash, int groupId)
    {
        return (hash >>> ID_INDEXED_LONG_HASH_SHIFT) << ID_INDEXED_LONG_HASH_SHIFT | (groupId + 1);
    }

    private static int decodeIdIndexedLongGroup(int encoded)
    {
        return (encoded & ID_INDEXED_LONG_GROUP_MASK) - 1;
    }

    void disableStagedLongDirectGrouping()
    {
        stagedLongDirectGroupingDisabled = true;
    }

    boolean usesFlatSingleRecordIdentity()
    {
        return flatSingleNullInTable;
    }

    /**
     * Pre-grows the single-long table so it can absorb {@code additional} more keys without rehashing, so
     * the fused kernel's inlined probe needs no mid-loop rehash branch. Requires {@link #usesSingleLongGrouping}.
     */
    void reserveSingleLongTable(int additional)
    {
        if (useLongDirectGrouping) {
            ensureLongGroupingCapacity(nextGroupId + additional);
            return;
        }
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

    void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result, boolean moreInputExpected)
    {
        assignGroups(new Vector[] {values}, new Vector[] {nulls}, mask, result, moreInputExpected);
    }

    /**
     * Registers the probe batch's key column before a run of {@link #contains} calls. Required for the flat
     * grouping path, whose per-batch dictionary hash/bound-dictionary cache is keyed to the batch's vectors;
     * probing without it would read the previous (membership-build) batch's stale cache. Must be paired with
     * {@link #endContainsBatch}. A no-op for the long/dictionary/hash-map paths.
     */
    public void beginContainsBatch(Vector values, Vector nulls)
    {
        initializeIfNecessary(new Vector[] {values}, new Vector[] {nulls}, null);
        if (structuralGrouping != null) {
            return;
        }
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
        initializeIfNecessary(new Vector[] {values}, new Vector[] {nulls}, null);
        if (OperatorVectorSupport.isNull(nulls, position)) {
            return false;
        }
        if (structuralGrouping != null) {
            return structuralGrouping.contains(new Vector[] {values}, new Vector[] {nulls}, position);
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
        initializeIfNecessary(values, nulls, null);
    }

    public void initializeSchema(Vector[] values, Vector[] nulls, Mask mask)
    {
        initializeIfNecessary(values, nulls, requireNonNull(mask, "mask is null"));
    }

    @SuppressWarnings("unchecked")
    public void assignGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        assignGroups(values, nulls, mask, result, false);
    }

    void assignGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result, boolean moreInputExpected)
    {
        assignGroups(values, nulls, mask, result, moreInputExpected, false);
    }

    /**
     * Assigns groups for a blocking aggregation that consumes its complete source. Unlike a streaming group-id
     * producer, this lifecycle does not need to call {@code source.hasNext()} while the current batch is borrowed
     * merely to prove a second batch exists. The first-batch cardinality sample still decides whether the wider
     * self-contained hash slots pay for this input.
     */
    void assignGroupsForBlockingAggregation(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        assignGroups(values, nulls, mask, result, false, true);
    }

    boolean assignGroupsDiscardingResults(Vector[] values, Vector[] nulls, Mask mask)
    {
        try {
            if (!initialized) {
                initializeIfNecessary(values, nulls, mask);
            }
            if (structuralGrouping != null) {
                return false;
            }
            if (values.length == 1 && values[0] instanceof DictionaryVector dictionary) {
                decideDictionaryFlatSingleIdentity(values, nulls, mask);
                assignDictionaryGroupsDiscardingResults(dictionary, nulls[0], mask);
                return true;
            }
            if (!discardMultiLongResults) {
                return false;
            }
            boolean promotedNow = ((AdaptiveLongGroupingTable) multiLongTable).promoteForDiscardedResults(nextGroupId);
            if (!promotedNow) {
                reserveAdditionalGroups(mask.count() + 1L);
            }
            assignMultiLongGroupsDiscardingResults(values, nulls, mask);
            return true;
        }
        finally {
            accountRetainedState();
        }
    }

    private void assignGroups(
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            I64Vector result,
            boolean moreInputExpected,
            boolean blockingAggregation)
    {
        try {
            assignGroupsInternal(values, nulls, mask, result, moreInputExpected, blockingAggregation);
        }
        finally {
            accountRetainedState();
        }
    }

    private void assignGroupsInternal(
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            I64Vector result,
            boolean moreInputExpected,
            boolean blockingAggregation)
    {
        moreInputExpectedForCurrentBatch = moreInputExpected;
        blockingAggregationForCurrentBatch = blockingAggregation;
        if (!initialized && allowsLegacyKeyShortcuts) {
            useFullWidthPairPackedIdentity = admitsFullWidthPairPackedIdentity(values, nulls, mask);
        }
        initializeIfNecessary(values, nulls, mask);
        decideDictionaryFlatSingleIdentity(values, nulls, mask);
        if (structuralGrouping != null) {
            nextGroupId = structuralGrouping.assignGroups(values, nulls, mask, result, nextGroupId);
            return;
        }
        if (nextGroupId == 0 && useMultiLongGrouping &&
                admitsFullWidthPairPackedIdentity(values, nulls, mask)) {
            multiLongTable.releaseBuffers();
            multiLongTable = null;
            useMultiLongGrouping = false;
            useFlatGrouping = true;
            useFullWidthPairPackedIdentity = true;
            flatGroupingLayout = BigintPairFlatKeyLayout.create(
                    values,
                    hasNullableKeys(nulls),
                    arrayPool,
                    codeGeneration,
                    flatKeyTablePolicy,
                    keyTypes);
            flatGroupingTable = new FlatGroupingTable(
                    flatGroupingLayout,
                    initialExpectedSize(values, mask),
                    true,
                    true);
            flatPackedIdentityAdmissionDecided = true;
            if (compositePolicy.debugGroupingShapes()) {
                System.err.printf("[full-width-pair-packed-identity] rows=%d deferred=true%n", mask.count());
            }
        }
        reserveAdditionalGroups(mask.count() + 1L);
        // A dictionary-encoded long key is still represented by the single-long table so grouped-key output and
        // later flat batches share one exact state.  Resolve each dictionary entry into that table once per
        // content generation, then make the hot row loop an id lookup instead of hashing every occurrence.
        if (useLongGrouping && values[0] instanceof DictionaryVector dictionary) {
            assignDictionaryLongGroups(dictionary, nulls[0], mask, result);
            return;
        }
        if (useLongGrouping) {
            assignLongGroups(values[0], nulls[0], mask, result);
            return;
        }
        if (usePackedIntPairGrouping) {
            if (packedIntGroupingArity == 2) {
                assignPackedIntPairGroups(values, nulls, mask, result);
            }
            else {
                assignPackedIntTripleGroups(values, nulls, mask, result);
            }
            return;
        }
        if (useMultiLongGrouping) {
            assignMultiLongGroups(values, nulls, mask, result);
            return;
        }
        if (useFlatGrouping && values.length == 1 && values[0] instanceof DictionaryVector dictionary) {
            assignFlatDictionaryGroups(dictionary, nulls[0], mask, result);
            return;
        }
        if (useFlatGrouping) {
            long previousGroupCount = nextGroupId;
            assignFlatGroups(values, nulls, mask, result);
            reserveFlatGroupingLookahead(mask.count(), nextGroupId - previousGroupCount);
            return;
        }
        if (useSharedDictionaryGrouping && assignSharedDictionaryGroups(values, nulls, mask, result)) {
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

    private void accountRetainedState()
    {
        if (allocator != null) {
            allocator.setRetainedBytes(allocationContext, this, retainedBytes());
        }
    }

    private long retainedBytes()
    {
        long bytes = longArrayBytes(longGroupKeys);
        bytes += intArrayBytes(longGroupIds);
        bytes += intArrayBytes(longGroupHashes);
        bytes += referenceArrayBytes(reusableProbeKeys);
        bytes += referenceArrayBytes(keyHandlers);
        bytes += referenceArrayBytes(binaryTraits);
        bytes += longArrayBytes(longKeysByGroup);
        bytes += longArrayBytes(dictionaryGroupsById);
        bytes += intArrayBytes(dictionaryGenerations);
        bytes += referenceArrayBytes(cachedSharedDictionaryValues);
        bytes += longArrayBytes(cachedSharedDictionaryGenerations);
        bytes += longArrayBytes(sharedDictionaryEntriesById);
        bytes += packedIntPairControl == null ? 0 : packedIntPairControl.length;
        bytes += intArrayBytes(packedIntTripleThirdByGroup);
        bytes += packedIntTripleNullMasksByGroup.length;
        bytes += intArrayBytes(packedIntTripleTailByGroup);
        bytes += intArrayBytes(densePositionsCache);
        bytes += multiLongTable == null ? 0 : multiLongTable.retainedBytes();
        bytes += flatGroupingTable == null ? 0 : flatGroupingTable.retainedBytes();
        return bytes;
    }

    void finishInput()
    {
        if (flatGroupingTable != null) {
            flatGroupingTable.finishInput();
            accountRetainedState();
        }
    }

    boolean supportsProgressiveOutputRelease()
    {
        return flatGroupingTable != null && flatGroupingTable.supportsProgressiveOutputRelease();
    }

    void releaseOutputThrough(int exclusiveGroupId)
    {
        if (!supportsProgressiveOutputRelease()) {
            return;
        }
        flatGroupingTable.releaseOutputThrough(exclusiveGroupId);
        accountRetainedState();
    }

    private static long intArrayBytes(int[] values)
    {
        return values == null ? 0 : (long) values.length * Integer.BYTES;
    }

    private static long longArrayBytes(long[] values)
    {
        return values == null ? 0 : (long) values.length * Long.BYTES;
    }

    private static long referenceArrayBytes(Object[] values)
    {
        return values == null ? 0 : (long) values.length * Long.BYTES;
    }

    private void reserveFlatGroupingLookahead(int selectedRows, long newGroups)
    {
        if (!compositePolicy.adaptiveFlatLookahead()) {
            return;
        }
        flatGroupingBatchCount++;
        if (flatGroupingBatchCount < compositePolicy.adaptiveFlatLookaheadStartBatch() ||
                selectedRows < compositePolicy.adaptiveFlatLookaheadMinRows() ||
                newGroups * 100 < (long) selectedRows * compositePolicy.adaptiveFlatLookaheadMinNewPercent()) {
            return;
        }
        // A sustained high-cardinality stream will otherwise discover the same table sizes through repeated
        // rehashes. Reserve a bounded number of observed batches ahead; short and low-cardinality inputs never
        // qualify, while an unexpectedly changing distribution remains exact because this is capacity only.
        long expectedGroups = nextGroupId + newGroups * compositePolicy.adaptiveFlatLookaheadBatches();
        flatGroupingTable.ensureCapacity(expectedGroups);
        if (compositePolicy.debugGroupingShapes()) {
            System.err.printf(
                    "[flat-group-lookahead] batch=%d rows=%d new-groups=%d current=%d expected=%d%n",
                    flatGroupingBatchCount,
                    selectedRows,
                    newGroups,
                    nextGroupId,
                    expectedGroups);
        }
    }

    private boolean assignSharedDictionaryGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        int[] ids = sharedDictionaryIds(values, mask);
        if (ids == null || !sharedDictionaryNullsCompatible(nulls, ids, mask)) {
            return false;
        }
        Vector[] dictionaryValues = new Vector[values.length];
        int dictionarySize = 0;
        for (int index = 0; index < values.length; index++) {
            dictionaryValues[index] = ((DictionaryVector) values[index]).values();
            dictionarySize = Math.max(dictionarySize, dictionaryValues[index].length());
        }
        ensureSharedDictionaryCacheCapacity(dictionarySize);
        int generation = currentSharedDictionaryGeneration(dictionaryValues);
        OperatorKeySemantics.Key[] probeKeys = sharedDictionaryFlatBacking ? null : new OperatorKeySemantics.Key[values.length];
        long[] output = result.values();
        if (sharedDictionaryFlatBacking) {
            flatGroupingTable.beginBatch(values, nulls, mask);
        }
        try {
            for (int position : mask) {
                int dictionaryId = ids[position];
                long entry = sharedDictionaryEntriesById[dictionaryId];
                if (sharedDictionaryEntryGeneration(entry) != generation) {
                    long groupId;
                    if (sharedDictionaryFlatBacking) {
                        long newGroupId = nextGroupId;
                        groupId = flatGroupingTable.assignGroup(values, nulls, position, newGroupId);
                        if (groupId == newGroupId) {
                            nextGroupId++;
                        }
                    }
                    else {
                        for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                            probeKeys[keyIndex] = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, reusableProbeKeys[keyIndex]);
                        }
                        groupId = groupForKeys(probeKeys);
                    }
                    if (!canPackSharedDictionaryGroup(groupId)) {
                        return false;
                    }
                    entry = sharedDictionaryEntry(generation, groupId);
                    sharedDictionaryEntriesById[dictionaryId] = entry;
                }
                output[position] = sharedDictionaryEntryGroup(entry);
            }
            return true;
        }
        finally {
            if (sharedDictionaryFlatBacking) {
                flatGroupingTable.endBatch();
                flatGroupingTable.releasePositionIndexedScratchIfOversized(mask);
            }
        }
    }

    private static int[] sharedDictionaryIds(Vector[] values, Mask mask)
    {
        if (values.length < 2 || !(values[0] instanceof DictionaryVector first)) {
            return null;
        }
        int[] ids = first.ids();
        for (int index = 1; index < values.length; index++) {
            if (!(values[index] instanceof DictionaryVector dictionary) || dictionary.length() != first.length()) {
                return null;
            }
            if (dictionary.ids() != ids && !sameDictionaryIds(dictionary.ids(), ids, mask)) {
                return null;
            }
        }
        return ids;
    }

    private static boolean sharedDictionaryNullsCompatible(Vector[] nulls, int[] ids, Mask mask)
    {
        for (Vector nullVector : nulls) {
            if (VectorAccess.isAllFalseNulls(nullVector)) {
                continue;
            }
            if (!(nullVector instanceof DictionaryVector dictionary) || dictionary.length() != mask.size()) {
                return false;
            }
            if (dictionary.ids() != ids && !sameDictionaryIds(dictionary.ids(), ids, mask)) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameDictionaryIds(int[] left, int[] right, Mask mask)
    {
        for (int ordinal = 0; ordinal < mask.count(); ordinal++) {
            int position = mask.position(ordinal);
            if (left[position] != right[position]) {
                return false;
            }
        }
        return true;
    }

    private void ensureSharedDictionaryCacheCapacity(int size)
    {
        if (sharedDictionaryEntriesById.length >= size) {
            return;
        }
        int newSize = Math.max(size, Math.max(16, sharedDictionaryEntriesById.length * 2));
        long[] previous = sharedDictionaryEntriesById;
        sharedDictionaryEntriesById = arrayPool.borrowLongs(newSize);
        System.arraycopy(previous, 0, sharedDictionaryEntriesById, 0, previous.length);
        // A pooled array retains packed (generation, group-id) entries from its previous owner.
        // Generations restart for each GroupingState, so stale entries can otherwise look current
        // and return group ids that this state has never assigned.
        Arrays.fill(sharedDictionaryEntriesById, previous.length, newSize, 0);
        arrayPool.release(previous);
    }

    private int currentSharedDictionaryGeneration(Vector[] dictionaryValues)
    {
        if (!sameVectorContents(cachedSharedDictionaryValues, cachedSharedDictionaryGenerations, dictionaryValues)) {
            cachedSharedDictionaryValues = dictionaryValues.clone();
            cachedSharedDictionaryGenerations = new long[dictionaryValues.length];
            for (int index = 0; index < dictionaryValues.length; index++) {
                cachedSharedDictionaryGenerations[index] = dictionaryValues[index].contentGeneration();
            }
            if (sharedDictionaryGeneration == Integer.MAX_VALUE) {
                Arrays.fill(sharedDictionaryEntriesById, 0);
                sharedDictionaryGeneration = 0;
            }
            return ++sharedDictionaryGeneration;
        }
        return sharedDictionaryGeneration;
    }

    private static boolean sameVectorContents(Vector[] cachedValues, long[] cachedGenerations, Vector[] values)
    {
        if (cachedValues.length != values.length || cachedGenerations.length != values.length) {
            return false;
        }
        for (int index = 0; index < values.length; index++) {
            long generation = values[index].contentGeneration();
            if (generation < 0 || cachedValues[index] != values[index] || cachedGenerations[index] != generation) {
                return false;
            }
        }
        return true;
    }

    private static long sharedDictionaryEntry(int generation, long groupId)
    {
        return ((long) generation << Integer.SIZE) | (groupId & 0xFFFF_FFFFL);
    }

    private static boolean canPackSharedDictionaryGroup(long groupId)
    {
        return (groupId & ~0xFFFF_FFFFL) == 0;
    }

    private static int sharedDictionaryEntryGeneration(long entry)
    {
        return (int) (entry >>> Integer.SIZE);
    }

    private static long sharedDictionaryEntryGroup(long entry)
    {
        return entry & 0xFFFF_FFFFL;
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

    private void initializeIfNecessary(Vector[] values, Vector[] nulls, Mask mask)
    {
        validateKeyTypes(values);
        if (initialized) {
            return;
        }
        initialized = true;

        if (!allowsLegacyKeyShortcuts) {
            structuralGrouping = new StructuralGroupingIndex(
                    requireNonNull(allocator, "allocator is null"),
                    requireNonNull(allocationContext, "allocationContext is null"),
                    structuralKeyKernels);
            return;
        }

        if (compositePolicy.debugGroupingShapes()) {
            StringBuilder shape = new StringBuilder("[grouping-shape]");
            for (Vector value : values) {
                shape.append(' ').append(value.getClass().getSimpleName()).append('(').append(value.length());
                if (value instanceof DictionaryVector dictionary) {
                    shape.append("->").append(dictionary.values().getClass().getSimpleName())
                            .append('(').append(dictionary.values().length()).append(')');
                }
                shape.append(')');
            }
            System.err.println(shape);
        }

        keyHandlers = new FlatTypeHandler[values.length];
        binaryTraits = (Set<BinaryVector.Trait>[]) new Set<?>[values.length];
        for (int index = 0; index < values.length; index++) {
            keyHandlers[index] = FlatTypeHandlers.forVector(
                    values[index],
                    index < keyTypes.size() ? keyTypes.get(index) : null);
            binaryTraits[index] = OperatorVectorSupport.binaryTraits(values[index]);
        }

        if (values.length == 1 && isSingleLongGroupingCandidate(values[0])) {
            useLongGrouping = true;
            initLongGroupTable(initialLongGroupExpectedSize(values[0], nulls[0], mask));
            return;
        }
        boolean nullableCompositeKeys = values.length > 1 && hasNullableKeys(nulls);
        if (useFullWidthPairPackedIdentity) {
            useFlatGrouping = true;
            flatGroupingLayout = BigintPairFlatKeyLayout.create(
                    values,
                    nullableCompositeKeys,
                    arrayPool,
                    codeGeneration,
                    flatKeyTablePolicy,
                    keyTypes);
            flatGroupingTable = new FlatGroupingTable(
                    flatGroupingLayout,
                    initialExpectedSize(values, mask),
                    true,
                    true);
            flatPackedIdentityAdmissionDecided = true;
            if (compositePolicy.debugGroupingShapes()) {
                System.err.printf("[full-width-pair-packed-identity] rows=%d%n", mask.count());
            }
            return;
        }
        if (values.length >= 2 && values.length <= AbstractMultiLongGroupingTable.MAX_ARITY && allSingleLongGroupingCandidates(values)) {
            if (compositePolicy.adaptiveCompactLong() ||
                    (compositePolicy.generatedCompactLongPair() && values.length == 2) ||
                    values.length >= compositePolicy.generatedCompactLongMinArity()) {
                useMultiLongGrouping = true;
                multiLongArity = values.length;
                multiLongTable = AdaptiveLongGroupingTable.create(
                        values.length,
                        initialExpectedSize(values, mask),
                        arrayPool,
                        codeGeneration,
                        adaptiveLongGroupingPolicy);
                ((AdaptiveLongGroupingTable) multiLongTable).compactRetainedColumns(compactRetainedLongColumns(values, nulls, mask));
                discardMultiLongResults = mask != null && sampleContainsValueOutsideCompactDomain(values, nulls, mask);
                return;
            }
            if (values.length == 2 && compositePolicy.packedIntPair()) {
                usePackedIntPairGrouping = true;
                packedIntGroupingArity = 2;
                initPackedIntPairTable(initialExpectedSize(values, mask));
                return;
            }
            if (values.length == 3 && compositePolicy.packedIntTriple()) {
                usePackedIntPairGrouping = true;
                packedIntGroupingArity = 3;
                initPackedIntPairTable(initialExpectedSize(values, mask));
                return;
            }
            // Generate (once per arity) a grouping table specialized to this many long keys — the row loop
            // is emitted as bytecode so the keys live in registers exactly like the former 2/3/4-key tables.
            useMultiLongGrouping = true;
            multiLongArity = values.length;
            multiLongTable = codeGeneration.multiLongGrouping().create(
                    values.length,
                    initialExpectedSize(values, mask),
                    arrayPool,
                    adaptiveLongGroupingPolicy);
            return;
        }

        FlatKeyLayout flatKeyLayout = FlatKeyLayout.tryCreate(
                values,
                nullableCompositeKeys,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                keyTypes);
        if (compositePolicy.sharedDictionaryComposite() &&
                values.length > 1 &&
                values.length <= compositePolicy.sharedDictionaryMaxFields() &&
                sharedDictionaryIds(values, mask) != null &&
                admitsSharedDictionaryGrouping(values, nulls, mask, flatKeyLayout)) {
            useSharedDictionaryGrouping = true;
            if (compositePolicy.sharedDictionaryFlatBacking() &&
                    values.length >= compositePolicy.sharedDictionaryFlatBackingMinFields() &&
                    mask.count() >= compositePolicy.sharedDictionaryFlatBackingMinRows() &&
                    flatKeyLayout != null) {
                sharedDictionaryFlatBacking = true;
                flatGroupingLayout = flatKeyLayout;
                flatGroupingTable = new FlatGroupingTable(
                        flatKeyLayout,
                        initialExpectedSize(values, mask),
                        true);
                if (compositePolicy.debugGroupingShapes()) {
                    System.err.printf("[shared-dictionary-flat-backing] fields=%d rows=%d%n", values.length, mask.count());
                }
            }
            else {
                initializeObjectKeyGrouping(values);
            }
            return;
        }

        if (flatKeyLayout != null) {
            useFlatGrouping = true;
            flatGroupingLayout = flatKeyLayout;
            // Single-key grouping assigns NULL a group id outside the table, so its ids can contain a hole.
            // Composite grouping stores its null combinations in the table and remains insertion-order dense.
            flatGroupingTable = new FlatGroupingTable(
                    flatKeyLayout,
                    initialExpectedSize(values, mask),
                    values.length > 1);
            return;
        }

        initializeObjectKeyGrouping(values);
    }

    private static int initialExpectedSize(Vector[] values, Mask mask)
    {
        // Encoded and join-produced vectors can retain a large addressable domain while exposing only a small
        // active selection. Grouping state is populated from the active rows, so sizing from Vector.length()
        // needlessly allocates for filtered-out positions and can trip adaptive partial-aggregation flush limits.
        return Math.max(16, mask == null ? values[0].length() : mask.count());
    }

    private int compactRetainedLongColumns(Vector[] values, Vector[] nulls, Mask mask)
    {
        int compactColumns = (1 << values.length) - 1;
        int rowCount = mask == null ? values[0].length() : mask.count();
        int sampleSize = Math.min(rowCount, compositePolicy.flatSingleKeyRecordIdentitySampleSize());
        for (int column = 0; column < values.length; column++) {
            VectorAccess.LongValues accessor = VectorAccess.longValues(values[column]);
            for (int sample = 0; sample < sampleSize; sample++) {
                int selectedIndex = (int) ((long) sample * rowCount / sampleSize);
                int position = mask == null ? selectedIndex : mask.position(selectedIndex);
                if (!OperatorVectorSupport.isNull(nulls[column], position)) {
                    long value = accessor.value(position);
                    if ((long) (int) value != value) {
                        compactColumns &= ~(1 << column);
                        break;
                    }
                }
            }
        }
        return compactColumns;
    }

    private int initialLongGroupExpectedSize(Vector values, Vector nulls, Mask mask)
    {
        int rowCount = mask == null ? values.length() : mask.selectedCount();
        int sampleSize = Math.min(rowCount, longPolicy.initialCardinalitySampleSize());
        if (sampleSize == 0) {
            return 16;
        }

        VectorAccess.LongValues keyValues = VectorAccess.longValues(values);
        long[] distinctValues = new long[sampleSize];
        int distinctCount = 0;
        boolean sampledNull = false;
        for (int sample = 0; sample < sampleSize; sample++) {
            int sampleOrdinal = (int) ((long) sample * rowCount / sampleSize);
            int position = mask == null ? sampleOrdinal : mask.position(sampleOrdinal);
            if (nulls != null && OperatorVectorSupport.booleanValue(nulls, nulls.length() == 1 ? 0 : position)) {
                if (!sampledNull) {
                    sampledNull = true;
                    distinctCount++;
                }
                continue;
            }
            long value = keyValues.value(position);
            boolean found = false;
            for (int index = 0; index < distinctCount - (sampledNull ? 1 : 0); index++) {
                if (distinctValues[index] == value) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                distinctValues[distinctCount - (sampledNull ? 1 : 0)] = value;
                distinctCount++;
            }
        }
        if ((long) distinctCount * 100 >= (long) sampleSize * longPolicy.initialHighCardinalityPercent()) {
            return Math.max(16, rowCount);
        }
        return Math.max(16, distinctCount * longPolicy.initialLowCardinalityHeadroom());
    }

    private boolean admitsFullWidthPairPackedIdentity(Vector[] values, Vector[] nulls, Mask mask)
    {
        if (!compositePolicy.fullWidthPairPackedIdentity() || values.length != 2 ||
                values[0] instanceof DictionaryVector || values[1] instanceof DictionaryVector ||
                mask.count() < compositePolicy.fullWidthPairPackedIdentityMinBatchRows() ||
                !allSingleLongGroupingCandidates(values) ||
                !sampleContainsValueOutsideCompactDomain(values, nulls, mask)) {
            return false;
        }
        int sampled = Math.min(mask.count(), compositePolicy.flatSingleKeyRecordIdentitySampleSize());
        FlatKeyLayout layout = BigintPairFlatKeyLayout.create(
                values,
                hasNullableKeys(nulls),
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                keyTypes);
        int distinct = sampledDistinctFlatKeys(layout, values, nulls, mask);
        return (long) distinct * 100 >= (long) sampled * compositePolicy.packedFlatIdentityMinDistinctPercent();
    }

    /**
     * Detect whether any sampled key falls outside the generated compact table's signed-32 domain. Pair identity
     * admission uses this to avoid duplicate full-width storage; grouping-only consumers use the same physical
     * evidence to select a generated table that retains output keys without storing unconsumed per-row group ids.
     * Sampling is conservative: a missed later wide value preserves the ordinary exact promotion path.
     */
    private boolean sampleContainsValueOutsideCompactDomain(Vector[] values, Vector[] nulls, Mask mask)
    {
        int sampleSize = Math.min(mask.count(), compositePolicy.flatSingleKeyRecordIdentitySampleSize());
        VectorAccess.LongValues[] accessors = new VectorAccess.LongValues[values.length];
        for (int column = 0; column < values.length; column++) {
            accessors[column] = VectorAccess.longValues(values[column]);
        }
        for (int sample = 0; sample < sampleSize; sample++) {
            int selectedIndex = (int) ((long) sample * mask.count() / sampleSize);
            int position = mask.position(selectedIndex);
            for (int column = 0; column < accessors.length; column++) {
                if (OperatorVectorSupport.isNull(nulls[column], position)) {
                    continue;
                }
                long value = accessors[column].value(position);
                if ((long) (int) value != value) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The shared-id cache pays when many rows resolve to the same composite value, but a near-unique dictionary
     * merely routes every row through object-key construction and hashing. Sample the generic flat layout once at
     * the batch boundary and retain the shortcut only when there is meaningful value reuse. Hash collisions can
     * only bias this conservative admission toward the correctness-equivalent shared path.
     */
    private boolean admitsSharedDictionaryGrouping(Vector[] values, Vector[] nulls, Mask mask, FlatKeyLayout layout)
    {
        if (layout == null || compositePolicy.sharedDictionarySampleSize() <= 0) {
            return true;
        }
        int selectedRows = mask.count();
        int sampleSize = Math.min(selectedRows, compositePolicy.sharedDictionarySampleSize());
        if (sampleSize == 0) {
            return true;
        }

        long[] hashes = arrayPool.borrowLongs(sampleSize);
        try {
            layout.beginBatch(values, nulls);
            try {
                for (int sample = 0; sample < sampleSize; sample++) {
                    int position = mask.position((int) ((long) sample * selectedRows / sampleSize));
                    hashes[sample] = layout.hash(values, nulls, position);
                }
            }
            finally {
                layout.endBatch();
            }
            Arrays.sort(hashes, 0, sampleSize);
            int distinct = 1;
            for (int index = 1; index < sampleSize; index++) {
                if (hashes[index] != hashes[index - 1]) {
                    distinct++;
                }
            }
            if (compositePolicy.debugGroupingShapes()) {
                System.err.printf("[grouping-shared-dictionary] sampled=%d distinct-hashes=%d%n", sampleSize, distinct);
            }
            return (long) distinct * 100 <=
                    (long) sampleSize * compositePolicy.sharedDictionaryMaxDistinctPercent();
        }
        finally {
            arrayPool.release(hashes);
        }
    }

    private void validateKeyTypes(Vector[] values)
    {
        if (keyTypes.isEmpty()) {
            return;
        }
        if (keyTypes.size() != values.length) {
            throw new IllegalArgumentException("Grouping key type count does not match input arity");
        }
        for (int index = 0; index < values.length; index++) {
            TypeBinding type = keyTypes.get(index);
            if (type.isSpecified() && !type.supportsVector(values[index])) {
                throw new IllegalArgumentException("Grouping key vector is not supported by type " + type.identity());
            }
        }
    }

    private void initializeObjectKeyGrouping(Vector[] values)
    {
        reusableProbeKeys = new OperatorKeySemantics.Key[values.length];
        for (int index = 0; index < values.length; index++) {
            // Compact long handlers describe the physical bytes stored in a flat table. Object grouping stores
            // logical keys instead, so its output must use the vector's semantic handler rather than a flat
            // storage codec that cannot materialize fallback keys.
            keyHandlers[index] = FlatTypeHandlers.forVector(values[index]);
            reusableProbeKeys[index] = OperatorKeySemantics.reusableProbeKey(values[index]);
            keysByGroupColumns.add(new ArrayList<>());
        }
        if (values.length > 1) {
            reusableCompositeProbeKey = OperatorKeySemantics.reusableCompositeProbeKey(values.length);
        }
    }

    private void assignFlatGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        if (compositePolicy.flatSingleKeyRecordIdentity() &&
                values.length == 1 &&
                !flatSingleIdentityAdmissionDecided &&
                !mask.none()) {
            decideFlatSingleIdentity(values, nulls, mask);
        }
        if (values.length > 1 && !flatPackedIdentityAdmissionDecided && !mask.none()) {
            decideFlatPackedIdentity(values, nulls, mask);
        }
        flatGroupingTable.beginBatch(values, nulls, mask);
        try {
            if (values.length == 1 &&
                    !flatSingleNullInTable &&
                    !VectorAccess.isAllFalseNulls(nulls[0])) {
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
            long generatedNextGroupId = flatGroupingTable.assignGeneratedDictionaryBatch(
                    values, nulls, mask, result, nextGroupId);
            if (generatedNextGroupId >= 0) {
                nextGroupId = generatedNextGroupId;
                return;
            }
            long normalizedNextGroupId = flatGroupingTable.assignNormalizedIntBatch(
                    values, nulls, mask, result, nextGroupId);
            if (normalizedNextGroupId >= 0) {
                nextGroupId = normalizedNextGroupId;
                return;
            }
            flatGroupingTable.prepareBatchHashes(values, nulls, mask);
            long preparedNextGroupId = flatGroupingTable.assignPreparedPhysicalBatch(
                    values, nulls, mask, result, nextGroupId);
            if (preparedNextGroupId >= 0) {
                nextGroupId = preparedNextGroupId;
                return;
            }
            long batchNextGroupId = flatGroupingTable.assignMixedComposite3Batch(
                    values, nulls, mask, result, nextGroupId);
            if (batchNextGroupId >= 0) {
                nextGroupId = batchNextGroupId;
                return;
            }
            long prefetchedNextGroupId = flatGroupingTable.assignPrefetchedBatch(
                    values, nulls, mask, result, nextGroupId);
            if (prefetchedNextGroupId >= 0) {
                nextGroupId = prefetchedNextGroupId;
                return;
            }
            if (flatGroupingTable.singleDictionaryGroupCacheActive()) {
                for (int position : mask) {
                    long newGroupId = nextGroupId;
                    long groupId = flatGroupingTable.assignGroupCached(values, nulls, position, newGroupId);
                    if (groupId == newGroupId) {
                        nextGroupId++;
                    }
                    result.values()[position] = groupId;
                }
                return;
            }
            if (values.length == 1 && !flatSingleNullInTable) {
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

            boolean hashedOnly = compositePolicy.earlyRejectMixedComposite() &&
                    !flatGroupingTable.batchArrayModeEligible();
            for (int position : mask) {
                long newGroupId = nextGroupId;
                long groupId = hashedOnly
                        ? flatGroupingTable.assignGroupHashed(values, nulls, position, newGroupId)
                        : flatGroupingTable.assignGroup(values, nulls, position, newGroupId);
                if (groupId == newGroupId) {
                    nextGroupId++;
                }
                result.values()[position] = groupId;
            }
        }
        finally {
            flatGroupingTable.endBatch();
            flatGroupingTable.releasePositionIndexedScratchIfOversized(mask);
        }
    }

    private void decideDictionaryFlatSingleIdentity(Vector[] values, Vector[] nulls, Mask mask)
    {
        if (useFlatGrouping &&
                values.length == 1 &&
                values[0] instanceof DictionaryVector &&
                !flatSingleIdentityAdmissionDecided &&
                !mask.none()) {
            decideFlatSingleIdentity(values, nulls, mask);
        }
    }

    /** Cold, one-shot admission path kept out of the compiled per-batch assignment kernel. */
    private void decideFlatSingleIdentity(Vector[] values, Vector[] nulls, Mask mask)
    {
        flatSingleIdentityAdmissionDecided = true;
        // Decide before the first hot row. High-volume, high-cardinality first batches are the sustained grouping
        // case where the two logical-id maps become material; sparse or categorical pipelines retain the ordinary
        // representation. The table is still empty, so replace it once with an immutable nullable identity layout.
        // A later NULL becomes an ordinary record and cannot introduce a logical-id hole or a steady-state mode
        // branch.
        boolean largeBatch = nextGroupId == 0 &&
                mask.count() >= compositePolicy.flatSingleKeyRecordIdentityMinBatchRows();
        boolean dictionaryCardinalityEligible = !(values[0] instanceof DictionaryVector dictionary) ||
                (long) dictionary.values().length() * 100 >=
                        (long) mask.count() * compositePolicy.flatSingleKeyRecordIdentityMinDistinctPercent();
        FlatKeyLayout identityLayout = largeBatch && dictionaryCardinalityEligible
                ? FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy, keyTypes)
                : null;
        int sampledDistinct = identityLayout != null
                ? sampledDistinctFlatKeys(identityLayout, values, nulls, mask)
                : 0;
        int sampled = identityLayout != null
                ? Math.min(mask.count(), compositePolicy.flatSingleKeyRecordIdentitySampleSize())
                : 0;
        if (sampled > 0 &&
                (long) sampledDistinct * 100 >=
                        (long) sampled * compositePolicy.flatSingleKeyRecordIdentityMinDistinctPercent()) {
            flatGroupingTable.releaseBuffers();
            // A single key needs only the bucket-to-record index. Packing a second hash discriminator into a
            // wider slot trades one dependent record-hash read for twice the slot bandwidth, which loses for the
            // high-cardinality binary stream this admission serves. Composite identity tables can still amortize
            // packed slots by eliminating their additional group-id maps through decideFlatPackedIdentity.
            boolean packedSlots = false;
            flatGroupingLayout = identityLayout;
            flatGroupingTable = new FlatGroupingTable(
                    identityLayout,
                    Math.max(16, mask.count()),
                    true,
                    packedSlots);
            flatSingleNullInTable = true;
        }
        flatPackedIdentityAdmissionDecided = true;
        if (compositePolicy.debugGroupingShapes()) {
            System.err.printf(
                    "[flat-single-identity] selected=%d sampled=%d distinct=%d admitted=%s%n",
                    mask.count(),
                    sampled,
                    sampledDistinct,
                    flatSingleNullInTable);
        }
    }

    /** Selects the immutable slot/record split before the first composite key is inserted. */
    private void decideFlatPackedIdentity(Vector[] values, Vector[] nulls, Mask mask)
    {
        flatPackedIdentityAdmissionDecided = true;
        int sampled = Math.min(mask.count(), compositePolicy.flatSingleKeyRecordIdentitySampleSize());
        // Two-field streams already have dedicated direct-pair and producer-lookahead admissions; broadening the
        // blocking hint to dictionary-backed pairs moved their allocation and translation regime. Use this extra
        // lifecycle evidence only for a dense, otherwise-unserved three-field cohort. A partial first-batch mask
        // retains the compact ordinary slots: filtered three-field cohorts did not consistently amortize them.
        boolean blockingHorizon = blockingAggregationForCurrentBatch &&
                values.length == 3 &&
                mask.all() &&
                mask.count() >= compositePolicy.packedFlatIdentityBlockingMinBatchRows();
        boolean sustainedInput = moreInputExpectedForCurrentBatch || blockingHorizon;
        boolean eligibleFields = values.length <= compositePolicy.packedFlatIdentityMaxFields() || blockingHorizon;
        int sampledDistinct = sustainedInput &&
                flatGroupingLayout != null &&
                compositePolicy.packedFlatIdentitySlots() &&
                eligibleFields &&
                mask.count() >= compositePolicy.packedFlatIdentityMinBatchRows()
                ? sampledDistinctFlatKeys(flatGroupingLayout, values, nulls, mask)
                : 0;
        boolean admitted = nextGroupId == 0 && sustainedInput && eligibleFields &&
                compositePolicy.packedFlatIdentitySlots() &&
                mask.count() >= compositePolicy.packedFlatIdentityMinBatchRows() &&
                sampled > 0 &&
                (long) sampledDistinct * 100 >=
                        (long) sampled * compositePolicy.packedFlatIdentityMinDistinctPercent() &&
                (!blockingHorizon || sampledDistinct == sampled);
        if (admitted) {
            flatGroupingTable.releaseBuffers();
            flatGroupingTable = new FlatGroupingTable(
                    flatGroupingLayout,
                    Math.max(16, mask.count()),
                    true,
                    true);
        }
        if (compositePolicy.debugGroupingShapes() || compositePolicy.debugFlatPackedIdentity()) {
            System.err.printf(
                    "[flat-packed-identity] fields=%d selected=%d sampled=%d distinct=%d admitted=%s%n",
                    values.length,
                    mask.count(),
                    sampled,
                    sampledDistinct,
                    admitted);
        }
    }

    private boolean admitsFlatPackedIdentity(int fields, int selectedRows, int sampled, int sampledDistinct)
    {
        return compositePolicy.packedFlatIdentitySlots() &&
                fields <= compositePolicy.packedFlatIdentityMaxFields() &&
                selectedRows >= compositePolicy.packedFlatIdentityMinBatchRows() &&
                sampled > 0 &&
                (long) sampledDistinct * 100 >=
                        (long) sampled * compositePolicy.packedFlatIdentityMinDistinctPercent();
    }

    /**
     * Record-identity tables remove two logical/physical id mappings, which pays only when the input actually
     * creates many groups. A large batch with a tiny categorical domain instead pays for nullable records while
     * saving no material mapping space. Sample evenly across the selected rows before the first insertion and use
     * layout hashes as a conservative distinctness estimate: collisions can reject admission but cannot make an
     * unsafe representation decision. The decision is immutable for the lifetime of this grouping state.
     */
    private int sampledDistinctFlatKeys(FlatKeyLayout layout, Vector[] values, Vector[] nulls, Mask mask)
    {
        int sampleSize = Math.min(mask.count(), compositePolicy.flatSingleKeyRecordIdentitySampleSize());
        if (sampleSize <= 0) {
            return 0;
        }

        long[] hashes = arrayPool.borrowLongs(sampleSize);
        try {
            layout.beginBatch(values, nulls);
            try {
                for (int sample = 0; sample < sampleSize; sample++) {
                    int selectedIndex = (int) ((long) sample * mask.count() / sampleSize);
                    hashes[sample] = layout.hash(values, nulls, mask.position(selectedIndex));
                }
            }
            finally {
                layout.endBatch();
            }
            Arrays.sort(hashes, 0, sampleSize);
            int distinct = 1;
            for (int index = 1; index < sampleSize; index++) {
                if (hashes[index] != hashes[index - 1]) {
                    distinct++;
                }
            }
            return distinct;
        }
        finally {
            arrayPool.release(hashes);
        }
    }

    private void assignLongGroups(Vector values, Vector nullVector, Mask mask, I64Vector result)
    {
        // Hoist Vector type dispatch once per batch so the per-position loop body reads
        // through monomorphic accessor lambdas instead of OperatorVectorSupport's switch.
        VectorAccess.LongValues keyValues = VectorAccess.longValues(values);
        VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nullVector);
        long[] out = result.values();
        boolean directBatchPrepared = false;
        // Keep a rejected/non-candidate state on the original compact hash loop. In particular, do not pay a
        // helper call on every later batch after one out-of-domain key has permanently closed admission.
        if (longPolicy.direct() && !longDirectGroupingDisabled && !stagedLongDirectGroupingDisabled &&
                (useLongDirectGrouping || nextGroupId >= longDirectNextCheck)) {
            directBatchPrepared = prepareSingleLongDirectGrouping(mask, keyValues, nullValues);
        }
        // prepareSingleLongDirectGrouping already proves that every non-null key in this batch fits the direct
        // table (and grows it when needed). Retain the defensive pass only for callers that established direct
        // mode outside this ordinary staged path.
        if (useLongDirectGrouping && !directBatchPrepared) {
            int[] positions = mask.selectedPositions();
            int count = mask.count();
            for (int index = 0; index < count; index++) {
                int position = positions == null ? index : positions[index];
                if (nullValues.value(position)) {
                    continue;
                }
                long key = keyValues.value(position);
                if (key < 0 || key >= longGroupIds.length) {
                    disableLongDirectGrouping();
                    longDirectGroupingDisabled = true;
                    break;
                }
            }
        }
        if (useLongDirectGrouping) {
            int[] directGroups = longGroupIds;
            int[] positions = mask.selectedPositions();
            int count = mask.count();
            if (useCompressedLongDirectGrouping) {
                long compressionMask = longDirectCompressionMask;
                for (int index = 0; index < count; index++) {
                    int position = positions == null ? index : positions[index];
                    if (nullValues.value(position)) {
                        out[position] = nullGroup();
                        continue;
                    }
                    long key = keyValues.value(position);
                    int directKey = toIntExact(Long.compress(key, compressionMask));
                    int encodedGroup = directGroups[directKey];
                    if (encodedGroup == 0) {
                        int groupId = (int) nextGroupId++;
                        directGroups[directKey] = groupId + 1;
                        ensureLongGroupingCapacity(groupId);
                        longKeysByGroup[groupId] = key;
                        longGroupCount++;
                        out[position] = groupId;
                    }
                    else {
                        out[position] = encodedGroup - 1;
                    }
                }
                return;
            }
            for (int index = 0; index < count; index++) {
                int position = positions == null ? index : positions[index];
                if (nullValues.value(position)) {
                    out[position] = nullGroup();
                    continue;
                }
                int key = toIntExact(keyValues.value(position));
                int encodedGroup = directGroups[key];
                if (encodedGroup == 0) {
                    int groupId = (int) nextGroupId++;
                    directGroups[key] = groupId + 1;
                    ensureLongGroupingCapacity(groupId);
                    longKeysByGroup[groupId] = key;
                    longGroupCount++;
                    out[position] = groupId;
                }
                else {
                    out[position] = encodedGroup - 1;
                }
            }
            return;
        }
        // Hoist the table into locals; refresh after a rehash.
        long[] tableKeys = longGroupKeys;
        int[] tableIds = longGroupIds;
        int tableMask = longGroupMask;
        boolean cached = longRunCacheValid;
        long cachedKey = longRunCacheKey;
        int cachedGroupId = longRunCacheGroupId;
        boolean useRunCache = longPolicy.runCache() && hasFrequentLongRuns(keyValues, nullValues, mask, cached, cachedKey);
        int count = mask.count();
        if (!useRunCache && count >= longPolicy.hashPrecomputeMinBatchSize()) {
            int[] positions = mask.selectedPositions();
            ensureLongGroupHashCapacity(count);
            int[] hashes = longGroupHashes;
            precomputeLongGroupHashes(keyValues, nullValues, positions, count, hashes, out);
            for (int index = 0; index < count; index++) {
                int position = positions == null ? index : positions[index];
                if (nullValues.value(position)) {
                    out[position] = nullGroup();
                    continue;
                }

                long key = out[position];
                int hash = hashes[index];
                int slot = hash & tableMask;
                while (true) {
                    int encoded = tableIds[slot];
                    if (useIdIndexedLongGrouping ? encoded == 0 : encoded == -1) {
                        int groupId = (int) nextGroupId++;
                        if (!useIdIndexedLongGrouping) {
                            tableKeys[slot] = key;
                        }
                        tableIds[slot] = useIdIndexedLongGrouping ? encodeIdIndexedLongGroup(hash, groupId) : groupId;
                        ensureLongGroupingCapacity(groupId);
                        longKeysByGroup[groupId] = key;
                        out[position] = groupId;
                        cached = true;
                        cachedKey = key;
                        cachedGroupId = groupId;
                        if (++longGroupCount >= longGroupMaxFill) {
                            rehashLongGroupTable();
                            tableKeys = longGroupKeys;
                            tableIds = longGroupIds;
                            tableMask = longGroupMask;
                        }
                        break;
                    }
                    int id = useIdIndexedLongGrouping ? decodeIdIndexedLongGroup(encoded) : encoded;
                    if ((!useIdIndexedLongGrouping || encoded >>> ID_INDEXED_LONG_HASH_SHIFT == hash >>> ID_INDEXED_LONG_HASH_SHIFT)
                            && (useIdIndexedLongGrouping ? longKeysByGroup[id] : tableKeys[slot]) == key) {
                        out[position] = id;
                        cached = true;
                        cachedKey = key;
                        cachedGroupId = id;
                        break;
                    }
                    slot = (slot + 1) & tableMask;
                }
            }
            longRunCacheValid = cached;
            longRunCacheKey = cachedKey;
            longRunCacheGroupId = cachedGroupId;
            return;
        }
        if (!useRunCache) {
            for (int position : mask) {
                if (nullValues.value(position)) {
                    out[position] = nullGroup();
                    continue;
                }

                long key = keyValues.value(position);
                int hash = hashLong(key);
                int slot = hash & tableMask;
                while (true) {
                    int encoded = tableIds[slot];
                    if (useIdIndexedLongGrouping ? encoded == 0 : encoded == -1) {
                        int groupId = (int) nextGroupId++;
                        if (!useIdIndexedLongGrouping) {
                            tableKeys[slot] = key;
                        }
                        tableIds[slot] = useIdIndexedLongGrouping ? encodeIdIndexedLongGroup(hash, groupId) : groupId;
                        ensureLongGroupingCapacity(groupId);
                        longKeysByGroup[groupId] = key;
                        out[position] = groupId;
                        cached = true;
                        cachedKey = key;
                        cachedGroupId = groupId;
                        if (++longGroupCount >= longGroupMaxFill) {
                            rehashLongGroupTable();
                            tableKeys = longGroupKeys;
                            tableIds = longGroupIds;
                            tableMask = longGroupMask;
                        }
                        break;
                    }
                    int id = useIdIndexedLongGrouping ? decodeIdIndexedLongGroup(encoded) : encoded;
                    if ((!useIdIndexedLongGrouping || encoded >>> ID_INDEXED_LONG_HASH_SHIFT == hash >>> ID_INDEXED_LONG_HASH_SHIFT)
                            && (useIdIndexedLongGrouping ? longKeysByGroup[id] : tableKeys[slot]) == key) {
                        out[position] = id;
                        cached = true;
                        cachedKey = key;
                        cachedGroupId = id;
                        break;
                    }
                    slot = (slot + 1) & tableMask;
                }
            }
            longRunCacheValid = cached;
            longRunCacheKey = cachedKey;
            longRunCacheGroupId = cachedGroupId;
            return;
        }
        for (int position : mask) {
            if (nullValues.value(position)) {
                out[position] = nullGroup();
                continue;
            }

            long key = keyValues.value(position);
            if (cached && key == cachedKey) {
                out[position] = cachedGroupId;
                continue;
            }
            int hash = hashLong(key);
            int slot = hash & tableMask;
            while (true) {
                int encoded = tableIds[slot];
                if (useIdIndexedLongGrouping ? encoded == 0 : encoded == -1) {
                    int groupId = (int) nextGroupId++;
                    if (!useIdIndexedLongGrouping) {
                        tableKeys[slot] = key;
                    }
                    tableIds[slot] = useIdIndexedLongGrouping ? encodeIdIndexedLongGroup(hash, groupId) : groupId;
                    ensureLongGroupingCapacity(groupId);
                    longKeysByGroup[groupId] = key;
                    out[position] = groupId;
                    cached = true;
                    cachedKey = key;
                    cachedGroupId = groupId;
                    if (++longGroupCount >= longGroupMaxFill) {
                        rehashLongGroupTable();
                        tableKeys = longGroupKeys;
                        tableIds = longGroupIds;
                        tableMask = longGroupMask;
                    }
                    break;
                }
                int id = useIdIndexedLongGrouping ? decodeIdIndexedLongGroup(encoded) : encoded;
                if ((!useIdIndexedLongGrouping || encoded >>> ID_INDEXED_LONG_HASH_SHIFT == hash >>> ID_INDEXED_LONG_HASH_SHIFT)
                        && (useIdIndexedLongGrouping ? longKeysByGroup[id] : tableKeys[slot]) == key) {
                    out[position] = id;
                    cached = true;
                    cachedKey = key;
                    cachedGroupId = id;
                    break;
                }
                slot = (slot + 1) & tableMask;
            }
        }
        longRunCacheValid = cached;
        longRunCacheKey = cachedKey;
        longRunCacheGroupId = cachedGroupId;
    }

    private void ensureLongGroupHashCapacity(int size)
    {
        if (longGroupHashes.length >= size) {
            return;
        }
        int capacity = Integer.highestOneBit(size - 1) << 1;
        longGroupHashes = new int[capacity];
    }

    private static void precomputeLongGroupHashes(
            VectorAccess.LongValues keyValues,
            VectorAccess.BooleanValues nullValues,
            int[] positions,
            int count,
            int[] hashes,
            long[] keys)
    {
        for (int index = 0; index < count; index++) {
            int position = positions == null ? index : positions[index];
            if (!nullValues.value(position)) {
                long key = keyValues.value(position);
                keys[position] = key;
                hashes[index] = hashLong(key);
            }
        }
    }

    /**
     * Admit the adjacent-key cache once per batch instead of putting an unpredictable
     * cache-hit branch into every single-key grouping loop.  The bounded sample is
     * deliberately independent of key values and query shape: ordered/run-heavy input
     * takes the cache loop, while high-cardinality shuffled input keeps the plain hash
     * loop.  Sampling only reads existing vectors and allocates no state.
     */
    private static boolean hasFrequentLongRuns(
            VectorAccess.LongValues keyValues,
            VectorAccess.BooleanValues nullValues,
            Mask mask,
            boolean previousValid,
            long previousKey)
    {
        int comparisons = 0;
        int hits = 0;
        boolean havePrevious = previousValid;
        long lastKey = previousKey;
        for (int position : mask) {
            if (nullValues.value(position)) {
                havePrevious = false;
                continue;
            }
            long key = keyValues.value(position);
            if (havePrevious) {
                comparisons++;
                if (key == lastKey) {
                    hits++;
                }
                if (comparisons == 64) {
                    break;
                }
            }
            havePrevious = true;
            lastKey = key;
        }
        return comparisons >= 4 && hits * 2 >= comparisons;
    }

    private void assignPackedIntPairGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
        VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
        long[] out = result.values();
        byte[] tableControl = packedIntPairControl;
        int[] tableIds = longGroupIds;
        int tableMask = longGroupMask;
        for (int position : mask) {
            long first = firstValues.value(position);
            long second = secondValues.value(position);
            if (firstNulls.value(position) || secondNulls.value(position) || first != (int) first || second != (int) second) {
                promotePackedIntGrouping(mask.count());
                assignMultiLongGroups(values, nulls, mask, result);
                return;
            }

            long key = packIntPair((int) first, (int) second);
            int hash = hashLong(key);
            byte fragment = AbstractMultiLongGroupingTable.controlFragment(hash);
            int slot = hash & tableMask;
            while (true) {
                byte control = tableControl[slot];
                if (control == 0) {
                    int groupId = (int) nextGroupId++;
                    tableIds[slot] = groupId;
                    ensureLongGroupingCapacity(groupId);
                    longKeysByGroup[groupId] = key;
                    tableControl[slot] = fragment;
                    out[position] = groupId;
                    if (++longGroupCount >= longGroupMaxFill) {
                        rehashPackedIntPairTable();
                        tableControl = packedIntPairControl;
                        tableIds = longGroupIds;
                        tableMask = longGroupMask;
                    }
                    break;
                }
                int id = tableIds[slot];
                if (control == fragment && longKeysByGroup[id] == key) {
                    out[position] = id;
                    break;
                }
                slot = (slot + 1) & tableMask;
            }
        }
    }

    private void assignPackedIntTripleGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
        VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
        VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
        VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
        boolean nullFree = VectorAccess.isAllFalseNulls(nulls[0]) &&
                VectorAccess.isAllFalseNulls(nulls[1]) &&
                VectorAccess.isAllFalseNulls(nulls[2]);
        long[] out = result.values();
        byte[] tableControl = packedIntPairControl;
        int[] tableIds = longGroupIds;
        int tableMask = longGroupMask;
        for (int position : mask) {
            long first = firstValues.value(position);
            long second = secondValues.value(position);
            long third = thirdValues.value(position);
            boolean firstNull = !nullFree && firstNulls.value(position);
            boolean secondNull = !nullFree && secondNulls.value(position);
            boolean thirdNull = !nullFree && thirdNulls.value(position);
            if ((!firstNull && first != (int) first) ||
                    (!secondNull && second != (int) second) ||
                    (!thirdNull && (third != (int) third ||
                            (compositePolicy.packedIntTriplePackedTail() &&
                                    (third < PACKED_INT_TRIPLE_MIN_THIRD || third > PACKED_INT_TRIPLE_MAX_THIRD))))) {
                promotePackedIntGrouping(mask.count());
                assignMultiLongGroups(values, nulls, mask, result);
                return;
            }

            long pair = packIntPair(firstNull ? 0 : (int) first, secondNull ? 0 : (int) second);
            int thirdInt = thirdNull ? 0 : (int) third;
            byte nullMask = (byte) ((firstNull ? 1 : 0) | (secondNull ? 2 : 0) | (thirdNull ? 4 : 0));
            int hash = hashPackedIntTriple(pair, thirdInt, nullMask);
            byte fragment = AbstractMultiLongGroupingTable.controlFragment(hash);
            int slot = hash & tableMask;
            while (true) {
                int encoded = compositePolicy.packedIntTripleCombinedControl() ? tableIds[slot] : 0;
                byte control = compositePolicy.packedIntTripleCombinedControl()
                        ? (byte) (encoded >>> 24)
                        : tableControl[slot];
                if (control == 0) {
                    if (compositePolicy.packedIntTripleCombinedControl() && nextGroupId > PACKED_GROUP_ID_MASK) {
                        promotePackedIntGrouping(mask.count());
                        assignMultiLongGroups(values, nulls, mask, result);
                        return;
                    }
                    int groupId = (int) nextGroupId++;
                    tableIds[slot] = compositePolicy.packedIntTripleCombinedControl()
                            ? (fragment & 0xFF) << 24 | groupId
                            : groupId;
                    ensureLongGroupingCapacity(groupId);
                    longKeysByGroup[groupId] = pair;
                    if (compositePolicy.packedIntTriplePackedTail()) {
                        packedIntTripleTailByGroup[groupId] = packIntTripleTail(thirdInt, nullMask);
                    }
                    else {
                        packedIntTripleThirdByGroup[groupId] = thirdInt;
                        packedIntTripleNullMasksByGroup[groupId] = nullMask;
                    }
                    if (!compositePolicy.packedIntTripleCombinedControl()) {
                        tableControl[slot] = fragment;
                    }
                    out[position] = groupId;
                    if (++longGroupCount >= longGroupMaxFill) {
                        rehashPackedIntPairTable();
                        tableControl = packedIntPairControl;
                        tableIds = longGroupIds;
                        tableMask = longGroupMask;
                    }
                    break;
                }
                int id = compositePolicy.packedIntTripleCombinedControl()
                        ? encoded & PACKED_GROUP_ID_MASK
                        : tableIds[slot];
                if (control == fragment &&
                        longKeysByGroup[id] == pair &&
                        packedIntTripleThird(id) == thirdInt &&
                        packedIntTripleNullMask(id) == nullMask) {
                    out[position] = id;
                    break;
                }
                slot = (slot + 1) & tableMask;
            }
        }
    }

    private void promotePackedIntGrouping(int upcomingRows)
    {
        multiLongArity = packedIntGroupingArity;
        multiLongTable = codeGeneration.multiLongGrouping().create(
                multiLongArity,
                Math.max(16, toIntExact(Math.min(Integer.MAX_VALUE, nextGroupId + upcomingRows))),
                arrayPool,
                adaptiveLongGroupingPolicy);
        long[] packedByGroup = longKeysByGroup;
        int[] thirdByGroup = packedIntTripleThirdByGroup;
        byte[] nullMasksByGroup = packedIntTripleNullMasksByGroup;
        int[] tailByGroup = packedIntTripleTailByGroup;
        long migratedGroups = 0;
        long[] ignoredResults = arrayPool.borrowLongs(10_000);
        VectorAccess.BooleanValues neverNull = _ -> false;
        while (migratedGroups < nextGroupId) {
            int offset = toIntExact(migratedGroups);
            int count = (int) Math.min(ignoredResults.length, nextGroupId - migratedGroups);
            VectorAccess.LongValues[] keyAccessors = packedIntGroupingArity == 2
                    ? new VectorAccess.LongValues[] {
                            position -> unpackFirstInt(packedByGroup[offset + position]),
                            position -> unpackSecondInt(packedByGroup[offset + position])}
                    : new VectorAccess.LongValues[] {
                            position -> unpackFirstInt(packedByGroup[offset + position]),
                            position -> unpackSecondInt(packedByGroup[offset + position]),
                            position -> compositePolicy.packedIntTriplePackedTail()
                                    ? unpackIntTripleThird(tailByGroup[offset + position])
                                    : thirdByGroup[offset + position]};
            VectorAccess.BooleanValues[] nullAccessors = packedIntGroupingArity == 2
                    ? new VectorAccess.BooleanValues[] {neverNull, neverNull}
                    : new VectorAccess.BooleanValues[] {
                            position -> ((compositePolicy.packedIntTriplePackedTail()
                                    ? (byte) (tailByGroup[offset + position] >>> 29)
                                    : nullMasksByGroup[offset + position]) & 1) != 0,
                            position -> ((compositePolicy.packedIntTriplePackedTail()
                                    ? (byte) (tailByGroup[offset + position] >>> 29)
                                    : nullMasksByGroup[offset + position]) & 2) != 0,
                            position -> ((compositePolicy.packedIntTriplePackedTail()
                                    ? (byte) (tailByGroup[offset + position] >>> 29)
                                    : nullMasksByGroup[offset + position]) & 4) != 0};
            migratedGroups = multiLongTable.assignBatch(
                    keyAccessors,
                    nullAccessors,
                    densePositions(count),
                    count,
                    ignoredResults,
                    migratedGroups);
        }
        arrayPool.release(ignoredResults);
        arrayPool.release(packedIntPairControl);
        packedIntPairControl = null;
        arrayPool.release(longGroupIds);
        longGroupIds = null;
        arrayPool.release(longKeysByGroup);
        longKeysByGroup = new long[0];
        longGroupHashes = new int[0];
        arrayPool.release(packedIntTripleThirdByGroup);
        packedIntTripleThirdByGroup = new int[0];
        arrayPool.release(packedIntTripleNullMasksByGroup);
        packedIntTripleNullMasksByGroup = new byte[0];
        arrayPool.release(packedIntTripleTailByGroup);
        packedIntTripleTailByGroup = new int[0];
        usePackedIntPairGrouping = false;
        packedIntGroupingArity = 0;
        useMultiLongGrouping = true;
    }

    private void initPackedIntPairTable(int expectedSize)
    {
        int capacity = 16;
        while (capacity < expectedSize / LONG_GROUP_LOAD_FACTOR) {
            capacity <<= 1;
        }
        if (packedIntGroupingArity == 3 && compositePolicy.packedIntTripleCombinedControl()) {
            packedIntPairControl = null;
        }
        else {
            packedIntPairControl = arrayPool.borrowBytes(capacity);
            Arrays.fill(packedIntPairControl, (byte) 0);
        }
        longGroupIds = arrayPool.borrowInts(capacity);
        if (packedIntGroupingArity == 3 && compositePolicy.packedIntTripleCombinedControl()) {
            Arrays.fill(longGroupIds, 0);
        }
        longGroupMask = capacity - 1;
        longGroupMaxFill = (int) (capacity * LONG_GROUP_LOAD_FACTOR);
        longGroupCount = 0;
    }

    private void rehashPackedIntPairTable()
    {
        byte[] previousControl = packedIntPairControl;
        int[] previousIds = longGroupIds;
        boolean combinedControl = packedIntGroupingArity == 3 && compositePolicy.packedIntTripleCombinedControl();
        int previousCapacity = previousIds.length;
        int capacity = previousCapacity * 2;
        if (combinedControl) {
            packedIntPairControl = null;
        }
        else {
            packedIntPairControl = arrayPool.borrowBytes(capacity);
            Arrays.fill(packedIntPairControl, (byte) 0);
        }
        longGroupIds = arrayPool.borrowInts(capacity);
        if (combinedControl) {
            Arrays.fill(longGroupIds, 0);
        }
        longGroupMask = capacity - 1;
        longGroupMaxFill = (int) (capacity * LONG_GROUP_LOAD_FACTOR);
        for (int index = 0; index < previousCapacity; index++) {
            int previousEntry = previousIds[index];
            if (combinedControl ? previousEntry == 0 : previousControl[index] == 0) {
                continue;
            }
            int id = combinedControl ? previousEntry & PACKED_GROUP_ID_MASK : previousEntry;
            long key = longKeysByGroup[id];
            int hash = packedIntGroupingArity == 2
                    ? hashLong(key)
                    : hashPackedIntTriple(key, packedIntTripleThird(id), packedIntTripleNullMask(id));
            int slot = hash & longGroupMask;
            while (combinedControl ? longGroupIds[slot] != 0 : packedIntPairControl[slot] != 0) {
                slot = (slot + 1) & longGroupMask;
            }
            byte fragment = AbstractMultiLongGroupingTable.controlFragment(hash);
            longGroupIds[slot] = combinedControl ? (fragment & 0xFF) << 24 | id : id;
            if (!combinedControl) {
                packedIntPairControl[slot] = fragment;
            }
        }
        arrayPool.release(previousControl);
        arrayPool.release(previousIds);
    }

    private static int hashPackedIntTriple(long firstTwo, int third, byte nullMask)
    {
        return hashLong(firstTwo + (long) third * 0xD6E8FEB86659FD93L + nullMask);
    }

    private int packedIntTripleThird(int groupId)
    {
        return compositePolicy.packedIntTriplePackedTail()
                ? unpackIntTripleThird(packedIntTripleTailByGroup[groupId])
                : packedIntTripleThirdByGroup[groupId];
    }

    private byte packedIntTripleNullMask(int groupId)
    {
        return compositePolicy.packedIntTriplePackedTail()
                ? (byte) (packedIntTripleTailByGroup[groupId] >>> 29)
                : packedIntTripleNullMasksByGroup[groupId];
    }

    private static int packIntTripleTail(int third, byte nullMask)
    {
        return (third & 0x1FFF_FFFF) | (nullMask & 0x7) << 29;
    }

    private static int unpackIntTripleThird(int packed)
    {
        return packed << 3 >> 3;
    }

    private static long packIntPair(int first, int second)
    {
        return ((long) first << Integer.SIZE) | (second & 0xFFFF_FFFFL);
    }

    private static long unpackFirstInt(long packed)
    {
        return (int) (packed >> Integer.SIZE);
    }

    private static long unpackSecondInt(long packed)
    {
        return (int) packed;
    }

    private void initLongGroupTable(int expectedSize)
    {
        int capacity = 16;
        while (capacity < expectedSize / LONG_GROUP_LOAD_FACTOR) {
            capacity <<= 1;
        }
        longGroupKeys = arrayPool.borrowLongs(capacity);
        longGroupIds = arrayPool.borrowInts(capacity);
        Arrays.fill(longGroupIds, useIdIndexedLongGrouping ? 0 : -1);
        longGroupMask = capacity - 1;
        longGroupMaxFill = (int) (capacity * LONG_GROUP_LOAD_FACTOR);
        longGroupCount = 0;
    }

    private void rehashLongGroupTable()
    {
        long[] previousKeys = longGroupKeys;
        int[] previousIds = longGroupIds;
        int capacity = previousIds.length * 2;
        longGroupKeys = useIdIndexedLongGrouping ? new long[0] : arrayPool.borrowLongs(capacity);
        longGroupIds = arrayPool.borrowInts(capacity);
        Arrays.fill(longGroupIds, useIdIndexedLongGrouping ? 0 : -1);
        longGroupMask = capacity - 1;
        longGroupMaxFill = (int) (capacity * LONG_GROUP_LOAD_FACTOR);
        if (useIdIndexedLongGrouping && longPolicy.idIndexedDenseRehash()) {
            // Dense group ids and the canonical reverse map are a cheaper iteration domain than the sparse old
            // slots. This also removes the unpredictable occupied/empty branch from large-table rehashes.
            for (int id = 0; id < longGroupCount; id++) {
                if (id == nullGroup) {
                    continue;
                }
                long key = longKeysByGroup[id];
                int hash = hashLong(key);
                int slot = hash & longGroupMask;
                while (longGroupIds[slot] != 0) {
                    slot = (slot + 1) & longGroupMask;
                }
                longGroupIds[slot] = encodeIdIndexedLongGroup(hash, id);
            }
        }
        else {
            for (int index = 0; index < previousIds.length; index++) {
                int encoded = previousIds[index];
                if (useIdIndexedLongGrouping ? encoded == 0 : encoded == -1) {
                    continue;
                }
                int id = useIdIndexedLongGrouping ? decodeIdIndexedLongGroup(encoded) : encoded;
                long key = useIdIndexedLongGrouping ? longKeysByGroup[id] : previousKeys[index];
                int hash = hashLong(key);
                int slot = hash & longGroupMask;
                while (longGroupIds[slot] != (useIdIndexedLongGrouping ? 0 : -1)) {
                    slot = (slot + 1) & longGroupMask;
                }
                if (!useIdIndexedLongGrouping) {
                    longGroupKeys[slot] = key;
                }
                longGroupIds[slot] = useIdIndexedLongGrouping ? encodeIdIndexedLongGroup(hash, id) : id;
            }
        }
        arrayPool.release(previousKeys);
        arrayPool.release(previousIds);
    }

    private void rebuildLongDirectTable(int capacity, long observedMax)
    {
        int[] previousIds = longGroupIds;
        long[] previousKeys = longGroupKeys;
        boolean previousDirect = useLongDirectGrouping;
        longGroupIds = borrowZeroedLongDirectIds(capacity);
        longGroupKeys = new long[0];
        for (int group = 0; group < nextGroupId; group++) {
            if (group == nullGroup) {
                continue;
            }
            long originalKey = longKeysByGroup[group];
            int key = useCompressedLongDirectGrouping
                    ? toIntExact(Long.compress(originalKey, longDirectCompressionMask))
                    : toIntExact(originalKey);
            if (longGroupIds[key] != 0) {
                throw new IllegalStateException("Duplicate key while building direct grouping table: " + originalKey);
            }
            longGroupIds[key] = group + 1;
        }
        longGroupMask = capacity - 1;
        longGroupMaxFill = capacity;
        useLongDirectGrouping = true;
        if (longPolicy.debugDirect()) {
            System.err.printf(
                    "[long-direct-grouping] enable groups=%d max=%d capacity=%d compressed=%s mask=%x%n",
                    nextGroupId,
                    observedMax,
                    capacity,
                    useCompressedLongDirectGrouping,
                    longDirectCompressionMask);
        }
        releaseLongGroupIds(previousIds, previousDirect);
        arrayPool.release(previousKeys);
    }

    private void disableLongDirectGrouping()
    {
        if (!useLongDirectGrouping) {
            return;
        }
        int capacity = 16;
        int nonNullGroups = toIntExact(nextGroupId - (nullGroup == -1 ? 0 : 1));
        while (capacity < nonNullGroups / LONG_GROUP_LOAD_FACTOR) {
            capacity <<= 1;
        }
        int[] previousIds = longGroupIds;
        longGroupKeys = arrayPool.borrowLongs(capacity);
        longGroupIds = arrayPool.borrowInts(capacity);
        Arrays.fill(longGroupIds, -1);
        longGroupMask = capacity - 1;
        longGroupMaxFill = (int) (capacity * LONG_GROUP_LOAD_FACTOR);
        for (int group = 0; group < nextGroupId; group++) {
            if (group == nullGroup) {
                continue;
            }
            long key = longKeysByGroup[group];
            int slot = hashLong(key) & longGroupMask;
            while (longGroupIds[slot] != -1) {
                slot = (slot + 1) & longGroupMask;
            }
            longGroupKeys[slot] = key;
            longGroupIds[slot] = group;
        }
        releaseLongGroupIds(previousIds, true);
        useLongDirectGrouping = false;
        useCompressedLongDirectGrouping = false;
        longDirectCompressionMask = -1;
        longDirectConstantBits = 0;
        if (longPolicy.debugDirect()) {
            System.err.printf("[long-direct-grouping] disable groups=%d hashCapacity=%d%n", nextGroupId, capacity);
        }
    }

    private int[] borrowZeroedLongDirectIds(int capacity)
    {
        if (!resources.poolZeroedLongDirectIds()) {
            int[] ids = arrayPool.borrowInts(capacity);
            Arrays.fill(ids, 0);
            return ids;
        }
        int[] ids = arrayPool.borrow(resources.zeroedLongDirectIdsFamily(), capacity, int[].class);
        return ids == null ? new int[capacity] : ids;
    }

    private void releaseLongGroupIds(int[] ids, boolean direct)
    {
        if (ids == null) {
            return;
        }
        if (!direct || !resources.poolZeroedLongDirectIds()) {
            arrayPool.release(ids);
            return;
        }
        for (int group = 0; group < nextGroupId; group++) {
            if (group == nullGroup) {
                continue;
            }
            long key = longKeysByGroup[group];
            long directKey = useCompressedLongDirectGrouping
                    ? Long.compress(key, longDirectCompressionMask)
                    : key;
            if (directKey >= 0 && directKey < ids.length) {
                ids[(int) directKey] = 0;
            }
        }
        arrayPool.retain(
                resources.zeroedLongDirectIdsFamily(),
                ids.length,
                (long) ids.length * Integer.BYTES,
                ids);
    }

    private static int toPowerOfTwoCapacity(int needed)
    {
        int capacity = 16;
        while (capacity < needed) {
            capacity = Math.multiplyExact(capacity, 2);
        }
        return capacity;
    }

    private int longGroupGet(long key)
    {
        if (useLongDirectGrouping) {
            if (key < 0) {
                return -1;
            }
            long directKey = key;
            if (useCompressedLongDirectGrouping) {
                if ((key & ~longDirectCompressionMask) != longDirectConstantBits) {
                    return -1;
                }
                directKey = Long.compress(key, longDirectCompressionMask);
            }
            if (directKey >= longGroupIds.length) {
                return -1;
            }
            return longGroupIds[(int) directKey] - 1;
        }
        int hash = hashLong(key);
        int slot = hash & longGroupMask;
        while (true) {
            int encoded = longGroupIds[slot];
            if (useIdIndexedLongGrouping) {
                if (encoded == 0) {
                    return -1;
                }
                int id = decodeIdIndexedLongGroup(encoded);
                if (encoded >>> ID_INDEXED_LONG_HASH_SHIFT == hash >>> ID_INDEXED_LONG_HASH_SHIFT && longKeysByGroup[id] == key) {
                    return id;
                }
            }
            else if (encoded == -1 || longGroupKeys[slot] == key) {
                return encoded;
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
        int nullableColumns = 0;
        for (Vector nullVector : nulls) {
            if (!VectorAccess.isAllFalseNulls(nullVector)) {
                nullableColumns++;
            }
        }
        VectorAccess.BooleanValues[] nullAccessors = nullableColumns == 0 && multiLongTable.supportsSparseNullAccessors()
                ? null
                : new VectorAccess.BooleanValues[arity];
        for (int key = 0; key < arity; key++) {
            keyAccessors[key] = VectorAccess.longValues(values[key]);
            if (nullAccessors != null) {
                nullAccessors[key] = VectorAccess.isAllFalseNulls(nulls[key])
                        ? multiLongTable.supportsSparseNullAccessors() ? null : NEVER_NULL
                        : VectorAccess.booleanValues(nulls[key]);
            }
        }
        int[] positions = mask.all()
                ? multiLongTable.supportsImplicitDensePositions() ? null : densePositions(mask.count())
                : mask.selectedPositions();
        nextGroupId = multiLongTable.assignBatch(keyAccessors, nullAccessors, positions, mask.count(), result.values(), nextGroupId);
    }

    private void assignMultiLongGroupsDiscardingResults(Vector[] values, Vector[] nulls, Mask mask)
    {
        int arity = multiLongArity;
        VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
        VectorAccess.BooleanValues[] nullAccessors = new VectorAccess.BooleanValues[arity];
        int nullableColumns = 0;
        for (int column = 0; column < arity; column++) {
            keyAccessors[column] = VectorAccess.longValues(values[column]);
            if (nulls != null && nulls[column] != null) {
                nullAccessors[column] = VectorAccess.booleanValues(nulls[column]);
                nullableColumns++;
            }
        }
        if (nullableColumns == 0 && multiLongTable.supportsSparseNullAccessors()) {
            nullAccessors = null;
        }
        int[] positions = mask.all()
                ? multiLongTable.supportsImplicitDensePositions() ? null : densePositions(mask.count())
                : mask.selectedPositions();
        nextGroupId = multiLongTable.assignBatchDiscardingResults(keyAccessors, nullAccessors, positions, mask.count(), nextGroupId);
    }

    // Reused 0..size-1 index array for the all-selected case (the generated batch kernel takes an int[]).
    private int[] densePositions(int size)
    {
        if (densePositionsCache.length < size) {
            int[] previous = densePositionsCache;
            densePositionsCache = arrayPool.borrowInts(size);
            for (int index = 0; index < size; index++) {
                densePositionsCache[index] = index;
            }
            arrayPool.release(previous);
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

    /**
     * Resolves each physical dictionary entry through the authoritative flat table once per dictionary generation,
     * then maps logical rows through their ids. Single-key flat tables keep NULL outside the table, so their generic
     * table-local dictionary cache cannot safely assume dense identity group ids; this owner-level path preserves
     * that null contract while avoiding a full hash probe for every repeated logical row. The flat table is bound
     * to the base values rather than the dictionary wrapper, so correctness never depends on dictionary identity
     * caches or on callers reporting raw-content mutation through {@link Vector#contentGeneration()}.
     */
    private void assignFlatDictionaryGroups(DictionaryVector dictionary, Vector nullVector, Mask mask, I64Vector result)
    {
        int[] ids = dictionary.ids();
        Vector dictionaryValues = dictionary.values();
        ensureDictionaryCacheCapacity(dictionaryValues.length());
        int generation = nextDictionaryGeneration();
        Vector[] domainValues = {dictionaryValues};
        Vector[] domainNulls = {null};
        long[] output = result.values();

        flatGroupingTable.beginBatch(domainValues, domainNulls);
        try {
            for (int position : mask) {
                if (OperatorVectorSupport.isNull(nullVector, position)) {
                    output[position] = nullGroup();
                    continue;
                }

                int dictionaryId = ids[position];
                if (dictionaryGenerations[dictionaryId] != generation) {
                    long newGroupId = nextGroupId;
                    long groupId = flatGroupingTable.assignGroup(
                            domainValues,
                            domainNulls,
                            dictionaryId,
                            newGroupId);
                    if (groupId == newGroupId) {
                        nextGroupId++;
                    }
                    dictionaryGroupsById[dictionaryId] = groupId;
                    dictionaryGenerations[dictionaryId] = generation;
                }
                output[position] = dictionaryGroupsById[dictionaryId];
            }
        }
        finally {
            flatGroupingTable.endBatch();
        }
    }

    /**
     * Counts a single dictionary key's logical occurrences and resolves each used physical value to a group once.
     * The extra domain slot represents top-level nulls.  This deliberately lives beside the grouping
     * representations: callers need not know whether the key was admitted to a primitive, flat, or object table.
     */
    int assignSingleDictionaryDomain(
            DictionaryVector dictionary,
            Vector nullVector,
            Mask mask,
            int[] counts,
            int[] domainGroups,
            int[] representatives)
    {
        int domainSize = dictionary.values().length();
        int slots = domainSize + 1;
        Arrays.fill(counts, 0, slots, 0);
        Arrays.fill(representatives, 0, domainSize, -1);

        int[] ids = dictionary.ids();
        int used = 0;
        for (int position : mask) {
            int domain = OperatorVectorSupport.isNull(nullVector, position) ? domainSize : ids[position];
            if (counts[domain]++ == 0) {
                used++;
                if (domain < domainSize) {
                    representatives[domain] = position;
                }
            }
        }
        reserveAdditionalGroups(used);

        Vector dictionaryValues = dictionary.values();
        if (useLongGrouping) {
            VectorAccess.LongValues values = VectorAccess.longValues(dictionaryValues);
            for (int domain = 0; domain < domainSize; domain++) {
                if (counts[domain] != 0) {
                    domainGroups[domain] = groupForLongKey(values.value(domain));
                }
            }
        }
        else if (useFlatGrouping) {
            Vector[] values = {dictionary};
            Vector[] nulls = {nullVector};
            flatGroupingTable.beginBatch(values, nulls);
            try {
                for (int domain = 0; domain < domainSize; domain++) {
                    if (counts[domain] != 0) {
                        long newGroupId = nextGroupId;
                        long groupId = flatGroupingTable.assignGroup(values, nulls, representatives[domain], newGroupId);
                        if (groupId == newGroupId) {
                            nextGroupId++;
                        }
                        domainGroups[domain] = toIntExact(groupId);
                    }
                }
            }
            finally {
                flatGroupingTable.endBatch();
            }
        }
        else {
            for (int domain = 0; domain < domainSize; domain++) {
                if (counts[domain] != 0) {
                    OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(
                            dictionaryValues, null, domain, reusableProbeKeys[0]);
                    domainGroups[domain] = toIntExact(groupForSingleKey(key));
                }
            }
        }
        if (counts[domainSize] != 0) {
            domainGroups[domainSize] = toIntExact(nullGroup());
        }
        accountRetainedState();
        return slots;
    }

    private void assignDictionaryLongGroups(DictionaryVector dictionary, Vector nullVector, Mask mask, I64Vector result)
    {
        int[] ids = dictionary.ids();
        Vector dictionaryValues = dictionary.values();
        VectorAccess.LongValues values = VectorAccess.longValues(dictionaryValues);
        ensureDictionaryCacheCapacity(dictionaryValues.length());
        int generation = currentDictionaryGeneration(dictionaryValues);
        long[] output = result.values();

        for (int position : mask) {
            if (OperatorVectorSupport.isNull(nullVector, position)) {
                output[position] = nullGroup();
                continue;
            }

            int dictionaryId = ids[position];
            if (dictionaryGenerations[dictionaryId] != generation) {
                dictionaryGroupsById[dictionaryId] = groupForLongKey(values.value(dictionaryId));
                dictionaryGenerations[dictionaryId] = generation;
            }
            output[position] = dictionaryGroupsById[dictionaryId];
        }
    }

    /**
     * Resolves only dictionary values that occur when the caller does not consume row group IDs. Once every domain
     * value has been observed (and NULL is either impossible or already known), the remaining logical rows cannot
     * change grouping state and need not be visited.
     */
    private void assignDictionaryGroupsDiscardingResults(DictionaryVector dictionary, Vector nullVector, Mask mask)
    {
        int[] ids = dictionary.ids();
        Vector dictionaryValues = dictionary.values();
        int domainSize = dictionaryValues.length();
        ensureDictionaryCacheCapacity(domainSize);
        int generation = useFlatGrouping ? nextDictionaryGeneration() : currentDictionaryGeneration(dictionaryValues);
        int unresolved = 0;
        for (int dictionaryId = 0; dictionaryId < domainSize; dictionaryId++) {
            if (dictionaryGenerations[dictionaryId] != generation) {
                unresolved++;
            }
        }

        boolean nullResolved = VectorAccess.isAllFalseNulls(nullVector) || (!flatSingleNullInTable && nullGroup >= 0);
        if (unresolved == 0 && nullResolved) {
            return;
        }
        reserveAdditionalGroups(unresolved + (nullResolved ? 0L : 1L));
        VectorAccess.LongValues longValues = useLongGrouping ? VectorAccess.longValues(dictionaryValues) : null;
        Vector[] domainValues = useFlatGrouping ? new Vector[] {dictionary} : null;
        Vector[] domainNulls = useFlatGrouping ? new Vector[] {nullVector} : null;
        if (useFlatGrouping) {
            flatGroupingTable.beginBatch(domainValues, domainNulls);
        }
        try {
            for (int position : mask) {
                if (OperatorVectorSupport.isNull(nullVector, position)) {
                    if (useFlatGrouping && flatSingleNullInTable) {
                        if (!nullResolved) {
                            long newGroupId = nextGroupId;
                            if (flatGroupingTable.assignGroup(domainValues, domainNulls, position, newGroupId) == newGroupId) {
                                nextGroupId++;
                            }
                        }
                    }
                    else {
                        nullGroup();
                    }
                    nullResolved = true;
                }
                else {
                    int dictionaryId = ids[position];
                    if (dictionaryGenerations[dictionaryId] != generation) {
                        long groupId;
                        if (useLongGrouping) {
                            groupId = groupForLongKey(longValues.value(dictionaryId));
                        }
                        else if (useFlatGrouping) {
                            long newGroupId = nextGroupId;
                            groupId = flatGroupingTable.assignGroup(
                                    domainValues,
                                    domainNulls,
                                    position,
                                    newGroupId);
                            if (groupId == newGroupId) {
                                nextGroupId++;
                            }
                        }
                        else {
                            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(
                                    dictionaryValues, null, dictionaryId, reusableProbeKeys[0]);
                            groupId = groupForSingleKey(key);
                        }
                        dictionaryGroupsById[dictionaryId] = groupId;
                        dictionaryGenerations[dictionaryId] = generation;
                        unresolved--;
                    }
                }
                if (unresolved == 0 && nullResolved) {
                    return;
                }
            }
        }
        finally {
            if (useFlatGrouping) {
                flatGroupingTable.endBatch();
            }
        }
    }

    int groupForLongKey(long key)
    {
        if (useLongDirectGrouping) {
            long directKey = useCompressedLongDirectGrouping ? Long.compress(key, longDirectCompressionMask) : key;
            boolean compatible = key >= 0 &&
                    (!useCompressedLongDirectGrouping || (key & ~longDirectCompressionMask) == longDirectConstantBits) &&
                    directKey < longGroupIds.length;
            if (compatible) {
                int encodedGroup = longGroupIds[(int) directKey];
                if (encodedGroup != 0) {
                    return encodedGroup - 1;
                }
                int groupId = (int) nextGroupId++;
                longGroupIds[(int) directKey] = groupId + 1;
                ensureLongGroupingCapacity(groupId);
                longKeysByGroup[groupId] = key;
                longGroupCount++;
                return groupId;
            }
            disableLongDirectGrouping();
            longDirectGroupingDisabled = true;
        }

        int hash = hashLong(key);
        int slot = hash & longGroupMask;
        while (true) {
            int encoded = longGroupIds[slot];
            if (useIdIndexedLongGrouping ? encoded == 0 : encoded == -1) {
                int groupId = (int) nextGroupId++;
                if (!useIdIndexedLongGrouping) {
                    longGroupKeys[slot] = key;
                }
                longGroupIds[slot] = useIdIndexedLongGrouping ? encodeIdIndexedLongGroup(hash, groupId) : groupId;
                ensureLongGroupingCapacity(groupId);
                longKeysByGroup[groupId] = key;
                if (++longGroupCount >= longGroupMaxFill) {
                    rehashLongGroupTable();
                }
                return groupId;
            }
            int groupId = useIdIndexedLongGrouping ? decodeIdIndexedLongGroup(encoded) : encoded;
            if ((!useIdIndexedLongGrouping || encoded >>> ID_INDEXED_LONG_HASH_SHIFT == hash >>> ID_INDEXED_LONG_HASH_SHIFT) &&
                    (useIdIndexedLongGrouping ? longKeysByGroup[groupId] : longGroupKeys[slot]) == key) {
                return groupId;
            }
            slot = (slot + 1) & longGroupMask;
        }
    }

    private void ensureDictionaryCacheCapacity(int size)
    {
        if (dictionaryGroupsById.length >= size) {
            return;
        }
        int newSize = Math.max(size, Math.max(16, dictionaryGroupsById.length * 2));
        long[] previousGroups = dictionaryGroupsById;
        int[] previousGenerations = dictionaryGenerations;
        dictionaryGroupsById = arrayPool.borrowLongs(newSize);
        dictionaryGenerations = arrayPool.borrowInts(newSize);
        System.arraycopy(previousGroups, 0, dictionaryGroupsById, 0, previousGroups.length);
        System.arraycopy(previousGenerations, 0, dictionaryGenerations, 0, previousGenerations.length);
        Arrays.fill(dictionaryGenerations, previousGenerations.length, dictionaryGenerations.length, 0);
        arrayPool.release(previousGroups);
        arrayPool.release(previousGenerations);
    }

    private int currentDictionaryGeneration(Vector dictionaryValues)
    {
        long contentGeneration = dictionaryValues.contentGeneration();
        if (contentGeneration < 0 ||
                cachedDictionaryValues != dictionaryValues ||
                cachedDictionaryContentGeneration != contentGeneration) {
            cachedDictionaryValues = dictionaryValues;
            cachedDictionaryContentGeneration = contentGeneration;
            if (dictionaryGeneration == Integer.MAX_VALUE) {
                Arrays.fill(dictionaryGenerations, 0);
                dictionaryGeneration = 0;
            }
            return ++dictionaryGeneration;
        }
        return dictionaryGeneration;
    }

    private int nextDictionaryGeneration()
    {
        cachedDictionaryValues = null;
        cachedDictionaryContentGeneration = -1;
        if (dictionaryGeneration == Integer.MAX_VALUE) {
            Arrays.fill(dictionaryGenerations, 0);
            dictionaryGeneration = 0;
        }
        return ++dictionaryGeneration;
    }

    public Streams groupedValues(int groupedColumnIndex, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        if (structuralGrouping != null) {
            return structuralGrouping.groupedValues(groupedColumnIndex, mask, output, allocator, allocationContext);
        }
        if (usePackedIntPairGrouping) {
            return Streams.ofValuesAndNulls(
                    materializePackedIntGroupedValues(groupedColumnIndex, mask, output == null ? null : output.values(), allocator, allocationContext),
                    packedIntGroupingArity == 2
                            ? materializeNonNulls(mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext)
                            : materializePackedIntTripleNulls(groupedColumnIndex, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        }
        if (useLongGrouping) {
            return Streams.ofValuesAndNulls(
                    materializeLongGroupedValues(mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeLongNulls(mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        }
        if (useMultiLongGrouping) {
            return Streams.ofValuesAndNulls(
                    materializeMultiLongGroupedValues(groupedColumnIndex, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeMultiLongNulls(groupedColumnIndex, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        }
        if (useFlatGrouping || sharedDictionaryFlatBacking) {
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

    /**
     * Copies one logical group key into compact caller-owned output. Returning {@code null}
     * permits a fallback for grouping layouts that do not yet expose a direct copy path.
     */
    public Streams copyGroupedValuePosition(int groupedColumnIndex, Streams output, int sourcePosition, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        if (structuralGrouping != null) {
            return structuralGrouping.copyGroupedValuePosition(
                    groupedColumnIndex, output, sourcePosition, outputPosition, size, allocator, allocationContext);
        }
        if (useFlatGrouping || sharedDictionaryFlatBacking) {
            return flatGroupingTable.copyGroupedValuePosition(
                    groupedColumnIndex, output, sourcePosition, outputPosition, size, allocator, allocationContext);
        }
        if (!useLongGrouping && !usePackedIntPairGrouping && !useMultiLongGrouping) {
            return null;
        }

        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.getOrNull(Stream.VALUES),
                I64Vector.class,
                size,
                I64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                size);

        boolean nullValue;
        long value = 0;
        if (usePackedIntPairGrouping) {
            nullValue = sourcePosition >= longKeysByGroup.length;
            if (!nullValue && packedIntGroupingArity == 3) {
                nullValue = (packedIntTripleNullMask(sourcePosition) & (1 << groupedColumnIndex)) != 0;
            }
            if (!nullValue) {
                value = packedIntGroupedValue(groupedColumnIndex, sourcePosition);
            }
        }
        else if (useLongGrouping) {
            nullValue = sourcePosition == nullGroup || sourcePosition >= longKeysByGroup.length;
            if (!nullValue) {
                value = longKeysByGroup[sourcePosition];
            }
        }
        else {
            nullValue = sourcePosition >= nextGroupId || multiLongTable.groupedValueIsNull(groupedColumnIndex, sourcePosition);
            if (!nullValue) {
                value = multiLongTable.groupedValue(groupedColumnIndex, sourcePosition);
            }
        }
        values.values()[outputPosition] = value;
        nulls.values()[outputPosition] = nullValue;
        return Streams.ofValuesAndNulls(values, nulls);
    }

    boolean supportsGroupedValuePositionComparison(int groupedColumnIndex)
    {
        if (useFlatGrouping || sharedDictionaryFlatBacking) {
            return flatGroupingTable.supportsGroupedValuePositionComparison(groupedColumnIndex);
        }
        return structuralGrouping == null &&
                (useLongGrouping || usePackedIntPairGrouping || useMultiLongGrouping);
    }

    boolean groupedValuePositionIsNull(int groupedColumnIndex, int sourcePosition)
    {
        if (useFlatGrouping || sharedDictionaryFlatBacking) {
            return flatGroupingTable.groupedValuePositionIsNull(groupedColumnIndex, sourcePosition);
        }
        if (usePackedIntPairGrouping) {
            return sourcePosition >= longKeysByGroup.length ||
                    (packedIntGroupingArity == 3 && (packedIntTripleNullMask(sourcePosition) & (1 << groupedColumnIndex)) != 0);
        }
        if (useLongGrouping) {
            return sourcePosition == nullGroup || sourcePosition >= longKeysByGroup.length;
        }
        return sourcePosition >= nextGroupId || multiLongTable.groupedValueIsNull(groupedColumnIndex, sourcePosition);
    }

    int compareGroupedValuePosition(int groupedColumnIndex, int sourcePosition, Vector otherValues, int otherPosition)
    {
        if (useFlatGrouping || sharedDictionaryFlatBacking) {
            return flatGroupingTable.compareGroupedValuePosition(
                    groupedColumnIndex, sourcePosition, otherValues, otherPosition);
        }
        long value;
        if (usePackedIntPairGrouping) {
            value = packedIntGroupedValue(groupedColumnIndex, sourcePosition);
        }
        else if (useLongGrouping) {
            value = longKeysByGroup[sourcePosition];
        }
        else {
            value = multiLongTable.groupedValue(groupedColumnIndex, sourcePosition);
        }
        return Long.compare(value, OperatorVectorSupport.longValue(otherValues, otherPosition));
    }

    int compareGroupedValuePositions(int groupedColumnIndex, int leftPosition, int rightPosition)
    {
        if (useFlatGrouping || sharedDictionaryFlatBacking) {
            return flatGroupingTable.compareGroupedValuePositions(
                    groupedColumnIndex, leftPosition, rightPosition);
        }
        long leftValue;
        long rightValue;
        if (usePackedIntPairGrouping) {
            leftValue = packedIntGroupedValue(groupedColumnIndex, leftPosition);
            rightValue = packedIntGroupedValue(groupedColumnIndex, rightPosition);
        }
        else if (useLongGrouping) {
            leftValue = longKeysByGroup[leftPosition];
            rightValue = longKeysByGroup[rightPosition];
        }
        else {
            leftValue = multiLongTable.groupedValue(groupedColumnIndex, leftPosition);
            rightValue = multiLongTable.groupedValue(groupedColumnIndex, rightPosition);
        }
        return Long.compare(leftValue, rightValue);
    }

    public Streams copyGroupedValuePositions(
            int groupedColumnIndex,
            Streams output,
            int[] sourcePositions,
            int sourceStart,
            int sourceCount,
            int outputStart,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        if (useFlatGrouping || sharedDictionaryFlatBacking) {
            return flatGroupingTable.copyGroupedValuePositions(
                    groupedColumnIndex,
                    output,
                    sourcePositions,
                    sourceStart,
                    sourceCount,
                    outputStart,
                    size,
                    allocator,
                    allocationContext);
        }
        Streams result = output;
        for (int index = 0; index < sourceCount; index++) {
            result = copyGroupedValuePosition(
                    groupedColumnIndex,
                    result,
                    sourcePositions[sourceStart + index],
                    outputStart + index,
                    size,
                    allocator,
                    allocationContext);
        }
        return result;
    }

    Streams groupedValueRangeAsDictionary(
            int groupedColumnIndex,
            int sourceStart,
            int size,
            Mask outputMask,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        if (!useFlatGrouping && !sharedDictionaryFlatBacking) {
            return null;
        }
        return flatGroupingTable.groupedValueRangeAsDictionary(
                groupedColumnIndex, sourceStart, size, outputMask, allocator, allocationContext);
    }

    Streams groupedValueRange(
            int groupedColumnIndex,
            int sourceStart,
            int size,
            Mask outputMask,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        Streams dictionary = groupedValueRangeAsDictionary(
                groupedColumnIndex, sourceStart, size, outputMask, allocator, allocationContext);
        if (dictionary != null) {
            return dictionary;
        }
        if ((useFlatGrouping || sharedDictionaryFlatBacking) && outputMask.all()) {
            int[] sourcePositions = arrayPool.borrowInts(size);
            try {
                for (int position = 0; position < size; position++) {
                    sourcePositions[position] = sourceStart + position;
                }
                return flatGroupingTable.copyGroupedValuePositions(
                        groupedColumnIndex,
                        null,
                        sourcePositions,
                        0,
                        size,
                        0,
                        size,
                        allocator,
                        allocationContext);
            }
            finally {
                arrayPool.release(sourcePositions);
            }
        }
        if (structuralGrouping != null ||
                useLongGrouping ||
                usePackedIntPairGrouping ||
                useMultiLongGrouping ||
                useFlatGrouping ||
                sharedDictionaryFlatBacking) {
            return null;
        }

        List<OperatorKeySemantics.Key> keysByGroup = keysByGroupColumns.get(groupedColumnIndex)
                .subList(sourceStart, sourceStart + size);
        Streams values = OperatorKeySemantics.materializeGroupedValues(
                keyHandlers[groupedColumnIndex],
                size,
                outputMask,
                keysByGroup,
                null,
                allocator,
                allocationContext,
                binaryTraits[groupedColumnIndex]);
        return Streams.ofValuesAndNulls(
                values.values(),
                materializeNulls(size, outputMask, keysByGroup, null, allocator, allocationContext));
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

    private I64Vector materializePackedIntGroupedValues(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index < longKeysByGroup.length) {
                result.values()[index] = packedIntGroupedValue(groupedColumnIndex, index);
            }
        }
        return result;
    }

    private long packedIntGroupedValue(int groupedColumnIndex, int groupId)
    {
        if (groupedColumnIndex == 0) {
            return unpackFirstInt(longKeysByGroup[groupId]);
        }
        if (groupedColumnIndex == 1) {
            return unpackSecondInt(longKeysByGroup[groupId]);
        }
        return packedIntTripleThird(groupId);
    }

    private BooleanVector materializeNonNulls(Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), 0, size, false);
        return result;
    }

    private BooleanVector materializePackedIntTripleNulls(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), true);
        int nullBit = 1 << groupedColumnIndex;
        for (int index : mask) {
            result.values()[index] = index >= longKeysByGroup.length ||
                    (packedIntTripleNullMask(index) & nullBit) != 0;
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

    private BooleanVector materializeMultiLongNulls(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            result.values()[index] = index < nextGroupId && multiLongTable.groupedValueIsNull(groupedColumnIndex, index);
        }
        return result;
    }

    private I64Vector materializeMultiLongGroupedValues(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index < nextGroupId && !multiLongTable.groupedValueIsNull(groupedColumnIndex, index)) {
                result.values()[index] = multiLongTable.groupedValue(groupedColumnIndex, index);
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
        if (key == null) {
            return nullGroup();
        }
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
            if (!useFlatGrouping && !useLongGrouping && !usePackedIntPairGrouping && !useMultiLongGrouping) {
                for (ArrayList<OperatorKeySemantics.Key> keysByGroup : keysByGroupColumns) {
                    keysByGroup.add(null);
                }
            }
            nullGroup = nextGroupId++;
            if (!useFlatGrouping && !useLongGrouping && !usePackedIntPairGrouping && !useMultiLongGrouping &&
                    keysByGroupColumns.size() == 1) {
                // Dictionary grouping routes a top-level SQL null through this compact singleton. Keep the
                // authoritative object table in sync so a later flat batch resolves the same null to this group.
                groups.put(null, nullGroup);
            }
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
        long[] previous = longKeysByGroup;
        longKeysByGroup = arrayPool.borrowLongs(newSize);
        System.arraycopy(previous, 0, longKeysByGroup, 0, previous.length);
        arrayPool.release(previous);
        if (usePackedIntPairGrouping && packedIntGroupingArity == 3) {
            if (compositePolicy.packedIntTriplePackedTail()) {
                int[] previousTail = packedIntTripleTailByGroup;
                packedIntTripleTailByGroup = arrayPool.borrowInts(newSize);
                System.arraycopy(previousTail, 0, packedIntTripleTailByGroup, 0, previousTail.length);
                arrayPool.release(previousTail);
            }
            else {
                int[] previousThird = packedIntTripleThirdByGroup;
                packedIntTripleThirdByGroup = arrayPool.borrowInts(newSize);
                System.arraycopy(previousThird, 0, packedIntTripleThirdByGroup, 0, previousThird.length);
                arrayPool.release(previousThird);
                byte[] previousNullMasks = packedIntTripleNullMasksByGroup;
                packedIntTripleNullMasksByGroup = arrayPool.borrowBytes(newSize);
                System.arraycopy(previousNullMasks, 0, packedIntTripleNullMasksByGroup, 0, previousNullMasks.length);
                arrayPool.release(previousNullMasks);
            }
        }
    }

    void releaseBuffers()
    {
        if (compositePolicy.debugGroupingShapes()) {
            System.err.printf(
                    "[grouping-final] groups=%d single-long=%s direct=%s id-indexed=%s disabled=%s staged-disabled=%s next-check=%d%n",
                    nextGroupId,
                    useLongGrouping,
                    useLongDirectGrouping,
                    useIdIndexedLongGrouping,
                    longDirectGroupingDisabled,
                    stagedLongDirectGroupingDisabled,
                    longDirectNextCheck);
        }
        arrayPool.release(packedIntPairControl);
        packedIntPairControl = null;
        arrayPool.release(longGroupKeys);
        longGroupKeys = null;
        releaseLongGroupIds(longGroupIds, useLongDirectGrouping);
        longGroupIds = null;
        arrayPool.release(longKeysByGroup);
        longKeysByGroup = new long[0];
        arrayPool.release(packedIntTripleThirdByGroup);
        packedIntTripleThirdByGroup = new int[0];
        arrayPool.release(packedIntTripleNullMasksByGroup);
        packedIntTripleNullMasksByGroup = new byte[0];
        arrayPool.release(packedIntTripleTailByGroup);
        packedIntTripleTailByGroup = new int[0];
        arrayPool.release(dictionaryGroupsById);
        dictionaryGroupsById = new long[0];
        arrayPool.release(dictionaryGenerations);
        dictionaryGenerations = new int[0];
        arrayPool.release(sharedDictionaryEntriesById);
        sharedDictionaryEntriesById = new long[0];
        arrayPool.release(densePositionsCache);
        densePositionsCache = new int[0];
        if (multiLongTable != null) {
            multiLongTable.releaseBuffers();
            multiLongTable = null;
        }
        if (flatGroupingTable != null) {
            flatGroupingTable.releaseBuffers();
            flatGroupingTable = null;
        }
        if (structuralGrouping != null) {
            structuralGrouping.releaseBuffers();
            structuralGrouping = null;
        }
        flatGroupingLayout = null;
    }

    private static StructuralKeyKernel[] structuralKeyKernels(
            List<TypeBinding> keyTypes,
            OperatorCodeGenerationResources codeGeneration)
    {
        StructuralKeyKernel[] kernels = new StructuralKeyKernel[keyTypes.size()];
        StructuralTypeKernelFactory structuralTypes = codeGeneration.structuralTypes();
        for (int keyIndex = 0; keyIndex < keyTypes.size(); keyIndex++) {
            kernels[keyIndex] = structuralTypes.key(keyTypes.get(keyIndex));
        }
        return kernels;
    }

    private static boolean allowsLegacyPhysicalShortcuts(StructuralKeyKernel[] kernels)
    {
        for (StructuralKeyKernel kernel : kernels) {
            if (!kernel.allowsLegacyPhysicalShortcuts()) {
                return false;
            }
        }
        return true;
    }

    private static final class StructuralGroupingIndex
    {
        private final Allocator allocator;
        private final Allocator.Context allocationContext;
        private final StructuralKeyKernel[] kernels;
        private final Object2LongOpenHashMap<StructuralGroupingKey> groups = new Object2LongOpenHashMap<>();
        private final ArrayList<StructuralGroupingKey> representatives = new ArrayList<>();

        private StructuralGroupingIndex(
                Allocator allocator,
                Allocator.Context allocationContext,
                StructuralKeyKernel[] kernels)
        {
            this.allocator = allocator;
            this.allocationContext = allocationContext;
            this.kernels = kernels.clone();
            groups.defaultReturnValue(-1);
        }

        private long assignGroups(
                Vector[] values,
                Vector[] nulls,
                Mask mask,
                I64Vector result,
                long nextGroupId)
        {
            groups.ensureCapacity(groups.size() + mask.count());
            Object2LongOpenHashMap<StructuralGroupingKey> newGroups = new Object2LongOpenHashMap<>();
            newGroups.defaultReturnValue(-1);
            int[] newGroupPositions = new int[mask.count()];
            int newGroupCount = 0;
            long[] output = result.values();
            for (int position : mask) {
                StructuralGroupingKey probe = new StructuralGroupingKey(kernels, values, nulls, position);
                long groupId = groups.getLong(probe);
                if (groupId == -1) {
                    groupId = newGroups.getLong(probe);
                    if (groupId == -1) {
                        groupId = nextGroupId + newGroupCount;
                        newGroups.put(probe, groupId);
                        newGroupPositions[newGroupCount++] = position;
                    }
                }
                output[position] = groupId;
            }
            if (newGroupCount == 0) {
                return nextGroupId;
            }

            int[] positions = Arrays.copyOf(newGroupPositions, newGroupCount);
            Vector[] ownedValues = copyVectors(values, positions);
            Vector[] ownedNulls = copyNullableVectors(nulls, positions);
            for (int position = 0; position < newGroupCount; position++) {
                StructuralGroupingKey key = new StructuralGroupingKey(kernels, ownedValues, ownedNulls, position);
                groups.put(key, nextGroupId + position);
                representatives.add(key);
            }
            return nextGroupId + newGroupCount;
        }

        private boolean contains(Vector[] values, Vector[] nulls, int position)
        {
            return groups.getLong(new StructuralGroupingKey(kernels, values, nulls, position)) != -1;
        }

        private Streams groupedValues(
                int groupedColumnIndex,
                Mask mask,
                Streams output,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            int size = mask.none() ? 0 : mask.maxPosition() + 1;
            Vector outputValues = output == null ? null : output.values();
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    allocator,
                    allocationContext,
                    output == null ? null : output.getOrNull(Stream.NULLS),
                    size);
            Arrays.fill(outputNulls.values(), 0, size, true);
            for (int groupId : mask) {
                StructuralGroupingKey representative = representatives.get(groupId);
                outputValues = representative.values[groupedColumnIndex].copySinglePositionInto(
                        allocator,
                        allocationContext,
                        outputValues,
                        representative.position,
                        groupId,
                        size);
                outputNulls.values()[groupId] =
                        OperatorVectorSupport.isNull(representative.nulls[groupedColumnIndex], representative.position);
            }
            return Streams.ofValuesAndNulls(outputValues, outputNulls);
        }

        private Streams copyGroupedValuePosition(
                int groupedColumnIndex,
                Streams output,
                int sourcePosition,
                int outputPosition,
                int size,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            StructuralGroupingKey representative = representatives.get(sourcePosition);
            Vector outputValues = representative.values[groupedColumnIndex].copySinglePositionInto(
                    allocator,
                    allocationContext,
                    output == null ? null : output.values(),
                    representative.position,
                    outputPosition,
                    size);
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    allocator,
                    allocationContext,
                    output == null ? null : output.getOrNull(Stream.NULLS),
                    size);
            outputNulls.values()[outputPosition] =
                    OperatorVectorSupport.isNull(representative.nulls[groupedColumnIndex], representative.position);
            return Streams.ofValuesAndNulls(outputValues, outputNulls);
        }

        private Vector[] copyVectors(Vector[] vectors, int[] positions)
        {
            Vector[] copies = new Vector[vectors.length];
            for (int index = 0; index < vectors.length; index++) {
                copies[index] = allocator.copyVector(allocationContext, vectors[index], positions);
            }
            return copies;
        }

        private Vector[] copyNullableVectors(Vector[] vectors, int[] positions)
        {
            Vector[] copies = new Vector[vectors.length];
            for (int index = 0; index < vectors.length; index++) {
                if (vectors[index] != null) {
                    copies[index] = allocator.copyVector(allocationContext, vectors[index], positions);
                }
            }
            return copies;
        }

        private void releaseBuffers()
        {
            groups.clear();
            representatives.clear();
        }
    }

    private static final class StructuralGroupingKey
    {
        private static final int NULL_HASH = 0x9E3779B9;

        private final StructuralKeyKernel[] kernels;
        private final Vector[] values;
        private final Vector[] nulls;
        private final int position;

        private StructuralGroupingKey(
                StructuralKeyKernel[] kernels,
                Vector[] values,
                Vector[] nulls,
                int position)
        {
            this.kernels = kernels;
            this.values = values;
            this.nulls = nulls;
            this.position = position;
        }

        @Override
        public int hashCode()
        {
            int hash = 1;
            for (int keyIndex = 0; keyIndex < kernels.length; keyIndex++) {
                int keyHash = OperatorVectorSupport.isNull(nulls[keyIndex], position)
                        ? NULL_HASH
                        : Long.hashCode(kernels[keyIndex].hash(values[keyIndex], nulls[keyIndex], position));
                hash = 31 * hash + keyHash;
            }
            return hash;
        }

        @Override
        public boolean equals(Object object)
        {
            if (!(object instanceof StructuralGroupingKey other) || kernels != other.kernels) {
                return false;
            }
            for (int keyIndex = 0; keyIndex < kernels.length; keyIndex++) {
                boolean leftNull = OperatorVectorSupport.isNull(nulls[keyIndex], position);
                boolean rightNull = OperatorVectorSupport.isNull(other.nulls[keyIndex], other.position);
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!kernels[keyIndex].identical(
                        values[keyIndex],
                        nulls[keyIndex],
                        position,
                        other.values[keyIndex],
                        other.nulls[keyIndex],
                        other.position)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static boolean isSingleLongGroupingCandidate(Vector values)
    {
        FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
        return handler != null && handler.kind() == FlatTypeHandler.Kind.LONG;
    }
}
