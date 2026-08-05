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

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

/**
 * A structural, arity-independent compact representation for long-key grouping. Values start in a
 * record-major signed-32 layout and promote exactly to the Classfile-generated full-width table if a
 * later batch contains a wider value or the combined control/id slot exhausts its id domain.
 */
class AdaptiveLongGroupingTable
        implements LongGroupingTable
{
    private static final int MAX_COMPACT_GROUP_ID = 0x00FF_FFFF;
    static final long COMPACT_DOMAIN_EXCEEDED = Long.MIN_VALUE;
    // A grouping-owned compact pair can otherwise cross 75% near the end of a very large build and double a
    // 64 MiB slot plane for only the final few percent of groups. At 2^24 slots, admit a bounded denser terminal
    // generation; smaller generations retain the established collision profile, and DISTINCT-owned tables retain
    // their generated grouped-probe policy. The disable switch is retained for adjacent whole-query controls.
    // Once a grouping-owned compact pair reaches one quarter of the established high-density terminal capacity,
    // skip the otherwise short-lived half-capacity generation only when an exact terminal slot plane is already idle
    // in the shared primitive pool. This avoids a full rehash in repeated compatible pipelines without speculatively
    // doubling the working set of streams that stop in the half-capacity generation. A disable switch provides an
    // adjacent whole-query control and allows broad activation audits before retention.
    private static final int GROUPED_PROBE_LANES = IntVector.SPECIES_256.length();

    final int arity;
    private final boolean groupedProbeEligible;
    private final PrimitiveArrayPool arrayPool;
    private final OperatorCodeGenerationResources codeGeneration;
    private final AdaptiveLongGroupingPolicy policy;

    int[] slots;
    int slotMask;
    int maxFill;
    int size;

    // Structural packed lanes: each long stores two signed-32 fields; an odd tail uses one int.
    // This gives every arity the narrowest primitive-array layout without a special pair/triple table.
    long[][] keyPairsByGroup;
    int[] tailKeysByGroup;
    byte[] nullMasksByGroup;
    int reverseCapacity;

    // Reused batch normalization frame. The hot probe reads concrete primitive arrays only.
    int[] batchKeys = new int[0];
    byte[] batchNullMasks = new byte[0];
    int[] batchHashes = new int[0];

    private LongGroupingTable promoted;
    private boolean promotedForDiscardedResults;
    private int compactRetainedColumns;
    private VectorAccess.BooleanValues[] nonNullAccessors;
    private VectorAccess.BooleanValues[] promotionNullAccessors;
    private int debugNullFreeBatches;
    private int debugNullableBatches;
    private boolean debugHighDensityCapacityUsed;
    private boolean debugTerminalJumpUsed;
    private boolean containsNullableGroups;
    private int[] densePositions = new int[0];
    private long[] promotedAssignedGroups = new long[0];
    private int[] reusedTerminalSlots;

    AdaptiveLongGroupingTable(
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            AdaptiveLongGroupingPolicy policy,
            int arity,
            int expectedSize,
            boolean groupedProbeEligible)
    {
        if (arity < 2 || arity > AbstractMultiLongGroupingTable.MAX_ARITY) {
            throw new IllegalArgumentException("Unsupported grouping arity: " + arity);
        }
        this.arity = arity;
        this.arrayPool = arrayPool;
        this.codeGeneration = codeGeneration;
        this.policy = policy;
        this.groupedProbeEligible = groupedProbeEligible;
        if (policy.debugShapes()) {
            System.err.printf("[adaptive-long-grouping] create arity=%d expected=%d%n", arity, expectedSize);
        }
        int capacity = 16;
        while (capacity < expectedSize / policy.loadFactor()) {
            capacity <<= 1;
        }
        allocateSlots(capacity);
        // Power-of-two reverse buckets are shared across structural arities/query instances by the primitive pool.
        // Starting at the common minimum avoids exact-size families that cannot be reused by adjacent group shapes.
        reverseCapacity = 16;
        keyPairsByGroup = new long[arity / 2][];
        for (int pair = 0; pair < keyPairsByGroup.length; pair++) {
            keyPairsByGroup[pair] = arrayPool.borrowLongs(reverseCapacity);
        }
        tailKeysByGroup = (arity & 1) == 0 ? new int[0] : arrayPool.borrowInts(reverseCapacity);
        // Even arities need a separate null mask only after the first potentially nullable batch. Keeping it absent
        // makes the generated null-free probe a two-array layout (slot + packed keys), matching the physical minimum.
        nullMasksByGroup = new byte[0];
    }

    static AdaptiveLongGroupingTable create(
            int arity,
            int expectedSize,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            AdaptiveLongGroupingPolicy policy)
    {
        return codeGeneration.adaptiveLongGrouping().create(arity, expectedSize, false, arrayPool, codeGeneration, policy);
    }

    static AdaptiveLongGroupingTable createDistinct(
            int arity,
            int expectedSize,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            AdaptiveLongGroupingPolicy policy)
    {
        return codeGeneration.adaptiveLongGrouping().create(arity, expectedSize, true, arrayPool, codeGeneration, policy);
    }

    boolean promoteForDiscardedResults(long groupCount)
    {
        if (promoted != null) {
            return false;
        }
        promote(groupCount, true);
        return true;
    }

    void compactRetainedColumns(int compactRetainedColumns)
    {
        if (promoted != null) {
            throw new IllegalStateException("Retained key widths must be selected before promotion");
        }
        this.compactRetainedColumns = compactRetainedColumns;
    }

    @Override
    public int arity()
    {
        return arity;
    }

    @Override
    public boolean supportsImplicitDensePositions()
    {
        return true;
    }

    @Override
    public boolean supportsSparseNullAccessors()
    {
        return true;
    }

    @Override
    public long assignBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            long[] result,
            long startGroupId)
    {
        if (promoted != null) {
            if (promotedForDiscardedResults) {
                throw new IllegalStateException("Grouping table was promoted without per-row group ids");
            }
            return promoted.assignBatch(keyAccessors, nullableAccessors(nullAccessors), explicitPositions(positions, positionCount), positionCount, result, startGroupId);
        }
        if (startGroupId + positionCount > MAX_COMPACT_GROUP_ID) {
            promote(startGroupId);
            return promoted.assignBatch(keyAccessors, nullableAccessors(nullAccessors), explicitPositions(positions, positionCount), positionCount, result, startGroupId);
        }

        long nextGroupId;
        if (nullAccessors == null && !containsNullableGroups) {
            if (policy.debugShapes()) {
                debugNullFreeBatches++;
            }
            nextGroupId = positions == null
                    ? assignCompactDenseNullFreeBatch(keyAccessors, null, null, positionCount, result, startGroupId)
                    : assignCompactNullFreeBatch(keyAccessors, null, positions, positionCount, result, startGroupId);
        }
        else {
            if (nullAccessors != null) {
                containsNullableGroups = true;
                ensureNullableStorage();
            }
            if (policy.debugShapes()) {
                debugNullableBatches++;
            }
            VectorAccess.BooleanValues[] compactNullAccessors = nullAccessors == null ? nullableAccessors(null) : nullAccessors;
            nextGroupId = positions == null
                    ? assignCompactDenseBatch(keyAccessors, compactNullAccessors, null, positionCount, result, startGroupId)
                    : assignCompactBatch(keyAccessors, compactNullAccessors, positions, positionCount, result, startGroupId);
        }
        if (nextGroupId != COMPACT_DOMAIN_EXCEEDED) {
            return nextGroupId;
        }
        // A generated fused loop may discover a wide value after inserting an earlier prefix. Migrate that exact
        // prefix, then replay the complete batch: existing keys retain their ids and only the suffix creates ids.
        long compactGroupCount = size;
        promote(compactGroupCount);
        return promoted.assignBatch(keyAccessors, nullableAccessors(nullAccessors), explicitPositions(positions, positionCount), positionCount, result, compactGroupCount);
    }

    @Override
    public long assignBatchDiscardingResults(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            long startGroupId)
    {
        if (promoted == null) {
            throw new IllegalStateException("Outputless assignment requires a generated grouping table");
        }
        return promoted.assignBatchDiscardingResults(
                keyAccessors,
                nullableAccessors(nullAccessors),
                explicitPositions(positions, positionCount),
                positionCount,
                startGroupId);
    }

    /**
     * Assigns a proven-non-null batch and emits only the first physical position for each newly inserted key.
     * Generated compact tables write those positions in the insertion loop; a promoted table uses the ordinary
     * group-id result contract and collects new sequential ids during the rare fallback.
     */
    long assignDistinctBatch(
            VectorAccess.LongValues[] keyAccessors,
            int[] positions,
            int positionCount,
            int resultLength,
            int[] distinctPositions,
            long startGroupId)
    {
        if (promoted != null) {
            return assignPromotedDistinctBatch(
                    keyAccessors, positions, positionCount, resultLength, distinctPositions, 0, startGroupId);
        }

        long nextGroupId = positions == null
                ? assignCompactDenseDistinctNullFreeBatch(keyAccessors, null, null, positionCount, distinctPositions, startGroupId)
                : assignCompactDistinctNullFreeBatch(keyAccessors, null, positions, positionCount, distinctPositions, startGroupId);
        if (nextGroupId != COMPACT_DOMAIN_EXCEEDED) {
            return nextGroupId;
        }

        long compactGroupCount = size;
        int prefixCount = toIntExact(compactGroupCount - startGroupId);
        promote(compactGroupCount);
        return assignPromotedDistinctBatch(
                keyAccessors, positions, positionCount, resultLength, distinctPositions, prefixCount, compactGroupCount);
    }

    long assignCompactDistinctNullFreeBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] ignoredNullAccessors,
            int[] positions,
            int positionCount,
            int[] distinctPositions,
            long startGroupId)
    {
        throw new UnsupportedOperationException("Generated compact distinct kernel is unavailable");
    }

    long assignCompactDenseDistinctNullFreeBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] ignoredNullAccessors,
            int[] ignoredPositions,
            int positionCount,
            int[] distinctPositions,
            long startGroupId)
    {
        throw new UnsupportedOperationException("Generated compact dense distinct kernel is unavailable");
    }

    private long assignPromotedDistinctBatch(
            VectorAccess.LongValues[] keyAccessors,
            int[] positions,
            int positionCount,
            int resultLength,
            int[] distinctPositions,
            int outputOffset,
            long startGroupId)
    {
        ensurePromotedAssignedCapacity(resultLength);
        int[] explicitPositions = explicitPositions(positions, positionCount);
        long nextGroupId = promoted.assignBatch(
                keyAccessors, nullableAccessors(null), explicitPositions, positionCount, promotedAssignedGroups, startGroupId);
        long expectedGroupId = startGroupId;
        int outputIndex = outputOffset;
        for (int row = 0; row < positionCount; row++) {
            int position = explicitPositions[row];
            if (promotedAssignedGroups[position] == expectedGroupId) {
                distinctPositions[outputIndex++] = position;
                expectedGroupId++;
            }
        }
        if (expectedGroupId != nextGroupId) {
            throw new IllegalStateException("Promoted distinct table returned non-sequential group ids");
        }
        return nextGroupId;
    }

    private void ensurePromotedAssignedCapacity(int size)
    {
        if (promotedAssignedGroups.length >= size) {
            return;
        }
        arrayPool.release(promotedAssignedGroups);
        promotedAssignedGroups = arrayPool.borrowLongs(size);
    }

    /** Generated subclasses omit all null accessors and branches for an all-false-null batch. */
    long assignCompactNullFreeBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] ignoredNullAccessors,
            int[] positions,
            int positionCount,
            long[] result,
            long startGroupId)
    {
        return assignCompactBatch(keyAccessors, nullableAccessors(null), positions, positionCount, result, startGroupId);
    }

    long assignCompactDenseBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] ignoredPositions,
            int positionCount,
            long[] result,
            long startGroupId)
    {
        return assignCompactBatch(keyAccessors, nullAccessors, explicitPositions(null, positionCount), positionCount, result, startGroupId);
    }

    long assignCompactDenseNullFreeBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] ignoredNullAccessors,
            int[] ignoredPositions,
            int positionCount,
            long[] result,
            long startGroupId)
    {
        return assignCompactNullFreeBatch(keyAccessors, null, explicitPositions(null, positionCount), positionCount, result, startGroupId);
    }

    private int[] explicitPositions(int[] positions, int positionCount)
    {
        if (positions != null) {
            return positions;
        }
        if (densePositions.length < positionCount) {
            arrayPool.release(densePositions);
            densePositions = arrayPool.borrowInts(positionCount);
            for (int position = 0; position < positionCount; position++) {
                densePositions[position] = position;
            }
        }
        return densePositions;
    }

    private VectorAccess.BooleanValues[] nullableAccessors(VectorAccess.BooleanValues[] accessors)
    {
        if (accessors != null && allAccessorsPresent(accessors)) {
            return accessors;
        }
        if (nonNullAccessors == null) {
            nonNullAccessors = new VectorAccess.BooleanValues[arity];
            VectorAccess.BooleanValues nonNull = ignored -> false;
            Arrays.fill(nonNullAccessors, nonNull);
        }
        if (accessors == null) {
            return nonNullAccessors;
        }
        if (promotionNullAccessors == null) {
            promotionNullAccessors = new VectorAccess.BooleanValues[arity];
        }
        for (int index = 0; index < arity; index++) {
            promotionNullAccessors[index] = accessors[index] == null ? nonNullAccessors[index] : accessors[index];
        }
        return promotionNullAccessors;
    }

    private static boolean allAccessorsPresent(VectorAccess.BooleanValues[] accessors)
    {
        for (VectorAccess.BooleanValues accessor : accessors) {
            if (accessor == null) {
                return false;
            }
        }
        return true;
    }

    private void ensureNullableStorage()
    {
        if ((arity & 1) != 0 || nullMasksByGroup.length != 0) {
            return;
        }
        nullMasksByGroup = arrayPool.borrowBytes(reverseCapacity);
        Arrays.fill(nullMasksByGroup, (byte) 0);
    }

    /** Generated subclasses fuse normalization and probing; this is the shape-neutral interpreted fallback. */
    long assignCompactBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount,
            long[] result,
            long startGroupId)
    {
        ensureBatchCapacity(positionCount);
        // Normalize once at the batch boundary. Any out-of-domain value promotes before this batch mutates state.
        if (!normalizeBatch(keyAccessors, nullAccessors, positions, positionCount)) {
            return COMPACT_DOMAIN_EXCEEDED;
        }

        long nextGroupId = startGroupId;
        for (int row = 0; row < positionCount; row++) {
            int position = positions[row];
            int keyBase = row * arity;
            int hash = batchHashes[row];
            byte nullMask = batchNullMasks[row];
            int fragment = (hash >>> 24) | 0x80;
            int slot = hash & slotMask;
            while (true) {
                int encoded = slots[slot];
                if (encoded == 0) {
                    int groupId = toIntExact(nextGroupId++);
                    ensureReverseCapacity(groupId);
                    storeRecord(keyBase, groupId);
                    if ((arity & 1) == 0) {
                        nullMasksByGroup[groupId] = nullMask;
                    }
                    slots[slot] = fragment << 24 | groupId;
                    result[position] = groupId;
                    if (++size >= maxFill) {
                        rehash(slots.length * 2);
                    }
                    break;
                }
                if ((encoded >>> 24) == fragment) {
                    int groupId = encoded & MAX_COMPACT_GROUP_ID;
                    if (groupNullMask(groupId) == nullMask && equalsRecord(keyBase, groupId)) {
                        result[position] = groupId;
                        break;
                    }
                }
                slot = (slot + 1) & slotMask;
            }
        }
        return nextGroupId;
    }

    /** Generated subclasses unroll this method for their structural arity. */
    boolean normalizeBatch(
            VectorAccess.LongValues[] keyAccessors,
            VectorAccess.BooleanValues[] nullAccessors,
            int[] positions,
            int positionCount)
    {
        for (int row = 0; row < positionCount; row++) {
            int position = positions[row];
            int base = row * arity;
            int nullMask = 0;
            for (int column = 0; column < arity; column++) {
                boolean isNull = nullAccessors[column].value(position);
                long value = isNull ? 0 : keyAccessors[column].value(position);
                if (value != (int) value) {
                    return false;
                }
                int compactValue = (int) value;
                if (column == arity - 1 && (arity & 1) != 0 && !tailValueFits(compactValue, arity)) {
                    return false;
                }
                batchKeys[base + column] = compactValue;
                if (isNull) {
                    nullMask |= 1 << column;
                }
            }
            batchNullMasks[row] = (byte) nullMask;
            batchHashes[row] = hashCompactRecord(batchKeys, base, nullMask, arity);
        }
        return true;
    }

    /** Generated subclasses unroll this method for their structural arity. */
    boolean equalsRecord(int batchBase, int groupId)
    {
        for (int pair = 0; pair < keyPairsByGroup.length; pair++) {
            int column = pair * 2;
            if (packPair(batchKeys[batchBase + column], batchKeys[batchBase + column + 1]) != keyPairsByGroup[pair][groupId]) {
                return false;
            }
        }
        if ((arity & 1) != 0 && batchKeys[batchBase + arity - 1] != unpackTailValue(tailKeysByGroup[groupId], arity)) {
            return false;
        }
        return true;
    }

    /** Generated subclasses unroll this method for their structural arity. */
    void storeRecord(int batchBase, int groupId)
    {
        for (int pair = 0; pair < keyPairsByGroup.length; pair++) {
            int column = pair * 2;
            keyPairsByGroup[pair][groupId] = packPair(batchKeys[batchBase + column], batchKeys[batchBase + column + 1]);
        }
        if ((arity & 1) != 0) {
            tailKeysByGroup[groupId] = packTail(batchKeys[batchBase + arity - 1], batchNullMasks[batchBase / arity], arity);
        }
    }

    static long packPair(int first, int second)
    {
        return (long) first << 32 | second & 0xFFFF_FFFFL;
    }

    static int packTail(int value, int nullMask, int arity)
    {
        int valueBits = 32 - arity;
        int payloadMask = (1 << valueBits) - 1;
        return value & payloadMask | nullMask << valueBits;
    }

    static int unpackTailValue(int packed, int arity)
    {
        return packed << arity >> arity;
    }

    static boolean tailValueFits(int value, int arity)
    {
        int valueBits = 32 - arity;
        int minimum = -(1 << (valueBits - 1));
        int maximum = (1 << (valueBits - 1)) - 1;
        return value >= minimum && value <= maximum;
    }

    static long compactHashPrime(int lane)
    {
        return AbstractMultiLongGroupingTable.HASH_PRIMES[Math.min(lane * 2 + 1, AbstractMultiLongGroupingTable.HASH_PRIMES.length - 1)];
    }

    private byte groupNullMask(int groupId)
    {
        return (byte) ((arity & 1) == 0
                ? (nullMasksByGroup.length == 0 ? 0 : nullMasksByGroup[groupId])
                : tailKeysByGroup[groupId] >>> (32 - arity));
    }

    static int hashCompactRecord(int[] keys, int base, int nullMask, int arity)
    {
        int pairCount = arity / 2;
        long hash = nullMask;
        if (pairCount != 0) {
            hash += packPair(keys[base], keys[base + 1]);
            for (int pair = 1; pair < pairCount; pair++) {
                int column = pair * 2;
                hash += packPair(keys[base + column], keys[base + column + 1]) * compactHashPrime(pair);
            }
        }
        if ((arity & 1) != 0) {
            hash += (long) keys[base + arity - 1] * compactHashPrime(pairCount);
        }
        return mixHash(hash);
    }

    @Override
    public void ensureCapacity(long expectedSize)
    {
        if (promoted != null) {
            promoted.ensureCapacity(expectedSize);
            return;
        }
        if (expectedSize > MAX_COMPACT_GROUP_ID) {
            promote(size);
            promoted.ensureCapacity(expectedSize);
            return;
        }
        // The caller supplies an upper bound (active rows), not a distinct-key estimate. Pre-sizing the cheap slot
        // index avoids repeated full-table rehashes, but reverse key payload grows only on realized insertions.
        int capacity = slots.length;
        while (expectedSize >= (long) (capacity * loadFactor(capacity))) {
            capacity = nextGrowthCapacity(capacity);
        }
        if (capacity != slots.length) {
            rehash(capacity);
        }
    }

    int nextGrowthCapacity(int capacity)
    {
        if (terminalReuseEligible(capacity)) {
            reusedTerminalSlots = arrayPool.tryBorrowInts(policy.highDensityPairMinSlots());
            if (reusedTerminalSlots != null) {
                if (policy.debugShapes()) {
                    debugTerminalJumpUsed = true;
                }
                return policy.highDensityPairMinSlots();
            }
        }
        // A single unusually large reservation can cross the reused terminal generation and its load threshold in
        // one ensureCapacity call. Return the exact lease before continuing to the next generation; never repeat a
        // capacity or carry a mismatched lease into rehash().
        if (reusedTerminalSlots != null) {
            arrayPool.release(reusedTerminalSlots);
            reusedTerminalSlots = null;
        }
        return capacity << 1;
    }

    boolean terminalReuseEligible(int capacity)
    {
        return policy.highDensityPairTerminalJump() &&
                policy.highDensityPairCapacity() &&
                arity == 2 &&
                !groupedProbeEligible &&
                capacity == policy.highDensityPairTerminalJumpSlots();
    }

    @Override
    public long groupedValue(int column, int groupId)
    {
        if (promoted != null) {
            return promoted.groupedValue(column, groupId);
        }
        if (column == arity - 1 && (arity & 1) != 0) {
            return unpackTailValue(tailKeysByGroup[groupId], arity);
        }
        long pair = keyPairsByGroup[column / 2][groupId];
        return (column & 1) == 0 ? (int) (pair >>> 32) : (int) pair;
    }

    @Override
    public boolean groupedValueIsNull(int column, int groupId)
    {
        return promoted == null
                ? (groupNullMask(groupId) & (1 << column)) != 0
                : promoted.groupedValueIsNull(column, groupId);
    }

    private void promote(long groupCount)
    {
        promote(groupCount, false);
    }

    private void promote(long groupCount, boolean discardResults)
    {
        if (promoted != null) {
            return;
        }
        if (policy.debugShapes()) {
            System.err.printf("[adaptive-long-grouping] promote arity=%d groups=%d slots=%d%n", arity, groupCount, slots.length);
        }
        LongGroupingTable target = discardResults
                ? codeGeneration.multiLongGrouping().createDiscardingResults(
                        arity, Math.max(16, toIntExact(groupCount)), compactRetainedColumns, arrayPool, policy)
                : codeGeneration.multiLongGrouping().create(
                        arity, Math.max(16, toIntExact(groupCount)), arrayPool, policy);
        if (groupCount != 0) {
            VectorAccess.LongValues[] values = new VectorAccess.LongValues[arity];
            VectorAccess.BooleanValues[] nulls = new VectorAccess.BooleanValues[arity];
            for (int column = 0; column < arity; column++) {
                int field = column;
                values[column] = position -> groupedValue(field, position);
                nulls[column] = position -> (groupNullMask(position) & (1 << field)) != 0;
            }
            int count = toIntExact(groupCount);
            int[] positions = arrayPool.borrowInts(count);
            for (int position = 0; position < count; position++) {
                positions[position] = position;
            }
            long imported = discardResults
                    ? target.assignBatchDiscardingResults(values, nulls, positions, count, 0)
                    : importWithResults(target, values, nulls, positions, count);
            arrayPool.release(positions);
            if (imported != groupCount) {
                target.releaseBuffers();
                throw new IllegalStateException("Compact grouping promotion changed group cardinality");
            }
        }
        releaseCompactState();
        promoted = target;
        promotedForDiscardedResults = discardResults;
    }

    private long importWithResults(
            LongGroupingTable target,
            VectorAccess.LongValues[] values,
            VectorAccess.BooleanValues[] nulls,
            int[] positions,
            int count)
    {
        long[] ignored = arrayPool.borrowLongs(count);
        long imported = target.assignBatch(values, nulls, positions, count, ignored, 0);
        arrayPool.release(ignored);
        return imported;
    }

    private void ensureBatchCapacity(int rows)
    {
        int keyCount = Math.multiplyExact(rows, arity);
        if (batchKeys.length < keyCount) {
            arrayPool.release(batchKeys);
            batchKeys = arrayPool.borrowInts(keyCount);
        }
        if (batchNullMasks.length < rows) {
            arrayPool.release(batchNullMasks);
            batchNullMasks = arrayPool.borrowBytes(rows);
        }
        if (batchHashes.length < rows) {
            arrayPool.release(batchHashes);
            batchHashes = arrayPool.borrowInts(rows);
        }
    }

    final boolean ensureReverseCapacity(int groupId)
    {
        if (groupId < reverseCapacity) {
            return false;
        }
        int newCapacity = reverseCapacity;
        while (groupId >= newCapacity) {
            newCapacity *= 2;
        }
        long[][] previousPairs = keyPairsByGroup;
        int[] previousTail = tailKeysByGroup;
        byte[] previousNulls = nullMasksByGroup;
        keyPairsByGroup = new long[previousPairs.length][];
        for (int pair = 0; pair < keyPairsByGroup.length; pair++) {
            keyPairsByGroup[pair] = arrayPool.borrowLongs(newCapacity);
            System.arraycopy(previousPairs[pair], 0, keyPairsByGroup[pair], 0, size);
            arrayPool.release(previousPairs[pair]);
        }
        tailKeysByGroup = (arity & 1) == 0 ? new int[0] : arrayPool.borrowInts(newCapacity);
        if ((arity & 1) != 0) {
            System.arraycopy(previousTail, 0, tailKeysByGroup, 0, size);
            arrayPool.release(previousTail);
        }
        nullMasksByGroup = (arity & 1) == 0 && previousNulls.length != 0 ? arrayPool.borrowBytes(newCapacity) : new byte[0];
        if ((arity & 1) == 0 && previousNulls.length != 0) {
            System.arraycopy(previousNulls, 0, nullMasksByGroup, 0, size);
            arrayPool.release(previousNulls);
        }
        reverseCapacity = newCapacity;
        return true;
    }

    private void allocateSlots(int capacity)
    {
        slots = arrayPool.borrowInts(capacity);
        Arrays.fill(slots, 0);
        slotMask = capacity - 1;
        maxFill = (int) (capacity * loadFactor(capacity));
        debugHighDensityCapacityUsed |= loadFactor(capacity) != policy.loadFactor();
    }

    private float loadFactor(int capacity)
    {
        return policy.highDensityPairCapacity() && arity == 2 && !groupedProbeEligible &&
                capacity >= policy.highDensityPairMinSlots()
                ? policy.highDensityPairLoadFactor()
                : policy.loadFactor();
    }

    final void rehash(int capacity)
    {
        int[] previousSlots = slots;
        if (reusedTerminalSlots != null) {
            if (reusedTerminalSlots.length != capacity) {
                throw new IllegalStateException("Reused terminal slots do not match requested capacity");
            }
            slots = reusedTerminalSlots;
            reusedTerminalSlots = null;
            Arrays.fill(slots, 0);
            slotMask = capacity - 1;
            maxFill = (int) (capacity * loadFactor(capacity));
            debugHighDensityCapacityUsed |= loadFactor(capacity) != policy.loadFactor();
        }
        else {
            allocateSlots(capacity);
        }
        for (int groupId = 0; groupId < size; groupId++) {
            int nullMask = groupNullMask(groupId) & 0xFF;
            long hash = nullMask;
            if (keyPairsByGroup.length != 0) {
                hash += keyPairsByGroup[0][groupId];
                for (int pair = 1; pair < keyPairsByGroup.length; pair++) {
                    hash += keyPairsByGroup[pair][groupId] * compactHashPrime(pair);
                }
            }
            if ((arity & 1) != 0) {
                hash += (long) unpackTailValue(tailKeysByGroup[groupId], arity) * compactHashPrime(keyPairsByGroup.length);
            }
            int mixed = mixHash(hash);
            int fragment = (mixed >>> 24) | 0x80;
            int slot = mixed & slotMask;
            while (slots[slot] != 0) {
                slot = (slot + 1) & slotMask;
            }
            slots[slot] = fragment << 24 | groupId;
        }
        arrayPool.release(previousSlots);
    }

    static int mixHash(long hash)
    {
        hash ^= hash >>> 33;
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= hash >>> 33;
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;
        return (int) hash;
    }

    /**
     * Finds the next empty slot or matching hash fragment without changing linear-probe order. Exact key equality
     * remains in the generated arity-specific loop; this only skips runs that cannot possibly match.
     */
    final int nextProbeCandidate(int[] slots, int slot, int slotMask, int fragment)
    {
        if (!policy.groupedProbe() ||
                !groupedProbeEligible ||
                arity != 2 ||
                slots.length < policy.groupedProbeMinSlots()) {
            return slot;
        }
        int fragmentBits = fragment << 24;
        while (true) {
            while (slot > slots.length - GROUPED_PROBE_LANES) {
                int encoded = slots[slot];
                if (encoded == 0 || (encoded & 0xFF00_0000) == fragmentBits) {
                    return slot;
                }
                slot = (slot + 1) & slotMask;
            }
            IntVector entries = IntVector.fromArray(IntVector.SPECIES_256, slots, slot);
            VectorMask<Integer> candidates = entries.compare(VectorOperators.EQ, 0)
                    .or(entries.lanewise(VectorOperators.AND, 0xFF00_0000).compare(VectorOperators.EQ, fragmentBits));
            if (candidates.anyTrue()) {
                return slot + candidates.firstTrue();
            }
            slot = (slot + GROUPED_PROBE_LANES) & slotMask;
        }
    }

    @Override
    public void releaseBuffers()
    {
        if (policy.debugShapes()) {
            System.err.printf(
                    "[adaptive-long-grouping] release arity=%d groups=%d slots=%d groupedProbeEligible=%s highDensityCapacity=%s terminalJump=%s promoted=%s nullFreeBatches=%d nullableBatches=%d%n",
                    arity,
                    size,
                    slots == null ? 0 : slots.length,
                    groupedProbeEligible,
                    debugHighDensityCapacityUsed,
                    debugTerminalJumpUsed,
                    promoted != null,
                    debugNullFreeBatches,
                    debugNullableBatches);
        }
        if (promoted != null) {
            promoted.releaseBuffers();
            promoted = null;
        }
        releaseCompactState();
        arrayPool.release(batchKeys);
        batchKeys = new int[0];
        arrayPool.release(batchNullMasks);
        batchNullMasks = new byte[0];
        arrayPool.release(batchHashes);
        batchHashes = new int[0];
        arrayPool.release(reusedTerminalSlots);
        reusedTerminalSlots = null;
        arrayPool.release(densePositions);
        densePositions = new int[0];
        arrayPool.release(promotedAssignedGroups);
        promotedAssignedGroups = new long[0];
    }

    @Override
    public long retainedBytes()
    {
        long bytes = slots == null ? 0 : (long) slots.length * Integer.BYTES;
        if (keyPairsByGroup != null) {
            bytes += (long) keyPairsByGroup.length * Long.BYTES;
            for (long[] keys : keyPairsByGroup) {
                bytes += keys == null ? 0 : (long) keys.length * Long.BYTES;
            }
        }
        bytes += tailKeysByGroup == null ? 0 : (long) tailKeysByGroup.length * Integer.BYTES;
        bytes += nullMasksByGroup == null ? 0 : nullMasksByGroup.length;
        bytes += (long) batchKeys.length * Integer.BYTES;
        bytes += batchNullMasks.length;
        bytes += (long) batchHashes.length * Integer.BYTES;
        bytes += (long) densePositions.length * Integer.BYTES;
        bytes += (long) promotedAssignedGroups.length * Long.BYTES;
        bytes += reusedTerminalSlots == null ? 0 : (long) reusedTerminalSlots.length * Integer.BYTES;
        bytes += promoted == null ? 0 : promoted.retainedBytes();
        return bytes;
    }

    private void releaseCompactState()
    {
        arrayPool.release(slots);
        slots = null;
        if (keyPairsByGroup != null) {
            for (long[] pair : keyPairsByGroup) {
                arrayPool.release(pair);
            }
            keyPairsByGroup = null;
        }
        arrayPool.release(tailKeysByGroup);
        tailKeysByGroup = null;
        arrayPool.release(nullMasksByGroup);
        nullMasksByGroup = null;
        reverseCapacity = 0;
    }
}
