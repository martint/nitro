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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

final class FlatGroupingTable
{
    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    private static final int MIN_RECORDS_PER_CHUNK_SHIFT = 10;
    private static final int MAX_RECORDS_PER_CHUNK_SHIFT = 16;
    private static final double DEFAULT_LOAD_FACTOR = 15.0 / 16;
    private static final int PACKED_NORMALIZED_TRIPLE_BITS = 21;
    private static final long PACKED_NORMALIZED_TRIPLE_MASK = (1L << PACKED_NORMALIZED_TRIPLE_BITS) - 1;

    private final FlatKeyLayout layout;
    private final FlatKeyTablePolicy.Table policy;
    private final FlatVariableWidthArena variableWidthArena;
    private final int fixedRecordSize;
    private final int recordsPerChunkShift;
    private final int recordsPerChunk;
    private final int recordsPerChunkMask;
    private final int fixedRecordChunkSize;
    private final boolean identityGroupIds;
    private final boolean packedHashRecordSlots;
    private final boolean intHashRecords;
    private final PrimitiveArrayPool arrayPool;

    private byte[] control;
    private int[] groupIdsByHash;
    private int[] recordIndexesByHash;
    private long[] hashRecordsByHash;
    private byte[][] fixedRecordChunks;
    private int[] recordIndexByGroupId;
    private int nextRecordIndex;
    private int capacity;
    private int mask;
    private int maxFill;
    private int releasedRecordChunks;

    // Array-mode accelerator: a direct-index map from the layout's composite value id to the group ordinal.
    // Populated lazily on the first assignment of each composite; a hit returns the group with no hash, probe,
    // or record comparison. The hash table remains the source of truth, so composites that don't fit (a high
    // -cardinality or nullable key) simply fall through to it. Group ordinals are stable (rehash preserves them),
    // and composites are stable across batches, so the cache persists for the whole grouping.
    private int[] compositeCache;
    private long[] sparseCompositeKeys;
    private int[] sparseCompositeGroups;
    private int sparseCompositeMask;
    private int sparseCompositeSize;
    private int sparseCompositeMaxFill;
    private boolean sparseCompositeAdmissionDecided;
    private boolean sparseCompositeAdmitted;
    private boolean debugSparseCompositePrinted;

    // Reusable per-batch hash buffer for the decoupled hash-then-probe driver (see prepareBatchHashes). Sized to
    // the largest batch seen and held across batches so the path allocates nothing in steady state. When
    // batchHashesValid is set, assignGroupHashed reads the precomputed hash for the position instead of hashing
    // inline, so the probe pass issues independent, back-to-back record loads whose cache misses overlap.
    private long[] batchHashes;
    private boolean batchHashesValid;
    private long[] batchNormalizedFirst;
    private long[] batchNormalizedSecond;
    private byte[] batchNormalizedValid;
    private boolean batchNormalizedHashesValid;
    private long[] normalizedFirstByRecord;
    private long[] normalizedSecondByRecord;
    private int[] normalizedThirdByRecord;
    private long[] normalizedValidByRecord;
    private boolean packedNormalizedTripleRecords;
    private long normalizedInputCount;
    private int normalizedRecordCount;
    private long[] singleDictionaryGroups;
    private Vector singleDictionaryIdentity;
    private long singleDictionaryContentGeneration = -1;
    private int singleDictionaryEpoch;
    private boolean singleDictionaryGroupCacheActive;
    private int[] singleDictionaryIds;
    private int[] prefetchedBuckets;
    private long[] prefetchedControls;

    public FlatGroupingTable(FlatKeyLayout layout, int expectedSize)
    {
        this(layout, expectedSize, false);
    }

    public FlatGroupingTable(FlatKeyLayout layout, int expectedSize, boolean identityGroupIds)
    {
        this(layout, expectedSize, identityGroupIds, false);
    }

    FlatGroupingTable(FlatKeyLayout layout, int expectedSize, boolean identityGroupIds, boolean packedHashRecordSlots)
    {
        this.arrayPool = layout.primitiveArrays();
        this.layout = layout;
        this.policy = layout.tablePolicy();
        this.identityGroupIds = identityGroupIds &&
                policy.identityGroupIds();
        // Packing removes the record-index-to-record-hash dependent load, but widens each hash slot. Record-identity
        // tables already prove that physical record order is the logical group id, so they avoid paying for a second
        // group-id map and are the structurally compact cohort where the wider self-contained slot can win.
        // Normalized records already retain the complete equality key and can reconstruct their hash during
        // rehash. Keeping a packed (hash, record) long in every bucket therefore duplicates key information and
        // doubles slot storage versus the ordinary int record index. Mixed batches remain correct: rows that
        // cannot be normalized retain their hash in the fixed record, while normalized rows use their compact
        // key for equality and hash reconstruction.
        this.packedHashRecordSlots = packedHashRecordSlots && this.identityGroupIds && !layout.supportsNormalizedRecordWrite();
        this.intHashRecords = !this.packedHashRecordSlots && layout.hashesFitInt();
        this.packedNormalizedTripleRecords = layout.fieldCount() == 3 && layout.supportsNormalizedRecordWrite();
        this.variableWidthArena = layout.anyVariableWidth() ? new FlatVariableWidthArena(arrayPool) : null;
        this.fixedRecordSize = (this.packedHashRecordSlots ? 0 : intHashRecords ? Integer.BYTES : Long.BYTES) + layout.fixedRecordSize();
        int chunkShift = MIN_RECORDS_PER_CHUNK_SHIFT;
        if (policy.poolSizedRecordChunks()) {
            long minimumBytes = arrayPool.minRetainedBytes();
            while (chunkShift < MAX_RECORDS_PER_CHUNK_SHIFT &&
                    ((long) (1 << chunkShift) * fixedRecordSize) < minimumBytes &&
                    ((long) (1 << (chunkShift + 1)) * fixedRecordSize) <= Integer.MAX_VALUE) {
                chunkShift++;
            }
        }
        this.recordsPerChunkShift = chunkShift;
        this.recordsPerChunk = 1 << chunkShift;
        this.recordsPerChunkMask = recordsPerChunk - 1;
        this.fixedRecordChunkSize = toIntExact((long) recordsPerChunk * fixedRecordSize);
        this.capacity = max(VECTOR_LENGTH, computeCapacity(max(16, expectedSize), DEFAULT_LOAD_FACTOR));
        this.mask = capacity - 1;
        this.maxFill = calculateMaxFill(capacity);
        this.control = arrayPool.borrowBytes(capacity + VECTOR_LENGTH);
        Arrays.fill(control, (byte) 0);
        this.groupIdsByHash = this.identityGroupIds ? null : arrayPool.borrowInts(capacity);
        this.recordIndexesByHash = this.packedHashRecordSlots ? null : arrayPool.borrowInts(capacity);
        this.hashRecordsByHash = this.packedHashRecordSlots ? arrayPool.borrowLongs(capacity) : null;
        if (groupIdsByHash != null) {
            Arrays.fill(groupIdsByHash, -1);
        }
        if (recordIndexesByHash != null) {
            Arrays.fill(recordIndexesByHash, -1);
        }
        if (hashRecordsByHash != null) {
            Arrays.fill(hashRecordsByHash, 0);
        }
        this.fixedRecordChunks = new byte[recordGroupsRequiredForCapacity(capacity)][];
        this.recordIndexByGroupId = this.identityGroupIds ? null : arrayPool.borrowInts(max(16, expectedSize));
        if (recordIndexByGroupId != null) {
            Arrays.fill(recordIndexByGroupId, -1);
        }
    }

    /**
     * Hook wrapper so callers that drive per-position {@link #assignGroup}/{@link #findGroup} in a
     * tight loop can declare a batch boundary — lets the underlying {@link FlatKeyLayout} hoist
     * Vector type resolution and typed accessors once, rather than dispatching on every position.
     */
    public void beginBatch(Vector[] values, Vector[] nulls)
    {
        layout.beginBatch(values, nulls);
        batchHashesValid = false;
        batchNormalizedHashesValid = false;
        singleDictionaryGroupCacheActive = false;
        singleDictionaryIds = null;
    }

    public void endBatch()
    {
        layout.endBatch();
        batchHashesValid = false;
        batchNormalizedHashesValid = false;
        singleDictionaryGroupCacheActive = false;
        singleDictionaryIds = null;
    }

    private void prepareSingleDictionaryGroupCache(int selectedRows, boolean completePhysicalBatch)
    {
        singleDictionaryGroupCacheActive = false;
        singleDictionaryIds = null;
        if (!policy.singleDictionaryGroupCache() ||
                !identityGroupIds ||
                !completePhysicalBatch ||
                selectedRows == 0) {
            return;
        }
        boolean eligible = layout.batchSupportsSingleDictionaryGroupCache();
        int cardinality = eligible
                ? layout.batchSingleDictionaryGroupCardinality()
                : 0;
        singleDictionaryGroupCacheActive = eligible &&
                cardinality <= policy.singleDictionaryGroupCacheMaxCardinality() &&
                cardinality <=
                        (long) selectedRows * policy.singleDictionaryGroupCacheMaxCardinalityAmplification();
        if (!singleDictionaryGroupCacheActive) {
            return;
        }
        singleDictionaryIds = layout.batchSingleDictionaryGroupIds();

        Vector identity = layout.batchSingleDictionaryGroupIdentity();
        long generation = layout.batchSingleDictionaryGroupGeneration();
        if (identity != singleDictionaryIdentity || generation != singleDictionaryContentGeneration) {
            singleDictionaryIdentity = identity;
            singleDictionaryContentGeneration = generation;
            if (singleDictionaryEpoch == Integer.MAX_VALUE) {
                Arrays.fill(singleDictionaryGroups, 0);
                singleDictionaryEpoch = 0;
            }
            singleDictionaryEpoch++;
        }

        if (singleDictionaryGroups == null || singleDictionaryGroups.length < cardinality) {
            long[] previous = singleDictionaryGroups;
            singleDictionaryGroups = arrayPool.borrowLongs(cardinality);
            Arrays.fill(singleDictionaryGroups, 0);
            arrayPool.release(previous);
        }
    }

    /**
     * Phase one of the decoupled driver: precompute this batch's key hashes for the masked positions into a
     * reusable buffer, so the subsequent per-position {@link #assignGroup} calls probe with an already-resolved
     * hash. Separating the hash pass from the probe pass lets the independent probe loads overlap their cache
     * misses (memory-level parallelism), the structure Trino's {@code FlatGroupByHash} uses. No-op when an enabled
     * composite cache will resolve repeated keys without hashing; direct caches never hash, while sparse caches
     * hash only a composite's first occurrence.
     */
    public void prepareBatchHashes(Vector[] values, Vector[] nulls, Mask mask)
    {
        prepareSingleDictionaryGroupCache(mask.selectedCount(), mask.all());
        considerSparseCompositeAdmission(mask);
        if (skipBatchHashPrecompute() || mask.none()) {
            batchHashesValid = false;
            return;
        }
        int size = mask.maxPosition() + 1;
        if (batchHashes == null || batchHashes.length < size) {
            long[] previous = batchHashes;
            batchHashes = arrayPool.borrowLongs(size);
            arrayPool.release(previous);
        }
        if (mask.all() &&
                !layout.supportsNormalizedIntKeyShape() &&
                layout.prepareGeneratedDictionaryBatchHashes(size, batchHashes)) {
            batchNormalizedHashesValid = false;
            batchHashesValid = true;
            return;
        }
        batchNormalizedHashesValid = layout.batchSupportsNormalizedIntKey() &&
                shouldPrepareNormalizedScratch(policy, size, mask.selectedCount());
        if (batchNormalizedHashesValid) {
            ensureBatchNormalizedCapacity(size);
            Arrays.fill(batchNormalizedValid, 0, size, (byte) 0);
        }
        for (int position : mask) {
            batchHashes[position] = prepareBatchHash(values, nulls, position, batchNormalizedHashesValid);
        }
        batchHashesValid = true;
    }

    /**
     * Dense counterpart to {@link #prepareBatchHashes}: an admitted generated physical-key kernel computes the
     * exact logical hash and immediately probes the authoritative table. This avoids writing and rereading the
     * batch hash scratch for shapes where dictionary-entry hashes and resolved integer accessors already make the
     * hash calculation a compact straight-line loop. All other shapes retain the decoupled hash/probe driver.
     */
    long assignGeneratedDictionaryBatch(
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            I64Vector result,
            long nextGroupId)
    {
        prepareSingleDictionaryGroupCache(mask.selectedCount(), mask.all());
        considerSparseCompositeAdmission(mask);
        if (!policy.generatedDictionaryHashProbeBatch() ||
                !mask.all() ||
                mask.none() ||
                skipBatchHashPrecompute() ||
                layout.supportsNormalizedIntKeyShape()) {
            return -1;
        }
        return layout.assignGeneratedDictionaryBatch(
                mask.maxPosition() + 1,
                this,
                values,
                nulls,
                nextGroupId,
                result.values());
    }

    long assignNormalizedIntBatch(
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            I64Vector result,
            long nextGroupId)
    {
        prepareSingleDictionaryGroupCache(mask.selectedCount(), mask.all());
        considerSparseCompositeAdmission(mask);
        return layout.assignNormalizedIntBatch(this, values, nulls, mask, nextGroupId, result.values());
    }

    long assignPrefetchedBatch(
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            I64Vector result,
            long nextGroupId)
    {
        // Variable-width equality immediately follows the control probe with dependent arena loads. Keep that
        // cohort on the established ordered path; this lookahead is both useful and layout-independent for fixed
        // records, where the first random control load is the dominant dependency to overlap.
        if (!batchHashesValid || mask.none() || layout.anyVariableWidth()) {
            return -1;
        }
        ensureCapacity(nextGroupId + mask.selectedCount());
        int tileRows = policy.hashProbeTileRows();
        if (prefetchedBuckets == null || prefetchedBuckets.length < tileRows) {
            int[] previousBuckets = prefetchedBuckets;
            long[] previousControls = prefetchedControls;
            prefetchedBuckets = arrayPool.borrowInts(tileRows);
            prefetchedControls = arrayPool.borrowLongs(tileRows);
            arrayPool.release(previousBuckets);
            arrayPool.release(previousControls);
        }

        int[] positions = mask.selectedPositions();
        int count = mask.selectedCount();
        long[] output = result.values();
        for (int tileStart = 0; tileStart < count; tileStart += tileRows) {
            int tileEnd = Math.min(count, tileStart + tileRows);
            for (int selectedIndex = tileStart; selectedIndex < tileEnd; selectedIndex++) {
                int position = positions == null ? selectedIndex : positions[selectedIndex];
                long hash = batchHashes[position];
                int packedHash = packedHashRecordSlots ? packedTableHash(hash) : 0;
                int bucket = bucket(packedHashRecordSlots ? Integer.rotateRight(packedHash, 7) : (int) (hash >> 7));
                int tileIndex = selectedIndex - tileStart;
                prefetchedBuckets[tileIndex] = bucket;
                prefetchedControls[tileIndex] = (long) LONG_HANDLE.get(control, bucket);
            }
            for (int selectedIndex = tileStart; selectedIndex < tileEnd; selectedIndex++) {
                int position = positions == null ? selectedIndex : positions[selectedIndex];
                int tileIndex = selectedIndex - tileStart;
                long newGroupId = nextGroupId;
                long groupId = assignGroupHashedPrefetched(
                        values,
                        nulls,
                        position,
                        newGroupId,
                        prefetchedBuckets[tileIndex],
                        prefetchedControls[tileIndex]);
                if (groupId == newGroupId) {
                    nextGroupId++;
                }
                output[position] = groupId;
            }
        }
        return nextGroupId;
    }

    /** Position-list counterpart used when a caller has already removed rows that will not probe the table. */
    public void prepareBatchHashes(Vector[] values, Vector[] nulls, int[] positions, int positionCount)
    {
        prepareSingleDictionaryGroupCache(positionCount, false);
        considerSparseCompositeAdmission(positions, positionCount);
        if (skipBatchHashPrecompute() || positionCount == 0) {
            batchHashesValid = false;
            return;
        }
        int size = 0;
        for (int index = 0; index < positionCount; index++) {
            size = Math.max(size, positions[index] + 1);
        }
        if (batchHashes == null || batchHashes.length < size) {
            long[] previous = batchHashes;
            batchHashes = arrayPool.borrowLongs(size);
            arrayPool.release(previous);
        }
        batchNormalizedHashesValid = layout.batchSupportsNormalizedIntKey() &&
                shouldPrepareNormalizedScratch(policy, size, positionCount);
        if (batchNormalizedHashesValid) {
            ensureBatchNormalizedCapacity(size);
            Arrays.fill(batchNormalizedValid, 0, size, (byte) 0);
        }
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            batchHashes[position] = prepareBatchHash(values, nulls, position, batchNormalizedHashesValid);
        }
        batchHashesValid = true;
    }

    private long prepareBatchHash(Vector[] values, Vector[] nulls, int position, boolean normalize)
    {
        if (layout.tryPrepareNormalizedIntKey(values, nulls, position)) {
            if (policy.debugNormalizedIntKey()) {
                normalizedInputCount++;
            }
            long first = layout.preparedNormalizedFirst();
            long second = layout.preparedNormalizedSecond();
            if (normalize) {
                batchNormalizedFirst[position] = first;
                batchNormalizedSecond[position] = second;
                batchNormalizedValid[position] = 1;
            }
            return FlatKeyLayout.normalizedIntKeyHash(first, second);
        }
        return layout.hash(values, nulls, position);
    }

    /**
     * Dense scratch is addressed by logical position, so a sparse high-position mask can otherwise allocate many
     * bytes for every live row. Keep the accelerator only when that address space remains proportional to the work;
     * sparse batches retain the existing exact hash and record-equality path.
     */
    static boolean shouldPrepareNormalizedScratch(
            FlatKeyTablePolicy.Table policy,
            int addressablePositions,
            int selectedPositions)
    {
        return selectedPositions >= policy.normalizedScratchMinPositions() &&
                (long) addressablePositions <= (long) selectedPositions * policy.normalizedScratchMaxAmplification();
    }

    private void ensureBatchNormalizedCapacity(int size)
    {
        if (!layout.batchSupportsNormalizedIntKey()) {
            return;
        }
        if (batchNormalizedFirst == null || batchNormalizedFirst.length < size) {
            if (policy.debugNormalizedIntKey()) {
                System.err.printf("[normalized-int-key-scratch] fields=%d positions=%d selected-capacity-bytes=%d%n",
                        layout.fieldCount(), size, (long) size * (Long.BYTES * 2L + Byte.BYTES));
            }
            long[] previousFirst = batchNormalizedFirst;
            long[] previousSecond = batchNormalizedSecond;
            byte[] previousValid = batchNormalizedValid;
            batchNormalizedFirst = arrayPool.borrowLongs(size);
            batchNormalizedSecond = arrayPool.borrowLongs(size);
            batchNormalizedValid = arrayPool.borrowBytes(size);
            arrayPool.release(previousFirst);
            arrayPool.release(previousSecond);
            arrayPool.release(previousValid);
        }
    }

    public long assignGroup(Vector[] values, Vector[] nulls, int position, long newGroupId)
    {
        long composite = layout.compositeValueId(position);
        if (composite >= 0) {
            if (!layout.batchDirectCompositeEligible()) {
                if (policy.sparseCompositeGroupCache() && sparseCompositeAdmitted) {
                    int cached = sparseCompositeGroup(composite);
                    if (cached >= 0) {
                        return cached;
                    }
                    long group = assignGroupHashed(values, nulls, position, newGroupId);
                    cacheSparseComposite(composite, toIntExact(group));
                    return group;
                }
                return assignGroupHashed(values, nulls, position, newGroupId);
            }
            int slot = (int) composite;
            if (compositeCache != null && slot < compositeCache.length && compositeCache[slot] >= 0) {
                return compositeCache[slot];
            }
            long group = assignGroupHashed(values, nulls, position, newGroupId);
            cacheComposite(slot, (int) group);
            return group;
        }
        return assignGroupHashed(values, nulls, position, newGroupId);
    }

    private int sparseCompositeGroup(long composite)
    {
        if (sparseCompositeGroups == null) {
            return -1;
        }
        int slot = sparseCompositeHash(composite) & sparseCompositeMask;
        while (true) {
            int group = sparseCompositeGroups[slot];
            if (group < 0) {
                return -1;
            }
            if (sparseCompositeKeys[slot] == composite) {
                return group;
            }
            slot = (slot + 1) & sparseCompositeMask;
        }
    }

    private void cacheSparseComposite(long composite, int group)
    {
        if (policy.debugSparseCompositeGroupCache() && !debugSparseCompositePrinted) {
            debugSparseCompositePrinted = true;
            System.err.printf("[sparse-composite-group-cache] fields=%d%n", layout.fieldCount());
        }
        if (sparseCompositeGroups == null) {
            initializeSparseCompositeCache(16);
        }
        else if (sparseCompositeSize >= sparseCompositeMaxFill) {
            rehashSparseCompositeCache(sparseCompositeGroups.length << 1);
        }
        int slot = sparseCompositeHash(composite) & sparseCompositeMask;
        while (sparseCompositeGroups[slot] >= 0) {
            if (sparseCompositeKeys[slot] == composite) {
                sparseCompositeGroups[slot] = group;
                return;
            }
            slot = (slot + 1) & sparseCompositeMask;
        }
        sparseCompositeKeys[slot] = composite;
        sparseCompositeGroups[slot] = group;
        sparseCompositeSize++;
    }

    private void initializeSparseCompositeCache(int capacity)
    {
        sparseCompositeKeys = arrayPool.borrowLongs(capacity);
        sparseCompositeGroups = arrayPool.borrowInts(capacity);
        Arrays.fill(sparseCompositeGroups, -1);
        sparseCompositeMask = capacity - 1;
        sparseCompositeMaxFill = capacity * 3 / 4;
    }

    private void rehashSparseCompositeCache(int capacity)
    {
        long[] previousKeys = sparseCompositeKeys;
        int[] previousGroups = sparseCompositeGroups;
        initializeSparseCompositeCache(capacity);
        sparseCompositeSize = 0;
        for (int index = 0; index < previousGroups.length; index++) {
            if (previousGroups[index] >= 0) {
                cacheSparseComposite(previousKeys[index], previousGroups[index]);
            }
        }
        arrayPool.release(previousKeys);
        arrayPool.release(previousGroups);
    }

    private static int sparseCompositeHash(long composite)
    {
        long hash = composite;
        hash ^= hash >>> 33;
        hash *= 0xff51afd7ed558ccdL;
        hash ^= hash >>> 33;
        return (int) hash;
    }

    boolean batchArrayModeEligible()
    {
        // A direct composite cache never hashes. A sparse composite cache hashes only the first occurrence of a
        // key, so eagerly hashing every row would throw away most of its benefit. When the sparse cache is
        // explicitly disabled, preserve the established decoupled hash-precompute path for non-direct layouts;
        // this makes the opt-out a causal cache control instead of silently selecting an inferior inline-hash
        // driver.
        return layout.batchDirectCompositeEligible() ||
                (policy.sparseCompositeGroupCache() && sparseCompositeAdmitted && layout.batchArrayModeEligible());
    }

    private boolean skipBatchHashPrecompute()
    {
        return layout.batchDirectCompositeEligible() ||
                (policy.sparseCompositeGroupCache() &&
                        sparseCompositeAdmitted &&
                        sparseCompositeGroups != null &&
                        layout.batchArrayModeEligible());
    }

    private void considerSparseCompositeAdmission(Mask mask)
    {
        if (!sparseCompositeAdmissionCandidate(mask.selectedCount())) {
            return;
        }
        long[] samples = arrayPool.borrowLongs(policy.sparseCompositeAdmissionSampleSize());
        int count = 0;
        for (int position : mask) {
            samples[count++] = layout.compositeValueId(position);
            if (count == policy.sparseCompositeAdmissionSampleSize()) {
                break;
            }
        }
        finishSparseCompositeAdmission(samples);
        arrayPool.release(samples);
    }

    private void considerSparseCompositeAdmission(int[] positions, int positionCount)
    {
        if (!sparseCompositeAdmissionCandidate(positionCount)) {
            return;
        }
        long[] samples = arrayPool.borrowLongs(policy.sparseCompositeAdmissionSampleSize());
        for (int index = 0; index < policy.sparseCompositeAdmissionSampleSize(); index++) {
            samples[index] = layout.compositeValueId(positions[index]);
        }
        finishSparseCompositeAdmission(samples);
        arrayPool.release(samples);
    }

    private boolean sparseCompositeAdmissionCandidate(int positionCount)
    {
        if (!policy.sparseCompositeGroupCache() ||
                sparseCompositeAdmissionDecided ||
                positionCount < policy.sparseCompositeAdmissionSampleSize() ||
                layout.batchDirectCompositeEligible() ||
                !layout.batchArrayModeEligible()) {
            return false;
        }
        int fields = layout.fieldCount();
        if (fields > 3 && fields < policy.sparseCompositeExpensiveMinFields()) {
            sparseCompositeAdmissionDecided = true;
            return false;
        }
        return true;
    }

    private void finishSparseCompositeAdmission(long[] samples)
    {
        int distinct = 0;
        for (int index = 0; index < policy.sparseCompositeAdmissionSampleSize(); index++) {
            long composite = samples[index];
            if (composite < 0) {
                distinct = policy.sparseCompositeAdmissionSampleSize();
                break;
            }
            boolean seen = false;
            for (int previous = 0; previous < distinct; previous++) {
                if (samples[previous] == composite) {
                    seen = true;
                    break;
                }
            }
            if (!seen) {
                samples[distinct++] = composite;
            }
        }
        // Narrow composites need visible repetition before an auxiliary cache can repay its probe. Expensive wide
        // composites have a different break-even point: a locally near-constant prefix is already cheap for the
        // authoritative hash table and can later expand into a large cache, while a broad but bounded first window
        // can amortize its costly full-key hash when the same stable composite ids recur in later batches.
        sparseCompositeAdmitted = distinct <= policy.sparseCompositeAdmissionMaxDistinct() &&
                (layout.fieldCount() < policy.sparseCompositeExpensiveMinFields() ||
                        distinct >= policy.sparseCompositeExpensiveMinDistinct());
        sparseCompositeAdmissionDecided = true;
        if (policy.debugSparseCompositeGroupCache()) {
            System.err.printf("[sparse-composite-admission] fields=%d samples=%d distinct=%d admitted=%s%n",
                    layout.fieldCount(), policy.sparseCompositeAdmissionSampleSize(), distinct, sparseCompositeAdmitted);
        }
    }

    int recordCount()
    {
        return nextRecordIndex;
    }

    int sparseCompositeSize()
    {
        return sparseCompositeSize;
    }

    boolean usesPackedHashRecordSlots()
    {
        return packedHashRecordSlots;
    }

    boolean usesIntHashRecords()
    {
        return intHashRecords;
    }

    void ensureCapacity(long expectedGroups)
    {
        int expected = toIntExact(Math.min(expectedGroups, 1L << 29));
        while (expected >= maxFill) {
            rehash();
        }
        if (!identityGroupIds && expected > 0) {
            ensureGroupIdCapacity(expected - 1);
        }
    }

    long assignGroupHashed(Vector[] values, Vector[] nulls, int position, long newGroupId)
    {
        return assignGroupHashedInternal(values, nulls, position, newGroupId);
    }

    boolean singleDictionaryGroupCacheActive()
    {
        return singleDictionaryGroupCacheActive;
    }

    long assignGroupCached(Vector[] values, Vector[] nulls, int position, long newGroupId)
    {
        int dictionaryId = singleDictionaryIds[position];
        long entry = singleDictionaryGroups[dictionaryId];
        if ((int) (entry >>> Integer.SIZE) == singleDictionaryEpoch) {
            return entry & 0xFFFF_FFFFL;
        }
        long groupId = assignGroup(values, nulls, position, newGroupId);
        cacheSingleDictionaryGroup(dictionaryId, groupId);
        return groupId;
    }

    private long assignGroupHashedInternal(Vector[] values, Vector[] nulls, int position, long newGroupId)
    {
        boolean normalized;
        long normalizedFirst = 0;
        long normalizedSecond = 0;
        long hash;
        if (batchHashesValid) {
            hash = batchHashes[position];
            normalized = batchNormalizedHashesValid && batchNormalizedValid[position] != 0;
            if (normalized) {
                normalizedFirst = batchNormalizedFirst[position];
                normalizedSecond = batchNormalizedSecond[position];
            }
        }
        else {
            normalized = layout.tryPrepareNormalizedIntKey(values, nulls, position);
            if (normalized) {
                if (policy.debugNormalizedIntKey()) {
                    normalizedInputCount++;
                }
                normalizedFirst = layout.preparedNormalizedFirst();
                normalizedSecond = layout.preparedNormalizedSecond();
                hash = FlatKeyLayout.normalizedIntKeyHash(normalizedFirst, normalizedSecond);
            }
            else {
                hash = layout.hash(values, nulls, position);
            }
        }
        int index = getIndex(values, nulls, position, hash, normalized, normalizedFirst, normalizedSecond);
        if (index >= 0) {
            return identityGroupIds ? recordIndexByHash(index) : groupIdsByHash[index];
        }

        addNewGroup(-index - 1, values, nulls, position, hash, newGroupId, normalized, normalizedFirst, normalizedSecond);
        if (nextRecordIndex >= maxFill) {
            rehash();
        }
        return newGroupId;
    }

    private long assignGroupHashedPrefetched(
            Vector[] values,
            Vector[] nulls,
            int position,
            long newGroupId,
            int prefetchedBucket,
            long prefetchedControl)
    {
        boolean normalized = batchNormalizedHashesValid && batchNormalizedValid[position] != 0;
        long normalizedFirst = normalized ? batchNormalizedFirst[position] : 0;
        long normalizedSecond = normalized ? batchNormalizedSecond[position] : 0;
        long hash = batchHashes[position];
        int index = getIndex(
                values,
                nulls,
                position,
                hash,
                normalized,
                normalizedFirst,
                normalizedSecond,
                prefetchedBucket,
                prefetchedControl);
        if (index >= 0) {
            return identityGroupIds ? recordIndexByHash(index) : groupIdsByHash[index];
        }
        addNewGroup(-index - 1, values, nulls, position, hash, newGroupId, normalized, normalizedFirst, normalizedSecond);
        return newGroupId;
    }

    long assignGroupWithHash(
            Vector[] values,
            Vector[] nulls,
            int position,
            long newGroupId,
            long hash)
    {
        int index = getIndex(values, nulls, position, hash, false, 0, 0);
        if (index >= 0) {
            return identityGroupIds ? recordIndexByHash(index) : groupIdsByHash[index];
        }

        addNewGroup(-index - 1, values, nulls, position, hash, newGroupId, false, 0, 0);
        if (nextRecordIndex >= maxFill) {
            rehash();
        }
        return newGroupId;
    }

    long assignNormalizedGroup(
            Vector[] values,
            Vector[] nulls,
            int position,
            long newGroupId,
            long normalizedFirst,
            long normalizedSecond)
    {
        if (policy.debugNormalizedIntKey()) {
            normalizedInputCount++;
        }
        long hash = FlatKeyLayout.normalizedIntKeyHash(normalizedFirst, normalizedSecond);
        int index = getIndex(values, nulls, position, hash, true, normalizedFirst, normalizedSecond);
        if (index >= 0) {
            return identityGroupIds ? recordIndexByHash(index) : groupIdsByHash[index];
        }
        addNewGroup(-index - 1, values, nulls, position, hash, newGroupId, true, normalizedFirst, normalizedSecond);
        if (nextRecordIndex >= maxFill) {
            rehash();
        }
        return newGroupId;
    }

    long assignGroupWithoutNormalization(Vector[] values, Vector[] nulls, int position, long newGroupId)
    {
        long hash = layout.hash(values, nulls, position);
        return assignGroupWithHash(values, nulls, position, newGroupId, hash);
    }

    private void cacheSingleDictionaryGroup(int dictionaryId, long groupId)
    {
        if (dictionaryId >= 0 && (groupId & ~0xFFFF_FFFFL) == 0) {
            singleDictionaryGroups[dictionaryId] = ((long) singleDictionaryEpoch << Integer.SIZE) | groupId;
        }
    }

    int cachedCompositeGroup(int composite)
    {
        return compositeCache != null && composite < compositeCache.length ? compositeCache[composite] : -1;
    }

    int[] prepareCompositeCache(int requiredSize)
    {
        if (compositeCache == null) {
            int initial = Integer.highestOneBit(Math.max(16, requiredSize - 1)) << 1;
            compositeCache = arrayPool.borrowInts(initial);
            java.util.Arrays.fill(compositeCache, -1);
        }
        else if (requiredSize > compositeCache.length) {
            int oldLength = compositeCache.length;
            int newLength = oldLength;
            while (newLength < requiredSize) {
                newLength <<= 1;
            }
            int[] previous = compositeCache;
            compositeCache = arrayPool.borrowInts(newLength);
            System.arraycopy(previous, 0, compositeCache, 0, oldLength);
            java.util.Arrays.fill(compositeCache, oldLength, newLength, -1);
            arrayPool.release(previous);
        }
        return compositeCache;
    }

    void cacheComposite(int composite, int group)
    {
        if (compositeCache == null) {
            int initial = Integer.highestOneBit(Math.max(16, composite)) << 1;
            compositeCache = arrayPool.borrowInts(initial);
            java.util.Arrays.fill(compositeCache, -1);
        }
        else if (composite >= compositeCache.length) {
            int oldLength = compositeCache.length;
            int newLength = oldLength;
            while (newLength <= composite) {
                newLength <<= 1;
            }
            int[] previous = compositeCache;
            compositeCache = arrayPool.borrowInts(newLength);
            System.arraycopy(previous, 0, compositeCache, 0, oldLength);
            java.util.Arrays.fill(compositeCache, oldLength, newLength, -1);
            arrayPool.release(previous);
        }
        compositeCache[composite] = group;
    }

    long assignMixedComposite3Batch(
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            I64Vector result,
            long nextGroupId)
    {
        return layout.assignMixedComposite3Batch(this, values, nulls, mask, result, nextGroupId);
    }

    public long assignGroup(Vector[] values, int position, long newGroupId)
    {
        return assignGroup(values, null, position, newGroupId);
    }

    public long findGroup(Vector[] values, Vector[] nulls, int position)
    {
        boolean normalized;
        long normalizedFirst = 0;
        long normalizedSecond = 0;
        long hash;
        if (batchHashesValid) {
            hash = batchHashes[position];
            normalized = batchNormalizedHashesValid && batchNormalizedValid[position] != 0;
            if (normalized) {
                normalizedFirst = batchNormalizedFirst[position];
                normalizedSecond = batchNormalizedSecond[position];
            }
        }
        else {
            normalized = layout.tryPrepareNormalizedIntKey(values, nulls, position);
            if (normalized) {
                normalizedFirst = layout.preparedNormalizedFirst();
                normalizedSecond = layout.preparedNormalizedSecond();
                hash = FlatKeyLayout.normalizedIntKeyHash(normalizedFirst, normalizedSecond);
            }
            else {
                hash = layout.hash(values, nulls, position);
            }
        }
        int index = getIndex(values, nulls, position, hash, normalized, normalizedFirst, normalizedSecond);
        if (index < 0) {
            return -1;
        }
        return identityGroupIds ? recordIndexByHash(index) : groupIdsByHash[index];
    }

    public long findGroup(Vector[] values, int position)
    {
        return findGroup(values, null, position);
    }

    public Streams groupedValues(int groupedColumnIndex, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        FlatKeyLayout.Field field = layout.field(groupedColumnIndex);
        Vector values = layout.tryGroupedValuesAsDictionary(this, groupedColumnIndex, size, mask, allocator, allocationContext);
        if (values == null) {
            values = layout.tryMaterializeIdBackedBinaryValues(this, groupedColumnIndex, size, mask, output == null ? null : output.values(), allocator, allocationContext);
        }
        if (values == null && field.handler().kind() == FlatTypeHandler.Kind.LONG) {
            I64Vector result = allocator.allocateOrGrow(
                    allocationContext,
                    output == null ? null : (I64Vector) output.values(),
                    I64Vector.class,
                    size,
                    I64Vector::new);
            Arrays.fill(result.values(), 0);
            for (int groupId : mask) {
                int recordIndex = recordIndex(groupId);
                if (recordIndex < 0 || fieldNull(recordIndex, groupedColumnIndex)) {
                    continue;
                }
                result.values()[groupId] = normalizedRecordValid(recordIndex)
                        ? normalizedLongValue(recordIndex, groupedColumnIndex)
                        : field.handler().readLong(
                                fixedChunk(recordIndex),
                                keyOffset(fixedOffset(recordIndex)) + field.fixedOffset());
            }
            values = result;
        }
        if (values == null) {
            values = field.handler().materializeValues(this, field, groupedColumnIndex, size, mask, -1, output == null ? null : output.values(), allocator, allocationContext);
        }
        return Streams.ofValuesAndNulls(
                values,
                materializeNulls(groupedColumnIndex, size, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
    }

    Streams groupedValueRangeAsDictionary(
            int groupedColumnIndex,
            int sourceStart,
            int size,
            Mask outputMask,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        // The dictionary base is indexed by generation-wide value ids. A proper subrange would therefore repeat
        // that generation-wide domain in every output batch, and independently owned batches cannot promise that
        // the domain remains shared across exchange or re-grouping. Preserve the encoding only when this batch owns
        // the complete group-id range; chunked session output uses the ordinary flat materialization path.
        if (sourceStart != 0 || size != recordCount()) {
            return null;
        }
        Vector values = layout.tryGroupedValueRangeAsDictionary(
                this, groupedColumnIndex, sourceStart, size, outputMask, allocator, allocationContext);
        if (values == null) {
            return null;
        }
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator, allocationContext, null, size);
        for (int outputPosition : outputMask) {
            int recordIndex = recordIndex(sourceStart + outputPosition);
            nulls.values()[outputPosition] = recordIndex < 0 || fieldNull(recordIndex, groupedColumnIndex);
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    public Streams copyGroupedValuePosition(
            int groupedColumnIndex,
            Streams output,
            int sourcePosition,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        FlatKeyLayout.Field field = layout.field(groupedColumnIndex);
        int recordIndex = recordIndex(sourcePosition);
        boolean nullValue = recordIndex < 0 || fieldNull(recordIndex, groupedColumnIndex);
        Vector outputValues = output == null ? null : output.values();
        if (!nullValue) {
            int fixedOffset = keyOffset(fixedOffset(recordIndex)) + field.fixedOffset();
            Vector idBackedBinary = layout.tryCopyIdBackedBinaryValue(
                    this,
                    groupedColumnIndex,
                    recordIndex,
                    outputValues,
                    outputPosition,
                    size,
                    allocator,
                    allocationContext);
            if (idBackedBinary != null) {
                outputValues = idBackedBinary;
            }
            else if (normalizedRecordValid(recordIndex) && field.handler().kind() == FlatTypeHandler.Kind.LONG) {
                I64Vector result = allocator.allocateOrGrow(
                        allocationContext,
                        (I64Vector) outputValues,
                        I64Vector.class,
                        size,
                        I64Vector::new);
                result.values()[outputPosition] = normalizedLongValue(recordIndex, groupedColumnIndex);
                outputValues = result;
            }
            else {
                outputValues = field.handler().copyFlatValue(
                            field,
                            fixedChunk(recordIndex),
                            fixedOffset,
                            variableWidthArena,
                            outputValues,
                            outputPosition,
                            size,
                            allocator,
                            allocationContext);
            }
        }
        else if (outputValues == null) {
            outputValues = field.handler().materializeValues(
                    this,
                    field,
                    groupedColumnIndex,
                    size,
                    Mask.none(size),
                    -1,
                    null,
                    allocator,
                    allocationContext);
        }
        else if (nullValue && outputValues instanceof BinaryVector binary) {
            // Variable-width offsets are cumulative. A null copied after a non-null value must carry the prior end
            // offset forward; leaving the pooled slot untouched makes the vector's final byte length smaller than
            // an earlier entry and a later compact copy drops live bytes.
            binary.setNull(outputPosition);
        }

        BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                size);
        outputNulls.values()[outputPosition] = nullValue;
        return Streams.ofValuesAndNulls(outputValues, outputNulls);
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
        if (sourceCount == 0) {
            return output;
        }
        FlatKeyLayout.Field field = layout.field(groupedColumnIndex);
        if (outputStart != 0) {
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

        Vector existingValues = output == null ? null : output.values();
        FlatTypeHandler.Kind kind = field.handler().kind();
        Vector values;
        boolean idBackedBinary = false;
        if (kind == FlatTypeHandler.Kind.BINARY) {
            BinaryVector existing = existingValues instanceof BinaryVector binary ? binary : null;
            BinaryVector binaryValues = layout.tryPrepareIdBackedBinaryOutput(
                    this,
                    groupedColumnIndex,
                    sourcePositions,
                    sourceStart,
                    sourceCount,
                    size,
                    existing,
                    allocator,
                    allocationContext);
            idBackedBinary = binaryValues != null;
            if (!idBackedBinary) {
                long totalBytes = 0;
                for (int index = 0; index < sourceCount; index++) {
                    int recordIndex = recordIndex(sourcePositions[sourceStart + index]);
                    if (recordIndex >= 0 && !fieldNull(recordIndex, groupedColumnIndex)) {
                        int fixedOffset = keyOffset(fixedOffset(recordIndex)) + field.fixedOffset();
                        totalBytes += field.handler().binaryLength(fixedChunk(recordIndex), fixedOffset);
                    }
                }
                if (totalBytes > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Grouped binary output exceeds maximum byte capacity: " + totalBytes);
                }
                binaryValues = BinaryVector.allocateOrGrow(allocator, allocationContext, existing, size, (int) totalBytes);
                Arrays.fill(binaryValues.offsets(), 0);
                binaryValues.clearTraits();
                binaryValues.addTraits(field.binaryTraits());
            }
            values = binaryValues;
        }
        else {
            values = switch (kind) {
                case LONG -> allocator.allocateOrGrow(allocationContext, (I64Vector) existingValues, I64Vector.class, size, I64Vector::new);
                case BOOLEAN -> VectorAccess.writableBooleanVector(allocator, allocationContext, existingValues, size);
                case DOUBLE -> allocator.allocateOrGrow(allocationContext, (F64Vector) existingValues, F64Vector.class, size, F64Vector::new);
                case BINARY -> throw new IllegalStateException("binary output was not prepared");
            };
        }

        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                size);
        switch (kind) {
            case LONG -> copyGroupedLongPositions(
                    field, groupedColumnIndex, sourcePositions, sourceStart, sourceCount, (I64Vector) values, nulls);
            case BOOLEAN -> copyGroupedBooleanPositions(
                    field, groupedColumnIndex, sourcePositions, sourceStart, sourceCount, (BooleanVector) values, nulls);
            case DOUBLE -> copyGroupedDoublePositions(
                    field, groupedColumnIndex, sourcePositions, sourceStart, sourceCount, (F64Vector) values, nulls);
            case BINARY -> copyGroupedBinaryPositions(
                    field, groupedColumnIndex, sourcePositions, sourceStart, sourceCount, (BinaryVector) values, nulls, idBackedBinary);
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private void copyGroupedLongPositions(
            FlatKeyLayout.Field field,
            int groupedColumnIndex,
            int[] sourcePositions,
            int sourceStart,
            int sourceCount,
            I64Vector values,
            BooleanVector nulls)
    {
        long[] outputValues = values.values();
        boolean[] outputNulls = nulls.values();
        for (int index = 0; index < sourceCount; index++) {
            int recordIndex = recordIndex(sourcePositions[sourceStart + index]);
            boolean nullValue = recordIndex < 0 || fieldNull(recordIndex, groupedColumnIndex);
            outputNulls[index] = nullValue;
            if (!nullValue) {
                if (normalizedRecordValid(recordIndex)) {
                    outputValues[index] = normalizedLongValue(recordIndex, groupedColumnIndex);
                }
                else {
                    int fixedOffset = keyOffset(fixedOffset(recordIndex)) + field.fixedOffset();
                    outputValues[index] = field.handler().readLong(fixedChunk(recordIndex), fixedOffset);
                }
            }
        }
    }

    private void copyGroupedBooleanPositions(
            FlatKeyLayout.Field field,
            int groupedColumnIndex,
            int[] sourcePositions,
            int sourceStart,
            int sourceCount,
            BooleanVector values,
            BooleanVector nulls)
    {
        boolean[] outputValues = values.values();
        boolean[] outputNulls = nulls.values();
        for (int index = 0; index < sourceCount; index++) {
            int recordIndex = recordIndex(sourcePositions[sourceStart + index]);
            boolean nullValue = recordIndex < 0 || fieldNull(recordIndex, groupedColumnIndex);
            outputNulls[index] = nullValue;
            if (!nullValue) {
                int fixedOffset = keyOffset(fixedOffset(recordIndex)) + field.fixedOffset();
                outputValues[index] = field.handler().readBoolean(fixedChunk(recordIndex), fixedOffset);
            }
        }
    }

    private void copyGroupedDoublePositions(
            FlatKeyLayout.Field field,
            int groupedColumnIndex,
            int[] sourcePositions,
            int sourceStart,
            int sourceCount,
            F64Vector values,
            BooleanVector nulls)
    {
        double[] outputValues = values.values();
        boolean[] outputNulls = nulls.values();
        for (int index = 0; index < sourceCount; index++) {
            int recordIndex = recordIndex(sourcePositions[sourceStart + index]);
            boolean nullValue = recordIndex < 0 || fieldNull(recordIndex, groupedColumnIndex);
            outputNulls[index] = nullValue;
            if (!nullValue) {
                int fixedOffset = keyOffset(fixedOffset(recordIndex)) + field.fixedOffset();
                outputValues[index] = Double.longBitsToDouble(field.handler().readDoubleBits(fixedChunk(recordIndex), fixedOffset));
            }
        }
    }

    private void copyGroupedBinaryPositions(
            FlatKeyLayout.Field field,
            int groupedColumnIndex,
            int[] sourcePositions,
            int sourceStart,
            int sourceCount,
            BinaryVector values,
            BooleanVector nulls,
            boolean idBackedBinary)
    {
        boolean[] outputNulls = nulls.values();
        for (int index = 0; index < sourceCount; index++) {
            int recordIndex = recordIndex(sourcePositions[sourceStart + index]);
            boolean nullValue = recordIndex < 0 || fieldNull(recordIndex, groupedColumnIndex);
            outputNulls[index] = nullValue;
            if (nullValue) {
                values.setNull(index);
                continue;
            }
            int fixedOffset = keyOffset(fixedOffset(recordIndex)) + field.fixedOffset();
            if (idBackedBinary) {
                layout.copyIdBackedBinaryValueToPrepared(this, groupedColumnIndex, recordIndex, values, index);
            }
            else {
                field.handler().copyBinaryTo(fixedChunk(recordIndex), fixedOffset, variableWidthArena, values, index);
            }
        }
    }

    private int getIndex(Vector[] values, Vector[] nulls, int position, long hash, boolean normalized, long normalizedFirst, long normalizedSecond)
    {
        return getIndex(values, nulls, position, hash, normalized, normalizedFirst, normalizedSecond, -1, 0);
    }

    private int getIndex(
            Vector[] values,
            Vector[] nulls,
            int position,
            long hash,
            boolean normalized,
            long normalizedFirst,
            long normalizedSecond,
            int prefetchedBucket,
            long prefetchedControl)
    {
        int packedHash = packedHashRecordSlots ? packedTableHash(hash) : 0;
        byte hashPrefix = (byte) ((packedHashRecordSlots ? packedHash : hash) & 0x7F | 0x80);
        int bucket = bucket(packedHashRecordSlots ? Integer.rotateRight(packedHash, 7) : (int) (hash >> 7));
        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            long controlVector = (long) LONG_HANDLE.get(control, bucket);
            if (bucket == prefetchedBucket) {
                // A prior source-order insertion may have changed this control word, so the current value remains
                // authoritative. Reading the staged value keeps the preceding tile load live as a software prefetch.
                controlVector = controlVector == prefetchedControl ? prefetchedControl : controlVector;
                prefetchedBucket = -1;
            }
            long controlMatches = match(controlVector, repeated);
            while (controlMatches != 0) {
                int index = bucket(bucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
                if (packedHashRecordSlots) {
                    long hashRecord = hashRecordsByHash[index];
                    int recordIndex = (int) hashRecord - 1;
                    boolean sameHash = (int) (hashRecord >>> Integer.SIZE) == packedHash;
                    boolean sameKey = recordIndex >= 0 && sameHash &&
                            identicalKey(recordIndex, values, nulls, position, normalized, normalizedFirst, normalizedSecond);
                    if (sameKey) {
                        return index;
                    }
                }
                else {
                    int recordIndex = recordIndexesByHash[index];
                    if (recordIndex >= 0 && identical(recordIndex, hash, values, nulls, position, normalized, normalizedFirst, normalizedSecond)) {
                        return index;
                    }
                }
                controlMatches &= controlMatches - 1;
            }

            long emptyMatches = match(controlVector, 0L);
            if (emptyMatches != 0) {
                return -bucket(bucket + (Long.numberOfTrailingZeros(emptyMatches) >>> 3)) - 1;
            }

            bucket = bucket(bucket + step);
            step += VECTOR_LENGTH;
        }
    }

    private int recordIndexByHash(int index)
    {
        if (!packedHashRecordSlots) {
            return recordIndexesByHash[index];
        }
        return (int) hashRecordsByHash[index] - 1;
    }

    private boolean identical(int recordIndex, long hash, Vector[] values, Vector[] nulls, int position, boolean normalized, long normalizedFirst, long normalizedSecond)
    {
        if (normalizedRecordValid(recordIndex)) {
            return identicalKey(recordIndex, values, nulls, position, normalized, normalizedFirst, normalizedSecond);
        }
        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        if (recordHash(fixedChunk, fixedOffset) != hash) {
            return false;
        }
        return identicalKey(recordIndex, values, nulls, position, normalized, normalizedFirst, normalizedSecond);
    }

    private boolean identicalKey(int recordIndex, Vector[] values, Vector[] nulls, int position, boolean normalized, long normalizedFirst, long normalizedSecond)
    {
        if (normalized && normalizedRecordValid(recordIndex)) {
            return normalizedFirst(recordIndex) == normalizedFirst &&
                    normalizedSecond(recordIndex) == normalizedSecond;
        }
        if (normalizedRecordValid(recordIndex)) {
            return layout.normalizedKeyMatchesInput(
                    normalizedFirst(recordIndex),
                    normalizedSecond(recordIndex),
                    values,
                    nulls,
                    position);
        }
        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        return layout.identicalRecordToInput(fixedChunk, keyOffset(fixedOffset), variableWidthArena, values, nulls, position, recordIndex);
    }

    private void addNewGroup(int index, Vector[] values, Vector[] nulls, int position, long hash, long groupId, boolean normalized, long normalizedFirst, long normalizedSecond)
    {
        layout.establishHashStrategy();
        int recordIndex = nextRecordIndex;
        setControl(index, (byte) ((packedHashRecordSlots ? packedTableHash(hash) : hash) & 0x7F | 0x80));
        nextRecordIndex++;
        if (identityGroupIds) {
            if (groupId != recordIndex) {
                throw new IllegalArgumentException("Identity group id does not match record index");
            }
        }
        else {
            groupIdsByHash[index] = toIntExact(groupId);
        }
        if (packedHashRecordSlots) {
            int packedHash = packedTableHash(hash);
            hashRecordsByHash[index] = ((long) packedHash << Integer.SIZE) |
                    ((recordIndex + 1L) & 0xFFFF_FFFFL);
        }
        else {
            recordIndexesByHash[index] = recordIndex;
        }
        if (!identityGroupIds) {
            ensureGroupIdCapacity(toIntExact(groupId));
            recordIndexByGroupId[toIntExact(groupId)] = recordIndex;
        }

        if (!normalized || !layout.supportsNormalizedRecordWrite()) {
            byte[] fixedChunk = fixedChunk(recordIndex);
            int fixedOffset = fixedOffset(recordIndex);
            if (!packedHashRecordSlots) {
                writeRecordHash(fixedChunk, fixedOffset, hash);
            }
            if (variableWidthArena != null) {
                variableWidthArena.beginRecord(recordIndex);
            }
            layout.writeRecord(fixedChunk, keyOffset(fixedOffset), variableWidthArena, values, nulls, position, recordIndex);
        }
        if (normalized) {
            if (packedNormalizedTripleRecords && !fitsPackedNormalizedTriple(normalizedFirst, normalizedSecond)) {
                promotePackedNormalizedTripleRecords();
            }
            ensureNormalizedRecordCapacity(recordIndex + 1);
            if (packedNormalizedTripleRecords) {
                normalizedFirstByRecord[recordIndex] = packNormalizedTriple(normalizedFirst, normalizedSecond);
            }
            else {
                normalizedFirstByRecord[recordIndex] = normalizedFirst;
                if (layout.fieldCount() == 3) {
                    normalizedThirdByRecord[recordIndex] = (int) normalizedSecond;
                }
                else {
                    normalizedSecondByRecord[recordIndex] = normalizedSecond;
                }
            }
            normalizedValidByRecord[recordIndex >>> 6] |= 1L << recordIndex;
            if (policy.debugNormalizedIntKey()) {
                normalizedRecordCount++;
            }
        }
    }

    boolean normalizedRecordValid(int recordIndex)
    {
        return normalizedValidByRecord != null && recordIndex < normalizedFirstByRecord.length &&
                (normalizedValidByRecord[recordIndex >>> 6] & (1L << recordIndex)) != 0;
    }

    boolean usesPackedNormalizedTripleRecords()
    {
        return packedNormalizedTripleRecords;
    }

    int normalizedBinaryId(int recordIndex, int fieldIndex)
    {
        if (!normalizedRecordValid(recordIndex) || fieldIndex < 0 || fieldIndex >= 4) {
            return -1;
        }
        long packed = fieldIndex < 2 ? normalizedFirst(recordIndex) : normalizedSecond(recordIndex);
        int encoded = (int) (packed >>> ((fieldIndex & 1) * Integer.SIZE));
        return encoded == 0 ? -1 : encoded - 1;
    }

    long normalizedLongValue(int recordIndex, int fieldIndex)
    {
        int encoded = normalizedEncoded(recordIndex, fieldIndex);
        if (encoded == 0) {
            throw new IllegalStateException("Normalized key field is null");
        }
        return (long) encoded - 1;
    }

    private int normalizedEncoded(int recordIndex, int fieldIndex)
    {
        long packed = fieldIndex < 2 ? normalizedFirst(recordIndex) : normalizedSecond(recordIndex);
        return (int) (packed >>> ((fieldIndex & 1) * Integer.SIZE));
    }

    private long normalizedFirst(int recordIndex)
    {
        if (!packedNormalizedTripleRecords) {
            return normalizedFirstByRecord[recordIndex];
        }
        long packed = normalizedFirstByRecord[recordIndex];
        return (packed & PACKED_NORMALIZED_TRIPLE_MASK) |
                (((packed >>> PACKED_NORMALIZED_TRIPLE_BITS) & PACKED_NORMALIZED_TRIPLE_MASK) << Integer.SIZE);
    }

    private long normalizedSecond(int recordIndex)
    {
        if (packedNormalizedTripleRecords) {
            return (normalizedFirstByRecord[recordIndex] >>> (PACKED_NORMALIZED_TRIPLE_BITS * 2)) &
                    PACKED_NORMALIZED_TRIPLE_MASK;
        }
        return normalizedThirdByRecord == null
                ? normalizedSecondByRecord[recordIndex]
                : Integer.toUnsignedLong(normalizedThirdByRecord[recordIndex]);
    }

    private static boolean fitsPackedNormalizedTriple(long first, long second)
    {
        return Integer.toUnsignedLong((int) first) <= PACKED_NORMALIZED_TRIPLE_MASK &&
                Integer.toUnsignedLong((int) (first >>> Integer.SIZE)) <= PACKED_NORMALIZED_TRIPLE_MASK &&
                Integer.toUnsignedLong((int) second) <= PACKED_NORMALIZED_TRIPLE_MASK;
    }

    private static long packNormalizedTriple(long first, long second)
    {
        return Integer.toUnsignedLong((int) first) |
                (Integer.toUnsignedLong((int) (first >>> Integer.SIZE)) << PACKED_NORMALIZED_TRIPLE_BITS) |
                (Integer.toUnsignedLong((int) second) << (PACKED_NORMALIZED_TRIPLE_BITS * 2));
    }

    private void promotePackedNormalizedTripleRecords()
    {
        if (!packedNormalizedTripleRecords) {
            return;
        }
        packedNormalizedTripleRecords = false;
        if (normalizedFirstByRecord == null) {
            return;
        }
        normalizedThirdByRecord = arrayPool.borrowInts(normalizedFirstByRecord.length);
        for (int recordIndex = 0; recordIndex < nextRecordIndex; recordIndex++) {
            if (!normalizedRecordValid(recordIndex)) {
                continue;
            }
            long packed = normalizedFirstByRecord[recordIndex];
            normalizedFirstByRecord[recordIndex] = (packed & PACKED_NORMALIZED_TRIPLE_MASK) |
                    (((packed >>> PACKED_NORMALIZED_TRIPLE_BITS) & PACKED_NORMALIZED_TRIPLE_MASK) << Integer.SIZE);
            normalizedThirdByRecord[recordIndex] = (int) ((packed >>> (PACKED_NORMALIZED_TRIPLE_BITS * 2)) &
                    PACKED_NORMALIZED_TRIPLE_MASK);
        }
    }

    private void ensureNormalizedRecordCapacity(int size)
    {
        if (normalizedFirstByRecord != null && normalizedFirstByRecord.length >= size) {
            return;
        }
        int newSize = normalizedFirstByRecord == null ? 16 : normalizedFirstByRecord.length;
        while (newSize < size) {
            newSize *= 2;
        }
        long[] previousFirst = normalizedFirstByRecord;
        long[] previousSecond = normalizedSecondByRecord;
        int[] previousThird = normalizedThirdByRecord;
        long[] previousValid = normalizedValidByRecord;
        normalizedFirstByRecord = arrayPool.borrowLongs(newSize);
        if (layout.fieldCount() == 3) {
            if (!packedNormalizedTripleRecords) {
                normalizedThirdByRecord = arrayPool.borrowInts(newSize);
            }
        }
        else {
            normalizedSecondByRecord = arrayPool.borrowLongs(newSize);
        }
        normalizedValidByRecord = arrayPool.borrowLongs((newSize + Long.SIZE - 1) / Long.SIZE);
        Arrays.fill(normalizedValidByRecord, 0);
        if (previousFirst != null) {
            System.arraycopy(previousFirst, 0, normalizedFirstByRecord, 0, previousFirst.length);
            if (previousThird != null) {
                System.arraycopy(previousThird, 0, normalizedThirdByRecord, 0, previousThird.length);
            }
            else if (previousSecond != null) {
                System.arraycopy(previousSecond, 0, normalizedSecondByRecord, 0, previousSecond.length);
            }
            System.arraycopy(previousValid, 0, normalizedValidByRecord, 0, previousValid.length);
        }
        arrayPool.release(previousFirst);
        arrayPool.release(previousSecond);
        arrayPool.release(previousThird);
        arrayPool.release(previousValid);
    }

    private void ensureGroupIdCapacity(int groupId)
    {
        if (identityGroupIds) {
            return;
        }
        if (recordIndexByGroupId.length > groupId) {
            return;
        }
        int newSize = recordIndexByGroupId.length;
        while (newSize <= groupId) {
            newSize = max(16, newSize * 2);
        }
        int previousLength = recordIndexByGroupId.length;
        int[] previous = recordIndexByGroupId;
        recordIndexByGroupId = arrayPool.borrowInts(newSize);
        System.arraycopy(previous, 0, recordIndexByGroupId, 0, previousLength);
        Arrays.fill(recordIndexByGroupId, previousLength, newSize, -1);
        arrayPool.release(previous);
    }

    private void rehash()
    {
        int previousCapacity = capacity;
        capacity *= 2;
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;
        fixedRecordChunks = Arrays.copyOf(fixedRecordChunks, recordGroupsRequiredForCapacity(capacity));

        byte[] previousControl = control;
        int[] previousGroupIds = groupIdsByHash;
        int[] previousRecordIndexes = recordIndexesByHash;
        long[] previousHashRecords = hashRecordsByHash;
        control = arrayPool.borrowBytes(capacity + VECTOR_LENGTH);
        Arrays.fill(control, (byte) 0);
        groupIdsByHash = identityGroupIds ? null : arrayPool.borrowInts(capacity);
        recordIndexesByHash = packedHashRecordSlots ? null : arrayPool.borrowInts(capacity);
        hashRecordsByHash = packedHashRecordSlots ? arrayPool.borrowLongs(capacity) : null;
        if (groupIdsByHash != null) {
            Arrays.fill(groupIdsByHash, -1);
        }
        if (recordIndexesByHash != null) {
            Arrays.fill(recordIndexesByHash, -1);
        }
        if (hashRecordsByHash != null) {
            Arrays.fill(hashRecordsByHash, 0);
        }

        if (packedHashRecordSlots) {
            for (int previousIndex = 0; previousIndex < previousCapacity; previousIndex++) {
                long hashRecord = previousHashRecords[previousIndex];
                if (hashRecord == 0) {
                    continue;
                }
                int packedHash = (int) (hashRecord >>> Integer.SIZE);
                int recordIndex = (int) hashRecord - 1;
                int groupId = identityGroupIds ? recordIndex : previousGroupIds[previousIndex];
                insertPackedHashRecord(packedHash, recordIndex, groupId);
            }
            arrayPool.release(previousControl);
            arrayPool.release(previousGroupIds);
            arrayPool.release(previousRecordIndexes);
            arrayPool.release(previousHashRecords);
            return;
        }

        int groupCount = identityGroupIds ? nextRecordIndex : recordIndexByGroupId.length;
        for (int groupId = 0; groupId < groupCount; groupId++) {
            int recordIndex = identityGroupIds ? groupId : recordIndexByGroupId[groupId];
            if (recordIndex < 0) {
                continue;
            }

            long hash = normalizedRecordValid(recordIndex)
                    ? FlatKeyLayout.normalizedIntKeyHash(normalizedFirst(recordIndex), normalizedSecond(recordIndex))
                    : recordHash(fixedChunk(recordIndex), fixedOffset(recordIndex));
            byte hashPrefix = (byte) (hash & 0x7F | 0x80);
            int bucket = bucket((int) (hash >> 7));
            int step = 1;
            while (true) {
                long controlVector = (long) LONG_HANDLE.get(control, bucket);
                long emptyMatches = match(controlVector, 0L);
                if (emptyMatches != 0) {
                    int index = bucket(bucket + (Long.numberOfTrailingZeros(emptyMatches) >>> 3));
                    setControl(index, hashPrefix);
                    if (!identityGroupIds) {
                        groupIdsByHash[index] = groupId;
                    }
                    if (packedHashRecordSlots) {
                        hashRecordsByHash[index] = ((long) (int) (hash >>> Integer.SIZE) << Integer.SIZE) |
                                ((recordIndex + 1L) & 0xFFFF_FFFFL);
                    }
                    else {
                        recordIndexesByHash[index] = recordIndex;
                    }
                    break;
                }
                bucket = bucket(bucket + step);
                step += VECTOR_LENGTH;
            }
        }
        arrayPool.release(previousControl);
        arrayPool.release(previousGroupIds);
        arrayPool.release(previousRecordIndexes);
        arrayPool.release(previousHashRecords);
    }

    private void insertPackedHashRecord(int packedHash, int recordIndex, int groupId)
    {
        byte hashPrefix = (byte) (packedHash & 0x7F | 0x80);
        int bucket = bucket(Integer.rotateRight(packedHash, 7));
        int step = 1;
        while (true) {
            long controlVector = (long) LONG_HANDLE.get(control, bucket);
            long emptyMatches = match(controlVector, 0L);
            if (emptyMatches != 0) {
                int index = bucket(bucket + (Long.numberOfTrailingZeros(emptyMatches) >>> 3));
                setControl(index, hashPrefix);
                if (!identityGroupIds) {
                    groupIdsByHash[index] = groupId;
                }
                hashRecordsByHash[index] = ((long) packedHash << Integer.SIZE) |
                        ((recordIndex + 1L) & 0xFFFF_FFFFL);
                return;
            }
            bucket = bucket(bucket + step);
            step += VECTOR_LENGTH;
        }
    }

    public boolean fieldNull(int recordIndex, int fieldIndex)
    {
        if (normalizedRecordValid(recordIndex)) {
            return normalizedEncoded(recordIndex, fieldIndex) == 0;
        }
        return layout.fieldNull(fixedChunk(recordIndex), keyOffset(fixedOffset(recordIndex)), fieldIndex);
    }

    private BooleanVector materializeNulls(int fieldIndex, int size, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            int recordIndex = recordIndex(index);
            result.values()[index] = recordIndex < 0 || fieldNull(recordIndex, fieldIndex);
        }
        return result;
    }

    int recordIndex(long groupId)
    {
        if (identityGroupIds) {
            return groupId >= 0 && groupId < nextRecordIndex ? (int) groupId : -1;
        }
        return groupId >= 0 && groupId < recordIndexByGroupId.length ? recordIndexByGroupId[(int) groupId] : -1;
    }

    byte[] fixedChunk(int recordIndex)
    {
        int groupIndex = recordIndex >> recordsPerChunkShift;
        byte[] chunk = fixedRecordChunks[groupIndex];
        if (chunk == null) {
            chunk = borrowChunk(FixedRecordChunk.class, fixedRecordChunkSize);
            fixedRecordChunks[groupIndex] = chunk;
        }
        return chunk;
    }

    int keyOffset(int fixedOffset)
    {
        return fixedOffset + (packedHashRecordSlots ? 0 : intHashRecords ? Integer.BYTES : Long.BYTES);
    }

    private long recordHash(byte[] fixedChunk, int fixedOffset)
    {
        return intHashRecords
                ? (int) INT_HANDLE.get(fixedChunk, fixedOffset)
                : (long) LONG_HANDLE.get(fixedChunk, fixedOffset);
    }

    private void writeRecordHash(byte[] fixedChunk, int fixedOffset, long hash)
    {
        if (intHashRecords) {
            INT_HANDLE.set(fixedChunk, fixedOffset, (int) hash);
        }
        else {
            LONG_HANDLE.set(fixedChunk, fixedOffset, hash);
        }
    }

    private byte[] borrowChunk(Object family, int size)
    {
        byte[] chunk = arrayPool.borrow(family, size, byte[].class);
        return chunk == null ? new byte[size] : chunk;
    }

    private void releaseChunk(Object family, byte[] chunk)
    {
        if (chunk != null) {
            arrayPool.retain(family, chunk.length, chunk.length, chunk);
        }
    }

    int fixedOffset(int recordIndex)
    {
        return (recordIndex & recordsPerChunkMask) * fixedRecordSize;
    }

    FlatVariableWidthArena variableWidthArena()
    {
        return variableWidthArena;
    }

    long retainedBytes()
    {
        long bytes = layout.retainedBytes();
        bytes += control == null ? 0 : control.length;
        bytes += intArrayBytes(groupIdsByHash);
        bytes += intArrayBytes(recordIndexesByHash);
        bytes += longArrayBytes(hashRecordsByHash);
        bytes += intArrayBytes(recordIndexByGroupId);
        bytes += intArrayBytes(compositeCache);
        bytes += longArrayBytes(sparseCompositeKeys);
        bytes += intArrayBytes(sparseCompositeGroups);
        bytes += longArrayBytes(batchHashes);
        bytes += longArrayBytes(batchNormalizedFirst);
        bytes += longArrayBytes(batchNormalizedSecond);
        bytes += batchNormalizedValid == null ? 0 : batchNormalizedValid.length;
        bytes += longArrayBytes(normalizedFirstByRecord);
        bytes += longArrayBytes(normalizedSecondByRecord);
        bytes += intArrayBytes(normalizedThirdByRecord);
        bytes += longArrayBytes(normalizedValidByRecord);
        bytes += longArrayBytes(singleDictionaryGroups);
        bytes += intArrayBytes(prefetchedBuckets);
        bytes += longArrayBytes(prefetchedControls);
        if (fixedRecordChunks != null) {
            bytes += (long) fixedRecordChunks.length * Long.BYTES;
            for (byte[] chunk : fixedRecordChunks) {
                bytes += chunk == null ? 0 : chunk.length;
            }
        }
        if (variableWidthArena != null) {
            bytes += variableWidthArena.retainedBytes();
        }
        return bytes;
    }

    /** Releases probe-only storage once no further keys can be assigned. Group-ordered records remain readable. */
    void finishInput()
    {
        arrayPool.release(control);
        control = null;
        arrayPool.release(groupIdsByHash);
        groupIdsByHash = null;
        arrayPool.release(recordIndexesByHash);
        recordIndexesByHash = null;
        arrayPool.release(hashRecordsByHash);
        hashRecordsByHash = null;
        arrayPool.release(compositeCache);
        compositeCache = null;
        arrayPool.release(sparseCompositeKeys);
        sparseCompositeKeys = null;
        arrayPool.release(sparseCompositeGroups);
        sparseCompositeGroups = null;
        arrayPool.release(batchHashes);
        batchHashes = null;
        arrayPool.release(batchNormalizedFirst);
        batchNormalizedFirst = null;
        arrayPool.release(batchNormalizedSecond);
        batchNormalizedSecond = null;
        arrayPool.release(batchNormalizedValid);
        batchNormalizedValid = null;
        arrayPool.release(singleDictionaryGroups);
        singleDictionaryGroups = null;
        arrayPool.release(prefetchedBuckets);
        prefetchedBuckets = null;
        arrayPool.release(prefetchedControls);
        prefetchedControls = null;
    }

    boolean supportsProgressiveOutputRelease()
    {
        return identityGroupIds;
    }

    void releaseOutputThrough(int exclusiveRecordIndex)
    {
        if (!identityGroupIds || exclusiveRecordIndex < 0 || exclusiveRecordIndex > nextRecordIndex) {
            throw new IllegalArgumentException("invalid progressive output release");
        }
        int completeChunks = exclusiveRecordIndex == nextRecordIndex
                ? fixedRecordChunks.length
                : exclusiveRecordIndex >> recordsPerChunkShift;
        while (releasedRecordChunks < completeChunks) {
            byte[] chunk = fixedRecordChunks[releasedRecordChunks];
            arrayPool.release(chunk);
            fixedRecordChunks[releasedRecordChunks] = null;
            releasedRecordChunks++;
        }
        if (variableWidthArena != null) {
            variableWidthArena.releaseThroughRecord(exclusiveRecordIndex, exclusiveRecordIndex == nextRecordIndex);
        }
    }

    void releaseBuffers()
    {
        if (policy.debugNormalizedIntKey() && normalizedInputCount > 0) {
            System.err.printf("[normalized-int-key] fields=%d inputs=%d records=%d%n", layout.fieldCount(), normalizedInputCount, normalizedRecordCount);
        }
        layout.releaseBuffers();
        arrayPool.release(control);
        control = null;
        arrayPool.release(groupIdsByHash);
        groupIdsByHash = null;
        arrayPool.release(recordIndexesByHash);
        recordIndexesByHash = null;
        arrayPool.release(hashRecordsByHash);
        hashRecordsByHash = null;
        arrayPool.release(recordIndexByGroupId);
        recordIndexByGroupId = null;
        arrayPool.release(compositeCache);
        compositeCache = null;
        arrayPool.release(sparseCompositeKeys);
        sparseCompositeKeys = null;
        arrayPool.release(sparseCompositeGroups);
        sparseCompositeGroups = null;
        arrayPool.release(batchHashes);
        batchHashes = null;
        arrayPool.release(batchNormalizedFirst);
        batchNormalizedFirst = null;
        arrayPool.release(batchNormalizedSecond);
        batchNormalizedSecond = null;
        arrayPool.release(batchNormalizedValid);
        batchNormalizedValid = null;
        arrayPool.release(normalizedFirstByRecord);
        normalizedFirstByRecord = null;
        arrayPool.release(normalizedSecondByRecord);
        normalizedSecondByRecord = null;
        arrayPool.release(normalizedThirdByRecord);
        normalizedThirdByRecord = null;
        arrayPool.release(normalizedValidByRecord);
        normalizedValidByRecord = null;
        arrayPool.release(singleDictionaryGroups);
        singleDictionaryGroups = null;
        arrayPool.release(prefetchedBuckets);
        prefetchedBuckets = null;
        arrayPool.release(prefetchedControls);
        prefetchedControls = null;
        singleDictionaryIdentity = null;
        if (fixedRecordChunks != null) {
            for (byte[] chunk : fixedRecordChunks) {
                releaseChunk(FixedRecordChunk.class, chunk);
            }
            fixedRecordChunks = null;
        }
        if (variableWidthArena != null) {
            variableWidthArena.releaseBuffers();
        }
    }

    private int bucket(int hash)
    {
        return hash & mask;
    }

    private void setControl(int index, byte hashPrefix)
    {
        control[index] = hashPrefix;
        if (index < VECTOR_LENGTH) {
            control[index + capacity] = hashPrefix;
        }
    }

    private static int computeCapacity(int maxSize, double loadFactor)
    {
        int capacity = (int) (maxSize / loadFactor);
        return max((int) (1L << (64 - Long.numberOfLeadingZeros(capacity - 1))), 16);
    }

    private static int calculateMaxFill(int capacity)
    {
        return (int) (capacity * 15L / 16);
    }

    private static long repeat(byte value)
    {
        return ((value & 0xFFL) * 0x01_01_01_01_01_01_01_01L);
    }

    private static long intArrayBytes(int[] values)
    {
        return values == null ? 0 : (long) values.length * Integer.BYTES;
    }

    private static long longArrayBytes(long[] values)
    {
        return values == null ? 0 : (long) values.length * Long.BYTES;
    }

    private static int packedTableHash(long hash)
    {
        return (int) hash;
    }

    private static long match(long vector, long repeatedValue)
    {
        long comparison = vector ^ repeatedValue;
        return (comparison - 0x01_01_01_01_01_01_01_01L) & ~comparison & 0x80_80_80_80_80_80_80_80L;
    }

    private int recordGroupsRequiredForCapacity(int capacity)
    {
        return max(1, (capacity + recordsPerChunk - 1) >> recordsPerChunkShift);
    }

    static final class FlatVariableWidthArena
    {
        private static final int CHUNK_SIZE = 1 << 20;
        private final PrimitiveArrayPool arrayPool;

        private byte[][] chunks;
        private int[] lastRecordByChunk;
        private int chunkIndex;
        private int chunkOffset;
        private int currentRecord = -1;
        private int releasedChunks;

        private FlatVariableWidthArena(PrimitiveArrayPool arrayPool)
        {
            this.arrayPool = arrayPool;
            this.chunks = new byte[][] {borrowChunk()};
            this.lastRecordByChunk = new int[] {-1};
        }

        public void beginRecord(int recordIndex)
        {
            currentRecord = recordIndex;
        }

        public long append(byte[] source, int sourceOffset, int length)
        {
            if (currentRecord < 0) {
                throw new IllegalStateException("variable-width append has no current record");
            }
            if (length == 0) {
                lastRecordByChunk[chunkIndex] = currentRecord;
                return pointer(chunkIndex, chunkOffset);
            }

            if (chunkOffset + length > CHUNK_SIZE) {
                chunkIndex++;
                chunkOffset = 0;
                if (chunkIndex >= chunks.length) {
                    chunks = Arrays.copyOf(chunks, chunks.length * 2);
                    int previousLength = lastRecordByChunk.length;
                    lastRecordByChunk = Arrays.copyOf(lastRecordByChunk, chunks.length);
                    Arrays.fill(lastRecordByChunk, previousLength, lastRecordByChunk.length, -1);
                }
                if (chunks[chunkIndex] == null) {
                    chunks[chunkIndex] = borrowChunk();
                }
            }

            byte[] chunk = chunks[chunkIndex];
            int offset = chunkOffset;
            System.arraycopy(source, sourceOffset, chunk, offset, length);
            chunkOffset += length;
            lastRecordByChunk[chunkIndex] = currentRecord;
            return pointer(chunkIndex, offset);
        }

        private void releaseThroughRecord(int exclusiveRecordIndex, boolean releaseAll)
        {
            while (releasedChunks < chunks.length &&
                    (releaseAll || (lastRecordByChunk[releasedChunks] >= 0 &&
                            lastRecordByChunk[releasedChunks] < exclusiveRecordIndex))) {
                arrayPool.release(chunks[releasedChunks]);
                chunks[releasedChunks] = null;
                releasedChunks++;
            }
        }

        public byte[] chunk(int index)
        {
            return chunks[index];
        }

        private long retainedBytes()
        {
            if (chunks == null) {
                return 0;
            }
            long bytes = (long) chunks.length * Long.BYTES;
            for (byte[] chunk : chunks) {
                bytes += chunk == null ? 0 : chunk.length;
            }
            return bytes;
        }

        private void releaseBuffers()
        {
            if (chunks == null) {
                return;
            }
            for (byte[] chunk : chunks) {
                if (chunk != null) {
                    arrayPool.retain(VariableWidthChunk.class, chunk.length, chunk.length, chunk);
                }
            }
            chunks = null;
        }

        private byte[] borrowChunk()
        {
            byte[] chunk = arrayPool.borrow(VariableWidthChunk.class, CHUNK_SIZE, byte[].class);
            return chunk == null ? new byte[CHUNK_SIZE] : chunk;
        }

        public static long pointer(int chunkIndex, int chunkOffset)
        {
            return (((long) chunkIndex) << 32) | (chunkOffset & 0xFFFF_FFFFL);
        }

        public static int chunkIndex(long pointer)
        {
            return (int) (pointer >>> 32);
        }

        public static int chunkOffset(long pointer)
        {
            return (int) pointer;
        }
    }

    private static final class FixedRecordChunk {}

    private static final class VariableWidthChunk {}
}
