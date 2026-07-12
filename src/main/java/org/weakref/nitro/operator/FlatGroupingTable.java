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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

final class FlatGroupingTable
{
    private static final boolean DEBUG_NORMALIZED_INT_KEY = Boolean.getBoolean("nitro.debug.normalizedIntKey");
    private static final Object FIXED_RECORD_CHUNK_FAMILY = new Object();
    private static final Object VARIABLE_WIDTH_CHUNK_FAMILY = new Object();
    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final int MIN_RECORDS_PER_CHUNK_SHIFT = 10;
    private static final int MAX_RECORDS_PER_CHUNK_SHIFT = 16;
    private static final int MIN_NORMALIZED_SCRATCH_POSITIONS = 128;
    private static final int MAX_NORMALIZED_SCRATCH_AMPLIFICATION = 4;
    private static final boolean POOL_SIZED_RECORD_CHUNKS =
            Boolean.parseBoolean(System.getProperty("nitro.flatGrouping.poolSizedRecordChunks", "true"));
    private static final double DEFAULT_LOAD_FACTOR = 15.0 / 16;

    private final FlatKeyLayout layout;
    private final FlatVariableWidthArena variableWidthArena;
    private final int fixedRecordSize;
    private final int recordsPerChunkShift;
    private final int recordsPerChunk;
    private final int recordsPerChunkMask;
    private final int fixedRecordChunkSize;
    private final boolean identityGroupIds;
    private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();

    private byte[] control;
    private int[] groupIdsByHash;
    private int[] recordIndexesByHash;
    private byte[][] fixedRecordChunks;
    private int[] recordIndexByGroupId;
    private int nextRecordIndex;
    private int capacity;
    private int mask;
    private int maxFill;

    // Array-mode accelerator: a direct-index map from the layout's composite value id to the group ordinal.
    // Populated lazily on the first assignment of each composite; a hit returns the group with no hash, probe,
    // or record comparison. The hash table remains the source of truth, so composites that don't fit (a high
    // -cardinality or nullable key) simply fall through to it. Group ordinals are stable (rehash preserves them),
    // and composites are stable across batches, so the cache persists for the whole grouping.
    private int[] compositeCache;

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
    private long[] normalizedValidByRecord;
    private long normalizedInputCount;
    private int normalizedRecordCount;

    public FlatGroupingTable(FlatKeyLayout layout, int expectedSize)
    {
        this(layout, expectedSize, false);
    }

    public FlatGroupingTable(FlatKeyLayout layout, int expectedSize, boolean identityGroupIds)
    {
        this.layout = layout;
        this.identityGroupIds = identityGroupIds &&
                Boolean.parseBoolean(System.getProperty("nitro.flatGrouping.identityGroupIds", "true"));
        this.variableWidthArena = layout.anyVariableWidth() ? new FlatVariableWidthArena() : null;
        this.fixedRecordSize = Long.BYTES + layout.fixedRecordSize();
        int chunkShift = MIN_RECORDS_PER_CHUNK_SHIFT;
        if (POOL_SIZED_RECORD_CHUNKS) {
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
        this.recordIndexesByHash = arrayPool.borrowInts(capacity);
        if (groupIdsByHash != null) {
            Arrays.fill(groupIdsByHash, -1);
        }
        Arrays.fill(recordIndexesByHash, -1);
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
    }

    public void endBatch()
    {
        layout.endBatch();
        batchHashesValid = false;
        batchNormalizedHashesValid = false;
    }

    /**
     * Phase one of the decoupled driver: precompute this batch's key hashes for the masked positions into a
     * reusable buffer, so the subsequent per-position {@link #assignGroup} calls probe with an already-resolved
     * hash. Separating the hash pass from the probe pass lets the independent probe loads overlap their cache
     * misses (memory-level parallelism), the structure Trino's {@code FlatGroupByHash} uses. No-op when the batch
     * is array-mode eligible, since that path resolves groups by composite id without hashing at all.
     */
    public void prepareBatchHashes(Vector[] values, Vector[] nulls, Mask mask)
    {
        if (layout.batchArrayModeEligible() || mask.none()) {
            batchHashesValid = false;
            return;
        }
        int size = mask.maxPosition() + 1;
        if (batchHashes == null || batchHashes.length < size) {
            long[] previous = batchHashes;
            batchHashes = arrayPool.borrowLongs(size);
            arrayPool.release(previous);
        }
        batchNormalizedHashesValid = layout.batchSupportsNormalizedIntKey() &&
                shouldPrepareNormalizedScratch(size, mask.selectedCount());
        if (batchNormalizedHashesValid) {
            ensureBatchNormalizedCapacity(size);
            Arrays.fill(batchNormalizedValid, 0, size, (byte) 0);
        }
        for (int position : mask) {
            batchHashes[position] = prepareBatchHash(values, nulls, position, batchNormalizedHashesValid);
        }
        batchHashesValid = true;
    }

    /** Position-list counterpart used when a caller has already removed rows that will not probe the table. */
    public void prepareBatchHashes(Vector[] values, Vector[] nulls, int[] positions, int positionCount)
    {
        if (layout.batchArrayModeEligible() || positionCount == 0) {
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
                shouldPrepareNormalizedScratch(size, positionCount);
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
        if (normalize && layout.tryPrepareNormalizedIntKey(values, nulls, position)) {
            if (DEBUG_NORMALIZED_INT_KEY) {
                normalizedInputCount++;
            }
            batchNormalizedFirst[position] = layout.preparedNormalizedFirst();
            batchNormalizedSecond[position] = layout.preparedNormalizedSecond();
            batchNormalizedValid[position] = 1;
            return FlatKeyLayout.normalizedIntKeyHash(batchNormalizedFirst[position], batchNormalizedSecond[position]);
        }
        return layout.hash(values, nulls, position);
    }

    /**
     * Dense scratch is addressed by logical position, so a sparse high-position mask can otherwise allocate many
     * bytes for every live row. Keep the accelerator only when that address space remains proportional to the work;
     * sparse batches retain the existing exact hash and record-equality path.
     */
    static boolean shouldPrepareNormalizedScratch(int addressablePositions, int selectedPositions)
    {
        return selectedPositions >= MIN_NORMALIZED_SCRATCH_POSITIONS &&
                (long) addressablePositions <= (long) selectedPositions * MAX_NORMALIZED_SCRATCH_AMPLIFICATION;
    }

    private void ensureBatchNormalizedCapacity(int size)
    {
        if (!layout.batchSupportsNormalizedIntKey()) {
            return;
        }
        if (batchNormalizedFirst == null || batchNormalizedFirst.length < size) {
            if (DEBUG_NORMALIZED_INT_KEY) {
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

    boolean batchArrayModeEligible()
    {
        return layout.batchArrayModeEligible();
    }

    int recordCount()
    {
        return nextRecordIndex;
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
                if (DEBUG_NORMALIZED_INT_KEY) {
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
            return identityGroupIds ? recordIndexesByHash[index] : groupIdsByHash[index];
        }

        addNewGroup(-index - 1, values, nulls, position, hash, newGroupId, normalized, normalizedFirst, normalizedSecond);
        if (nextRecordIndex >= maxFill) {
            rehash();
        }
        return newGroupId;
    }

    private void cacheComposite(int composite, int group)
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

    public long assignGroup(Vector[] values, int position, long newGroupId)
    {
        return assignGroup(values, null, position, newGroupId);
    }

    public long findGroup(Vector[] values, Vector[] nulls, int position)
    {
        boolean normalized = layout.tryPrepareNormalizedIntKey(values, nulls, position);
        long normalizedFirst = normalized ? layout.preparedNormalizedFirst() : 0;
        long normalizedSecond = normalized ? layout.preparedNormalizedSecond() : 0;
        long hash = normalized ? FlatKeyLayout.normalizedIntKeyHash(normalizedFirst, normalizedSecond) : layout.hash(values, nulls, position);
        int index = getIndex(values, nulls, position, hash, normalized, normalizedFirst, normalizedSecond);
        if (index < 0) {
            return -1;
        }
        return identityGroupIds ? recordIndexesByHash[index] : groupIdsByHash[index];
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
        if (values == null) {
            values = field.handler().materializeValues(this, field, groupedColumnIndex, size, mask, -1, output == null ? null : output.values(), allocator, allocationContext);
        }
        return Streams.ofValuesAndNulls(
                values,
                materializeNulls(groupedColumnIndex, size, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
    }

    private int getIndex(Vector[] values, Vector[] nulls, int position, long hash, boolean normalized, long normalizedFirst, long normalizedSecond)
    {
        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) (hash >> 7));
        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            long controlVector = (long) LONG_HANDLE.get(control, bucket);
            long controlMatches = match(controlVector, repeated);
            while (controlMatches != 0) {
                int index = bucket(bucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
                int recordIndex = recordIndexesByHash[index];
                if (recordIndex >= 0 && identical(recordIndex, hash, values, nulls, position, normalized, normalizedFirst, normalizedSecond)) {
                    return index;
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

    private boolean identical(int recordIndex, long hash, Vector[] values, Vector[] nulls, int position, boolean normalized, long normalizedFirst, long normalizedSecond)
    {
        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        if ((long) LONG_HANDLE.get(fixedChunk, fixedOffset) != hash) {
            return false;
        }
        if (normalized && normalizedRecordValid(recordIndex)) {
            return normalizedFirstByRecord[recordIndex] == normalizedFirst &&
                    normalizedSecondByRecord[recordIndex] == normalizedSecond;
        }
        return layout.identicalRecordToInput(fixedChunk, keyOffset(fixedOffset), variableWidthArena, values, nulls, position, recordIndex);
    }

    private void addNewGroup(int index, Vector[] values, Vector[] nulls, int position, long hash, long groupId, boolean normalized, long normalizedFirst, long normalizedSecond)
    {
        setControl(index, (byte) (hash & 0x7F | 0x80));
        int recordIndex = nextRecordIndex++;
        if (identityGroupIds) {
            if (groupId != recordIndex) {
                throw new IllegalArgumentException("Identity group id does not match record index");
            }
        }
        else {
            groupIdsByHash[index] = toIntExact(groupId);
        }
        recordIndexesByHash[index] = recordIndex;
        if (!identityGroupIds) {
            ensureGroupIdCapacity(toIntExact(groupId));
            recordIndexByGroupId[toIntExact(groupId)] = recordIndex;
        }

        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        LONG_HANDLE.set(fixedChunk, fixedOffset, hash);
        layout.writeRecord(fixedChunk, keyOffset(fixedOffset), variableWidthArena, values, nulls, position, recordIndex);
        if (normalized) {
            ensureNormalizedRecordCapacity(recordIndex + 1);
            normalizedFirstByRecord[recordIndex] = normalizedFirst;
            normalizedSecondByRecord[recordIndex] = normalizedSecond;
            normalizedValidByRecord[recordIndex >>> 6] |= 1L << recordIndex;
            if (DEBUG_NORMALIZED_INT_KEY) {
                normalizedRecordCount++;
            }
        }
    }

    private boolean normalizedRecordValid(int recordIndex)
    {
        return normalizedValidByRecord != null && recordIndex < normalizedFirstByRecord.length &&
                (normalizedValidByRecord[recordIndex >>> 6] & (1L << recordIndex)) != 0;
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
        long[] previousValid = normalizedValidByRecord;
        normalizedFirstByRecord = arrayPool.borrowLongs(newSize);
        normalizedSecondByRecord = arrayPool.borrowLongs(newSize);
        normalizedValidByRecord = arrayPool.borrowLongs((newSize + Long.SIZE - 1) / Long.SIZE);
        Arrays.fill(normalizedValidByRecord, 0);
        if (previousFirst != null) {
            System.arraycopy(previousFirst, 0, normalizedFirstByRecord, 0, previousFirst.length);
            System.arraycopy(previousSecond, 0, normalizedSecondByRecord, 0, previousSecond.length);
            System.arraycopy(previousValid, 0, normalizedValidByRecord, 0, previousValid.length);
        }
        arrayPool.release(previousFirst);
        arrayPool.release(previousSecond);
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
        capacity *= 2;
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;
        fixedRecordChunks = Arrays.copyOf(fixedRecordChunks, recordGroupsRequiredForCapacity(capacity));

        byte[] previousControl = control;
        int[] previousGroupIds = groupIdsByHash;
        int[] previousRecordIndexes = recordIndexesByHash;
        control = arrayPool.borrowBytes(capacity + VECTOR_LENGTH);
        Arrays.fill(control, (byte) 0);
        groupIdsByHash = identityGroupIds ? null : arrayPool.borrowInts(capacity);
        recordIndexesByHash = arrayPool.borrowInts(capacity);
        if (groupIdsByHash != null) {
            Arrays.fill(groupIdsByHash, -1);
        }
        Arrays.fill(recordIndexesByHash, -1);

        int groupCount = identityGroupIds ? nextRecordIndex : recordIndexByGroupId.length;
        for (int groupId = 0; groupId < groupCount; groupId++) {
            int recordIndex = identityGroupIds ? groupId : recordIndexByGroupId[groupId];
            if (recordIndex < 0) {
                continue;
            }

            long hash = (long) LONG_HANDLE.get(fixedChunk(recordIndex), fixedOffset(recordIndex));
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
                    recordIndexesByHash[index] = recordIndex;
                    break;
                }
                bucket = bucket(bucket + step);
                step += VECTOR_LENGTH;
            }
        }
        arrayPool.release(previousControl);
        arrayPool.release(previousGroupIds);
        arrayPool.release(previousRecordIndexes);
    }

    public boolean fieldNull(int recordIndex, int fieldIndex)
    {
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
            chunk = borrowChunk(FIXED_RECORD_CHUNK_FAMILY, fixedRecordChunkSize);
            fixedRecordChunks[groupIndex] = chunk;
        }
        return chunk;
    }

    int keyOffset(int fixedOffset)
    {
        return fixedOffset + Long.BYTES;
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

    void releaseBuffers()
    {
        if (DEBUG_NORMALIZED_INT_KEY && normalizedInputCount > 0) {
            System.err.printf("[normalized-int-key] fields=%d inputs=%d records=%d%n", layout.fieldCount(), normalizedInputCount, normalizedRecordCount);
        }
        layout.releaseBuffers();
        arrayPool.release(control);
        control = null;
        arrayPool.release(groupIdsByHash);
        groupIdsByHash = null;
        arrayPool.release(recordIndexesByHash);
        recordIndexesByHash = null;
        arrayPool.release(recordIndexByGroupId);
        recordIndexByGroupId = null;
        arrayPool.release(compositeCache);
        compositeCache = null;
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
        arrayPool.release(normalizedValidByRecord);
        normalizedValidByRecord = null;
        if (fixedRecordChunks != null) {
            for (byte[] chunk : fixedRecordChunks) {
                releaseChunk(FIXED_RECORD_CHUNK_FAMILY, chunk);
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
        private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();

        private byte[][] chunks = new byte[][] {borrowChunk()};
        private int chunkIndex;
        private int chunkOffset;

        public long append(byte[] source, int sourceOffset, int length)
        {
            if (length == 0) {
                return pointer(chunkIndex, chunkOffset);
            }

            if (chunkOffset + length > CHUNK_SIZE) {
                chunkIndex++;
                chunkOffset = 0;
                if (chunkIndex >= chunks.length) {
                    chunks = Arrays.copyOf(chunks, chunks.length * 2);
                }
                if (chunks[chunkIndex] == null) {
                    chunks[chunkIndex] = borrowChunk();
                }
            }

            byte[] chunk = chunks[chunkIndex];
            int offset = chunkOffset;
            System.arraycopy(source, sourceOffset, chunk, offset, length);
            chunkOffset += length;
            return pointer(chunkIndex, offset);
        }

        public byte[] chunk(int index)
        {
            return chunks[index];
        }

        private void releaseBuffers()
        {
            if (chunks == null) {
                return;
            }
            for (byte[] chunk : chunks) {
                if (chunk != null) {
                    arrayPool.retain(VARIABLE_WIDTH_CHUNK_FAMILY, chunk.length, chunk.length, chunk);
                }
            }
            chunks = null;
        }

        private byte[] borrowChunk()
        {
            byte[] chunk = arrayPool.borrow(VARIABLE_WIDTH_CHUNK_FAMILY, CHUNK_SIZE, byte[].class);
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
}
