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

import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class LongPairJoinIndex
        extends JoinIndex
{
    private static final long NO_MATCH_ROW_REFERENCE = -1L;

    private static final float LOAD_FACTOR = 0.75f;
    // Large compact pair tables otherwise scatter both the normalized key and row reference across the full
    // hash capacity. Keep only a dense entry ordinal in each random-access slot; append exact keys and row
    // references sequentially. This is the same general separation used by native row-container hash tables,
    // while small tables retain co-located slots for one-load hit verification.
    // Indirection is repaid only once the legacy random slot payload spans hundreds of MiB. At 2^25 slots the
    // compact {key,row} table is 512 MiB; dense ordinals reduce that random footprint to 128 MiB and keep exact
    // keys/rows append-only. Smaller tables retain co-located slots, avoiding an extra load on cache-resident
    // probes. This is a physical table-size boundary, independent of query, columns, or logical data types.
    // A power-of-two capacity jump can also leave a large table less than half occupied. At that point dense
    // records save at least half of the random {key,row} slot footprint and repay their ordinal indirection at a
    // lower absolute boundary. Use only the exact build-row upper bound and physical capacity: ordinary well-filled
    // tables (including negative-probe-heavy layouts) keep their co-located hit path.
    // A dense payload stream from one coalesced build batch needs only the logical row position. Preserve that
    // full non-negative int domain instead of prematurely promoting at the generic 16-bit packed-position limit;
    // if a later batch appears, promote existing positions and duplicate rows exactly to ordinary long refs.
    // Duplicate state is lazy, so a rare duplicate must not pre-size storage from the whole build. Once a dense
    // table fills its first bounded duplicate page and duplicate rows already cover at least half the distinct-key
    // count, the build has established a long reuse horizon; jump once to the exact build-row upper bound instead
    // of retaining every geometric generation in the engine-owned pool.
    // A tag match is rare on negative probes. On JDK 26, converting every 16-lane VectorMask to a scalar bitset
    // is substantially more expensive than an anyTrue reduction. Guard the conversion and pay it only when a
    // candidate key must be inspected; empty-slot tests never need lane bits during probing.
    // Swiss/F14-style SIMD-tag-bucket table (cf. Velox HashTable): probing scans a GROUP of slots at a time.
    // Each slot carries a 1-byte tag (top hash bits, high bit set so 0 means empty) held in a contiguous
    // byte[] separate from the keys/rows. A probe loads GROUP tags with one vector load and compares them
    // to the wanted tag in one instruction, so a whole bucket is filtered without touching any key; the full
    // key is compared only on a tag hit. This replaces the previous per-slot open-addressing linear probe,
    // where every collision step was another full {first,second,row} cache-miss load.
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
    private static final int GROUP = SPECIES.length();
    private final HashJoinIndexPolicy policy;
    private final HashJoinOutputPolicy outputPolicy;
    private final HashJoinExecutionPolicy executionPolicy;
    private final PrimitiveArrayPool arrayPool;

    private byte[] tags;
    // Co-located entry table. Compact slots contain {normalizedKey,rowReference}; wide slots contain
    // {firstKey,secondKey,rowReference}. A matching probe's key verify and row read then hit one contiguous region
    // instead of three separate long[] (firstKeys/secondKeys/singleRows). Tags stay in their own byte[].
    private long[] entries;
    private int[] entryIds;
    private long[] denseKeys;
    // Non-negative values are packed row references. Negative values encode -(duplicateGroup + 1), avoiding
    // capacity-sized head/tail/count arrays when only a small fraction of keys has duplicates.
    private int[] denseRowStates32;
    private long[] denseRowStates;
    private boolean denseRowsFit32 = true;
    private boolean denseRowsSingleBatch;
    private int denseRowsBatchIndex = -1;
    private boolean denseCompactEntries;
    private int denseEntryCount;
    private int denseEntryCapacity;
    private final int initialDenseEntryCapacity;
    // Most warehouse keys are logically INTEGER even though the vector contract exposes longs. Pack two
    // signed-32-bit keys into one normalized long, reducing each slot from three longs to two. If a later
    // key does not fit, promote every live entry once to the full-width layout before inserting it.
    private boolean compactKeys;
    private final boolean keyOnlyBuild;
    private final boolean batchBuild;
    private int[] keyOnlyCounts;
    // Duplicate rows live in one pooled append-only store. Per-slot head/tail/count metadata links each key's
    // rows without allocating a LongArrayList object and backing array for every duplicate key.
    private int[] duplicateHead;
    private int[] duplicateTail;
    private int[] duplicateCount;
    private int[] duplicateNext;
    private int[] duplicateRows32;
    private long[] duplicateRows;
    private int duplicateRowCount;
    private int duplicateRowCapacity;
    private int duplicateGroupCount;
    private int duplicateGroupCapacity;
    private boolean duplicateRowsFit32 = true;
    private final int initialDuplicateRowCapacity;
    private final int expectedBuildRows;
    private int mask;
    private int maxFill;
    private int size;
    private boolean pairHasDuplicates;
    private boolean finalized;
    private long[] compactedRows;
    private long[] compactedRanges;
    private final JoinMatchScratch matchScratch = new JoinMatchScratch();
    private final boolean ownsStorage;

    LongPairJoinIndex(
            HashJoinIndexPolicy policy,
            HashJoinOutputPolicy outputPolicy,
            HashJoinExecutionPolicy executionPolicy,
            PrimitiveArrayPool arrayPool,
            int expectedSize,
            boolean keyOnlyBuild,
            boolean capInitialHash,
            boolean batchBuild)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.outputPolicy = requireNonNull(outputPolicy, "outputPolicy is null");
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
        this.arrayPool = arrayPool;
        this.expectedBuildRows = expectedSize;
        this.compactKeys = policy.compactLongPairKeys();
        this.denseRowsSingleBatch = policy.compactDensePairSingleBatchRowReferences();
        this.keyOnlyBuild = keyOnlyBuild && policy.compactKeyOnlyLongPairBuild();
        this.batchBuild = batchBuild;
        this.ownsStorage = true;
        this.initialDuplicateRowCapacity = capInitialHash ? expectedSize : Math.min(expectedSize, policy.initialHashExpectedCap());
        int initialExpectedSize = capInitialHash ? Math.min(expectedSize, policy.initialHashExpectedCap()) : expectedSize;
        this.initialDenseEntryCapacity = Math.max(16, initialExpectedSize);
        int capacity = GROUP;
        while (capacity < initialExpectedSize / LOAD_FACTOR) {
            capacity <<= 1;
        }
        denseCompactEntries = policy.denseCompactPairEntries() && compactKeys && !this.keyOnlyBuild &&
                (capacity >= policy.denseCompactPairMinCapacity() ||
                        (policy.denseCompactSparsePairEntries() && capacity >= policy.denseCompactSparsePairMinCapacity() &&
                                expectedSize <= capacity / 2));
        allocate(capacity);
    }

    private LongPairJoinIndex(LongPairJoinIndex prepared)
    {
        this.policy = prepared.policy;
        this.outputPolicy = prepared.outputPolicy;
        this.executionPolicy = prepared.executionPolicy;
        this.arrayPool = prepared.arrayPool;
        this.tags = prepared.tags;
        this.entries = prepared.entries;
        this.entryIds = prepared.entryIds;
        this.denseKeys = prepared.denseKeys;
        this.denseRowStates32 = prepared.denseRowStates32;
        this.denseRowStates = prepared.denseRowStates;
        this.denseRowsFit32 = prepared.denseRowsFit32;
        this.denseRowsSingleBatch = prepared.denseRowsSingleBatch;
        this.denseRowsBatchIndex = prepared.denseRowsBatchIndex;
        this.denseCompactEntries = prepared.denseCompactEntries;
        this.denseEntryCount = prepared.denseEntryCount;
        this.denseEntryCapacity = prepared.denseEntryCapacity;
        this.initialDenseEntryCapacity = prepared.initialDenseEntryCapacity;
        this.compactKeys = prepared.compactKeys;
        this.keyOnlyBuild = prepared.keyOnlyBuild;
        this.batchBuild = prepared.batchBuild;
        this.keyOnlyCounts = prepared.keyOnlyCounts;
        this.duplicateHead = prepared.duplicateHead;
        this.duplicateTail = prepared.duplicateTail;
        this.duplicateCount = prepared.duplicateCount;
        this.duplicateNext = prepared.duplicateNext;
        this.duplicateRows32 = prepared.duplicateRows32;
        this.duplicateRows = prepared.duplicateRows;
        this.duplicateRowCount = prepared.duplicateRowCount;
        this.duplicateRowCapacity = prepared.duplicateRowCapacity;
        this.duplicateGroupCount = prepared.duplicateGroupCount;
        this.duplicateGroupCapacity = prepared.duplicateGroupCapacity;
        this.duplicateRowsFit32 = prepared.duplicateRowsFit32;
        this.initialDuplicateRowCapacity = prepared.initialDuplicateRowCapacity;
        this.expectedBuildRows = prepared.expectedBuildRows;
        this.mask = prepared.mask;
        this.maxFill = prepared.maxFill;
        this.size = prepared.size;
        this.pairHasDuplicates = prepared.pairHasDuplicates;
        this.finalized = prepared.finalized;
        this.compactedRows = prepared.compactedRows;
        this.compactedRanges = prepared.compactedRanges;
        this.ownsStorage = false;
    }

    LongPairJoinIndex newProbeView()
    {
        return new LongPairJoinIndex(this);
    }

    private void allocate(int capacity)
    {
        tags = arrayPool.borrowBytes(capacity);
        Arrays.fill(tags, (byte) 0);
        if (denseCompactEntries) {
            entryIds = arrayPool.borrowInts(capacity);
            Arrays.fill(entryIds, 0);
            denseEntryCapacity = initialDenseEntryCapacity;
            denseKeys = arrayPool.borrowLongs(denseEntryCapacity);
            denseRowStates32 = arrayPool.borrowInts(denseEntryCapacity);
        }
        else {
            entries = arrayPool.borrowLongs(capacity * entryStride());
        }
        keyOnlyCounts = null;
        duplicateHead = null;
        duplicateTail = null;
        duplicateCount = null;
        mask = capacity - 1;
        maxFill = (int) (capacity * LOAD_FACTOR);
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    @Override
    public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
    {
        if (JoinIndex.hasNull(nulls, position)) {
            return;
        }
        addNoNulls(values, position, rowReference);
    }

    @Override
    public void addNoNulls(Vector[] values, int position, long rowReference)
    {
        addRow(
                OperatorVectorSupport.longValue(values[0], position),
                OperatorVectorSupport.longValue(values[1], position),
                rowReference);
    }

    @Override
    boolean addBuildRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            BufferedJoinInput.InnerBatch batch,
            int startPosition,
            int length,
            int batchIndex)
    {
        if (!batchBuild) {
            return false;
        }
        if (denseCompactEntries) {
            addDenseRows(values, nulls, hasNulls, batch, startPosition, length, batchIndex);
            return true;
        }
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
        VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
        int endPosition = startPosition + length;
        int[] sourcePositions = batch.positions();
        if (sourcePositions == null) {
            for (int position = startPosition; position < endPosition; position++) {
                if ((firstNulls == null || !firstNulls.value(position)) &&
                        (secondNulls == null || !secondNulls.value(position))) {
                    addRow(firstValues.value(position), secondValues.value(position), JoinRowReference.pack(batchIndex, position));
                }
            }
            return true;
        }
        for (int position = startPosition; position < endPosition; position++) {
            int sourcePosition = sourcePositions[position];
            if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                    (secondNulls == null || !secondNulls.value(sourcePosition))) {
                addRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), JoinRowReference.pack(batchIndex, position));
            }
        }
        return true;
    }

    @Override
    boolean addBuildRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            Mask mask,
            int batchIndex)
    {
        if (!batchBuild) {
            return false;
        }
        if (denseCompactEntries) {
            addDenseRows(values, nulls, hasNulls, mask, batchIndex);
            return true;
        }
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
        VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
        int count = mask.count();
        for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
            int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
            if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                    (secondNulls == null || !secondNulls.value(sourcePosition))) {
                addRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), JoinRowReference.pack(batchIndex, logicalPosition));
            }
        }
        return true;
    }

    @Override
    String probeKind()
    {
        return "pair";
    }

    private void addDenseRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            BufferedJoinInput.InnerBatch batch,
            int startPosition,
            int length,
            int batchIndex)
    {
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
        VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
        int endPosition = startPosition + length;
        int[] sourcePositions = batch.positions();
        if (sourcePositions == null) {
            for (int position = startPosition; position < endPosition; position++) {
                if ((firstNulls == null || !firstNulls.value(position)) &&
                        (secondNulls == null || !secondNulls.value(position))) {
                    addDenseRow(firstValues.value(position), secondValues.value(position), JoinRowReference.pack(batchIndex, position));
                }
            }
            return;
        }
        for (int position = startPosition; position < endPosition; position++) {
            int sourcePosition = sourcePositions[position];
            if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                    (secondNulls == null || !secondNulls.value(sourcePosition))) {
                addDenseRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), JoinRowReference.pack(batchIndex, position));
            }
        }
    }

    private void addDenseRows(Vector[] values, Vector[] nulls, boolean hasNulls, Mask mask, int batchIndex)
    {
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        VectorAccess.BooleanValues firstNulls = hasNulls && nulls[0] != null ? VectorAccess.booleanValues(nulls[0]) : null;
        VectorAccess.BooleanValues secondNulls = hasNulls && nulls[1] != null ? VectorAccess.booleanValues(nulls[1]) : null;
        int count = mask.count();
        for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
            int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
            if ((firstNulls == null || !firstNulls.value(sourcePosition)) &&
                    (secondNulls == null || !secondNulls.value(sourcePosition))) {
                addDenseRow(firstValues.value(sourcePosition), secondValues.value(sourcePosition), JoinRowReference.pack(batchIndex, logicalPosition));
            }
        }
    }

    @Override
    public LongList matches(Vector[] values, Vector[] nulls, int position)
    {
        if (JoinIndex.hasNull(nulls, position)) {
            return LongLists.emptyList();
        }
        return matchesNoNulls(values, position);
    }

    @Override
    public LongList matchesNoNulls(Vector[] values, int position)
    {
        long first = OperatorVectorSupport.longValue(values[0], position);
        long second = OperatorVectorSupport.longValue(values[1], position);
        int slot = probe(first, second, hash64(first, second));
        if (slot < 0) {
            return LongLists.emptyList();
        }
        return matchesForSlot(slot, matchScratch.scalarSingle(), matchScratch.scalarChain());
    }

    @Override
    public void matchRows(Vector[] values, Vector[] nulls, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
    {
        matchScratch.prepareBatch(executionPolicy.maxBatchRows());
        VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
        if (!hasNulls) {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                long first = firstValues.value(position);
                long second = secondValues.value(position);
                int slot = probe(first, second, hash64(first, second));
                if (slot < 0) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                matches[index] = matchesForSlot(slot, singleMatches[index], matchScratch.batchChain(index));
            }
            return;
        }
        VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
        VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            if (firstNulls.value(position) || secondNulls.value(position)) {
                matches[index] = LongLists.emptyList();
                continue;
            }
            long first = firstValues.value(position);
            long second = secondValues.value(position);
            int slot = probe(first, second, hash64(first, second));
            if (slot < 0) {
                matches[index] = LongLists.emptyList();
                continue;
            }
            matches[index] = matchesForSlot(slot, singleMatches[index], matchScratch.batchChain(index));
        }
    }

    private LongList matchesForSlot(int slot, SingleLongList single, ChainLongList chain)
    {
        if (compactedRows != null) {
            long range = compactedRange(slot);
            int start = (int) (range >>> Integer.SIZE);
            int count = (int) range;
            return count == 1
                    ? single.withValue(compactedRows[start])
                    : chain.resetRange(compactedRows, start, count);
        }
        if (keyOnlyBuild) {
            int count = keyOnlyCounts == null || keyOnlyCounts[slot] == 0 ? 1 : keyOnlyCounts[slot];
            return count == 1 ? single.withValue(0) : chain.resetRepeated(0, count);
        }
        if (denseCompactEntries) {
            int ordinal = denseOrdinal(slot);
            long state = denseRowState(ordinal);
            if (state >= 0) {
                return single.withValue(decodeDenseRowReference(state));
            }
            int group = toIntExact(-state - 1);
            return duplicateRows32 != null
                    ? denseRowsSingleBatch
                            ? chain.resetCompactSingleBatch(duplicateRows32, duplicateNext, duplicateHead[group], duplicateCount[group], denseRowsBatchIndex)
                            : chain.resetCompact(duplicateRows32, duplicateNext, duplicateHead[group], duplicateCount[group])
                    : chain.reset(duplicateRows, duplicateNext, duplicateHead[group], duplicateCount[group]);
        }
        int head = duplicateHead == null ? -1 : duplicateHead[slot];
        if (head < 0) {
            return single.withValue(rowReference(slot));
        }
        return duplicateRows32 != null
                ? denseRowsSingleBatch
                        ? chain.resetCompactSingleBatch(duplicateRows32, duplicateNext, head, duplicateCount[slot], denseRowsBatchIndex)
                        : chain.resetCompact(duplicateRows32, duplicateNext, head, duplicateCount[slot])
                : chain.reset(duplicateRows, duplicateNext, head, duplicateCount[slot]);
    }

    @Override
    public boolean supportsSingleMatchRefs()
    {
        finalizeForProbe(executionPolicy.maxBatchRows());
        return !pairHasDuplicates;
    }

    @Override
    public boolean supportsRowRanges()
    {
        finalizeForProbe(executionPolicy.maxBatchRows());
        return outputPolicy.directCompactedRangeOutput() && compactedRows != null;
    }

    @Override
    public int matchRowRanges(
            Vector[] valuesArray,
            Vector[] nullsArray,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            int[] starts,
            int[] counts)
    {
        finalizeForProbe(positionCount);
        if (compactedRows == null) {
            return -1;
        }
        VectorAccess.LongValues firstValues = VectorAccess.longValues(valuesArray[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(valuesArray[1]);
        VectorAccess.BooleanValues firstNulls = hasNulls ? VectorAccess.booleanValues(nullsArray[0]) : null;
        VectorAccess.BooleanValues secondNulls = hasNulls ? VectorAccess.booleanValues(nullsArray[1]) : null;
        int matchCount = 0;
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            if (hasNulls && (firstNulls.value(position) || secondNulls.value(position))) {
                continue;
            }
            long first = firstValues.value(position);
            long second = secondValues.value(position);
            int slot = probe(first, second, hash64(first, second));
            if (slot < 0) {
                continue;
            }
            long range = compactedRange(slot);
            positions[matchCount] = position;
            starts[matchCount] = (int) (range >>> Integer.SIZE);
            counts[matchCount] = (int) range;
            matchCount++;
        }
        return matchCount;
    }

    @Override
    public void copyRowRange(int start, long[] output, int outputOffset, int length)
    {
        System.arraycopy(compactedRows, start, output, outputOffset, length);
    }

    @Override
    public void matchSingleRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, long[] refs)
    {
        VectorAccess.LongValues firstValues = VectorAccess.longValues(valuesArray[0]);
        VectorAccess.LongValues secondValues = VectorAccess.longValues(valuesArray[1]);
        if (!hasNulls) {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = singleRef(firstValues.value(position), secondValues.value(position));
            }
            return;
        }
        VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nullsArray[0]);
        VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nullsArray[1]);
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            refs[index] = firstNulls.value(position) || secondNulls.value(position)
                    ? NO_MATCH_ROW_REFERENCE
                    : singleRef(firstValues.value(position), secondValues.value(position));
        }
    }

    private long singleRef(long first, long second)
    {
        int slot = probe(first, second, hash64(first, second));
        return slot < 0 ? NO_MATCH_ROW_REFERENCE : rowReference(slot);
    }

    // Returns the slot holding (first, second), or -1 if absent. Scans GROUP tags per step: one vector load
    // plus one tag compare filters the whole bucket; a key is only read when its tag matches. A bucket with
    // any empty slot ends the search (open-addressing invariant: a present key precedes any empty in its probe
    // sequence, and there are no deletions).
    private int probe(long first, long second, long hash)
    {
        // Truncating an out-of-domain probe key could alias a compact build key. A compact table only contains
        // signed-32-bit keys, so an out-of-domain probe cannot match without first changing the build layout.
        if (compactKeys && (!fitsSignedInt(first) || !fitsSignedInt(second))) {
            return -1;
        }
        return compactKeys ? probeCompact(first, second, hash) : probeWide(first, second, hash);
    }

    private int probeCompact(long first, long second, long hash)
    {
        byte[] tagTable = tags;
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        long normalizedKey = normalizedKey(first, second);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, tagTable, group);
            long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                long storedKey = denseCompactEntries ? denseKeys[denseOrdinal(slot)] : entries[slot * entryStride()];
                if (storedKey == normalizedKey) {
                    return slot;
                }
                matchBits &= matchBits - 1;
            }
            if (hasEmptyTag(groupTags)) {
                return -1;
            }
            group = (group + GROUP) & mask;
        }
    }

    private int probeWide(long first, long second, long hash)
    {
        byte[] tagTable = tags;
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, tagTable, group);
            long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                int base = slot * entryStride();
                if (entries[base] == first && entries[base + 1] == second) {
                    return slot;
                }
                matchBits &= matchBits - 1;
            }
            if (hasEmptyTag(groupTags)) {
                return -1;
            }
            group = (group + GROUP) & mask;
        }
    }

    private void addRow(long first, long second, long rowReference)
    {
        if (compactKeys && (!fitsSignedInt(first) || !fitsSignedInt(second))) {
            promoteToWideEntries();
        }
        long hash = hash64(first, second);
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, tags, group);
            long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                if (keysEqual(slot, first, second)) {
                    if (keyOnlyBuild) {
                        ensureKeyOnlyCounts();
                        int count = keyOnlyCounts[slot];
                        keyOnlyCounts[slot] = count == 0 ? 2 : count + 1;
                        pairHasDuplicates = true;
                        return;
                    }
                    appendDuplicateRow(slot, denseCompactEntries ? 0 : rowReference(slot), rowReference);
                    pairHasDuplicates = true;
                    return;
                }
                matchBits &= matchBits - 1;
            }
            int emptyLane = firstEmptyTag(groupTags);
            if (emptyLane < GROUP) {
                int slot = group + emptyLane;
                tags[slot] = tag;
                writeEntry(slot, first, second, rowReference);
                size++;
                if (size >= maxFill) {
                    rehash();
                }
                return;
            }
            group = (group + GROUP) & mask;
        }
    }

    /**
     * Build loop selected once per batch for the dense compact representation. Keeping the representation
     * decision outside the row loop lets C2 compile the concrete memory shape instead of re-testing the
     * mutable promotion state at every key comparison and insertion. A key outside the compact domain performs
     * the ordinary exact promotion and then resumes through the general path.
     */
    private void addDenseRow(long first, long second, long rowReference)
    {
        if (!denseCompactEntries) {
            addRow(first, second, rowReference);
            return;
        }
        if (!fitsSignedInt(first) || !fitsSignedInt(second)) {
            promoteDenseToWideEntries();
            addRow(first, second, rowReference);
            return;
        }
        long hash = hash64(first, second);
        byte tag = (byte) ((hash >>> 56) | 0x80L);
        int group = ((int) hash) & mask & ~(GROUP - 1);
        long normalizedKey = normalizedKey(first, second);
        while (true) {
            ByteVector groupTags = ByteVector.fromArray(SPECIES, tags, group);
            long matchBits = matchingTagBits(groupTags.compare(VectorOperators.EQ, tag));
            while (matchBits != 0) {
                int slot = group + Long.numberOfTrailingZeros(matchBits);
                if (denseKeys[denseOrdinal(slot)] == normalizedKey) {
                    appendDenseDuplicateRow(slot, 0, rowReference);
                    pairHasDuplicates = true;
                    return;
                }
                matchBits &= matchBits - 1;
            }
            int emptyLane = firstEmptyTag(groupTags);
            if (emptyLane < GROUP) {
                int slot = group + emptyLane;
                tags[slot] = tag;
                appendDenseEntry(slot, normalizedKey, rowReference);
                size++;
                if (size >= maxFill) {
                    rehashDense();
                }
                return;
            }
            group = (group + GROUP) & mask;
        }
    }

    private void rehash()
    {
        if (denseCompactEntries) {
            rehashDense();
            return;
        }
        byte[] oldTags = tags;
        long[] oldEntries = entries;
        int[] oldDuplicateHead = duplicateHead;
        int[] oldDuplicateTail = duplicateTail;
        int[] oldDuplicateCount = duplicateCount;
        int[] oldCounts = keyOnlyCounts;
        boolean oldCompactKeys = compactKeys;
        allocate(oldTags.length * 2);
        if (oldDuplicateHead != null) {
            ensureDuplicateSlotState();
        }
        if (oldCounts != null) {
            ensureKeyOnlyCounts();
        }
        size = 0;
        for (int oldSlot = 0; oldSlot < oldTags.length; oldSlot++) {
            if (oldTags[oldSlot] == 0) {
                continue;
            }
            int oldStride = oldCompactKeys ? (keyOnlyBuild ? 1 : 2) : (keyOnlyBuild ? 2 : 3);
            int oldBase = oldSlot * oldStride;
            long first = oldCompactKeys ? (int) (oldEntries[oldBase] >>> Integer.SIZE) : oldEntries[oldBase];
            long second = oldCompactKeys ? (int) oldEntries[oldBase] : oldEntries[oldBase + 1];
            long hash = hash64(first, second);
            int slot = findEmpty(hash);
            tags[slot] = (byte) ((hash >>> 56) | 0x80L);
            writeEntry(slot, first, second, keyOnlyBuild ? 0 : oldEntries[oldBase + oldStride - 1]);
            if (oldDuplicateHead != null) {
                duplicateHead[slot] = oldDuplicateHead[oldSlot];
                duplicateTail[slot] = oldDuplicateTail[oldSlot];
                duplicateCount[slot] = oldDuplicateCount[oldSlot];
            }
            if (oldCounts != null) {
                keyOnlyCounts[slot] = oldCounts[oldSlot];
            }
            size++;
        }
        arrayPool.release(oldTags);
        arrayPool.release(oldEntries);
        arrayPool.release(oldDuplicateHead);
        arrayPool.release(oldDuplicateTail);
        arrayPool.release(oldDuplicateCount);
        arrayPool.release(oldCounts);
    }

    private void rehashDense()
    {
        byte[] oldTags = tags;
        int[] oldEntryIds = entryIds;
        int capacity = oldTags.length * 2;
        tags = arrayPool.borrowBytes(capacity);
        Arrays.fill(tags, (byte) 0);
        entryIds = arrayPool.borrowInts(capacity);
        Arrays.fill(entryIds, 0);
        mask = capacity - 1;
        maxFill = (int) (capacity * LOAD_FACTOR);
        size = 0;
        for (int oldSlot = 0; oldSlot < oldTags.length; oldSlot++) {
            if (oldTags[oldSlot] == 0) {
                continue;
            }
            int entryId = oldEntryIds[oldSlot];
            long key = denseKeys[entryId - 1];
            long first = (int) (key >>> Integer.SIZE);
            long second = (int) key;
            long hash = hash64(first, second);
            int slot = findEmpty(hash);
            tags[slot] = (byte) ((hash >>> 56) | 0x80L);
            entryIds[slot] = entryId;
            size++;
        }
        arrayPool.release(oldTags);
        arrayPool.release(oldEntryIds);
    }

    private void promoteToWideEntries()
    {
        if (denseCompactEntries) {
            promoteDenseToWideEntries();
            return;
        }
        long[] compactEntries = entries;
        int wideStride = keyOnlyBuild ? 2 : 3;
        long[] wideEntries = arrayPool.borrowLongs(tags.length * wideStride);
        for (int slot = 0; slot < tags.length; slot++) {
            if (tags[slot] == 0) {
                continue;
            }
            int compactStride = keyOnlyBuild ? 1 : 2;
            long key = compactEntries[slot * compactStride];
            int wideBase = slot * wideStride;
            wideEntries[wideBase] = (int) (key >>> Integer.SIZE);
            wideEntries[wideBase + 1] = (int) key;
            if (!keyOnlyBuild) {
                wideEntries[wideBase + 2] = compactEntries[slot * compactStride + 1];
            }
        }
        entries = wideEntries;
        compactKeys = false;
        arrayPool.release(compactEntries);
    }

    private void promoteDenseToWideEntries()
    {
        long[] wideEntries = arrayPool.borrowLongs(tags.length * 3);
        int[] wideDuplicateHead = null;
        int[] wideDuplicateTail = null;
        int[] wideDuplicateCount = null;
        if (duplicateGroupCount > 0) {
            wideDuplicateHead = arrayPool.borrowInts(tags.length);
            wideDuplicateTail = arrayPool.borrowInts(tags.length);
            wideDuplicateCount = arrayPool.borrowInts(tags.length);
            Arrays.fill(wideDuplicateHead, -1);
            Arrays.fill(wideDuplicateTail, -1);
            Arrays.fill(wideDuplicateCount, 0);
        }
        for (int slot = 0; slot < tags.length; slot++) {
            if (tags[slot] == 0) {
                continue;
            }
            int ordinal = denseOrdinal(slot);
            long key = denseKeys[ordinal];
            int base = slot * 3;
            wideEntries[base] = (int) (key >>> Integer.SIZE);
            wideEntries[base + 1] = (int) key;
            long state = denseRowState(ordinal);
            if (state >= 0) {
                wideEntries[base + 2] = decodeDenseRowReference(state);
            }
            else {
                int group = toIntExact(-state - 1);
                int head = duplicateHead[group];
                wideEntries[base + 2] = duplicateReference(head);
                wideDuplicateHead[slot] = head;
                wideDuplicateTail[slot] = duplicateTail[group];
                wideDuplicateCount[slot] = duplicateCount[group];
            }
        }
        arrayPool.release(entryIds);
        entryIds = null;
        arrayPool.release(denseKeys);
        denseKeys = null;
        arrayPool.release(denseRowStates32);
        denseRowStates32 = null;
        arrayPool.release(denseRowStates);
        denseRowStates = null;
        arrayPool.release(duplicateHead);
        arrayPool.release(duplicateTail);
        arrayPool.release(duplicateCount);
        duplicateHead = wideDuplicateHead;
        duplicateTail = wideDuplicateTail;
        duplicateCount = wideDuplicateCount;
        entries = wideEntries;
        denseCompactEntries = false;
        compactKeys = false;
    }

    private int entryStride()
    {
        return compactKeys ? (keyOnlyBuild ? 1 : 2) : (keyOnlyBuild ? 2 : 3);
    }

    private boolean keysEqual(int slot, long first, long second)
    {
        if (compactKeys) {
            return (denseCompactEntries ? denseKeys[denseOrdinal(slot)] : entries[slot * entryStride()]) ==
                    normalizedKey(first, second);
        }
        int base = slot * entryStride();
        return entries[base] == first && entries[base + 1] == second;
    }

    private long rowReference(int slot)
    {
        if (keyOnlyBuild) {
            return 0;
        }
        if (denseCompactEntries) {
            long state = denseRowState(denseOrdinal(slot));
            if (state < 0) {
                throw new IllegalStateException("duplicate pair key does not have a single row reference");
            }
            return decodeDenseRowReference(state);
        }
        return entries[slot * entryStride() + entryStride() - 1];
    }

    private void writeEntry(int slot, long first, long second, long rowReference)
    {
        if (denseCompactEntries) {
            appendDenseEntry(slot, normalizedKey(first, second), rowReference);
            return;
        }
        if (compactKeys) {
            int base = slot * entryStride();
            entries[base] = normalizedKey(first, second);
            if (!keyOnlyBuild) {
                entries[base + 1] = rowReference;
            }
            return;
        }
        int base = slot * entryStride();
        entries[base] = first;
        entries[base + 1] = second;
        if (!keyOnlyBuild) {
            entries[base + 2] = rowReference;
        }
    }

    private static boolean fitsSignedInt(long value)
    {
        return value == (int) value;
    }

    private static long normalizedKey(long first, long second)
    {
        return ((first & 0xFFFF_FFFFL) << Integer.SIZE) | (second & 0xFFFF_FFFFL);
    }

    private int denseOrdinal(int slot)
    {
        return entryIds[slot] - 1;
    }

    private void appendDenseEntry(int slot, long key, long rowReference)
    {
        prepareDenseRowReference(rowReference);
        if (denseRowsFit32 &&
                !denseRowsSingleBatch &&
                (JoinRowReference.batchIndex(rowReference) > JoinRowReference.MAX_COMPACT_BATCH_INDEX ||
                        JoinRowReference.position(rowReference) > JoinRowReference.MAX_COMPACT_POSITION)) {
            promoteDenseRowsToLong();
        }
        ensureDenseEntryCapacity();
        int ordinal = denseEntryCount++;
        denseKeys[ordinal] = key;
        if (denseRowStates32 != null) {
            denseRowStates32[ordinal] = encodeDenseRowReference32(rowReference);
        }
        else {
            denseRowStates[ordinal] = rowReference;
        }
        entryIds[slot] = ordinal + 1;
    }

    private long denseRowState(int ordinal)
    {
        return denseRowStates32 != null ? denseRowStates32[ordinal] : denseRowStates[ordinal];
    }

    private long decodeDenseRowReference(long state)
    {
        if (denseRowStates32 == null) {
            return state;
        }
        return denseRowsSingleBatch
                ? JoinRowReference.pack(denseRowsBatchIndex, toIntExact(state))
                : JoinRowReference.unpackCompact((int) state);
    }

    private int encodeDenseRowReference32(long rowReference)
    {
        return denseRowsSingleBatch
                ? JoinRowReference.position(rowReference)
                : JoinRowReference.packCompact(rowReference);
    }

    private void prepareDenseRowReference(long rowReference)
    {
        if (!denseRowsSingleBatch) {
            return;
        }
        int batchIndex = JoinRowReference.batchIndex(rowReference);
        if (denseRowsBatchIndex < 0) {
            denseRowsBatchIndex = batchIndex;
            return;
        }
        if (batchIndex == denseRowsBatchIndex) {
            return;
        }
        // Decode both stores while the shared single-batch representation is still active, then switch future
        // rows to ordinary packed/long references. Either store may already have been released during wide-key
        // promotion, so each promotion tolerates an absent compact array.
        promoteDenseRowsToLong();
        promoteDuplicateRowsToLong();
        denseRowsSingleBatch = false;
    }

    private void setDenseRowState(int ordinal, long state)
    {
        if (denseRowStates32 != null) {
            denseRowStates32[ordinal] = toIntExact(state);
        }
        else {
            denseRowStates[ordinal] = state;
        }
    }

    private void ensureDenseEntryCapacity()
    {
        if (denseEntryCount < denseEntryCapacity) {
            return;
        }
        int newCapacity = Math.multiplyExact(denseEntryCapacity, 2);
        long[] previousKeys = denseKeys;
        denseKeys = arrayPool.borrowLongs(newCapacity);
        System.arraycopy(previousKeys, 0, denseKeys, 0, denseEntryCount);
        arrayPool.release(previousKeys);
        if (denseRowStates32 != null) {
            int[] previousStates = denseRowStates32;
            denseRowStates32 = arrayPool.borrowInts(newCapacity);
            System.arraycopy(previousStates, 0, denseRowStates32, 0, denseEntryCount);
            arrayPool.release(previousStates);
        }
        else {
            long[] previousStates = denseRowStates;
            denseRowStates = arrayPool.borrowLongs(newCapacity);
            System.arraycopy(previousStates, 0, denseRowStates, 0, denseEntryCount);
            arrayPool.release(previousStates);
        }
        denseEntryCapacity = newCapacity;
    }

    private void promoteDenseRowsToLong()
    {
        denseRowsFit32 = false;
        if (denseRowStates32 == null) {
            return;
        }
        denseRowStates = arrayPool.borrowLongs(denseEntryCapacity);
        for (int ordinal = 0; ordinal < denseEntryCount; ordinal++) {
            int state = denseRowStates32[ordinal];
            denseRowStates[ordinal] = state < 0 ? state : decodeDenseRowReference(state);
        }
        arrayPool.release(denseRowStates32);
        denseRowStates32 = null;
    }

    void finalizeForProbe(int initialProbeRows)
    {
        if (finalized) {
            return;
        }
        finalized = true;
        if (pairHasDuplicates &&
                policy.compactChains() &&
                initialProbeRows >= policy.compactChainsMinProbeRows()) {
            compactDuplicateChains();
        }
    }

    /** Converts per-key linked duplicate rows into immutable insertion-ordered ranges once build is complete. */
    private void compactDuplicateChains()
    {
        int totalRows = 0;
        for (int slot = 0; slot < tags.length; slot++) {
            if (tags[slot] != 0) {
                totalRows = Math.addExact(totalRows, rowCount(slot));
            }
        }
        long[] ordered = arrayPool.borrowLongs(totalRows);
        int rangeCount = denseCompactEntries ? denseEntryCount : tags.length;
        long[] ranges = arrayPool.borrowLongs(rangeCount);
        int cursor = 0;
        for (int slot = 0; slot < tags.length; slot++) {
            if (tags[slot] == 0) {
                continue;
            }
            int count = rowCount(slot);
            int start = cursor;
            if (keyOnlyBuild) {
                Arrays.fill(ordered, cursor, cursor + count, 0);
                cursor += count;
            }
            else if (count == 1) {
                ordered[cursor++] = rowReference(slot);
            }
            else {
                int ordinal = duplicateHeadForSlot(slot);
                for (int index = 0; index < count; index++) {
                    ordered[cursor++] = duplicateReference(ordinal);
                    ordinal = duplicateNext[ordinal];
                }
            }
            int rangeIndex = denseCompactEntries ? denseOrdinal(slot) : slot;
            ranges[rangeIndex] = ((long) start << Integer.SIZE) | (count & 0xFFFF_FFFFL);
        }
        compactedRows = ordered;
        compactedRanges = ranges;

        arrayPool.release(duplicateHead);
        duplicateHead = null;
        arrayPool.release(duplicateTail);
        duplicateTail = null;
        arrayPool.release(duplicateCount);
        duplicateCount = null;
        arrayPool.release(duplicateNext);
        duplicateNext = null;
        arrayPool.release(duplicateRows32);
        duplicateRows32 = null;
        arrayPool.release(duplicateRows);
        duplicateRows = null;
        arrayPool.release(keyOnlyCounts);
        keyOnlyCounts = null;
    }

    private int rowCount(int slot)
    {
        if (keyOnlyBuild) {
            return keyOnlyCounts == null || keyOnlyCounts[slot] == 0 ? 1 : keyOnlyCounts[slot];
        }
        if (denseCompactEntries) {
            long state = denseRowState(denseOrdinal(slot));
            return state >= 0 ? 1 : duplicateCount[toIntExact(-state - 1)];
        }
        int head = duplicateHead == null ? -1 : duplicateHead[slot];
        return head < 0 ? 1 : duplicateCount[slot];
    }

    private int duplicateHeadForSlot(int slot)
    {
        if (denseCompactEntries) {
            long state = denseRowState(denseOrdinal(slot));
            return duplicateHead[toIntExact(-state - 1)];
        }
        return duplicateHead[slot];
    }

    private long compactedRange(int slot)
    {
        return compactedRanges[denseCompactEntries ? denseOrdinal(slot) : slot];
    }

    @Override
    public void releaseBuffers()
    {
        if (!ownsStorage) {
            return;
        }
        arrayPool.release(tags);
        tags = null;
        arrayPool.release(entries);
        entries = null;
        arrayPool.release(entryIds);
        entryIds = null;
        arrayPool.release(denseKeys);
        denseKeys = null;
        arrayPool.release(denseRowStates32);
        denseRowStates32 = null;
        arrayPool.release(denseRowStates);
        denseRowStates = null;
        arrayPool.release(duplicateHead);
        duplicateHead = null;
        arrayPool.release(duplicateTail);
        duplicateTail = null;
        arrayPool.release(duplicateCount);
        duplicateCount = null;
        arrayPool.release(duplicateNext);
        duplicateNext = null;
        arrayPool.release(duplicateRows32);
        duplicateRows32 = null;
        arrayPool.release(duplicateRows);
        duplicateRows = null;
        arrayPool.release(keyOnlyCounts);
        keyOnlyCounts = null;
        arrayPool.release(compactedRows);
        compactedRows = null;
        arrayPool.release(compactedRanges);
        compactedRanges = null;
    }

    @Override
    long retainedBytes()
    {
        if (!ownsStorage) {
            return matchScratch.retainedBytes();
        }
        long bytes = tags == null ? 0 : tags.length;
        bytes += entries == null ? 0 : (long) entries.length * Long.BYTES;
        bytes += entryIds == null ? 0 : (long) entryIds.length * Integer.BYTES;
        bytes += denseKeys == null ? 0 : (long) denseKeys.length * Long.BYTES;
        bytes += denseRowStates32 == null ? 0 : (long) denseRowStates32.length * Integer.BYTES;
        bytes += denseRowStates == null ? 0 : (long) denseRowStates.length * Long.BYTES;
        bytes += keyOnlyCounts == null ? 0 : (long) keyOnlyCounts.length * Integer.BYTES;
        bytes += duplicateHead == null ? 0 : (long) duplicateHead.length * Integer.BYTES;
        bytes += duplicateTail == null ? 0 : (long) duplicateTail.length * Integer.BYTES;
        bytes += duplicateCount == null ? 0 : (long) duplicateCount.length * Integer.BYTES;
        bytes += duplicateNext == null ? 0 : (long) duplicateNext.length * Integer.BYTES;
        bytes += duplicateRows32 == null ? 0 : (long) duplicateRows32.length * Integer.BYTES;
        bytes += duplicateRows == null ? 0 : (long) duplicateRows.length * Long.BYTES;
        bytes += compactedRows == null ? 0 : (long) compactedRows.length * Long.BYTES;
        bytes += compactedRanges == null ? 0 : (long) compactedRanges.length * Long.BYTES;
        return Math.addExact(bytes, matchScratch.retainedBytes());
    }

    private void appendDuplicateRow(int slot, long existingRowReference, long rowReference)
    {
        if (denseCompactEntries) {
            appendDenseDuplicateRow(slot, existingRowReference, rowReference);
            return;
        }
        ensureDuplicateSlotState();
        int tail = duplicateTail[slot];
        if (tail < 0) {
            int head = appendDuplicateReference(existingRowReference);
            tail = appendDuplicateReference(rowReference);
            duplicateNext[head] = tail;
            duplicateHead[slot] = head;
            duplicateTail[slot] = tail;
            duplicateCount[slot] = 2;
            return;
        }
        int ordinal = appendDuplicateReference(rowReference);
        duplicateNext[tail] = ordinal;
        duplicateTail[slot] = ordinal;
        duplicateCount[slot]++;
    }

    private void appendDenseDuplicateRow(int slot, long existingRowReference, long rowReference)
    {
        int ordinal = denseOrdinal(slot);
        long state = denseRowState(ordinal);
        if (state >= 0) {
            int group = newDuplicateGroup();
            int head = appendDuplicateReference(decodeDenseRowReference(state));
            int tail = appendDuplicateReference(rowReference);
            duplicateNext[head] = tail;
            duplicateHead[group] = head;
            duplicateTail[group] = tail;
            duplicateCount[group] = 2;
            setDenseRowState(ordinal, -(group + 1L));
            return;
        }
        int group = toIntExact(-state - 1);
        int tail = duplicateTail[group];
        int duplicate = appendDuplicateReference(rowReference);
        duplicateNext[tail] = duplicate;
        duplicateTail[group] = duplicate;
        duplicateCount[group]++;
    }

    private int newDuplicateGroup()
    {
        if (duplicateHead == null) {
            duplicateGroupCapacity = 16;
            duplicateHead = arrayPool.borrowInts(duplicateGroupCapacity);
            duplicateTail = arrayPool.borrowInts(duplicateGroupCapacity);
            duplicateCount = arrayPool.borrowInts(duplicateGroupCapacity);
        }
        else if (duplicateGroupCount == duplicateGroupCapacity) {
            int newCapacity = Math.multiplyExact(duplicateGroupCapacity, 2);
            duplicateHead = growDuplicateGroups(duplicateHead, newCapacity);
            duplicateTail = growDuplicateGroups(duplicateTail, newCapacity);
            duplicateCount = growDuplicateGroups(duplicateCount, newCapacity);
            duplicateGroupCapacity = newCapacity;
        }
        return duplicateGroupCount++;
    }

    private int[] growDuplicateGroups(int[] values, int newCapacity)
    {
        int[] grown = arrayPool.borrowInts(newCapacity);
        System.arraycopy(values, 0, grown, 0, duplicateGroupCount);
        arrayPool.release(values);
        return grown;
    }

    private long duplicateReference(int ordinal)
    {
        if (duplicateRows32 == null) {
            return duplicateRows[ordinal];
        }
        return denseRowsSingleBatch
                ? JoinRowReference.pack(denseRowsBatchIndex, duplicateRows32[ordinal])
                : JoinRowReference.unpackCompact(duplicateRows32[ordinal]);
    }

    private int appendDuplicateReference(long rowReference)
    {
        prepareDenseRowReference(rowReference);
        if (duplicateRowsFit32 &&
                !denseRowsSingleBatch &&
                (JoinRowReference.batchIndex(rowReference) > JoinRowReference.MAX_COMPACT_BATCH_INDEX ||
                        JoinRowReference.position(rowReference) > JoinRowReference.MAX_COMPACT_POSITION)) {
            promoteDuplicateRowsToLong();
        }
        ensureDuplicateRowCapacity();
        int ordinal = duplicateRowCount++;
        if (duplicateRows32 != null) {
            duplicateRows32[ordinal] = encodeDenseRowReference32(rowReference);
        }
        else {
            duplicateRows[ordinal] = rowReference;
        }
        duplicateNext[ordinal] = -1;
        return ordinal;
    }

    private void ensureDuplicateSlotState()
    {
        if (duplicateHead != null) {
            return;
        }
        duplicateHead = arrayPool.borrowInts(tags.length);
        duplicateTail = arrayPool.borrowInts(tags.length);
        duplicateCount = arrayPool.borrowInts(tags.length);
        Arrays.fill(duplicateHead, -1);
        Arrays.fill(duplicateTail, -1);
        Arrays.fill(duplicateCount, 0);
    }

    private void ensureDuplicateRowCapacity()
    {
        if (duplicateNext == null) {
            duplicateRowCapacity = Math.max(16, initialDuplicateRowCapacity);
            duplicateNext = arrayPool.borrowInts(duplicateRowCapacity);
            if (duplicateRowsFit32) {
                duplicateRows32 = arrayPool.borrowInts(duplicateRowCapacity);
            }
            else {
                duplicateRows = arrayPool.borrowLongs(duplicateRowCapacity);
            }
            return;
        }
        if (duplicateRowCount < duplicateRowCapacity) {
            return;
        }
        int newCapacity = Math.multiplyExact(duplicateRowCapacity, 2);
        if (policy.preSizeDensePairDuplicateRows() && denseCompactEntries &&
                duplicateRowCount >= denseEntryCount / 2 && expectedBuildRows > newCapacity) {
            newCapacity = expectedBuildRows;
        }
        int[] previousNext = duplicateNext;
        duplicateNext = arrayPool.borrowInts(newCapacity);
        System.arraycopy(previousNext, 0, duplicateNext, 0, duplicateRowCount);
        arrayPool.release(previousNext);
        if (duplicateRows32 != null) {
            int[] previousRows = duplicateRows32;
            duplicateRows32 = arrayPool.borrowInts(newCapacity);
            System.arraycopy(previousRows, 0, duplicateRows32, 0, duplicateRowCount);
            arrayPool.release(previousRows);
        }
        else {
            long[] previousRows = duplicateRows;
            duplicateRows = arrayPool.borrowLongs(newCapacity);
            System.arraycopy(previousRows, 0, duplicateRows, 0, duplicateRowCount);
            arrayPool.release(previousRows);
        }
        duplicateRowCapacity = newCapacity;
    }

    private void promoteDuplicateRowsToLong()
    {
        duplicateRowsFit32 = false;
        if (duplicateRows32 == null) {
            return;
        }
        duplicateRows = arrayPool.borrowLongs(duplicateRowCapacity);
        for (int index = 0; index < duplicateRowCount; index++) {
            duplicateRows[index] = denseRowsSingleBatch
                    ? JoinRowReference.pack(denseRowsBatchIndex, duplicateRows32[index])
                    : JoinRowReference.unpackCompact(duplicateRows32[index]);
        }
        arrayPool.release(duplicateRows32);
        duplicateRows32 = null;
    }

    private void ensureKeyOnlyCounts()
    {
        if (keyOnlyCounts != null) {
            return;
        }
        keyOnlyCounts = arrayPool.borrowInts(tags.length);
        Arrays.fill(keyOnlyCounts, 0);
    }

    // Distinct keys only (rehash): returns the first empty slot in the key's probe sequence.
    private int findEmpty(long hash)
    {
        int group = ((int) hash) & mask & ~(GROUP - 1);
        while (true) {
            int emptyLane = firstEmptyTag(ByteVector.fromArray(SPECIES, tags, group));
            if (emptyLane < GROUP) {
                return group + emptyLane;
            }
            group = (group + GROUP) & mask;
        }
    }

    private long matchingTagBits(jdk.incubator.vector.VectorMask<Byte> matches)
    {
        if (policy.guardPairTagMaskConversion() && !matches.anyTrue()) {
            return 0;
        }
        return matches.toLong();
    }

    private boolean hasEmptyTag(ByteVector tags)
    {
        var empty = tags.compare(VectorOperators.EQ, (byte) 0);
        return policy.guardPairTagMaskConversion() ? empty.anyTrue() : empty.toLong() != 0;
    }

    private int firstEmptyTag(ByteVector tags)
    {
        var empty = tags.compare(VectorOperators.EQ, (byte) 0);
        return policy.guardPairTagMaskConversion() ? empty.firstTrue() : Long.numberOfTrailingZeros(empty.toLong());
    }

    static long hash64(long first, long second)
    {
        // Fibonacci-prime combine + Murmur3 64-bit finalizer; the low bits index the bucket, the top byte is
        // the tag. TPC-DS surrogate keys have zero upper 32 bits and collide heavily under a naive combine.
        long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= hash >>> 33;
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;
        return hash;
    }
}
