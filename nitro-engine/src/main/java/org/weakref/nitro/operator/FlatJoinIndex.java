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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

final class FlatJoinIndex
        extends JoinIndex
{
    private static final long NO_MATCH_ROW_REFERENCE = -1L;

    private final HashJoinIndexPolicy policy;
    private final PrimitiveArrayPool arrayPool;
    private final FlatKeyLayout layout;
    private final FlatGroupingTable table;
    private final FlatJoinEncodedProbeCache encodedProbeCache;
    private final boolean batchBindingRequired;
    private final boolean primitiveSingleRows;
    private long[] singleRows;
    private LongArrayList[] duplicateRows;
    private LongArrayList[] legacyRowsByGroup;
    private final SingleLongList singleMatch = new SingleLongList();
    private boolean hasDuplicates;
    private long nextGroupId;
    private boolean debugProbeShapePrinted;
    private final boolean ownsStorage;

    FlatJoinIndex(HashJoinIndexPolicy policy, FlatKeyLayout layout, int expectedSize)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.arrayPool = layout.primitiveArrays();
        this.layout = layout;
        this.encodedProbeCache = new FlatJoinEncodedProbeCache(arrayPool, policy);
        this.batchBindingRequired = layout.requiresBatchBinding();
        int initialSize = Math.max(16, expectedSize);
        this.primitiveSingleRows = policy.flatPrimitiveSingleRows();
        this.ownsStorage = true;
        this.table = new FlatGroupingTable(
                layout,
                primitiveSingleRows ? initialSize : policy.flatLegacyInitialCapacity(),
                true);
        if (primitiveSingleRows) {
            this.singleRows = arrayPool.borrowLongs(initialSize);
        }
        else {
            this.legacyRowsByGroup = new LongArrayList[16];
        }
    }

    private FlatJoinIndex(FlatJoinIndex prepared)
    {
        this.policy = prepared.policy;
        this.arrayPool = prepared.arrayPool;
        this.layout = prepared.layout;
        this.table = prepared.table;
        this.encodedProbeCache = new FlatJoinEncodedProbeCache(arrayPool, policy);
        this.batchBindingRequired = prepared.batchBindingRequired;
        this.primitiveSingleRows = prepared.primitiveSingleRows;
        this.singleRows = prepared.singleRows;
        this.duplicateRows = prepared.duplicateRows;
        this.legacyRowsByGroup = prepared.legacyRowsByGroup;
        this.hasDuplicates = prepared.hasDuplicates;
        this.nextGroupId = prepared.nextGroupId;
        this.ownsStorage = false;
    }

    FlatJoinIndex newProbeView()
    {
        return new FlatJoinIndex(this);
    }

    @Override
    public boolean isEmpty()
    {
        return nextGroupId == 0;
    }

    @Override
    String probeKind()
    {
        return "flat";
    }

    @Override
    public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
    {
        if (!batchBindingRequired) {
            addBound(values, nulls, position, rowReference, true);
            return;
        }
        table.beginBatch(values, nulls);
        try {
            addBound(values, nulls, position, rowReference, true);
        }
        finally {
            table.endBatch();
        }
    }

    private void addBound(Vector[] values, Vector[] nulls, int position, long rowReference, boolean checkNulls)
    {
        if (checkNulls && keyHasNull(nulls, position, true)) {
            return;
        }
        long newGroupId = nextGroupId;
        long groupId = table.assignGroup(values, position, newGroupId);
        if (!primitiveSingleRows) {
            if (groupId == newGroupId) {
                ensureLegacyGroupCapacity((int) groupId);
                nextGroupId++;
            }
            legacyRowsByGroup[(int) groupId].add(rowReference);
            return;
        }
        if (groupId == newGroupId) {
            ensureGroupCapacity((int) groupId);
            singleRows[(int) groupId] = rowReference;
            nextGroupId++;
            return;
        }
        hasDuplicates = true;
        ensureDuplicateRows();
        LongArrayList rows = duplicateRows[(int) groupId];
        if (rows == null) {
            rows = new LongArrayList();
            rows.add(singleRows[(int) groupId]);
            duplicateRows[(int) groupId] = rows;
        }
        rows.add(rowReference);
    }

    @Override
    public LongList matches(Vector[] values, Vector[] nulls, int position)
    {
        if (!batchBindingRequired) {
            return matchesBound(values, nulls, position, true);
        }
        table.beginBatch(values, nulls);
        try {
            return matchesBound(values, nulls, position, true);
        }
        finally {
            table.endBatch();
        }
    }

    private LongList matchesBound(Vector[] values, Vector[] nulls, int position, boolean checkNulls)
    {
        if (checkNulls && keyHasNull(nulls, position, true)) {
            return LongLists.emptyList();
        }
        long groupId = table.findGroup(values, position);
        if (groupId < 0 || groupId >= nextGroupId) {
            return LongLists.emptyList();
        }
        if (!primitiveSingleRows) {
            return legacyRowsByGroup[(int) groupId];
        }
        LongArrayList rows = duplicateRows == null ? null : duplicateRows[(int) groupId];
        return rows == null ? singleMatch.withValue(singleRows[(int) groupId]) : rows;
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
        table.beginBatch(values, nulls);
        try {
            int endPosition = startPosition + length;
            int[] sourcePositions = batch.positions();
            for (int logicalPosition = startPosition; logicalPosition < endPosition; logicalPosition++) {
                int sourcePosition = sourcePositions == null ? logicalPosition : sourcePositions[logicalPosition];
                long rowReference = JoinRowReference.pack(batchIndex, logicalPosition);
                addBound(values, nulls, sourcePosition, rowReference, hasNulls || batchBindingRequired);
            }
        }
        finally {
            table.endBatch();
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
        table.beginBatch(values, nulls);
        try {
            table.prepareBatchHashes(values, nulls, mask);
            int count = mask.count();
            for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
                int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
                long rowReference = JoinRowReference.pack(batchIndex, logicalPosition);
                addBound(values, nulls, sourcePosition, rowReference, hasNulls || batchBindingRequired);
            }
        }
        finally {
            table.endBatch();
        }
        return true;
    }

    private void ensureGroupCapacity(int groupId)
    {
        if (groupId < singleRows.length) {
            return;
        }
        int newLength = Math.max(groupId + 1, singleRows.length * 2);
        long[] previous = singleRows;
        singleRows = arrayPool.borrowLongs(newLength);
        System.arraycopy(previous, 0, singleRows, 0, previous.length);
        arrayPool.release(previous);
        if (duplicateRows != null) {
            duplicateRows = Arrays.copyOf(duplicateRows, newLength);
        }
    }

    private void ensureDuplicateRows()
    {
        if (duplicateRows == null) {
            duplicateRows = new LongArrayList[singleRows.length];
        }
    }

    private void ensureLegacyGroupCapacity(int groupId)
    {
        if (groupId >= legacyRowsByGroup.length) {
            legacyRowsByGroup = Arrays.copyOf(legacyRowsByGroup, Math.max(groupId + 1, legacyRowsByGroup.length * 2));
        }
        legacyRowsByGroup[groupId] = new LongArrayList();
    }

    @Override
    public void matchRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            LongList[] matches,
            SingleLongList[] singleMatches)
    {
        // Prepared build storage is shared by every probe driver. FlatGroupingTable also carries batch-local
        // accessors and hash scratch, so one probe must not replace that state while another is using it.
        synchronized (table) {
            matchRowsLocked(values, nulls, hasNulls, positions, positionCount, matches, singleMatches);
        }
    }

    private void matchRowsLocked(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            LongList[] matches,
            SingleLongList[] singleMatches)
    {
        FlatJoinEncodedProbeCache.Prepared encodedProbe = encodedProbeCache.prepare(table, values, nulls, positionCount, nextGroupId);
        int[] encodedGroups = encodedProbe == null ? null : encodedProbe.groups();
        int[] encodedIds = encodedProbe == null ? null : encodedProbe.rowIds();
        if (encodedProbe == null) {
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, positions, positionCount);
        }
        try {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (encodedProbe == null && keyHasNull(nulls, position, hasNulls)) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                long groupId = encodedProbe == null
                        ? table.findGroup(values, position)
                        : encodedGroups[encodedIds == null ? 0 : encodedIds[position]];
                if (groupId < 0 || groupId >= nextGroupId) {
                    matches[index] = LongLists.emptyList();
                    continue;
                }
                if (!primitiveSingleRows) {
                    matches[index] = legacyRowsByGroup[(int) groupId];
                    continue;
                }
                LongArrayList rows = duplicateRows == null ? null : duplicateRows[(int) groupId];
                matches[index] = rows == null
                        ? singleMatches[index].withValue(singleRows[(int) groupId])
                        : rows;
            }
        }
        finally {
            if (encodedProbe == null) {
                table.endBatch();
            }
        }
    }

    @Override
    public boolean supportsSingleMatchRefs()
    {
        return primitiveSingleRows && !hasDuplicates;
    }

    @Override
    public void matchSingleRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            long[] refs)
    {
        synchronized (table) {
            matchSingleRowsLocked(values, nulls, hasNulls, positions, positionCount, refs);
        }
    }

    private void matchSingleRowsLocked(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            long[] refs)
    {
        if (policy.debugJoinIndex() && !debugProbeShapePrinted) {
            debugProbeShapePrinted = true;
            System.err.printf("[flat-join-probe] groups=%d rows=%d fields=%d shape=%s%n",
                    nextGroupId,
                    positionCount,
                    values.length,
                    Arrays.stream(values).map(FlatJoinIndex::probeShape).toList());
        }
        FlatJoinEncodedProbeCache.Prepared encodedProbe = encodedProbeCache.prepare(table, values, nulls, positionCount, nextGroupId);
        int[] encodedGroups = encodedProbe == null ? null : encodedProbe.groups();
        int[] encodedIds = encodedProbe == null ? null : encodedProbe.rowIds();
        if (encodedProbe == null) {
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, positions, positionCount);
        }
        try {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (encodedProbe == null && keyHasNull(nulls, position, hasNulls)) {
                    refs[index] = NO_MATCH_ROW_REFERENCE;
                    continue;
                }
                if (encodedProbe != null) {
                    int groupId = encodedGroups[encodedIds == null ? 0 : encodedIds[position]];
                    refs[index] = groupId < 0 || groupId >= nextGroupId
                            ? NO_MATCH_ROW_REFERENCE
                            : singleRows[groupId];
                }
                else {
                    long groupId = table.findGroup(values, position);
                    refs[index] = groupId < 0 || groupId >= nextGroupId
                            ? NO_MATCH_ROW_REFERENCE
                            : singleRows[(int) groupId];
                }
            }
        }
        finally {
            if (encodedProbe == null) {
                table.endBatch();
            }
        }
    }

    private static String probeShape(Vector value)
    {
        if (value instanceof DictionaryVector dictionary) {
            Vector base = dictionary.baseValues();
            return "DictionaryVector(" + dictionary.length() + ",depth=" + dictionary.dictionaryDepth() +
                    ",base=" + base.getClass().getSimpleName() + '(' + base.length() +
                    ",generation=" + base.contentGeneration() + "))";
        }
        return value.getClass().getSimpleName() + '(' + value.length() + ",generation=" + value.contentGeneration() + ')';
    }

    private boolean keyHasNull(Vector[] nulls, int position, boolean hasTopLevelNulls)
    {
        if (batchBindingRequired) {
            return layout.inputHasAnyNull(position);
        }
        return hasTopLevelNulls && JoinIndex.hasNull(nulls, position);
    }

    @Override
    long retainedBytes()
    {
        long bytes = encodedProbeCache.retainedBytes();
        if (!ownsStorage) {
            return bytes;
        }
        bytes += table.retainedBytes();
        bytes += singleRows == null ? 0 : (long) singleRows.length * Long.BYTES;
        bytes += listArrayBytes(duplicateRows);
        bytes += listArrayBytes(legacyRowsByGroup);
        return bytes;
    }

    private static long listArrayBytes(LongArrayList[] rowsByGroup)
    {
        if (rowsByGroup == null) {
            return 0;
        }
        long bytes = (long) rowsByGroup.length * Long.BYTES;
        for (LongArrayList rows : rowsByGroup) {
            if (rows != null) {
                bytes += (long) rows.elements().length * Long.BYTES;
            }
        }
        return bytes;
    }

    @Override
    public void releaseBuffers()
    {
        if (!ownsStorage) {
            releaseProbeBuffers();
            return;
        }
        table.releaseBuffers();
        if (singleRows != null) {
            arrayPool.release(singleRows);
        }
        encodedProbeCache.release();
        singleRows = null;
        duplicateRows = null;
        legacyRowsByGroup = null;
    }

    @Override
    void releaseProbeBuffers()
    {
        encodedProbeCache.release();
    }
}
