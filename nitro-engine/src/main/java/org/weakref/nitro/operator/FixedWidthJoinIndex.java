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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

/** Generated fixed-width build/probe with duplicate storage allocated only for keys that actually repeat. */
final class FixedWidthJoinIndex
        extends JoinIndex
{
    private static final long NO_MATCH_ROW_REFERENCE = -1;

    private final ResolvedFixedWidthKeyLayout layout;
    private final PrimitiveArrayPool arrayPool;
    private final AbstractMultiLongGroupingTable table;
    private final FixedWidthKeyBatchBindings bindings;
    private final boolean ownsStorage;
    private long[] firstReferencesByGroup;
    private LongArrayList[] duplicateReferencesByGroup;
    private long[] groupScratch;
    private final long[] singleKey;
    private final int[] singleLogicalPositions;
    private int rowCount;
    private boolean uniqueKeys = true;

    FixedWidthJoinIndex(
            ResolvedFixedWidthKeyLayout layout,
            int expectedSize,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            AdaptiveLongGroupingPolicy policy)
    {
        this.layout = layout;
        this.arrayPool = arrayPool;
        table = codeGeneration.multiLongGrouping().create(
                Arrays.stream(layout.lanes()).map(ResolvedFixedWidthKeyLayout.Lane::carrier).toList(),
                expectedSize,
                arrayPool,
                policy);
        bindings = new FixedWidthKeyBatchBindings(layout, arrayPool);
        firstReferencesByGroup = new long[Math.max(16, expectedSize)];
        Arrays.fill(firstReferencesByGroup, NO_MATCH_ROW_REFERENCE);
        duplicateReferencesByGroup = new LongArrayList[firstReferencesByGroup.length];
        singleKey = new long[layout.lanes().length];
        singleLogicalPositions = new int[layout.logicalKeyCount()];
        ownsStorage = true;
    }

    private FixedWidthJoinIndex(FixedWidthJoinIndex prepared)
    {
        layout = prepared.layout;
        arrayPool = prepared.arrayPool;
        table = prepared.table;
        bindings = new FixedWidthKeyBatchBindings(layout, arrayPool);
        firstReferencesByGroup = prepared.firstReferencesByGroup;
        duplicateReferencesByGroup = prepared.duplicateReferencesByGroup;
        singleKey = new long[layout.lanes().length];
        singleLogicalPositions = new int[layout.logicalKeyCount()];
        rowCount = prepared.rowCount;
        uniqueKeys = prepared.uniqueKeys;
        ownsStorage = false;
    }

    FixedWidthJoinIndex newProbeView()
    {
        return new FixedWidthJoinIndex(this);
    }

    @Override
    boolean isEmpty()
    {
        return rowCount == 0;
    }

    @Override
    void add(Vector[] values, Vector[] nulls, int position, long rowReference)
    {
        bindings.bind(values, nulls);
        try {
            Arrays.fill(singleLogicalPositions, position);
            byte nullMask = extractKey(singleLogicalPositions, singleKey);
            if (nullMask != 0) {
                return;
            }
            table.ensureCapacity(table.size + 1L);
            long group = table.assignKey(singleKey, (byte) 0, table.size);
            appendReference(toIntExact(group), rowReference);
        }
        finally {
            bindings.release();
        }
    }

    @Override
    LongList matches(Vector[] values, Vector[] nulls, int position)
    {
        bindings.bind(values, nulls);
        try {
            Arrays.fill(singleLogicalPositions, position);
            byte nullMask = extractKey(singleLogicalPositions, singleKey);
            if (nullMask != 0) {
                return LongLists.emptyList();
            }
            return matchesForGroup(table.findGroup(singleKey, (byte) 0));
        }
        finally {
            bindings.release();
        }
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
        int[] sourcePositions = arrayPool.borrowInts(length);
        int required = 0;
        for (int index = 0; index < length; index++) {
            int sourcePosition = batch.sourcePosition(startPosition + index);
            sourcePositions[index] = sourcePosition;
            required = Math.max(required, sourcePosition + 1);
        }
        try {
            assignBuildBatch(values, nulls, sourcePositions, length, required);
            for (int index = 0; index < length; index++) {
                long group = groupScratch[sourcePositions[index]];
                if (group != AbstractMultiLongGroupingTable.EMPTY_GROUP_ID) {
                    appendReference(
                            toIntExact(group),
                            JoinRowReference.pack(batchIndex, startPosition + index));
                }
            }
            return true;
        }
        finally {
            arrayPool.release(sourcePositions);
        }
    }

    @Override
    boolean addBuildRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            Mask mask,
            int batchIndex)
    {
        int[] positions = mask.all() ? null : mask.selectedPositions();
        assignBuildBatch(values, nulls, positions, mask.count(), mask.size());
        for (int logicalPosition = 0; logicalPosition < mask.count(); logicalPosition++) {
            int sourcePosition = mask.all() ? logicalPosition : positions[logicalPosition];
            long group = groupScratch[sourcePosition];
            if (group != AbstractMultiLongGroupingTable.EMPTY_GROUP_ID) {
                appendReference(toIntExact(group), JoinRowReference.pack(batchIndex, logicalPosition));
            }
        }
        return true;
    }

    private void assignBuildBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int requiredScratch)
    {
        bindings.bind(values, nulls);
        try {
            ensureGroupScratch(requiredScratch);
            table.ensureCapacity(table.size + (long) positionCount);
            table.assignPhysicalNonNullBatch(
                    bindings.keyArrays(),
                    bindings.keyMappings(),
                    bindings.keyMappingOffsets(),
                    bindings.keyBaseOffsets(),
                    bindings.nullArrays(),
                    bindings.nullMappings(),
                    bindings.nullMappingOffsets(),
                    bindings.nullBaseOffsets(),
                    positions,
                    positionCount,
                    groupScratch,
                    table.size);
        }
        finally {
            bindings.release();
        }
    }

    @Override
    void matchRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            LongList[] matches,
            SingleLongList[] singleMatches)
    {
        findGroups(values, nulls, positions, positionCount);
        for (int index = 0; index < positionCount; index++) {
            long group = groupScratch[positions[index]];
            if (group == AbstractMultiLongGroupingTable.EMPTY_GROUP_ID) {
                matches[index] = LongLists.emptyList();
                continue;
            }
            int groupId = toIntExact(group);
            LongArrayList duplicates = duplicateReferencesByGroup[groupId];
            matches[index] = duplicates == null
                    ? singleMatches[index].withValue(firstReferencesByGroup[groupId])
                    : duplicates;
        }
    }

    @Override
    boolean supportsSingleMatchRefs()
    {
        return uniqueKeys;
    }

    @Override
    void matchSingleRows(
            Vector[] values,
            Vector[] nulls,
            boolean hasNulls,
            int[] positions,
            int positionCount,
            long[] refs)
    {
        if (!uniqueKeys) {
            throw new IllegalStateException("Fixed-width join build is not unique");
        }
        findGroups(values, nulls, positions, positionCount);
        for (int index = 0; index < positionCount; index++) {
            long group = groupScratch[positions[index]];
            refs[index] = group == AbstractMultiLongGroupingTable.EMPTY_GROUP_ID
                    ? NO_MATCH_ROW_REFERENCE
                    : firstReferencesByGroup[toIntExact(group)];
        }
    }

    private void findGroups(Vector[] values, Vector[] nulls, int[] positions, int positionCount)
    {
        int required = 0;
        for (int index = 0; index < positionCount; index++) {
            required = Math.max(required, positions[index] + 1);
        }
        bindings.bind(values, nulls);
        try {
            ensureGroupScratch(required);
            table.findPhysicalBatch(
                    bindings.keyArrays(),
                    bindings.keyMappings(),
                    bindings.keyMappingOffsets(),
                    bindings.keyBaseOffsets(),
                    bindings.nullArrays(),
                    bindings.nullMappings(),
                    bindings.nullMappingOffsets(),
                    bindings.nullBaseOffsets(),
                    positions,
                    positionCount,
                    groupScratch);
        }
        finally {
            bindings.release();
        }
    }

    private void appendReference(int group, long rowReference)
    {
        ensureGroupCapacity(group + 1);
        long first = firstReferencesByGroup[group];
        if (first == NO_MATCH_ROW_REFERENCE) {
            firstReferencesByGroup[group] = rowReference;
        }
        else {
            uniqueKeys = false;
            LongArrayList duplicates = duplicateReferencesByGroup[group];
            if (duplicates == null) {
                duplicates = new LongArrayList(4);
                duplicates.add(first);
                duplicateReferencesByGroup[group] = duplicates;
            }
            duplicates.add(rowReference);
        }
        rowCount++;
    }

    private LongList matchesForGroup(long group)
    {
        if (group == AbstractMultiLongGroupingTable.EMPTY_GROUP_ID) {
            return LongLists.emptyList();
        }
        int groupId = toIntExact(group);
        LongArrayList duplicates = duplicateReferencesByGroup[groupId];
        return duplicates == null ? LongLists.singleton(firstReferencesByGroup[groupId]) : duplicates;
    }

    private void ensureGroupCapacity(int required)
    {
        if (firstReferencesByGroup.length >= required) {
            return;
        }
        int previousLength = firstReferencesByGroup.length;
        int capacity = Math.max(required, firstReferencesByGroup.length * 2);
        firstReferencesByGroup = Arrays.copyOf(firstReferencesByGroup, capacity);
        Arrays.fill(firstReferencesByGroup, previousLength, capacity, NO_MATCH_ROW_REFERENCE);
        duplicateReferencesByGroup = Arrays.copyOf(duplicateReferencesByGroup, capacity);
    }

    private void ensureGroupScratch(int required)
    {
        if (groupScratch != null && groupScratch.length >= required) {
            return;
        }
        arrayPool.release(groupScratch);
        groupScratch = arrayPool.borrowLongs(Math.max(16, required));
    }

    private byte extractKey(int[] logicalPositions, long[] result)
    {
        byte nullMask = 0;
        Object[] keyArrays = bindings.keyArrays();
        boolean[][] nullArrays = bindings.nullArrays();
        for (int lane = 0; lane < result.length; lane++) {
            int logicalPosition = logicalPositions[layout.lanes()[lane].logicalKey()];
            if (nullArrays[lane] != null && nullArrays[lane][physicalPosition(
                    bindings.nullMappings()[lane],
                    bindings.nullMappingOffsets()[lane],
                    bindings.nullBaseOffsets()[lane],
                    logicalPosition)]) {
                nullMask |= (byte) (1 << lane);
                result[lane] = 0;
                continue;
            }
            int physical = physicalPosition(
                    bindings.keyMappings()[lane],
                    bindings.keyMappingOffsets()[lane],
                    bindings.keyBaseOffsets()[lane],
                    logicalPosition);
            result[lane] = switch (layout.lanes()[lane].carrier()) {
                case I32 -> ((int[]) keyArrays[lane])[physical];
                case I64 -> ((long[]) keyArrays[lane])[physical];
                case F64 -> Double.doubleToRawLongBits(((double[]) keyArrays[lane])[physical]);
                case BOOLEAN -> ((boolean[]) keyArrays[lane])[physical] ? 1 : 0;
            };
        }
        return nullMask;
    }

    private static int physicalPosition(int[] mapping, int mappingOffset, int baseOffset, int logicalPosition)
    {
        int position = mapping == null ? logicalPosition : mapping[logicalPosition + mappingOffset];
        return position + baseOffset;
    }

    @Override
    String probeKind()
    {
        return "fixed-width";
    }

    @Override
    long retainedBytes()
    {
        if (!ownsStorage) {
            return groupScratch == null ? 0 : (long) groupScratch.length * Long.BYTES;
        }
        long bytes = table.retainedBytes() + (long) firstReferencesByGroup.length * Long.BYTES;
        bytes += (long) duplicateReferencesByGroup.length * Long.BYTES;
        for (LongArrayList duplicates : duplicateReferencesByGroup) {
            if (duplicates != null) {
                bytes += (long) duplicates.elements().length * Long.BYTES;
            }
        }
        if (groupScratch != null) {
            bytes += (long) groupScratch.length * Long.BYTES;
        }
        return bytes;
    }

    @Override
    void releaseProbeBuffers()
    {
        bindings.release();
        arrayPool.release(groupScratch);
        groupScratch = null;
    }

    @Override
    void releaseBuffers()
    {
        releaseProbeBuffers();
        if (ownsStorage) {
            table.releaseBuffers();
            Arrays.fill(duplicateReferencesByGroup, null);
            firstReferencesByGroup = new long[0];
            duplicateReferencesByGroup = new LongArrayList[0];
            rowCount = 0;
        }
    }
}
