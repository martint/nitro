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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.DictionaryVector;
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
    private final FlatGroupingTable table;
    private final boolean primitiveSingleRows;
    private long[] singleRows;
    private LongArrayList[] duplicateRows;
    private LongArrayList[] legacyRowsByGroup;
    private final SingleLongList singleMatch = new SingleLongList();
    private boolean hasDuplicates;
    private long nextGroupId;
    private boolean debugProbeShapePrinted;
    private Vector dictionaryProbeIdentity;
    private long dictionaryProbeGeneration = -1;
    private int[] dictionaryProbeGroups;
    private final Vector[] dictionaryProbeValues = new Vector[1];
    private boolean debugDictionaryProbeCachePrinted;

    FlatJoinIndex(HashJoinIndexPolicy policy, FlatKeyLayout layout, int expectedSize)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.arrayPool = layout.primitiveArrays();
        int initialSize = Math.max(16, expectedSize);
        this.primitiveSingleRows = policy.flatPrimitiveSingleRows();
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

    @Override
    public boolean isEmpty()
    {
        return nextGroupId == 0;
    }

    @Override
    public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
    {
        if (JoinIndex.hasNull(nulls, position)) {
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
        if (JoinIndex.hasNull(nulls, position)) {
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
        int[] dictionaryGroups = prepareDictionaryProbeCache(values, positionCount);
        DictionaryVector dictionary = dictionaryGroups == null ? null : (DictionaryVector) values[0];
        int dictionaryDepth = dictionary == null ? 0 : dictionary.dictionaryDepth();
        int[] dictionaryIds = dictionaryDepth == 1 ? dictionary.ids() : null;
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            if (hasNulls && JoinIndex.hasNull(nulls, position)) {
                matches[index] = LongLists.emptyList();
                continue;
            }
            long groupId = dictionaryGroups == null
                    ? table.findGroup(values, position)
                    : dictionaryGroups[dictionaryDepth == 1 ? dictionaryIds[position] : dictionary.basePosition(position, dictionaryDepth)];
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
        if (policy.debugJoinIndex() && !debugProbeShapePrinted) {
            debugProbeShapePrinted = true;
            System.err.printf("[flat-join-probe] groups=%d rows=%d fields=%d shape=%s%n",
                    nextGroupId,
                    positionCount,
                    values.length,
                    Arrays.stream(values).map(FlatJoinIndex::probeShape).toList());
        }
        int[] dictionaryGroups = prepareDictionaryProbeCache(values, positionCount);
        DictionaryVector dictionary = dictionaryGroups == null ? null : (DictionaryVector) values[0];
        int dictionaryDepth = dictionary == null ? 0 : dictionary.dictionaryDepth();
        int[] dictionaryIds = dictionaryDepth == 1 ? dictionary.ids() : null;
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            if (hasNulls && JoinIndex.hasNull(nulls, position)) {
                refs[index] = NO_MATCH_ROW_REFERENCE;
                continue;
            }
            if (dictionaryGroups != null) {
                int groupId = dictionaryGroups[dictionaryDepth == 1 ? dictionaryIds[position] : dictionary.basePosition(position, dictionaryDepth)];
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

    /**
     * Resolves a small encoded domain once, then maps probe rows through dictionary ids. The exact flat table
     * remains authoritative for each base entry's first lookup. Identity alone is insufficient because pooled
     * binary vectors are reused; a cache generation is valid only while both identity and content generation
     * match. Large or weakly reused dictionaries retain the ordinary row-at-a-time probe path.
     */
    private int[] prepareDictionaryProbeCache(Vector[] values, int positionCount)
    {
        if (!policy.flatDictionaryProbeCache() ||
                values.length != 1 ||
                !(values[0] instanceof DictionaryVector dictionary)) {
            return null;
        }
        Vector base = dictionary.baseValues();
        if (!(base instanceof BinaryVector)) {
            return null;
        }
        int cardinality = base.length();
        long generation = base.contentGeneration();
        if (generation < 0 ||
                cardinality == 0 ||
                cardinality > policy.flatDictionaryProbeCacheMaxCardinality() ||
                (long) cardinality * policy.flatDictionaryProbeCacheMinRowsPerEntry() > positionCount) {
            return null;
        }
        if (dictionaryProbeGroups == null || dictionaryProbeGroups.length < cardinality) {
            int[] previous = dictionaryProbeGroups;
            dictionaryProbeGroups = arrayPool.borrowInts(cardinality);
            arrayPool.release(previous);
            dictionaryProbeIdentity = null;
            dictionaryProbeGeneration = -1;
        }
        if (base != dictionaryProbeIdentity || generation != dictionaryProbeGeneration) {
            dictionaryProbeValues[0] = base;
            for (int dictionaryId = 0; dictionaryId < cardinality; dictionaryId++) {
                dictionaryProbeGroups[dictionaryId] = (int) table.findGroup(dictionaryProbeValues, dictionaryId);
            }
            dictionaryProbeIdentity = base;
            dictionaryProbeGeneration = generation;
        }
        if (policy.debugJoinIndex() && !debugDictionaryProbeCachePrinted) {
            debugDictionaryProbeCachePrinted = true;
            System.err.printf("[flat-join-dictionary-cache] groups=%d rows=%d cardinality=%d depth=%d%n",
                    nextGroupId,
                    positionCount,
                    cardinality,
                    dictionary.dictionaryDepth());
        }
        return dictionaryProbeGroups;
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

    @Override
    public void releaseBuffers()
    {
        table.releaseBuffers();
        if (singleRows != null) {
            arrayPool.release(singleRows);
        }
        if (dictionaryProbeGroups != null) {
            arrayPool.release(dictionaryProbeGroups);
        }
        singleRows = null;
        dictionaryProbeGroups = null;
        dictionaryProbeIdentity = null;
        duplicateRows = null;
        legacyRowsByGroup = null;
    }
}
