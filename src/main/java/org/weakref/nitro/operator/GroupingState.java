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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

final class GroupingState
{
    private final Object2LongMap<OperatorKeySemantics.Key> groups = new Object2LongOpenHashMap<>();
    private final Long2LongMap longGroups = new Long2LongOpenHashMap();
    private final ArrayList<ArrayList<OperatorKeySemantics.Key>> keysByGroupColumns = new ArrayList<>();
    private OperatorKeySemantics.Key[] reusableProbeKeys;
    private OperatorKeySemantics.CompositeProbeKey reusableCompositeProbeKey;
    private FlatGroupingTable flatGroupingTable;
    private FlatTypeHandler[] keyHandlers;
    private Set<BinaryVector.Trait>[] binaryTraits;
    private long[] longKeysByGroup = new long[0];
    private long[] firstLongPairKeysByGroup = new long[0];
    private long[] secondLongPairKeysByGroup = new long[0];
    private byte[] longPairNullMasksByGroup = new byte[0];
    private long[] firstLongTripleKeysByGroup = new long[0];
    private long[] secondLongTripleKeysByGroup = new long[0];
    private long[] thirdLongTripleKeysByGroup = new long[0];
    private byte[] longTripleNullMasksByGroup = new byte[0];
    private long[] firstLongQuadKeysByGroup = new long[0];
    private long[] secondLongQuadKeysByGroup = new long[0];
    private long[] thirdLongQuadKeysByGroup = new long[0];
    private long[] fourthLongQuadKeysByGroup = new long[0];
    private byte[] longQuadNullMasksByGroup = new byte[0];
    private Vector cachedDictionaryValues;
    private long[] dictionaryGroupsById = new long[0];
    private int[] dictionaryGenerations = new int[0];
    private int dictionaryGeneration;
    private long nextGroupId;
    private long nullGroup = -1;
    private boolean useLongGrouping;
    private boolean useLongPairGrouping;
    private boolean useLongTripleGrouping;
    private boolean useLongQuadGrouping;
    private boolean useFlatGrouping;
    private boolean initialized;
    private LongPairGroupingTable longPairGroupingTable;
    private LongTripleGroupingTable longTripleGroupingTable;
    private LongQuadGroupingTable longQuadGroupingTable;

    GroupingState()
    {
        groups.defaultReturnValue(-1);
        longGroups.defaultReturnValue(-1);
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    public void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result)
    {
        assignGroups(new Vector[] {values}, new Vector[] {nulls}, mask, result);
    }

    public boolean contains(Vector values, Vector nulls, int position)
    {
        initializeIfNecessary(new Vector[] {values}, new Vector[] {nulls});
        if (OperatorVectorSupport.isNull(nulls, position)) {
            return false;
        }
        if (useLongGrouping) {
            return longGroups.containsKey(OperatorVectorSupport.longValue(values, position));
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
        if (useLongPairGrouping) {
            assignLongPairGroups(values, nulls, mask, result);
            return;
        }
        if (useLongTripleGrouping) {
            assignLongTripleGroups(values, nulls, mask, result);
            return;
        }
        if (useLongQuadGrouping) {
            assignLongQuadGroups(values, nulls, mask, result);
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
        if (useLongPairGrouping) {
            longPairGroupingTable.ensureCapacity(expectedSize);
        }
        else if (useLongTripleGrouping) {
            longTripleGroupingTable.ensureCapacity(expectedSize);
        }
        else if (useLongQuadGrouping) {
            longQuadGroupingTable.ensureCapacity(expectedSize);
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
            return;
        }
        boolean nullableCompositeKeys = values.length > 1 && hasNullableKeys(nulls);
        if (values.length == 2 && isSingleLongGroupingCandidate(values[0]) && isSingleLongGroupingCandidate(values[1])) {
            useLongPairGrouping = true;
            longPairGroupingTable = new LongPairGroupingTable(Math.max(16, values[0].length()));
            return;
        }
        if (values.length == 3 &&
                isSingleLongGroupingCandidate(values[0]) &&
                isSingleLongGroupingCandidate(values[1]) &&
                isSingleLongGroupingCandidate(values[2])) {
            useLongTripleGrouping = true;
            longTripleGroupingTable = new LongTripleGroupingTable(Math.max(16, values[0].length()));
            return;
        }
        if (values.length == 4 &&
                isSingleLongGroupingCandidate(values[0]) &&
                isSingleLongGroupingCandidate(values[1]) &&
                isSingleLongGroupingCandidate(values[2]) &&
                isSingleLongGroupingCandidate(values[3])) {
            useLongQuadGrouping = true;
            longQuadGroupingTable = new LongQuadGroupingTable(Math.max(16, values[0].length()));
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

    private void assignLongGroups(Vector values, Vector nullVector, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            if (OperatorVectorSupport.isNull(nullVector, position)) {
                result.values()[position] = nullGroup();
                continue;
            }

            long key = OperatorVectorSupport.longValue(values, position);
            long groupId = longGroups.get(key);
            if (groupId == -1) {
                groupId = nextGroupId++;
                longGroups.put(key, groupId);
                ensureLongGroupingCapacity(groupId);
                longKeysByGroup[(int) groupId] = key;
            }
            result.values()[position] = groupId;
        }
    }

    private void assignLongPairGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            byte nullMask = nullMask(nulls, position);
            long first = isNull(nulls[0], position) ? 0 : OperatorVectorSupport.longValue(values[0], position);
            long second = isNull(nulls[1], position) ? 0 : OperatorVectorSupport.longValue(values[1], position);
            long groupId = longPairGroupingTable.assignGroup(first, second, nullMask, nextGroupId);
            if (groupId == nextGroupId) {
                ensureLongPairGroupingCapacity(groupId);
                firstLongPairKeysByGroup[(int) groupId] = first;
                secondLongPairKeysByGroup[(int) groupId] = second;
                longPairNullMasksByGroup[(int) groupId] = nullMask;
                nextGroupId++;
            }
            result.values()[position] = groupId;
        }
    }

    private void assignLongQuadGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            byte nullMask = nullMask(nulls, position);
            long first = isNull(nulls[0], position) ? 0 : OperatorVectorSupport.longValue(values[0], position);
            long second = isNull(nulls[1], position) ? 0 : OperatorVectorSupport.longValue(values[1], position);
            long third = isNull(nulls[2], position) ? 0 : OperatorVectorSupport.longValue(values[2], position);
            long fourth = isNull(nulls[3], position) ? 0 : OperatorVectorSupport.longValue(values[3], position);
            long groupId = longQuadGroupingTable.assignGroup(first, second, third, fourth, nullMask, nextGroupId);
            if (groupId == nextGroupId) {
                ensureLongQuadGroupingCapacity(groupId);
                firstLongQuadKeysByGroup[(int) groupId] = first;
                secondLongQuadKeysByGroup[(int) groupId] = second;
                thirdLongQuadKeysByGroup[(int) groupId] = third;
                fourthLongQuadKeysByGroup[(int) groupId] = fourth;
                longQuadNullMasksByGroup[(int) groupId] = nullMask;
                nextGroupId++;
            }
            result.values()[position] = groupId;
        }
    }

    private void assignLongTripleGroups(Vector[] values, Vector[] nulls, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            byte nullMask = nullMask(nulls, position);
            long first = isNull(nulls[0], position) ? 0 : OperatorVectorSupport.longValue(values[0], position);
            long second = isNull(nulls[1], position) ? 0 : OperatorVectorSupport.longValue(values[1], position);
            long third = isNull(nulls[2], position) ? 0 : OperatorVectorSupport.longValue(values[2], position);
            long groupId = longTripleGroupingTable.assignGroup(first, second, third, nullMask, nextGroupId);
            if (groupId == nextGroupId) {
                ensureLongTripleGroupingCapacity(groupId);
                firstLongTripleKeysByGroup[(int) groupId] = first;
                secondLongTripleKeysByGroup[(int) groupId] = second;
                thirdLongTripleKeysByGroup[(int) groupId] = third;
                longTripleNullMasksByGroup[(int) groupId] = nullMask;
                nextGroupId++;
            }
            result.values()[position] = groupId;
        }
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
        if (useLongPairGrouping) {
            return Streams.ofValuesAndNulls(
                    materializeLongPairGroupedValues(groupedColumnIndex, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeCompositeLongNulls(longPairNullMasksByGroup, groupedColumnIndex, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        }
        if (useLongTripleGrouping) {
            return Streams.ofValuesAndNulls(
                    materializeLongTripleGroupedValues(groupedColumnIndex, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeCompositeLongNulls(longTripleNullMasksByGroup, groupedColumnIndex, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        }
        if (useLongQuadGrouping) {
            return Streams.ofValuesAndNulls(
                    materializeLongQuadGroupedValues(groupedColumnIndex, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeCompositeLongNulls(longQuadNullMasksByGroup, groupedColumnIndex, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
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
        BooleanVector result = allocator.allocateOrGrow(allocationContext, (BooleanVector) output, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            result.values()[index] = index == nullGroup;
        }
        return result;
    }

    private BooleanVector materializeCompositeLongNulls(byte[] nullMasksByGroup, int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        BooleanVector result = allocator.allocateOrGrow(allocationContext, (BooleanVector) output, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            result.values()[index] = index < nullMasksByGroup.length && isNull(nullMasksByGroup, index, groupedColumnIndex);
        }
        return result;
    }

    private I64Vector materializeLongPairGroupedValues(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        long[] keysByGroup = groupedColumnIndex == 0 ? firstLongPairKeysByGroup : secondLongPairKeysByGroup;
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index < keysByGroup.length && !isNull(longPairNullMasksByGroup, index, groupedColumnIndex)) {
                result.values()[index] = keysByGroup[index];
            }
        }
        return result;
    }

    private I64Vector materializeLongQuadGroupedValues(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        long[] keysByGroup = switch (groupedColumnIndex) {
            case 0 -> firstLongQuadKeysByGroup;
            case 1 -> secondLongQuadKeysByGroup;
            case 2 -> thirdLongQuadKeysByGroup;
            case 3 -> fourthLongQuadKeysByGroup;
            default -> throw new IllegalArgumentException("Invalid grouped column index: " + groupedColumnIndex);
        };
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index < keysByGroup.length && !isNull(longQuadNullMasksByGroup, index, groupedColumnIndex)) {
                result.values()[index] = keysByGroup[index];
            }
        }
        return result;
    }

    private I64Vector materializeLongTripleGroupedValues(int groupedColumnIndex, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        long[] keysByGroup = switch (groupedColumnIndex) {
            case 0 -> firstLongTripleKeysByGroup;
            case 1 -> secondLongTripleKeysByGroup;
            case 2 -> thirdLongTripleKeysByGroup;
            default -> throw new IllegalArgumentException("Invalid grouped column index: " + groupedColumnIndex);
        };
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index < keysByGroup.length && !isNull(longTripleNullMasksByGroup, index, groupedColumnIndex)) {
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
            if (!useFlatGrouping && !useLongGrouping && !useLongPairGrouping && !useLongTripleGrouping && !useLongQuadGrouping) {
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
        BooleanVector result = allocator.allocateOrGrow(allocationContext, (BooleanVector) output, BooleanVector.class, size, BooleanVector::new);
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

    private void ensureLongPairGroupingCapacity(long groupId)
    {
        if (groupId < firstLongPairKeysByGroup.length) {
            return;
        }
        int newSize = Math.max(16, firstLongPairKeysByGroup.length);
        while (groupId >= newSize) {
            newSize *= 2;
        }
        firstLongPairKeysByGroup = Arrays.copyOf(firstLongPairKeysByGroup, newSize);
        secondLongPairKeysByGroup = Arrays.copyOf(secondLongPairKeysByGroup, newSize);
        longPairNullMasksByGroup = Arrays.copyOf(longPairNullMasksByGroup, newSize);
    }

    private void ensureLongQuadGroupingCapacity(long groupId)
    {
        if (groupId < firstLongQuadKeysByGroup.length) {
            return;
        }
        int newSize = Math.max(16, firstLongQuadKeysByGroup.length);
        while (groupId >= newSize) {
            newSize *= 2;
        }
        firstLongQuadKeysByGroup = Arrays.copyOf(firstLongQuadKeysByGroup, newSize);
        secondLongQuadKeysByGroup = Arrays.copyOf(secondLongQuadKeysByGroup, newSize);
        thirdLongQuadKeysByGroup = Arrays.copyOf(thirdLongQuadKeysByGroup, newSize);
        fourthLongQuadKeysByGroup = Arrays.copyOf(fourthLongQuadKeysByGroup, newSize);
        longQuadNullMasksByGroup = Arrays.copyOf(longQuadNullMasksByGroup, newSize);
    }

    private void ensureLongTripleGroupingCapacity(long groupId)
    {
        if (groupId < firstLongTripleKeysByGroup.length) {
            return;
        }
        int newSize = Math.max(16, firstLongTripleKeysByGroup.length);
        while (groupId >= newSize) {
            newSize *= 2;
        }
        firstLongTripleKeysByGroup = Arrays.copyOf(firstLongTripleKeysByGroup, newSize);
        secondLongTripleKeysByGroup = Arrays.copyOf(secondLongTripleKeysByGroup, newSize);
        thirdLongTripleKeysByGroup = Arrays.copyOf(thirdLongTripleKeysByGroup, newSize);
        longTripleNullMasksByGroup = Arrays.copyOf(longTripleNullMasksByGroup, newSize);
    }

    private static final class LongPairGroupingTable
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private byte[] nullMasks;
        private long[] groupIds;
        private int mask;
        private int maxFill;
        private int size;

        private LongPairGroupingTable(int expectedSize)
        {
            int capacity = 16;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            nullMasks = new byte[capacity];
            groupIds = new long[capacity];
            Arrays.fill(groupIds, -1);
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        public long assignGroup(long first, long second, byte nullMask, long newGroupId)
        {
            int index = mix(first, second, nullMask) & mask;
            while (true) {
                long groupId = groupIds[index];
                if (groupId == -1) {
                    firstKeys[index] = first;
                    secondKeys[index] = second;
                    nullMasks[index] = nullMask;
                    groupIds[index] = newGroupId;
                    size++;
                    if (size >= maxFill) {
                        rehash();
                    }
                    return newGroupId;
                }
                if (firstKeys[index] == first && secondKeys[index] == second && nullMasks[index] == nullMask) {
                    return groupId;
                }
                index = (index + 1) & mask;
            }
        }

        public void ensureCapacity(long expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = groupIds.length;
            while (expectedSize >= (long) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash()
        {
            rehash(groupIds.length * 2);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            byte[] previousNullMasks = nullMasks;
            long[] previousGroupIds = groupIds;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            nullMasks = new byte[capacity];
            groupIds = new long[capacity];
            Arrays.fill(groupIds, -1);
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousGroupIds.length; index++) {
                long groupId = previousGroupIds[index];
                if (groupId == -1) {
                    continue;
                }

                int newIndex = mix(previousFirstKeys[index], previousSecondKeys[index], previousNullMasks[index]) & mask;
                while (groupIds[newIndex] != -1) {
                    newIndex = (newIndex + 1) & mask;
                }
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                nullMasks[newIndex] = previousNullMasks[index];
                groupIds[newIndex] = groupId;
                size++;
            }
        }

        private static int mix(long first, long second, byte nullMask)
        {
            long hash = 31 * (31 * Long.hashCode(first) + Long.hashCode(second)) + nullMask;
            hash ^= (hash >>> 16);
            return (int) hash;
        }
    }

    private static final class LongQuadGroupingTable
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] thirdKeys;
        private long[] fourthKeys;
        private byte[] nullMasks;
        private long[] groupIds;
        private int mask;
        private int maxFill;
        private int size;

        private LongQuadGroupingTable(int expectedSize)
        {
            int capacity = 16;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            fourthKeys = new long[capacity];
            nullMasks = new byte[capacity];
            groupIds = new long[capacity];
            Arrays.fill(groupIds, -1);
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        public long assignGroup(long first, long second, long third, long fourth, byte nullMask, long newGroupId)
        {
            int index = mix(first, second, third, fourth, nullMask) & mask;
            while (true) {
                long groupId = groupIds[index];
                if (groupId == -1) {
                    firstKeys[index] = first;
                    secondKeys[index] = second;
                    thirdKeys[index] = third;
                    fourthKeys[index] = fourth;
                    nullMasks[index] = nullMask;
                    groupIds[index] = newGroupId;
                    size++;
                    if (size >= maxFill) {
                        rehash();
                    }
                    return newGroupId;
                }
                if (firstKeys[index] == first && secondKeys[index] == second && thirdKeys[index] == third && fourthKeys[index] == fourth && nullMasks[index] == nullMask) {
                    return groupId;
                }
                index = (index + 1) & mask;
            }
        }

        public void ensureCapacity(long expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = groupIds.length;
            while (expectedSize >= (long) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash()
        {
            rehash(groupIds.length * 2);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousThirdKeys = thirdKeys;
            long[] previousFourthKeys = fourthKeys;
            byte[] previousNullMasks = nullMasks;
            long[] previousGroupIds = groupIds;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            fourthKeys = new long[capacity];
            nullMasks = new byte[capacity];
            groupIds = new long[capacity];
            Arrays.fill(groupIds, -1);
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousGroupIds.length; index++) {
                long groupId = previousGroupIds[index];
                if (groupId == -1) {
                    continue;
                }

                int newIndex = mix(previousFirstKeys[index], previousSecondKeys[index], previousThirdKeys[index], previousFourthKeys[index], previousNullMasks[index]) & mask;
                while (groupIds[newIndex] != -1) {
                    newIndex = (newIndex + 1) & mask;
                }
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                thirdKeys[newIndex] = previousThirdKeys[index];
                fourthKeys[newIndex] = previousFourthKeys[index];
                nullMasks[newIndex] = previousNullMasks[index];
                groupIds[newIndex] = groupId;
                size++;
            }
        }

        private static int mix(long first, long second, long third, long fourth, byte nullMask)
        {
            long hash = 31L * (31L * (31L * (31L * Long.hashCode(first) + Long.hashCode(second)) + Long.hashCode(third)) + Long.hashCode(fourth)) + nullMask;
            hash ^= (hash >>> 16);
            return (int) hash;
        }
    }

    private static final class LongTripleGroupingTable
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] thirdKeys;
        private byte[] nullMasks;
        private long[] groupIds;
        private int mask;
        private int maxFill;
        private int size;

        private LongTripleGroupingTable(int expectedSize)
        {
            int capacity = 16;
            while (capacity < expectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            nullMasks = new byte[capacity];
            groupIds = new long[capacity];
            Arrays.fill(groupIds, -1);
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        public long assignGroup(long first, long second, long third, byte nullMask, long newGroupId)
        {
            int index = mix(first, second, third, nullMask) & mask;
            while (true) {
                long groupId = groupIds[index];
                if (groupId == -1) {
                    firstKeys[index] = first;
                    secondKeys[index] = second;
                    thirdKeys[index] = third;
                    nullMasks[index] = nullMask;
                    groupIds[index] = newGroupId;
                    size++;
                    if (size >= maxFill) {
                        rehash();
                    }
                    return newGroupId;
                }
                if (firstKeys[index] == first && secondKeys[index] == second && thirdKeys[index] == third && nullMasks[index] == nullMask) {
                    return groupId;
                }
                index = (index + 1) & mask;
            }
        }

        public void ensureCapacity(long expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = groupIds.length;
            while (expectedSize >= (long) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash()
        {
            rehash(groupIds.length * 2);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousThirdKeys = thirdKeys;
            byte[] previousNullMasks = nullMasks;
            long[] previousGroupIds = groupIds;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            nullMasks = new byte[capacity];
            groupIds = new long[capacity];
            Arrays.fill(groupIds, -1);
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousGroupIds.length; index++) {
                long groupId = previousGroupIds[index];
                if (groupId == -1) {
                    continue;
                }

                int newIndex = mix(previousFirstKeys[index], previousSecondKeys[index], previousThirdKeys[index], previousNullMasks[index]) & mask;
                while (groupIds[newIndex] != -1) {
                    newIndex = (newIndex + 1) & mask;
                }
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                thirdKeys[newIndex] = previousThirdKeys[index];
                nullMasks[newIndex] = previousNullMasks[index];
                groupIds[newIndex] = groupId;
                size++;
            }
        }

        private static int mix(long first, long second, long third, byte nullMask)
        {
            long hash = 31L * (31L * (31L * Long.hashCode(first) + Long.hashCode(second)) + Long.hashCode(third)) + nullMask;
            hash ^= (hash >>> 16);
            return (int) hash;
        }
    }
}
