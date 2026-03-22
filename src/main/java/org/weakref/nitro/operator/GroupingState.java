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
import org.weakref.nitro.data.F64Vector;
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
    private final ArrayList<ArrayList<OperatorKeySemantics.Key>> keysByGroupColumns = new ArrayList<>();
    private OperatorKeySemantics.Key[] reusableProbeKeys;
    private OperatorKeySemantics.CompositeProbeKey reusableCompositeProbeKey;
    private FlatGroupingTable flatGroupingTable;
    private GroupKind[] groupKinds;
    private Set<BinaryVector.Trait>[] binaryTraits;
    private Vector cachedDictionaryValues;
    private long[] dictionaryGroupsById = new long[0];
    private int[] dictionaryGenerations = new int[0];
    private int dictionaryGeneration;
    private long nextGroupId;
    private long nullGroup = -1;
    private boolean useFlatGrouping;
    private boolean initialized;

    GroupingState()
    {
        groups.defaultReturnValue(-1);
    }

    public void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result)
    {
        assignGroups(new Vector[] {values}, new BooleanVector[] {(BooleanVector) nulls}, mask, result);
    }

    @SuppressWarnings("unchecked")
    public void assignGroups(Vector[] values, BooleanVector[] nulls, Mask mask, I64Vector result)
    {
        initializeIfNecessary(values);
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
            boolean hasNull = false;
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, reusableProbeKeys[keyIndex]);
                if (key == null) {
                    hasNull = true;
                    break;
                }
                probeKeys[keyIndex] = key;
            }
            result.values()[position] = hasNull ? nullGroup() : groupForKeys(probeKeys);
        }
    }

    private void initializeIfNecessary(Vector[] values)
    {
        if (initialized) {
            return;
        }
        initialized = true;

        groupKinds = new GroupKind[values.length];
        binaryTraits = (Set<BinaryVector.Trait>[]) new Set<?>[values.length];
        for (int index = 0; index < values.length; index++) {
            groupKinds[index] = GroupKind.forVector(values[index]);
            binaryTraits[index] = OperatorVectorSupport.binaryTraits(values[index]);
        }

        FlatKeyLayout flatKeyLayout = FlatKeyLayout.tryCreate(values);
        if (flatKeyLayout != null) {
            useFlatGrouping = true;
            flatGroupingTable = new FlatGroupingTable(flatKeyLayout, 16);
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

    private void assignFlatGroups(Vector[] values, BooleanVector[] nulls, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            if (hasNull(nulls, position)) {
                result.values()[position] = nullGroup();
            }
            else {
                result.values()[position] = flatGroupingTable.assignGroup(values, position, () -> nextGroupId++);
            }
        }
    }

    private void assignDictionaryGroups(DictionaryVector dictionary, BooleanVector nullVector, Mask mask, I64Vector result)
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
        if (useFlatGrouping) {
            return flatGroupingTable.groupedValues(groupedColumnIndex, mask, nullGroup, output, allocator, allocationContext);
        }
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        List<OperatorKeySemantics.Key> keysByGroup = keysByGroupColumns.get(groupedColumnIndex);
        return switch (groupKinds[groupedColumnIndex]) {
            case LONG -> Streams.ofValuesAndNulls(
                    materializeLongValues(size, mask, keysByGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, keysByGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case BOOLEAN -> Streams.ofValuesAndNulls(
                    materializeBooleanValues(size, mask, keysByGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, keysByGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case DOUBLE -> Streams.ofValuesAndNulls(
                    materializeDoubleValues(size, mask, keysByGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, keysByGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case BINARY -> Streams.ofValuesAndNulls(
                    materializeBinaryValues(size, mask, groupedColumnIndex, keysByGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, keysByGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        };
    }

    private long groupForKeys(OperatorKeySemantics.Key[] probeKeys)
    {
        if (probeKeys.length == 1) {
            return groupForSingleKey(probeKeys[0]);
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
            if (!useFlatGrouping) {
                for (ArrayList<OperatorKeySemantics.Key> keysByGroup : keysByGroupColumns) {
                    keysByGroup.add(null);
                }
            }
            nullGroup = nextGroupId++;
        }
        return nullGroup;
    }

    private static boolean hasNull(BooleanVector[] nulls, int position)
    {
        for (BooleanVector nullVector : nulls) {
            if (OperatorVectorSupport.isNull(nullVector, position)) {
                return true;
            }
        }
        return false;
    }

    private I64Vector materializeLongValues(int size, Mask mask, List<OperatorKeySemantics.Key> keysByGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            OperatorKeySemantics.Key key = keysByGroup.get(index);
            if (key instanceof OperatorKeySemantics.LongKey value) {
                result.values()[index] = value.value();
            }
        }
        return result;
    }

    private BooleanVector materializeBooleanValues(int size, Mask mask, List<OperatorKeySemantics.Key> keysByGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector result = allocator.allocateOrGrow(allocationContext, (BooleanVector) output, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(result.values(), false);
        for (int index : mask) {
            OperatorKeySemantics.Key key = keysByGroup.get(index);
            if (key instanceof OperatorKeySemantics.BooleanKey value) {
                result.values()[index] = value.value();
            }
        }
        return result;
    }

    private F64Vector materializeDoubleValues(int size, Mask mask, List<OperatorKeySemantics.Key> keysByGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        F64Vector result = allocator.allocateOrGrow(allocationContext, (F64Vector) output, F64Vector.class, size, F64Vector::new);
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            OperatorKeySemantics.Key key = keysByGroup.get(index);
            if (key instanceof OperatorKeySemantics.DoubleKey value) {
                result.values()[index] = Double.longBitsToDouble(value.bits());
            }
        }
        return result;
    }

    private BinaryVector materializeBinaryValues(int size, Mask mask, int groupedColumnIndex, List<OperatorKeySemantics.Key> keysByGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        long totalBytes = 0;
        for (int index : mask) {
            OperatorKeySemantics.Key key = keysByGroup.get(index);
            if (key instanceof OperatorKeySemantics.BinaryKey value) {
                totalBytes += value.bytes().length;
            }
        }
        if (totalBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Grouped binary output exceeds maximum byte capacity: " + totalBytes);
        }

        BinaryVector result = allocator.allocateOrGrowBinary(allocationContext, (BinaryVector) output, size, (int) totalBytes);
        Arrays.fill(result.offsets(), 0);
        result.clearTraits();
        result.addTraits(binaryTraits[groupedColumnIndex]);
        for (int index : mask) {
            OperatorKeySemantics.Key key = keysByGroup.get(index);
            if (key instanceof OperatorKeySemantics.BinaryKey value) {
                result.setBytes(index, value.bytes());
            }
        }
        return result;
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

    private enum GroupKind
    {
        LONG,
        BOOLEAN,
        DOUBLE,
        BINARY;

        private static GroupKind forVector(Vector values)
        {
            return switch (OperatorVectorSupport.flatten(values)) {
                case org.weakref.nitro.data.I32Vector _, I64Vector _ -> LONG;
                case BooleanVector _ -> BOOLEAN;
                case F64Vector _ -> DOUBLE;
                case BinaryVector _ -> BINARY;
                default -> throw new IllegalArgumentException("Unsupported group vector: " + values.getClass().getSimpleName());
            };
        }
    }
}
