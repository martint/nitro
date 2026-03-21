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
import java.util.Set;

final class GroupingState
{
    private final Object2LongMap<OperatorKeySemantics.Key> groups = new Object2LongOpenHashMap<>();
    private final ArrayList<OperatorKeySemantics.Key> keysByGroup = new ArrayList<>();
    private OperatorKeySemantics.Key reusableProbeKey;
    private long nextGroupId;
    private long nullGroup = -1;
    private GroupKind groupKind;
    private Set<BinaryVector.Trait> binaryTraits = Set.of();

    GroupingState()
    {
        groups.defaultReturnValue(-1);
    }

    public void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result)
    {
        BooleanVector nullVector = (BooleanVector) nulls;
        if (reusableProbeKey == null) {
            reusableProbeKey = OperatorKeySemantics.reusableProbeKey(values);
            groupKind = GroupKind.forVector(values);
            binaryTraits = OperatorVectorSupport.binaryTraits(values);
        }
        if (values instanceof DictionaryVector dictionary) {
            assignDictionaryGroups(dictionary, nullVector, mask, result);
            return;
        }
        for (int position : mask) {
            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values, nullVector, position, reusableProbeKey);
            result.values()[position] = key == null ? nullGroup() : groupForKey(key);
        }
    }

    private void assignDictionaryGroups(DictionaryVector dictionary, BooleanVector nullVector, Mask mask, I64Vector result)
    {
        int[] ids = dictionary.ids();
        int[] groupsByDictionaryId = new int[dictionary.values().length()];
        Arrays.fill(groupsByDictionaryId, -1);

        for (int position : mask) {
            if (OperatorVectorSupport.isNull(nullVector, position)) {
                result.values()[position] = nullGroup();
                continue;
            }

            int dictionaryId = ids[position];
            int group = groupsByDictionaryId[dictionaryId];
            if (group == -1) {
                group = (int) groupForKey(OperatorKeySemantics.probeKey(dictionary.values(), null, dictionaryId, reusableProbeKey));
                groupsByDictionaryId[dictionaryId] = group;
            }
            result.values()[position] = group;
        }
    }

    public Streams groupedValues(Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        return switch (groupKind) {
            case LONG -> Streams.ofValuesAndNulls(
                    materializeLongValues(size, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case BOOLEAN -> Streams.ofValuesAndNulls(
                    materializeBooleanValues(size, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case DOUBLE -> Streams.ofValuesAndNulls(
                    materializeDoubleValues(size, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case BINARY -> Streams.ofValuesAndNulls(
                    materializeBinaryValues(size, mask, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        };
    }

    private long groupForKey(OperatorKeySemantics.Key key)
    {
        long group = groups.getLong(key);
        if (group != -1) {
            return group;
        }

        OperatorKeySemantics.Key ownedKey = OperatorKeySemantics.ownedKey(key);
        groups.put(ownedKey, nextGroupId);
        keysByGroup.add(ownedKey);
        return nextGroupId++;
    }

    private long nullGroup()
    {
        if (nullGroup == -1) {
            keysByGroup.add(null);
            nullGroup = nextGroupId++;
        }
        return nullGroup;
    }

    private I64Vector materializeLongValues(int size, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
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

    private BooleanVector materializeBooleanValues(int size, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
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

    private F64Vector materializeDoubleValues(int size, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
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

    private BinaryVector materializeBinaryValues(int size, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
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
        result.addTraits(binaryTraits);
        for (int index : mask) {
            OperatorKeySemantics.Key key = keysByGroup.get(index);
            if (key instanceof OperatorKeySemantics.BinaryKey value) {
                result.setBytes(index, value.bytes());
            }
        }
        return result;
    }

    private BooleanVector materializeNulls(int size, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
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
