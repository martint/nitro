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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

final class GroupingState
{
    private final Long2LongMap longGroups = new Long2LongOpenHashMap();
    private final Object2LongMap<OperatorKeySemantics.BinaryKey> binaryGroups = new Object2LongOpenHashMap<>();
    private long nextGroupId;
    private long nullGroup = -1;

    GroupingState()
    {
        longGroups.defaultReturnValue(-1);
        binaryGroups.defaultReturnValue(-1);
    }

    public void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result)
    {
        BooleanVector nullVector = (BooleanVector) nulls;
        switch (values) {
            case I64Vector vector -> groupI64(vector.values(), nullVector, mask, result);
            case DictionaryVector vector when vector.values() instanceof I64Vector dictionaryValues -> groupDictionaryI64(vector.ids(), dictionaryValues.values(), nullVector, mask, result);
            case BinaryVector vector -> groupBinary(vector, nullVector, mask, result);
            case DictionaryVector vector when vector.values() instanceof BinaryVector dictionaryValues -> groupDictionaryBinary(vector.ids(), dictionaryValues, nullVector, mask, result);
            default -> throw new IllegalArgumentException("Unsupported group-by vector: " + values.getClass().getSimpleName());
        }
    }

    private void groupI64(long[] values, BooleanVector nulls, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            result.values()[position] = OperatorVectorSupport.isNull(nulls, position)
                    ? nullGroup()
                    : groupForLong(values[position]);
        }
    }

    private void groupDictionaryI64(int[] ids, long[] values, BooleanVector nulls, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            result.values()[position] = OperatorVectorSupport.isNull(nulls, position)
                    ? nullGroup()
                    : groupForLong(values[ids[position]]);
        }
    }

    private void groupBinary(BinaryVector values, BooleanVector nulls, Mask mask, I64Vector result)
    {
        for (int position : mask) {
            result.values()[position] = OperatorVectorSupport.isNull(nulls, position)
                    ? nullGroup()
                    : groupForBinary(values, position);
        }
    }

    private void groupDictionaryBinary(int[] ids, BinaryVector values, BooleanVector nulls, Mask mask, I64Vector result)
    {
        long[] groupsById = new long[values.length()];
        java.util.Arrays.fill(groupsById, -1);

        for (int position : mask) {
            if (OperatorVectorSupport.isNull(nulls, position)) {
                result.values()[position] = nullGroup();
                continue;
            }

            int id = ids[position];
            long group = groupsById[id];
            if (group == -1) {
                group = groupForBinary(values, id);
                groupsById[id] = group;
            }
            result.values()[position] = group;
        }
    }

    private long groupForLong(long value)
    {
        long group = longGroups.putIfAbsent(value, nextGroupId);
        if (group == -1) {
            group = nextGroupId++;
        }
        return group;
    }

    private long groupForBinary(BinaryVector values, int position)
    {
        OperatorKeySemantics.BinaryKey key = OperatorKeySemantics.binaryKey(values.copyBytes(position));
        long group = binaryGroups.putIfAbsent(key, nextGroupId);
        if (group == -1) {
            group = nextGroupId++;
        }
        return group;
    }

    private long nullGroup()
    {
        if (nullGroup == -1) {
            nullGroup = nextGroupId++;
        }
        return nullGroup;
    }
}
