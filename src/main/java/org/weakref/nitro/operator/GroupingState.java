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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

final class GroupingState
{
    private final Object2LongMap<OperatorKeySemantics.Key> groups = new Object2LongOpenHashMap<>();
    private long nextGroupId;
    private long nullGroup = -1;

    GroupingState()
    {
        groups.defaultReturnValue(-1);
    }

    public void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result)
    {
        BooleanVector nullVector = (BooleanVector) nulls;
        for (int position : mask) {
            OperatorKeySemantics.Key key = OperatorKeySemantics.key(values, nullVector, position);
            result.values()[position] = key == null ? nullGroup() : groupForKey(key);
        }
    }

    private long groupForKey(OperatorKeySemantics.Key key)
    {
        long group = groups.putIfAbsent(key, nextGroupId);
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
