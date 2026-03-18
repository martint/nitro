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
    private OperatorKeySemantics.Key reusableProbeKey;
    private long nextGroupId;
    private long nullGroup = -1;

    GroupingState()
    {
        groups.defaultReturnValue(-1);
    }

    public void assignGroups(Vector values, Vector nulls, Mask mask, I64Vector result)
    {
        BooleanVector nullVector = (BooleanVector) nulls;
        if (reusableProbeKey == null) {
            reusableProbeKey = OperatorKeySemantics.reusableProbeKey(values);
        }
        for (int position : mask) {
            OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values, nullVector, position, reusableProbeKey);
            result.values()[position] = key == null ? nullGroup() : groupForKey(key);
        }
    }

    private long groupForKey(OperatorKeySemantics.Key key)
    {
        long group = groups.getLong(key);
        if (group != -1) {
            return group;
        }

        OperatorKeySemantics.Key ownedKey = OperatorKeySemantics.ownedKey(key);
        groups.put(ownedKey, nextGroupId);
        return nextGroupId++;
    }

    private long nullGroup()
    {
        if (nullGroup == -1) {
            nullGroup = nextGroupId++;
        }
        return nullGroup;
    }
}
