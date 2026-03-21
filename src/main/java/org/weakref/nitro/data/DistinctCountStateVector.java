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
package org.weakref.nitro.data;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.ArrayList;

public final class DistinctCountStateVector
        implements FlatVector
{
    private final ArrayList<ObjectOpenHashSet<Object>> keysByGroup = new ArrayList<>();
    private Object reusableProbeKey;

    @Override
    public int length()
    {
        return keysByGroup.size();
    }

    @Override
    public long retainedBytes()
    {
        return 0;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        DistinctCountStateVector copy = new DistinctCountStateVector();
        for (ObjectOpenHashSet<Object> keys : keysByGroup) {
            copy.keysByGroup.add(keys == null ? null : new ObjectOpenHashSet<>(keys));
        }
        copy.reusableProbeKey = reusableProbeKey;
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        throw new UnsupportedOperationException("DistinctCountStateVector does not support positional copy");
    }

    public void ensureGroupCapacity(int size)
    {
        while (keysByGroup.size() < size) {
            keysByGroup.add(null);
        }
    }

    public ObjectOpenHashSet<Object> keys(int group)
    {
        ensureGroupCapacity(group + 1);
        ObjectOpenHashSet<Object> keys = keysByGroup.get(group);
        if (keys == null) {
            keys = new ObjectOpenHashSet<>();
            keysByGroup.set(group, keys);
        }
        return keys;
    }

    public long distinctCount(int group)
    {
        if (group >= keysByGroup.size()) {
            return 0;
        }
        ObjectOpenHashSet<Object> keys = keysByGroup.get(group);
        return keys == null ? 0 : keys.size();
    }

    public Object reusableProbeKey()
    {
        return reusableProbeKey;
    }

    public void setReusableProbeKey(Object reusableProbeKey)
    {
        this.reusableProbeKey = reusableProbeKey;
    }
}
