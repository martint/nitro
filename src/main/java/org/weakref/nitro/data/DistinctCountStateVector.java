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

import java.util.Arrays;

public final class DistinctCountStateVector
        implements FlatVector
{
    private long[] distinctCounts = new long[0];
    private Object implementation;

    @Override
    public int length()
    {
        return distinctCounts.length;
    }

    @Override
    public long retainedBytes()
    {
        return (long) distinctCounts.length * Long.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        DistinctCountStateVector copy = new DistinctCountStateVector();
        copy.distinctCounts = Arrays.copyOf(distinctCounts, distinctCounts.length);
        copy.implementation = implementation;
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        throw new UnsupportedOperationException("DistinctCountStateVector does not support positional copy");
    }

    public void ensureGroupCapacity(int size)
    {
        if (distinctCounts.length < size) {
            distinctCounts = Arrays.copyOf(distinctCounts, size);
        }
    }

    public void incrementDistinctCount(int group)
    {
        ensureGroupCapacity(group + 1);
        distinctCounts[group]++;
    }

    public long distinctCount(int group)
    {
        return group >= distinctCounts.length ? 0 : distinctCounts[group];
    }

    public Object implementation()
    {
        return implementation;
    }

    public void setImplementation(Object implementation)
    {
        this.implementation = implementation;
    }
}
