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

public final class AvgStateVector
        implements FlatVector
{
    private final long[] sums;
    private final long[] counts;

    public AvgStateVector(int size)
    {
        this.sums = new long[size];
        this.counts = new long[size];
    }

    public long[] sums()
    {
        return sums;
    }

    public long[] counts()
    {
        return counts;
    }

    @Override
    public int length()
    {
        return sums.length;
    }

    @Override
    public long retainedBytes()
    {
        return (long) sums.length * Long.BYTES * 2;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        AvgStateVector copy = allocator.allocate(allocationContext, AvgStateVector.class, sums.length, AvgStateVector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        AvgStateVector copy = allocator.allocate(allocationContext, AvgStateVector.class, positions.length, AvgStateVector::new);
        for (int index = 0; index < positions.length; index++) {
            int position = positions[index];
            copy.sums()[index] = sums[position];
            copy.counts()[index] = counts[position];
        }
        return copy;
    }

    @Override
    public void copyInto(Vector target)
    {
        AvgStateVector avgTarget = (AvgStateVector) target;
        System.arraycopy(sums, 0, avgTarget.sums(), 0, sums.length);
        System.arraycopy(counts, 0, avgTarget.counts(), 0, counts.length);
    }

    @Override
    public void clearForReuse()
    {
        java.util.Arrays.fill(sums, 0);
        java.util.Arrays.fill(counts, 0);
    }

    @Override
    public PoolingMode poolingMode()
    {
        return PoolingMode.STANDARD;
    }
}
