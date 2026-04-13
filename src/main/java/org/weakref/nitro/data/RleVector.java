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
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

public final class RleVector
        implements Vector
{
    private final int length;
    private final int[] counts;
    private final Vector values;
    private int[] runEnds;

    public RleVector(int[] counts, Vector values)
    {
        checkArgument(counts.length == values.length(), "Run lengths counts (%s) must match the length of the underlying values vector (%s)", counts.length, values.length());

        this.values = values;
        this.counts = counts;
        length = Arrays.stream(counts).sum();
    }

    public static int computeTargetRleLength(RleVector left, RleVector right)
    {
        int result = 0;

        int leftIndex = 0;
        int rightIndex = 0;

        int leftCount = 0;
        int rightCount = 0;

        while (leftIndex < left.counts().length && rightIndex < right.counts().length) {
            result++;

            if (leftCount == 0) {
                leftCount = left.counts()[leftIndex];
            }
            if (rightCount == 0) {
                rightCount = right.counts()[rightIndex];
            }

            int count = Math.min(leftCount, rightCount);
            leftCount -= count;
            rightCount -= count;

            if (leftCount == 0) {
                leftIndex++;
            }
            if (rightCount == 0) {
                rightIndex++;
            }
        }

        return result;
    }

    public int[] counts()
    {
        return counts;
    }

    public Vector values()
    {
        return values;
    }

    public int runIndex(int position)
    {
        if (position < 0 || position >= length) {
            throw new IndexOutOfBoundsException("Position " + position + " is out of bounds for RLE vector of length " + length);
        }

        int[] ends = runEnds;
        if (ends == null) {
            ends = computeRunEnds();
            runEnds = ends;
        }

        int index = Arrays.binarySearch(ends, position + 1);
        return index >= 0 ? index : -index - 1;
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long retainedBytes()
    {
        return (long) counts.length * Integer.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocateRle(allocationContext, counts, values.copy(allocator, allocationContext));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        int[] runPositions = new int[positions.length];
        for (int index = 0; index < positions.length; index++) {
            runPositions[index] = runIndex(positions[index]);
        }
        return values.copy(allocator, allocationContext, runPositions);
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        for (int position : mask) {
            existing = values.copySinglePositionInto(allocator, allocationContext, existing, runIndex(position), position, length);
        }
        return existing;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        int[] runPositions = new int[sourceCount];
        for (int index = 0; index < sourceCount; index++) {
            runPositions[index] = runIndex(sourcePositions[index]);
        }
        return values.copyPositionsInto(allocator, allocationContext, existing, runPositions, sourceCount, outputStart, size);
    }

    @Override
    public Vector copySelectedPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, SelectedPositions sourcePositions, int outputStart, int size)
    {
        return values.copySelectedPositionsInto(allocator, allocationContext, existing, SelectedPositions.map(sourcePositions, this::runIndex), outputStart, size);
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        return values.copySinglePositionInto(allocator, allocationContext, existing, runIndex(sourcePosition), outputPosition, size);
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return values.emptyLike(allocator, allocationContext);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        return values.materializeRows(allocator, allocationContext, rows);
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        consumer.accept(values);
    }

    private int[] computeRunEnds()
    {
        int[] ends = new int[counts.length];
        int current = 0;
        for (int index = 0; index < counts.length; index++) {
            current += counts[index];
            ends[index] = current;
        }
        return ends;
    }

    @Override
    public String toString()
    {
        return "RLE {length: " + length + ", counts: " + Arrays.toString(counts) + ", values: " + values + "}";
    }
}
