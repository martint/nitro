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
    private final boolean ownsCounts;
    private final Vector values;
    private int[] runEnds;

    public RleVector(int[] counts, Vector values)
    {
        this(counts, values, true);
    }

    static RleVector wrapCounts(int[] counts, Vector values)
    {
        return new RleVector(counts, values, false);
    }

    private RleVector(int[] counts, Vector values, boolean ownsCounts)
    {
        checkArgument(counts.length == values.length(), "Run lengths counts (%s) must match the length of the underlying values vector (%s)", counts.length, values.length());

        this.values = values;
        this.counts = counts;
        this.ownsCounts = ownsCounts;
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

    /**
     * Returns the run index for {@code position}, using {@code hint} (typically the previous run index
     * returned by this method for a lower position) to skip binary search when access is monotonic.
     * <p>
     * For positions visited in ascending order this is amortized O(1) per call; backward jumps fall
     * back to binary search. The caller must thread the returned value back in as the next hint.
     */
    public int runIndexFromHint(int position, int hint)
    {
        if (position < 0 || position >= length) {
            throw new IndexOutOfBoundsException("Position " + position + " is out of bounds for RLE vector of length " + length);
        }
        int[] ends = runEnds;
        if (ends == null) {
            ends = computeRunEnds();
            runEnds = ends;
        }

        int idx = hint;
        if (idx < 0 || idx >= ends.length) {
            idx = 0;
        }
        // If the hint overshoots the position, fall back to binary search.
        if (idx > 0 && position < ends[idx - 1]) {
            int found = Arrays.binarySearch(ends, position + 1);
            return found >= 0 ? found : -found - 1;
        }
        while (idx < ends.length && position >= ends[idx]) {
            idx++;
        }
        return idx;
    }

    /** Returns the first logical position after the requested run. */
    public int runEnd(int runIndex)
    {
        if (runIndex < 0 || runIndex >= counts.length) {
            throw new IndexOutOfBoundsException("Run " + runIndex + " is out of bounds for RLE vector with " + counts.length + " runs");
        }
        int[] ends = runEnds;
        if (ends == null) {
            ends = computeRunEnds();
            runEnds = ends;
        }
        return ends[runIndex];
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long retainedBytes()
    {
        return ownsCounts ? (long) counts.length * Integer.BYTES : 0;
    }

    @Override
    public boolean isVariableWidth()
    {
        return values.isVariableWidth();
    }

    @Override
    public boolean requiresMonotonicOutputWrites()
    {
        return values.requiresMonotonicOutputWrites();
    }

    @Override
    public long contentFingerprint()
    {
        long valuesFingerprint = values.contentFingerprint();
        if (valuesFingerprint == NO_CONTENT_FINGERPRINT) {
            return NO_CONTENT_FINGERPRINT;
        }
        long hash = valuesFingerprint;
        for (int count : counts) {
            hash = (hash ^ count) * 0x100000001b3L;
        }
        return hash == NO_CONTENT_FINGERPRINT ? hash + 1 : hash;
    }

    @Override
    public boolean hasSameContent(Vector other)
    {
        return other instanceof RleVector rle &&
                Arrays.equals(counts, rle.counts) &&
                values.hasSameContent(rle.values);
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
        if (values.isVariableWidth()) {
            int[] positions = new int[length];
            for (int position = 0; position < length; position++) {
                positions[position] = position;
            }
            Vector materialized = copy(allocator, allocationContext, positions);
            Vector result = materialized.copyMasked(allocator, allocationContext, existing, mask);
            allocator.release(allocationContext, materialized);
            return result;
        }
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
    public int childVectorCount()
    {
        return 1;
    }

    @Override
    public Vector childVector(int index)
    {
        if (index == 0) {
            return values;
        }
        throw new IndexOutOfBoundsException(index);
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
