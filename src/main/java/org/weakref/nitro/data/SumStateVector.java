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

import org.weakref.nitro.core.function.aggregation.LongStateUpdate;

import java.util.Arrays;

public final class SumStateVector
        implements FlatVector, LongStateUpdate
{
    private static final long MAX_POOLED_RETAINED_BYTES = 8L * 1024 * 1024;

    private final int length;
    private final long[] sums;
    private final boolean[] nulls;
    private final long retainedBytes;
    // Number of slots in [0, length) whose nulls entry is currently true — i.e. groups that
    // are either uninitialised or have only received null-valued inputs so far. Maintained
    // incrementally: initialize(offset, count) adds count, increment(group) decrements by one
    // the first time it flips that group's null flag from true to false. Allows Sum.result to
    // decide in O(1) whether any visible group is null, instead of scanning the backing array.
    private int nullGroupCount;

    public SumStateVector(int length)
    {
        this.length = length;
        this.sums = new long[length];
        this.nulls = new boolean[length];
        Arrays.fill(nulls, true);
        this.retainedBytes = (long) length * Long.BYTES + length;
        this.nullGroupCount = length;
    }

    private SumStateVector(int length, long[] sums, boolean[] nulls)
    {
        this.length = length;
        this.sums = sums;
        this.nulls = nulls;
        this.retainedBytes = (long) sums.length * Long.BYTES + nulls.length;
    }

    public static SumStateVector grow(SumStateVector previous, int length)
    {
        if (length <= previous.sums.length) {
            // The backing arrays already cover the requested length; widen the logical view.
            // Slots in [previous.length, length) were filled true (uninitialised) at allocation
            // time, so they are correctly counted as null groups here.
            SumStateVector expanded = new SumStateVector(length, previous.sums, previous.nulls);
            expanded.nullGroupCount = previous.nullGroupCount + Math.max(0, length - previous.length);
            return expanded;
        }

        long[] sums = Arrays.copyOf(previous.sums, length);
        boolean[] nulls = Arrays.copyOf(previous.nulls, length);
        // copyOf zero-fills the boolean tail (false); the freshly grown range is uninitialised and
        // must read as null until initialize() or a non-null input touches it.
        Arrays.fill(nulls, previous.sums.length, length, true);
        SumStateVector grown = new SumStateVector(length, sums, nulls);
        grown.nullGroupCount = previous.nullGroupCount + Math.max(0, length - previous.length);
        return grown;
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long retainedBytes()
    {
        return retainedBytes;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        SumStateVector copy = new SumStateVector(length, Arrays.copyOf(sums, sums.length), Arrays.copyOf(nulls, nulls.length));
        copy.nullGroupCount = nullGroupCount;
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        SumStateVector copy = new SumStateVector(positions.length);
        int nullCount = 0;
        for (int index = 0; index < positions.length; index++) {
            int position = positions[index];
            copy.sums[index] = sums[position];
            boolean isNull = nulls[position];
            copy.nulls[index] = isNull;
            if (!isNull) {
                nullCount++;
            }
        }
        copy.nullGroupCount = positions.length - nullCount;
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public void clearForReuse()
    {
        Arrays.fill(sums, 0);
        Arrays.fill(nulls, true);
        nullGroupCount = length;
    }

    @Override
    public Object poolFamily()
    {
        return SumStateVector.class;
    }

    @Override
    public int poolCapacity()
    {
        return length();
    }

    @Override
    public int poolMaxRetained()
    {
        return retainedBytes <= MAX_POOLED_RETAINED_BYTES ? 2 : 0;
    }

    public void increment(int index, long value)
    {
        sums[index] += value;
        if (nulls[index]) {
            nulls[index] = false;
            nullGroupCount--;
        }
    }

    @Override
    public void update(int group, long value)
    {
        increment(group, value);
    }

    public long sum(int index)
    {
        return sums[index];
    }

    public boolean isNull(int index)
    {
        return nulls[index];
    }

    public void initialize(int offset, int length)
    {
        int end = offset + length;
        Arrays.fill(sums, offset, end, 0);
        // Only count transitions from false to true; if the slot was already null, we do not
        // double-count it (e.g., when initialize() is called over a range that overlaps with the
        // fresh-array region populated by the constructor/grow()).
        for (int slot = offset; slot < end; slot++) {
            if (!nulls[slot]) {
                nulls[slot] = true;
                nullGroupCount++;
            }
        }
    }

    /**
     * Returns true when any slot in {@code [0, length)} is still null. Uses the incrementally
     * maintained null-group counter so the check is O(1) regardless of state size.
     */
    public boolean hasAnyNull()
    {
        return nullGroupCount > 0;
    }

    public void copySumsTo(I64Vector output)
    {
        copySumsTo(output, length);
    }

    public void copySumsTo(I64Vector output, int count)
    {
        int copyLength = Math.min(count, length);
        if (copyLength > 0) {
            System.arraycopy(sums, 0, output.values(), 0, copyLength);
        }
    }

    public void copyNullsTo(BooleanVector output)
    {
        copyNullsTo(output, length);
    }

    public void copyNullsTo(BooleanVector output, int count)
    {
        int copyLength = Math.min(count, length);
        if (copyLength > 0) {
            System.arraycopy(nulls, 0, output.values(), 0, copyLength);
        }
    }
}
