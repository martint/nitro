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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.core.function.aggregation.LongStateUpdate;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.FlatVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorPoolRetentionClass;

import java.util.Arrays;

public final class SumStateVector
        implements FlatVector, LongStateUpdate
{
    private static final int CHUNK_SHIFT = 10;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final long[][] sums;
    private final boolean[][] nulls;
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
        this.sums = new long[chunkCount(length)][];
        this.nulls = new boolean[sums.length][];
        long retainedBytes = 0;
        for (int index = 0; index < sums.length; index++) {
            int chunkLength = chunkLength(length, index);
            sums[index] = new long[chunkLength];
            nulls[index] = new boolean[chunkLength];
            Arrays.fill(nulls[index], true);
            retainedBytes += (long) chunkLength * Long.BYTES + chunkLength;
        }
        this.retainedBytes = retainedBytes;
        this.nullGroupCount = length;
    }

    private SumStateVector(int length, long[][] sums, boolean[][] nulls, long retainedBytes)
    {
        this.length = length;
        this.sums = sums;
        this.nulls = nulls;
        this.retainedBytes = retainedBytes;
    }

    public static SumStateVector grow(SumStateVector previous, int length)
    {
        if (previous.length == 0) {
            return new SumStateVector(length);
        }
        int requiredChunkCount = chunkCount(length);
        if (previous.length < CHUNK_SIZE && length <= CHUNK_SIZE) {
            long[][] sums = {Arrays.copyOf(previous.sums[0], length)};
            boolean[][] nulls = {Arrays.copyOf(previous.nulls[0], length)};
            Arrays.fill(nulls[0], previous.length, length, true);
            SumStateVector grown = new SumStateVector(length, sums, nulls, (long) length * Long.BYTES + length);
            grown.nullGroupCount = previous.nullGroupCount + Math.max(0, length - previous.length);
            return grown;
        }

        if (requiredChunkCount <= previous.sums.length) {
            SumStateVector expanded = new SumStateVector(length, previous.sums, previous.nulls, previous.retainedBytes);
            expanded.nullGroupCount = previous.nullGroupCount + Math.max(0, length - previous.length);
            return expanded;
        }

        long[][] sums = new long[requiredChunkCount][];
        boolean[][] nulls = new boolean[requiredChunkCount][];
        long retainedBytes = 0;
        int firstNewChunk = 0;
        if (previous.length >= CHUNK_SIZE) {
            System.arraycopy(previous.sums, 0, sums, 0, previous.sums.length);
            System.arraycopy(previous.nulls, 0, nulls, 0, previous.nulls.length);
            retainedBytes = previous.retainedBytes;
            firstNewChunk = previous.sums.length;
        }
        for (int index = firstNewChunk; index < sums.length; index++) {
            int chunkLength = chunkLength(length, index);
            sums[index] = new long[chunkLength];
            nulls[index] = new boolean[chunkLength];
            Arrays.fill(nulls[index], true);
            retainedBytes += (long) chunkLength * Long.BYTES + chunkLength;
        }
        if (previous.length < CHUNK_SIZE) {
            System.arraycopy(previous.sums[0], 0, sums[0], 0, previous.length);
            System.arraycopy(previous.nulls[0], 0, nulls[0], 0, previous.length);
        }
        SumStateVector grown = new SumStateVector(length, sums, nulls, retainedBytes);
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
        long[][] sums = new long[this.sums.length][];
        boolean[][] nulls = new boolean[this.nulls.length][];
        long retainedBytes = 0;
        for (int index = 0; index < sums.length; index++) {
            sums[index] = Arrays.copyOf(this.sums[index], this.sums[index].length);
            nulls[index] = Arrays.copyOf(this.nulls[index], this.nulls[index].length);
            retainedBytes += (long) sums[index].length * Long.BYTES + nulls[index].length;
        }
        SumStateVector copy = new SumStateVector(length, sums, nulls, retainedBytes);
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
            copy.setSum(index, sum(position));
            boolean isNull = isNull(position);
            copy.setNull(index, isNull);
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
        for (int index = 0; index < sums.length; index++) {
            Arrays.fill(sums[index], 0);
            Arrays.fill(nulls[index], true);
        }
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
    public VectorPoolRetentionClass poolRetentionClass()
    {
        return VectorPoolRetentionClass.AGGREGATE_STATE;
    }

    public void increment(int index, long value)
    {
        int chunk = index >> CHUNK_SHIFT;
        int offset = index & CHUNK_MASK;
        sums[chunk][offset] += value;
        if (nulls[chunk][offset]) {
            nulls[chunk][offset] = false;
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
        return sums[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public boolean isNull(int index)
    {
        return nulls[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public void initialize(int offset, int length)
    {
        int end = offset + length;
        for (int slot = offset; slot < end; slot++) {
            int chunk = slot >> CHUNK_SHIFT;
            int chunkOffset = slot & CHUNK_MASK;
            sums[chunk][chunkOffset] = 0;
            if (!nulls[chunk][chunkOffset]) {
                nulls[chunk][chunkOffset] = true;
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
        int outputOffset = 0;
        for (long[] chunk : sums) {
            int chunkLength = Math.min(chunk.length, copyLength - outputOffset);
            if (chunkLength <= 0) {
                break;
            }
            System.arraycopy(chunk, 0, output.values(), outputOffset, chunkLength);
            outputOffset += chunkLength;
        }
    }

    public void copyNullsTo(BooleanVector output)
    {
        copyNullsTo(output, length);
    }

    public void copyNullsTo(BooleanVector output, int count)
    {
        int copyLength = Math.min(count, length);
        int outputOffset = 0;
        for (boolean[] chunk : nulls) {
            int chunkLength = Math.min(chunk.length, copyLength - outputOffset);
            if (chunkLength <= 0) {
                break;
            }
            System.arraycopy(chunk, 0, output.values(), outputOffset, chunkLength);
            outputOffset += chunkLength;
        }
    }

    private void setSum(int index, long value)
    {
        sums[index >> CHUNK_SHIFT][index & CHUNK_MASK] = value;
    }

    private void setNull(int index, boolean value)
    {
        nulls[index >> CHUNK_SHIFT][index & CHUNK_MASK] = value;
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }

    private static int chunkLength(int length, int chunk)
    {
        if (length <= CHUNK_SIZE) {
            return length;
        }
        return CHUNK_SIZE;
    }
}
