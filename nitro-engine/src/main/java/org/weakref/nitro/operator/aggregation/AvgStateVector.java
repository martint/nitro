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
import org.weakref.nitro.data.DynamicRetainedBytesVector;
import org.weakref.nitro.data.FlatVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorPoolRetentionClass;

import static java.util.Objects.requireNonNull;

public final class AvgStateVector
        implements FlatVector, LongStateUpdate, DynamicRetainedBytesVector
{
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;
    private static final int COMPACT_COUNT_MAX = 0xFF;

    private final int length;
    private final long[][] sumChunks;
    private final byte[][] compactCountChunks;
    private final long[][] wideCountChunks;
    private long retainedBytes;
    private Allocator retainedBytesAllocator;
    private Allocator.Context retainedBytesContext;

    public AvgStateVector(int length)
    {
        this.length = length;
        this.sumChunks = new long[chunkCount(length)][];
        this.compactCountChunks = new byte[sumChunks.length][];
        this.wideCountChunks = new long[sumChunks.length][];
        long retainedBytes = 0;
        for (int index = 0; index < sumChunks.length; index++) {
            sumChunks[index] = new long[CHUNK_SIZE];
            compactCountChunks[index] = new byte[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * (Long.BYTES + Byte.BYTES);
        }
        this.retainedBytes = retainedBytes;
    }

    private AvgStateVector(
            int length,
            long[][] sumChunks,
            byte[][] compactCountChunks,
            long[][] wideCountChunks,
            long retainedBytes)
    {
        this.length = length;
        this.sumChunks = sumChunks;
        this.compactCountChunks = compactCountChunks;
        this.wideCountChunks = wideCountChunks;
        this.retainedBytes = retainedBytes;
    }

    @Override
    public void bindRetainedBytesAccounting(Allocator allocator, Allocator.Context context)
    {
        retainedBytesAllocator = requireNonNull(allocator, "allocator is null");
        retainedBytesContext = requireNonNull(context, "context is null");
    }

    public static AvgStateVector grow(AvgStateVector previous, int length)
    {
        int requiredChunkCount = chunkCount(length);
        if (requiredChunkCount <= previous.sumChunks.length) {
            return new AvgStateVector(
                    length,
                    previous.sumChunks,
                    previous.compactCountChunks,
                    previous.wideCountChunks,
                    previous.retainedBytes);
        }

        long[][] sumChunks = java.util.Arrays.copyOf(previous.sumChunks, requiredChunkCount);
        byte[][] compactCountChunks = java.util.Arrays.copyOf(previous.compactCountChunks, requiredChunkCount);
        long[][] wideCountChunks = java.util.Arrays.copyOf(previous.wideCountChunks, requiredChunkCount);
        long retainedBytes = previous.retainedBytes;
        for (int index = previous.sumChunks.length; index < requiredChunkCount; index++) {
            sumChunks[index] = new long[CHUNK_SIZE];
            compactCountChunks[index] = new byte[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * (Long.BYTES + Byte.BYTES);
        }
        return new AvgStateVector(length, sumChunks, compactCountChunks, wideCountChunks, retainedBytes);
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
        long[][] sumChunks = new long[this.sumChunks.length][];
        byte[][] compactCountChunks = new byte[this.compactCountChunks.length][];
        long[][] wideCountChunks = new long[this.wideCountChunks.length][];
        for (int index = 0; index < sumChunks.length; index++) {
            sumChunks[index] = java.util.Arrays.copyOf(this.sumChunks[index], this.sumChunks[index].length);
            if (this.compactCountChunks[index] != null) {
                compactCountChunks[index] = java.util.Arrays.copyOf(this.compactCountChunks[index], this.compactCountChunks[index].length);
            }
            if (this.wideCountChunks[index] != null) {
                wideCountChunks[index] = java.util.Arrays.copyOf(this.wideCountChunks[index], this.wideCountChunks[index].length);
            }
        }
        AvgStateVector copy = allocator.adopt(
                allocationContext,
                new AvgStateVector(length, sumChunks, compactCountChunks, wideCountChunks, retainedBytes));
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        AvgStateVector copy = new AvgStateVector(positions.length);
        for (int index = 0; index < positions.length; index++) {
            int position = positions[index];
            copy.increment(index, sum(position), count(position));
        }
        copy = allocator.adopt(allocationContext, copy);
        return copy;
    }

    @Override
    public void clearForReuse()
    {
        for (long[] chunk : sumChunks) {
            java.util.Arrays.fill(chunk, 0);
        }
        for (int index = 0; index < compactCountChunks.length; index++) {
            if (compactCountChunks[index] != null) {
                java.util.Arrays.fill(compactCountChunks[index], (byte) 0);
            }
            else {
                java.util.Arrays.fill(wideCountChunks[index], 0);
            }
        }
    }

    @Override
    public Object poolFamily()
    {
        return AvgStateVector.class;
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

    public void increment(int index, long sum, long count)
    {
        int chunkIndex = index >> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;
        sumChunks[chunkIndex][chunkOffset] += sum;
        incrementCount(chunkIndex, chunkOffset, count);
    }

    public void increment(int index, long value)
    {
        increment(index, value, 1);
    }

    @Override
    public void update(int group, long value)
    {
        increment(group, value);
    }

    /**
     * The double-summing form: the sum chunk holds the running double's raw bits (an accumulator instance uses
     * either the long or the double form exclusively; the position-copying paths are bit-preserving, so both
     * survive group compaction).
     */
    public void incrementDouble(int index, double sum)
    {
        int chunkIndex = index >> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;
        long[] chunk = sumChunks[chunkIndex];
        chunk[chunkOffset] = Double.doubleToRawLongBits(Double.longBitsToDouble(chunk[chunkOffset]) + sum);
        incrementCount(chunkIndex, chunkOffset, 1);
    }

    public double doubleSum(int index)
    {
        return Double.longBitsToDouble(sumChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK]);
    }

    public long sum(int index)
    {
        return sumChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public long count(int index)
    {
        int chunkIndex = index >> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;
        long[] wide = wideCountChunks[chunkIndex];
        return wide == null
                ? Byte.toUnsignedInt(compactCountChunks[chunkIndex][chunkOffset])
                : wide[chunkOffset];
    }

    public void initialize(int offset, int length)
    {
        int end = offset + length;
        int position = offset;
        while (position < end) {
            int chunkIndex = position >> CHUNK_SHIFT;
            int chunkOffset = position & CHUNK_MASK;
            int copyLength = Math.min(end - position, CHUNK_SIZE - chunkOffset);
            java.util.Arrays.fill(sumChunks[chunkIndex], chunkOffset, chunkOffset + copyLength, 0);
            if (compactCountChunks[chunkIndex] != null) {
                java.util.Arrays.fill(compactCountChunks[chunkIndex], chunkOffset, chunkOffset + copyLength, (byte) 0);
            }
            else {
                java.util.Arrays.fill(wideCountChunks[chunkIndex], chunkOffset, chunkOffset + copyLength, 0);
            }
            position += copyLength;
        }
    }

    private void incrementCount(int chunkIndex, int chunkOffset, long count)
    {
        long[] wide = wideCountChunks[chunkIndex];
        if (wide != null) {
            wide[chunkOffset] += count;
            return;
        }

        byte[] compact = compactCountChunks[chunkIndex];
        long updated = Byte.toUnsignedInt(compact[chunkOffset]) + count;
        if (updated >= 0 && updated <= COMPACT_COUNT_MAX) {
            compact[chunkOffset] = (byte) updated;
            return;
        }

        promoteCountChunk(chunkIndex, compact);
        wideCountChunks[chunkIndex][chunkOffset] = updated;
    }

    private void promoteCountChunk(int chunkIndex, byte[] compact)
    {
        long previousRetainedBytes = retainedBytes;
        long[] wide = new long[CHUNK_SIZE];
        for (int index = 0; index < compact.length; index++) {
            wide[index] = Byte.toUnsignedInt(compact[index]);
        }
        wideCountChunks[chunkIndex] = wide;
        compactCountChunks[chunkIndex] = null;
        retainedBytes += (long) CHUNK_SIZE * (Long.BYTES - Byte.BYTES);
        if (retainedBytesAllocator != null) {
            retainedBytesAllocator.retainedBytesChanged(retainedBytesContext, this, previousRetainedBytes);
        }
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
