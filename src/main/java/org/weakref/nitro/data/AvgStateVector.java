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
    private static final long MAX_POOLED_RETAINED_BYTES = 8L * 1024 * 1024;
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final long[][] sumChunks;
    private final long[][] countChunks;
    private final long retainedBytes;

    public AvgStateVector(int length)
    {
        this.length = length;
        this.sumChunks = new long[chunkCount(length)][];
        this.countChunks = new long[chunkCount(length)][];
        long retainedBytes = 0;
        for (int index = 0; index < sumChunks.length; index++) {
            sumChunks[index] = new long[CHUNK_SIZE];
            countChunks[index] = new long[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * Long.BYTES * 2;
        }
        this.retainedBytes = retainedBytes;
    }

    private AvgStateVector(int length, long[][] sumChunks, long[][] countChunks, long retainedBytes)
    {
        this.length = length;
        this.sumChunks = sumChunks;
        this.countChunks = countChunks;
        this.retainedBytes = retainedBytes;
    }

    public static AvgStateVector grow(AvgStateVector previous, int length)
    {
        int requiredChunkCount = chunkCount(length);
        if (requiredChunkCount <= previous.sumChunks.length) {
            return new AvgStateVector(length, previous.sumChunks, previous.countChunks, previous.retainedBytes);
        }

        long[][] sumChunks = java.util.Arrays.copyOf(previous.sumChunks, requiredChunkCount);
        long[][] countChunks = java.util.Arrays.copyOf(previous.countChunks, requiredChunkCount);
        long retainedBytes = previous.retainedBytes;
        for (int index = previous.sumChunks.length; index < requiredChunkCount; index++) {
            sumChunks[index] = new long[CHUNK_SIZE];
            countChunks[index] = new long[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * Long.BYTES * 2;
        }
        return new AvgStateVector(length, sumChunks, countChunks, retainedBytes);
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
        long[][] countChunks = new long[this.countChunks.length][];
        long retainedBytes = 0;
        for (int index = 0; index < sumChunks.length; index++) {
            sumChunks[index] = java.util.Arrays.copyOf(this.sumChunks[index], this.sumChunks[index].length);
            countChunks[index] = java.util.Arrays.copyOf(this.countChunks[index], this.countChunks[index].length);
            retainedBytes += (long) this.sumChunks[index].length * Long.BYTES * 2;
        }
        return allocator.adopt(allocationContext, new AvgStateVector(length, sumChunks, countChunks, retainedBytes));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        AvgStateVector copy = new AvgStateVector(positions.length);
        for (int index = 0; index < positions.length; index++) {
            int position = positions[index];
            copy.increment(index, sum(position), count(position));
        }
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public void clearForReuse()
    {
        for (long[] chunk : sumChunks) {
            java.util.Arrays.fill(chunk, 0);
        }
        for (long[] chunk : countChunks) {
            java.util.Arrays.fill(chunk, 0);
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
    public int poolMaxRetained()
    {
        return retainedBytes <= MAX_POOLED_RETAINED_BYTES ? 2 : 0;
    }

    public void increment(int index, long sum, long count)
    {
        sumChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] += sum;
        countChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] += count;
    }

    public void increment(int index, long value)
    {
        increment(index, value, 1);
    }

    /**
     * The double-summing form: the sum chunk holds the running double's raw bits (an accumulator instance uses
     * either the long or the double form exclusively; the position-copying paths are bit-preserving, so both
     * survive group compaction).
     */
    public void incrementDouble(int index, double sum)
    {
        long[] chunk = sumChunks[index >> CHUNK_SHIFT];
        int offset = index & CHUNK_MASK;
        chunk[offset] = Double.doubleToRawLongBits(Double.longBitsToDouble(chunk[offset]) + sum);
        countChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK]++;
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
        return countChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
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
            java.util.Arrays.fill(countChunks[chunkIndex], chunkOffset, chunkOffset + copyLength, 0);
            position += copyLength;
        }
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
