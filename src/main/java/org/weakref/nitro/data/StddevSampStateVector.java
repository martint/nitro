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

public final class StddevSampStateVector
        implements FlatVector
{
    private static final long MAX_POOLED_RETAINED_BYTES = 16L * 1024 * 1024;
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final long[][] countChunks;
    private final double[][] meanChunks;
    private final double[][] m2Chunks;
    private final long retainedBytes;

    public StddevSampStateVector(int length)
    {
        this.length = length;
        this.countChunks = new long[chunkCount(length)][];
        this.meanChunks = new double[chunkCount(length)][];
        this.m2Chunks = new double[chunkCount(length)][];

        long retainedBytes = 0;
        for (int index = 0; index < countChunks.length; index++) {
            countChunks[index] = new long[CHUNK_SIZE];
            meanChunks[index] = new double[CHUNK_SIZE];
            m2Chunks[index] = new double[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * (Long.BYTES + (2L * Double.BYTES));
        }
        this.retainedBytes = retainedBytes;
    }

    private StddevSampStateVector(int length, long[][] countChunks, double[][] meanChunks, double[][] m2Chunks, long retainedBytes)
    {
        this.length = length;
        this.countChunks = countChunks;
        this.meanChunks = meanChunks;
        this.m2Chunks = m2Chunks;
        this.retainedBytes = retainedBytes;
    }

    public static StddevSampStateVector grow(StddevSampStateVector previous, int length)
    {
        int requiredChunkCount = chunkCount(length);
        if (requiredChunkCount <= previous.countChunks.length) {
            return new StddevSampStateVector(length, previous.countChunks, previous.meanChunks, previous.m2Chunks, previous.retainedBytes);
        }

        long[][] countChunks = java.util.Arrays.copyOf(previous.countChunks, requiredChunkCount);
        double[][] meanChunks = java.util.Arrays.copyOf(previous.meanChunks, requiredChunkCount);
        double[][] m2Chunks = java.util.Arrays.copyOf(previous.m2Chunks, requiredChunkCount);
        long retainedBytes = previous.retainedBytes;
        for (int index = previous.countChunks.length; index < requiredChunkCount; index++) {
            countChunks[index] = new long[CHUNK_SIZE];
            meanChunks[index] = new double[CHUNK_SIZE];
            m2Chunks[index] = new double[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * (Long.BYTES + (2L * Double.BYTES));
        }
        return new StddevSampStateVector(length, countChunks, meanChunks, m2Chunks, retainedBytes);
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
        long[][] countChunks = new long[this.countChunks.length][];
        double[][] meanChunks = new double[this.meanChunks.length][];
        double[][] m2Chunks = new double[this.m2Chunks.length][];
        long retainedBytes = 0;
        for (int index = 0; index < countChunks.length; index++) {
            countChunks[index] = java.util.Arrays.copyOf(this.countChunks[index], this.countChunks[index].length);
            meanChunks[index] = java.util.Arrays.copyOf(this.meanChunks[index], this.meanChunks[index].length);
            m2Chunks[index] = java.util.Arrays.copyOf(this.m2Chunks[index], this.m2Chunks[index].length);
            retainedBytes += (long) this.countChunks[index].length * (Long.BYTES + (2L * Double.BYTES));
        }
        return allocator.adopt(allocationContext, new StddevSampStateVector(length, countChunks, meanChunks, m2Chunks, retainedBytes));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        StddevSampStateVector copy = new StddevSampStateVector(positions.length);
        for (int index = 0; index < positions.length; index++) {
            int position = positions[index];
            copy.countChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] = count(position);
            copy.meanChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] = mean(position);
            copy.m2Chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] = m2(position);
        }
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public void clearForReuse()
    {
        for (long[] chunk : countChunks) {
            java.util.Arrays.fill(chunk, 0);
        }
        for (double[] chunk : meanChunks) {
            java.util.Arrays.fill(chunk, 0.0);
        }
        for (double[] chunk : m2Chunks) {
            java.util.Arrays.fill(chunk, 0.0);
        }
    }

    @Override
    public Object poolFamily()
    {
        return StddevSampStateVector.class;
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

    public void initialize(int offset, int length)
    {
        int end = offset + length;
        int position = offset;
        while (position < end) {
            int chunkIndex = position >> CHUNK_SHIFT;
            int chunkOffset = position & CHUNK_MASK;
            int copyLength = Math.min(end - position, CHUNK_SIZE - chunkOffset);
            java.util.Arrays.fill(countChunks[chunkIndex], chunkOffset, chunkOffset + copyLength, 0L);
            java.util.Arrays.fill(meanChunks[chunkIndex], chunkOffset, chunkOffset + copyLength, 0.0);
            java.util.Arrays.fill(m2Chunks[chunkIndex], chunkOffset, chunkOffset + copyLength, 0.0);
            position += copyLength;
        }
    }

    public void addSample(int index, long value)
    {
        int chunkIndex = index >> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;

        long count = countChunks[chunkIndex][chunkOffset] + 1;
        double mean = meanChunks[chunkIndex][chunkOffset];
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;

        countChunks[chunkIndex][chunkOffset] = count;
        meanChunks[chunkIndex][chunkOffset] = mean;
        m2Chunks[chunkIndex][chunkOffset] += delta * delta2;
    }

    public long count(int index)
    {
        return countChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public double mean(int index)
    {
        return meanChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public double m2(int index)
    {
        return m2Chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
