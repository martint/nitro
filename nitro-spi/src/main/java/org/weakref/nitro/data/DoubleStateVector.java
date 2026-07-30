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

/**
 * Incrementally grown double aggregation state.
 *
 * <p>Chunks keep growth proportional to newly admitted groups and avoid copying the complete
 * state while both old and replacement arrays are resident.
 */
public final class DoubleStateVector
        implements FlatVector
{
    private static final int CHUNK_SHIFT = 10;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final double[][] chunks;
    private final long retainedBytes;

    public DoubleStateVector(int length)
    {
        this.length = length;
        this.chunks = new double[chunkCount(length)][];
        long retainedBytes = 0;
        for (int index = 0; index < chunks.length; index++) {
            int chunkLength = chunkLength(length);
            chunks[index] = new double[chunkLength];
            retainedBytes += (long) chunkLength * Double.BYTES;
        }
        this.retainedBytes = retainedBytes;
    }

    private DoubleStateVector(int length, double[][] chunks, long retainedBytes)
    {
        this.length = length;
        this.chunks = chunks;
        this.retainedBytes = retainedBytes;
    }

    public static DoubleStateVector grow(DoubleStateVector previous, int length)
    {
        if (previous.length == 0) {
            return new DoubleStateVector(length);
        }
        int requiredChunkCount = chunkCount(length);
        if (previous.length < CHUNK_SIZE && length <= CHUNK_SIZE) {
            double[][] chunks = {Arrays.copyOf(previous.chunks[0], length)};
            return new DoubleStateVector(length, chunks, (long) length * Double.BYTES);
        }

        if (requiredChunkCount <= previous.chunks.length) {
            return new DoubleStateVector(length, previous.chunks, previous.retainedBytes);
        }

        double[][] chunks = new double[requiredChunkCount][];
        long retainedBytes = 0;
        int firstNewChunk = 0;
        if (previous.length >= CHUNK_SIZE) {
            System.arraycopy(previous.chunks, 0, chunks, 0, previous.chunks.length);
            retainedBytes = previous.retainedBytes;
            firstNewChunk = previous.chunks.length;
        }
        for (int index = firstNewChunk; index < chunks.length; index++) {
            chunks[index] = new double[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * Double.BYTES;
        }
        if (previous.length < CHUNK_SIZE) {
            System.arraycopy(previous.chunks[0], 0, chunks[0], 0, previous.length);
        }
        return new DoubleStateVector(length, chunks, retainedBytes);
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
        double[][] chunks = new double[this.chunks.length][];
        long retainedBytes = 0;
        for (int index = 0; index < chunks.length; index++) {
            chunks[index] = Arrays.copyOf(this.chunks[index], this.chunks[index].length);
            retainedBytes += (long) chunks[index].length * Double.BYTES;
        }
        return allocator.adopt(allocationContext, new DoubleStateVector(length, chunks, retainedBytes));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        DoubleStateVector copy = new DoubleStateVector(positions.length);
        for (int index = 0; index < positions.length; index++) {
            copy.set(index, get(positions[index]));
        }
        return allocator.adopt(allocationContext, copy);
    }

    public double get(int index)
    {
        return chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public void set(int index, double value)
    {
        chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] = value;
    }

    public void add(int index, double value)
    {
        chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] += value;
    }

    public void initialize(int offset, int length)
    {
        int end = offset + length;
        while (offset < end) {
            int chunk = offset >> CHUNK_SHIFT;
            int chunkOffset = offset & CHUNK_MASK;
            int chunkLength = Math.min(end - offset, CHUNK_SIZE - chunkOffset);
            Arrays.fill(chunks[chunk], chunkOffset, chunkOffset + chunkLength, 0);
            offset += chunkLength;
        }
    }

    public void copyTo(F64Vector output, int count)
    {
        int copyLength = Math.min(count, length);
        int outputOffset = 0;
        for (double[] chunk : chunks) {
            int chunkLength = Math.min(chunk.length, copyLength - outputOffset);
            if (chunkLength <= 0) {
                break;
            }
            System.arraycopy(chunk, 0, output.values(), outputOffset, chunkLength);
            outputOffset += chunkLength;
        }
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }

    private static int chunkLength(int length)
    {
        if (length <= CHUNK_SIZE) {
            return length;
        }
        return CHUNK_SIZE;
    }
}
