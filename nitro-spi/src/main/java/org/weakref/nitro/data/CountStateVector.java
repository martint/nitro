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

public final class CountStateVector
        implements FlatVector, LongStateUpdate
{
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final long[][] chunks;
    private final long retainedBytes;

    public CountStateVector(int length)
    {
        this.length = length;
        this.chunks = new long[chunkCount(length)][];
        long retainedBytes = 0;
        for (int index = 0; index < chunks.length; index++) {
            chunks[index] = new long[CHUNK_SIZE];
            retainedBytes += (long) chunks[index].length * Long.BYTES;
        }
        this.retainedBytes = retainedBytes;
    }

    private CountStateVector(int length, long[][] chunks, long retainedBytes)
    {
        this.length = length;
        this.chunks = chunks;
        this.retainedBytes = retainedBytes;
    }

    public static CountStateVector grow(CountStateVector previous, int length)
    {
        int requiredChunkCount = chunkCount(length);
        if (requiredChunkCount <= previous.chunks.length) {
            return new CountStateVector(length, previous.chunks, previous.retainedBytes);
        }

        long[][] chunks = java.util.Arrays.copyOf(previous.chunks, requiredChunkCount);
        long retainedBytes = previous.retainedBytes;
        for (int index = previous.chunks.length; index < chunks.length; index++) {
            chunks[index] = new long[CHUNK_SIZE];
            retainedBytes += (long) CHUNK_SIZE * Long.BYTES;
        }
        return new CountStateVector(length, chunks, retainedBytes);
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
        long[][] chunks = new long[this.chunks.length][];
        long retainedBytes = 0;
        for (int index = 0; index < chunks.length; index++) {
            chunks[index] = java.util.Arrays.copyOf(this.chunks[index], this.chunks[index].length);
            retainedBytes += (long) chunks[index].length * Long.BYTES;
        }
        return allocator.adopt(allocationContext, new CountStateVector(length, chunks, retainedBytes));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        CountStateVector copy = new CountStateVector(positions.length);
        for (int index = 0; index < positions.length; index++) {
            copy.increment(index, value(positions[index]));
        }
        return allocator.adopt(allocationContext, copy);
    }

    public void increment(int index, long count)
    {
        chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] += count;
    }

    @Override
    public void update(int group, long value)
    {
        increment(group, value);
    }

    public long value(int index)
    {
        return chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public void copyTo(I64Vector output)
    {
        long[] values = output.values();
        int offset = 0;
        for (long[] chunk : chunks) {
            int copyLength = Math.min(chunk.length, length - offset);
            if (copyLength <= 0) {
                break;
            }
            System.arraycopy(chunk, 0, values, offset, copyLength);
            offset += copyLength;
        }
    }

    public long bytes()
    {
        return retainedBytes;
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
