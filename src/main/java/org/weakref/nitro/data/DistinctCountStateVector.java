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

public final class DistinctCountStateVector
        implements FlatVector
{
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private int length;
    private long[][] chunks = new long[0][];
    private long retainedBytes;
    private Object implementation;

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
        DistinctCountStateVector copy = new DistinctCountStateVector();
        copy.length = length;
        copy.chunks = new long[chunks.length][];
        for (int index = 0; index < chunks.length; index++) {
            copy.chunks[index] = java.util.Arrays.copyOf(chunks[index], chunks[index].length);
        }
        copy.retainedBytes = retainedBytes;
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        throw new UnsupportedOperationException("DistinctCountStateVector does not support positional copy");
    }

    public void ensureGroupCapacity(int size)
    {
        if (length >= size) {
            return;
        }

        length = size;
        int requiredChunkCount = chunkCount(size);
        if (requiredChunkCount > chunks.length) {
            long[][] grown = java.util.Arrays.copyOf(chunks, requiredChunkCount);
            for (int index = chunks.length; index < requiredChunkCount; index++) {
                grown[index] = new long[CHUNK_SIZE];
                retainedBytes += (long) CHUNK_SIZE * Long.BYTES;
            }
            chunks = grown;
        }
    }

    public void incrementDistinctCount(int group)
    {
        incrementDistinctCount(group, 1);
    }

    public void incrementDistinctCount(int group, long count)
    {
        ensureGroupCapacity(group + 1);
        increment(group, count);
    }

    public long distinctCount(int group)
    {
        if (group >= length) {
            return 0;
        }
        return chunks[group >> CHUNK_SHIFT][group & CHUNK_MASK];
    }

    public Object implementation()
    {
        return implementation;
    }

    public void setImplementation(Object implementation)
    {
        this.implementation = implementation;
    }

    private void increment(int index, long count)
    {
        chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] += count;
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
