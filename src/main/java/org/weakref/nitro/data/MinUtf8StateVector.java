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

public final class MinUtf8StateVector
        implements FlatVector
{
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final byte[][][] chunks;
    private long retainedBytes;

    public MinUtf8StateVector(int length)
    {
        this.length = length;
        this.chunks = new byte[chunkCount(length)][][];
        for (int index = 0; index < chunks.length; index++) {
            chunks[index] = new byte[CHUNK_SIZE][];
        }
        this.retainedBytes = length;
    }

    private MinUtf8StateVector(int length, byte[][][] chunks, long retainedBytes)
    {
        this.length = length;
        this.chunks = chunks;
        this.retainedBytes = retainedBytes;
    }

    public static MinUtf8StateVector grow(MinUtf8StateVector source, int length)
    {
        int requiredChunkCount = chunkCount(length);
        if (requiredChunkCount <= source.chunks.length) {
            return new MinUtf8StateVector(length, source.chunks, source.retainedBytes - source.length + length);
        }

        byte[][][] chunks = java.util.Arrays.copyOf(source.chunks, requiredChunkCount);
        for (int index = source.chunks.length; index < requiredChunkCount; index++) {
            chunks[index] = new byte[CHUNK_SIZE][];
        }
        return new MinUtf8StateVector(length, chunks, source.retainedBytes - source.length + length);
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
        byte[][][] chunks = new byte[this.chunks.length][][];
        long retainedBytes = length;
        for (int chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex++) {
            chunks[chunkIndex] = new byte[CHUNK_SIZE][];
            for (int index = 0; index < CHUNK_SIZE; index++) {
                byte[] value = this.chunks[chunkIndex][index];
                chunks[chunkIndex][index] = value == null ? null : java.util.Arrays.copyOf(value, value.length);
                retainedBytes += value == null ? 0 : value.length;
            }
        }
        return allocator.adopt(allocationContext, new MinUtf8StateVector(length, chunks, retainedBytes));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        throw new UnsupportedOperationException("MinUtf8StateVector does not support positional copy");
    }

    public boolean isNull(int group)
    {
        return value(group) == null;
    }

    public byte[] value(int group)
    {
        return chunks[group >> CHUNK_SHIFT][group & CHUNK_MASK];
    }

    public void setValue(int group, byte[] value)
    {
        byte[][] chunk = chunks[group >> CHUNK_SHIFT];
        int offset = group & CHUNK_MASK;
        byte[] previous = chunk[offset];
        chunk[offset] = value;
        retainedBytes += (value == null ? 0 : value.length) - (previous == null ? 0 : previous.length);
    }

    public void initialize(int offset, int length)
    {
        int end = offset + length;
        for (int position = offset; position < end; position++) {
            setValue(position, null);
        }
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
