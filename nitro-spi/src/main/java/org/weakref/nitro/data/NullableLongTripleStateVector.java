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
 * Incrementally growing state for a nullable physical tuple of three longs.
 *
 * <p>The representation is deliberately free of aggregate semantics. Registry-provided
 * implementations can use the three lanes for fixed-width state without making the engine aware
 * of a function or logical type. Growth allocates only new chunks and retains existing chunks, so
 * implementations can request their exact logical group count without geometric headroom or
 * whole-state copies.
 */
public final class NullableLongTripleStateVector
        implements FlatVector
{
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final long[][] valueChunks;
    private final boolean[][] nullChunks;
    private final long retainedBytes;

    public NullableLongTripleStateVector(int length)
    {
        this.length = length;
        int chunkCount = chunkCount(length);
        valueChunks = new long[chunkCount][];
        nullChunks = new boolean[chunkCount][];
        for (int chunk = 0; chunk < chunkCount; chunk++) {
            valueChunks[chunk] = new long[CHUNK_SIZE * 3];
            nullChunks[chunk] = new boolean[CHUNK_SIZE];
            Arrays.fill(nullChunks[chunk], true);
        }
        retainedBytes = (long) chunkCount * CHUNK_SIZE * ((3L * Long.BYTES) + Byte.BYTES);
    }

    private NullableLongTripleStateVector(
            int length,
            long[][] valueChunks,
            boolean[][] nullChunks,
            long retainedBytes)
    {
        this.length = length;
        this.valueChunks = valueChunks;
        this.nullChunks = nullChunks;
        this.retainedBytes = retainedBytes;
    }

    public static NullableLongTripleStateVector grow(NullableLongTripleStateVector previous, int length)
    {
        int requiredChunks = chunkCount(length);
        if (requiredChunks <= previous.valueChunks.length) {
            return new NullableLongTripleStateVector(
                    length,
                    previous.valueChunks,
                    previous.nullChunks,
                    previous.retainedBytes);
        }

        long[][] valueChunks = Arrays.copyOf(previous.valueChunks, requiredChunks);
        boolean[][] nullChunks = Arrays.copyOf(previous.nullChunks, requiredChunks);
        for (int chunk = previous.valueChunks.length; chunk < requiredChunks; chunk++) {
            valueChunks[chunk] = new long[CHUNK_SIZE * 3];
            nullChunks[chunk] = new boolean[CHUNK_SIZE];
            Arrays.fill(nullChunks[chunk], true);
        }
        long retainedBytes = (long) requiredChunks * CHUNK_SIZE * ((3L * Long.BYTES) + Byte.BYTES);
        return new NullableLongTripleStateVector(length, valueChunks, nullChunks, retainedBytes);
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
        NullableLongTripleStateVector copy = new NullableLongTripleStateVector(length);
        for (int chunk = 0; chunk < valueChunks.length; chunk++) {
            System.arraycopy(valueChunks[chunk], 0, copy.valueChunks[chunk], 0, CHUNK_SIZE * 3);
            System.arraycopy(nullChunks[chunk], 0, copy.nullChunks[chunk], 0, CHUNK_SIZE);
        }
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        NullableLongTripleStateVector copy = new NullableLongTripleStateVector(positions.length);
        for (int output = 0; output < positions.length; output++) {
            int input = positions[output];
            copy.set(output, first(input), second(input), third(input), isNull(input));
        }
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public void clearForReuse()
    {
        initialize(0, length);
    }

    @Override
    public Object poolFamily()
    {
        return NullableLongTripleStateVector.class;
    }

    @Override
    public int poolCapacity()
    {
        return length;
    }

    @Override
    public VectorPoolRetentionClass poolRetentionClass()
    {
        return VectorPoolRetentionClass.WIDE_AGGREGATE_STATE;
    }

    public void initialize(int offset, int length)
    {
        int end = offset + length;
        int position = offset;
        while (position < end) {
            int chunk = position >> CHUNK_SHIFT;
            int chunkOffset = position & CHUNK_MASK;
            int count = Math.min(end - position, CHUNK_SIZE - chunkOffset);
            Arrays.fill(valueChunks[chunk], chunkOffset * 3, (chunkOffset + count) * 3, 0);
            Arrays.fill(nullChunks[chunk], chunkOffset, chunkOffset + count, true);
            position += count;
        }
    }

    public long first(int index)
    {
        return valueChunk(index)[valueOffset(index)];
    }

    public long second(int index)
    {
        return valueChunk(index)[valueOffset(index) + 1];
    }

    public long third(int index)
    {
        return valueChunk(index)[valueOffset(index) + 2];
    }

    public boolean isNull(int index)
    {
        return nullChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public void set(int index, long first, long second, long third, boolean isNull)
    {
        long[] values = valueChunk(index);
        int offset = valueOffset(index);
        values[offset] = first;
        values[offset + 1] = second;
        values[offset + 2] = third;
        nullChunk(index)[index & CHUNK_MASK] = isNull;
    }

    /** Returns the physical interleaved value chunk containing {@code index}. */
    public long[] valueChunk(int index)
    {
        return valueChunks[index >> CHUNK_SHIFT];
    }

    /** Returns the first-lane offset for {@code index} within {@link #valueChunk(int)}. */
    public int valueOffset(int index)
    {
        return (index & CHUNK_MASK) * 3;
    }

    /** Returns the physical null chunk containing {@code index}. */
    public boolean[] nullChunk(int index)
    {
        return nullChunks[index >> CHUNK_SHIFT];
    }

    /** Returns the null-lane offset for {@code index} within {@link #nullChunk(int)}. */
    public int nullOffset(int index)
    {
        return index & CHUNK_MASK;
    }

    public int chunkIndex(int index)
    {
        return index >> CHUNK_SHIFT;
    }

    public int chunkCount()
    {
        return valueChunks.length;
    }

    public int chunkLength(int chunk)
    {
        return Math.min(CHUNK_SIZE, length - (chunk * CHUNK_SIZE));
    }

    public long[] valueChunkByIndex(int chunk)
    {
        return valueChunks[chunk];
    }

    public boolean[] nullChunkByIndex(int chunk)
    {
        return nullChunks[chunk];
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
