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

public final class CountStateVector
        implements FlatVector
{
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final long[][] chunks;

    public CountStateVector(int length)
    {
        this.length = length;
        this.chunks = new long[chunkCount(length)][];
        for (int index = 0; index < chunks.length; index++) {
            chunks[index] = new long[chunkLength(index, length)];
        }
    }

    public CountStateVector(CountStateVector previous, int length)
    {
        this.length = length;
        this.chunks = new long[chunkCount(length)][];
        for (int index = 0; index < chunks.length; index++) {
            int requiredLength = chunkLength(index, length);
            if (index < previous.chunks.length) {
                long[] previousChunk = previous.chunks[index];
                chunks[index] = previousChunk.length == requiredLength
                        ? previousChunk
                        : Arrays.copyOf(previousChunk, requiredLength);
                continue;
            }
            chunks[index] = new long[requiredLength];
        }
    }

    @Override
    public int length()
    {
        return length;
    }

    public void increment(int index, long count)
    {
        chunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] += count;
    }

    public void copyTo(I64Vector output)
    {
        long[] values = output.values();
        int offset = 0;
        for (long[] chunk : chunks) {
            System.arraycopy(chunk, 0, values, offset, chunk.length);
            offset += chunk.length;
        }
    }

    public long bytes()
    {
        long bytes = 0;
        for (long[] chunk : chunks) {
            bytes += (long) chunk.length * Long.BYTES;
        }
        return bytes;
    }

    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }

    private static int chunkLength(int chunkIndex, int length)
    {
        int remaining = length - (chunkIndex << CHUNK_SHIFT);
        return Math.min(CHUNK_SIZE, remaining);
    }
}
