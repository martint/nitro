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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

import static java.util.Objects.requireNonNull;

public final class CountStateVector
        implements FlatVector, LongStateUpdate, DynamicRetainedBytesVector
{
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;
    private static final int COMPACT_MAX = 0xFF;

    private final int length;
    private final byte[][] compactChunks;
    private final long[][] wideChunks;
    private long retainedBytes;
    private Allocator retainedBytesAllocator;
    private Allocator.Context retainedBytesContext;

    public CountStateVector(int length)
    {
        this.length = length;
        this.compactChunks = new byte[chunkCount(length)][];
        this.wideChunks = new long[compactChunks.length][];
        long retainedBytes = 0;
        for (int index = 0; index < compactChunks.length; index++) {
            compactChunks[index] = new byte[CHUNK_SIZE];
            retainedBytes += CHUNK_SIZE;
        }
        this.retainedBytes = retainedBytes;
    }

    private CountStateVector(int length, byte[][] compactChunks, long[][] wideChunks, long retainedBytes)
    {
        this.length = length;
        this.compactChunks = compactChunks;
        this.wideChunks = wideChunks;
        this.retainedBytes = retainedBytes;
    }

    @Override
    public void bindRetainedBytesAccounting(Allocator allocator, Allocator.Context context)
    {
        retainedBytesAllocator = requireNonNull(allocator, "allocator is null");
        retainedBytesContext = requireNonNull(context, "context is null");
    }

    public static CountStateVector grow(CountStateVector previous, int length)
    {
        int requiredChunkCount = chunkCount(length);
        if (requiredChunkCount <= previous.compactChunks.length) {
            return new CountStateVector(length, previous.compactChunks, previous.wideChunks, previous.retainedBytes);
        }

        byte[][] compactChunks = java.util.Arrays.copyOf(previous.compactChunks, requiredChunkCount);
        long[][] wideChunks = java.util.Arrays.copyOf(previous.wideChunks, requiredChunkCount);
        long retainedBytes = previous.retainedBytes;
        for (int index = previous.compactChunks.length; index < compactChunks.length; index++) {
            compactChunks[index] = new byte[CHUNK_SIZE];
            retainedBytes += CHUNK_SIZE;
        }
        return new CountStateVector(length, compactChunks, wideChunks, retainedBytes);
    }

    public static CountStateVector growOwned(
            Allocator allocator,
            Allocator.Context allocationContext,
            CountStateVector previous,
            int length)
    {
        return allocator.replaceSharedGrowth(allocationContext, previous, grow(previous, length));
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
        byte[][] compactChunks = new byte[this.compactChunks.length][];
        long[][] wideChunks = new long[this.wideChunks.length][];
        for (int index = 0; index < compactChunks.length; index++) {
            if (this.compactChunks[index] != null) {
                compactChunks[index] = java.util.Arrays.copyOf(this.compactChunks[index], this.compactChunks[index].length);
            }
            if (this.wideChunks[index] != null) {
                wideChunks[index] = java.util.Arrays.copyOf(this.wideChunks[index], this.wideChunks[index].length);
            }
        }
        CountStateVector copy = allocator.adopt(
                allocationContext,
                new CountStateVector(length, compactChunks, wideChunks, retainedBytes));
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        CountStateVector copy = new CountStateVector(positions.length);
        for (int index = 0; index < positions.length; index++) {
            copy.increment(index, value(positions[index]));
        }
        copy = allocator.adopt(allocationContext, copy);
        return copy;
    }

    public void increment(int index, long count)
    {
        int chunkIndex = index >> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;
        long[] wide = wideChunks[chunkIndex];
        if (wide != null) {
            wide[chunkOffset] += count;
            return;
        }

        byte[] compact = compactChunks[chunkIndex];
        long updated = Byte.toUnsignedInt(compact[chunkOffset]) + count;
        if (updated >= 0 && updated <= COMPACT_MAX) {
            compact[chunkOffset] = (byte) updated;
            return;
        }

        promoteChunk(chunkIndex, compact);
        wideChunks[chunkIndex][chunkOffset] = updated;
    }

    @Override
    public void update(int group, long value)
    {
        increment(group, value);
    }

    public long value(int index)
    {
        int chunkIndex = index >> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;
        long[] wide = wideChunks[chunkIndex];
        return wide == null
                ? Byte.toUnsignedInt(compactChunks[chunkIndex][chunkOffset])
                : wide[chunkOffset];
    }

    public void clear(int offset, int count)
    {
        if (offset < 0 || offset > length || count < 0 || count > length - offset) {
            throw new IndexOutOfBoundsException("invalid count-state clear range");
        }
        int cleared = 0;
        while (cleared < count) {
            int position = offset + cleared;
            int chunkIndex = position >> CHUNK_SHIFT;
            int chunkOffset = position & CHUNK_MASK;
            int clearLength = Math.min(count - cleared, CHUNK_SIZE - chunkOffset);
            long[] wide = wideChunks[chunkIndex];
            if (wide != null) {
                java.util.Arrays.fill(wide, chunkOffset, chunkOffset + clearLength, 0);
            }
            else {
                java.util.Arrays.fill(compactChunks[chunkIndex], chunkOffset, chunkOffset + clearLength, (byte) 0);
            }
            cleared += clearLength;
        }
    }

    public void copyTo(I64Vector output)
    {
        copyRangeTo(output, 0, 0, length);
    }

    public void copyRangeTo(I64Vector output, int sourceStart, int outputStart, int count)
    {
        if (sourceStart < 0 || sourceStart > length ||
                outputStart < 0 || outputStart > output.length() ||
                count < 0 || count > length - sourceStart || count > output.length() - outputStart) {
            throw new IndexOutOfBoundsException("invalid count-state copy range");
        }
        long[] values = output.values();
        int copied = 0;
        while (copied < count) {
            int sourcePosition = sourceStart + copied;
            int chunkIndex = sourcePosition >> CHUNK_SHIFT;
            int chunkOffset = sourcePosition & CHUNK_MASK;
            int copyLength = Math.min(count - copied, CHUNK_SIZE - chunkOffset);
            long[] wide = wideChunks[chunkIndex];
            if (wide != null) {
                System.arraycopy(wide, chunkOffset, values, outputStart + copied, copyLength);
            }
            else {
                byte[] compact = compactChunks[chunkIndex];
                for (int index = 0; index < copyLength; index++) {
                    values[outputStart + copied + index] = Byte.toUnsignedInt(compact[chunkOffset + index]);
                }
            }
            copied += copyLength;
        }
    }

    public long bytes()
    {
        return retainedBytes;
    }

    private void promoteChunk(int chunkIndex, byte[] compact)
    {
        long previousRetainedBytes = retainedBytes;
        long[] wide = new long[CHUNK_SIZE];
        for (int index = 0; index < compact.length; index++) {
            wide[index] = Byte.toUnsignedInt(compact[index]);
        }
        wideChunks[chunkIndex] = wide;
        compactChunks[chunkIndex] = null;
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
