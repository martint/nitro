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

public final class SumStateVector
        implements FlatVector
{
    private static final long MAX_POOLED_RETAINED_BYTES = 8L * 1024 * 1024;
    private static final int CHUNK_SHIFT = 12;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    private final int length;
    private final long[][] sumChunks;
    private final boolean[][] nullChunks;
    private final long retainedBytes;
    // Number of slots in [0, length) whose nullChunks entry is currently true — i.e. groups that
    // are either uninitialised or have only received null-valued inputs so far. Maintained
    // incrementally: initialize(offset, count) adds count, increment(group) decrements by one
    // the first time it flips that group's null flag from true to false. Allows Sum.result to
    // decide in O(1) whether any visible group is null, instead of scanning the backing chunks.
    private int nullGroupCount;

    public SumStateVector(int length)
    {
        this.length = length;
        this.sumChunks = new long[chunkCount(length)][];
        this.nullChunks = new boolean[chunkCount(length)][];
        long retainedBytes = 0;
        for (int index = 0; index < sumChunks.length; index++) {
            sumChunks[index] = new long[CHUNK_SIZE];
            nullChunks[index] = new boolean[CHUNK_SIZE];
            java.util.Arrays.fill(nullChunks[index], true);
            retainedBytes += (long) CHUNK_SIZE * Long.BYTES;
            retainedBytes += CHUNK_SIZE;
        }
        this.retainedBytes = retainedBytes;
        this.nullGroupCount = length;
    }

    private SumStateVector(int length, long[][] sumChunks, boolean[][] nullChunks, long retainedBytes)
    {
        this.length = length;
        this.sumChunks = sumChunks;
        this.nullChunks = nullChunks;
        this.retainedBytes = retainedBytes;
    }

    public static SumStateVector grow(SumStateVector previous, int length)
    {
        int requiredChunkCount = chunkCount(length);
        if (requiredChunkCount <= previous.sumChunks.length) {
            SumStateVector expanded = new SumStateVector(length, previous.sumChunks, previous.nullChunks, previous.retainedBytes);
            // Existing chunks already carry the prior null states; logically, the new visible range
            // [previous.length, length) is all uninitialised (true) — previous.nullGroupCount covered
            // [0, previous.length), so adjust accordingly.
            expanded.nullGroupCount = previous.nullGroupCount + Math.max(0, length - previous.length);
            return expanded;
        }

        long[][] sumChunks = java.util.Arrays.copyOf(previous.sumChunks, requiredChunkCount);
        boolean[][] nullChunks = java.util.Arrays.copyOf(previous.nullChunks, requiredChunkCount);
        long retainedBytes = previous.retainedBytes;
        for (int index = previous.sumChunks.length; index < requiredChunkCount; index++) {
            sumChunks[index] = new long[CHUNK_SIZE];
            nullChunks[index] = new boolean[CHUNK_SIZE];
            java.util.Arrays.fill(nullChunks[index], true);
            retainedBytes += (long) CHUNK_SIZE * Long.BYTES;
            retainedBytes += CHUNK_SIZE;
        }
        SumStateVector grown = new SumStateVector(length, sumChunks, nullChunks, retainedBytes);
        grown.nullGroupCount = previous.nullGroupCount + Math.max(0, length - previous.length);
        return grown;
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
        boolean[][] nullChunks = new boolean[this.nullChunks.length][];
        long retainedBytes = 0;
        for (int index = 0; index < sumChunks.length; index++) {
            sumChunks[index] = java.util.Arrays.copyOf(this.sumChunks[index], this.sumChunks[index].length);
            nullChunks[index] = java.util.Arrays.copyOf(this.nullChunks[index], this.nullChunks[index].length);
            retainedBytes += (long) this.sumChunks[index].length * Long.BYTES;
            retainedBytes += this.nullChunks[index].length;
        }
        return allocator.adopt(allocationContext, new SumStateVector(length, sumChunks, nullChunks, retainedBytes));
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        SumStateVector copy = new SumStateVector(positions.length);
        for (int index = 0; index < positions.length; index++) {
            int position = positions[index];
            copy.sumChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] = sum(position);
            copy.nullChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] = isNull(position);
        }
        return allocator.adopt(allocationContext, copy);
    }

    @Override
    public void clearForReuse()
    {
        for (long[] chunk : sumChunks) {
            java.util.Arrays.fill(chunk, 0);
        }
        for (boolean[] chunk : nullChunks) {
            java.util.Arrays.fill(chunk, true);
        }
        nullGroupCount = length;
    }

    @Override
    public Object poolFamily()
    {
        return SumStateVector.class;
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

    public void increment(int index, long value)
    {
        sumChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK] += value;
        boolean[] nulls = nullChunks[index >> CHUNK_SHIFT];
        int slot = index & CHUNK_MASK;
        if (nulls[slot]) {
            nulls[slot] = false;
            nullGroupCount--;
        }
    }

    public long sum(int index)
    {
        return sumChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
    }

    public boolean isNull(int index)
    {
        return nullChunks[index >> CHUNK_SHIFT][index & CHUNK_MASK];
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
            boolean[] nulls = nullChunks[chunkIndex];
            // Only count transitions from false to true; if the slot was already null, we do not
            // double-count it (e.g., when initialize() is called over a range that overlaps with the
            // fresh-chunk region populated by the constructor/grow()).
            for (int slot = chunkOffset; slot < chunkOffset + copyLength; slot++) {
                if (!nulls[slot]) {
                    nulls[slot] = true;
                    nullGroupCount++;
                }
            }
            position += copyLength;
        }
    }

    /**
     * Returns true when any slot in {@code [0, length)} is still null. Uses the incrementally
     * maintained null-group counter so the check is O(1) regardless of state size.
     */
    public boolean hasAnyNull()
    {
        return nullGroupCount > 0;
    }

    public void copySumsTo(I64Vector output)
    {
        copySumsTo(output, length);
    }

    public void copySumsTo(I64Vector output, int count)
    {
        long[] values = output.values();
        int remaining = Math.min(count, length);
        int offset = 0;
        for (long[] chunk : sumChunks) {
            int copyLength = Math.min(chunk.length, remaining - offset);
            if (copyLength <= 0) {
                break;
            }
            System.arraycopy(chunk, 0, values, offset, copyLength);
            offset += copyLength;
        }
    }

    public void copyNullsTo(BooleanVector output)
    {
        copyNullsTo(output, length);
    }

    public void copyNullsTo(BooleanVector output, int count)
    {
        boolean[] values = output.values();
        int remaining = Math.min(count, length);
        int offset = 0;
        for (boolean[] chunk : nullChunks) {
            int copyLength = Math.min(chunk.length, remaining - offset);
            if (copyLength <= 0) {
                break;
            }
            System.arraycopy(chunk, 0, values, offset, copyLength);
            offset += copyLength;
        }
    }


    private static int chunkCount(int length)
    {
        return (length + CHUNK_MASK) >> CHUNK_SHIFT;
    }
}
