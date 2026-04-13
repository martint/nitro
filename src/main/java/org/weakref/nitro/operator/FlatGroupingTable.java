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
package org.weakref.nitro.operator;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

final class FlatGroupingTable
{
    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final int RECORDS_PER_GROUP_SHIFT = 10;
    private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;
    private static final double DEFAULT_LOAD_FACTOR = 15.0 / 16;

    private final FlatKeyLayout layout;
    private final FlatVariableWidthArena variableWidthArena;
    private final int fixedRecordSize;

    private byte[] control;
    private int[] groupIdsByHash;
    private int[] recordIndexesByHash;
    private byte[][] fixedRecordChunks;
    private int[] recordIndexByGroupId;
    private int nextRecordIndex;
    private int capacity;
    private int mask;
    private int maxFill;

    public FlatGroupingTable(FlatKeyLayout layout, int expectedSize)
    {
        this.layout = layout;
        this.variableWidthArena = layout.anyVariableWidth() ? new FlatVariableWidthArena() : null;
        this.fixedRecordSize = Long.BYTES + layout.fixedRecordSize();
        this.capacity = max(VECTOR_LENGTH, computeCapacity(max(16, expectedSize), DEFAULT_LOAD_FACTOR));
        this.mask = capacity - 1;
        this.maxFill = calculateMaxFill(capacity);
        this.control = new byte[capacity + VECTOR_LENGTH];
        this.groupIdsByHash = new int[capacity];
        this.recordIndexesByHash = new int[capacity];
        Arrays.fill(groupIdsByHash, -1);
        Arrays.fill(recordIndexesByHash, -1);
        this.fixedRecordChunks = new byte[recordGroupsRequiredForCapacity(capacity)][];
        this.recordIndexByGroupId = new int[max(16, expectedSize)];
        Arrays.fill(recordIndexByGroupId, -1);
    }

    public long assignGroup(Vector[] values, Vector[] nulls, int position, long newGroupId)
    {
        long hash = layout.hash(values, nulls, position);
        int index = getIndex(values, nulls, position, hash);
        if (index >= 0) {
            return groupIdsByHash[index];
        }

        addNewGroup(-index - 1, values, nulls, position, hash, newGroupId);
        if (nextRecordIndex >= maxFill) {
            rehash();
        }
        return newGroupId;
    }

    public long assignGroup(Vector[] values, int position, long newGroupId)
    {
        return assignGroup(values, null, position, newGroupId);
    }

    public long findGroup(Vector[] values, Vector[] nulls, int position)
    {
        long hash = layout.hash(values, nulls, position);
        int index = getIndex(values, nulls, position, hash);
        if (index < 0) {
            return -1;
        }
        return groupIdsByHash[index];
    }

    public long findGroup(Vector[] values, int position)
    {
        return findGroup(values, null, position);
    }

    public Streams groupedValues(int groupedColumnIndex, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        FlatKeyLayout.Field field = layout.field(groupedColumnIndex);
        return Streams.ofValuesAndNulls(
                field.handler().materializeValues(this, field, size, mask, -1, output == null ? null : output.values(), allocator, allocationContext),
                materializeNulls(groupedColumnIndex, size, mask, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
    }

    private int getIndex(Vector[] values, Vector[] nulls, int position, long hash)
    {
        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) (hash >> 7));
        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            long controlVector = (long) LONG_HANDLE.get(control, bucket);
            long controlMatches = match(controlVector, repeated);
            while (controlMatches != 0) {
                int index = bucket(bucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
                int recordIndex = recordIndexesByHash[index];
                if (recordIndex >= 0 && identical(recordIndex, hash, values, nulls, position)) {
                    return index;
                }
                controlMatches &= controlMatches - 1;
            }

            long emptyMatches = match(controlVector, 0L);
            if (emptyMatches != 0) {
                return -bucket(bucket + (Long.numberOfTrailingZeros(emptyMatches) >>> 3)) - 1;
            }

            bucket = bucket(bucket + step);
            step += VECTOR_LENGTH;
        }
    }

    private boolean identical(int recordIndex, long hash, Vector[] values, Vector[] nulls, int position)
    {
        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        long storedHash = (long) LONG_HANDLE.get(fixedChunk, fixedOffset);
        if (storedHash != hash) {
            return false;
        }
        return layout.identicalRecordToInput(fixedChunk, fixedOffset + Long.BYTES, variableWidthArena, values, nulls, position);
    }

    private void addNewGroup(int index, Vector[] values, Vector[] nulls, int position, long hash, long groupId)
    {
        setControl(index, (byte) (hash & 0x7F | 0x80));
        groupIdsByHash[index] = toIntExact(groupId);
        int recordIndex = nextRecordIndex++;
        recordIndexesByHash[index] = recordIndex;
        ensureGroupIdCapacity(toIntExact(groupId));
        recordIndexByGroupId[toIntExact(groupId)] = recordIndex;

        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        LONG_HANDLE.set(fixedChunk, fixedOffset, hash);
        layout.writeRecord(fixedChunk, fixedOffset + Long.BYTES, variableWidthArena, values, nulls, position);
    }

    private void ensureGroupIdCapacity(int groupId)
    {
        if (recordIndexByGroupId.length > groupId) {
            return;
        }
        int newSize = recordIndexByGroupId.length;
        while (newSize <= groupId) {
            newSize = max(16, newSize * 2);
        }
        int previousLength = recordIndexByGroupId.length;
        recordIndexByGroupId = Arrays.copyOf(recordIndexByGroupId, newSize);
        Arrays.fill(recordIndexByGroupId, previousLength, newSize, -1);
    }

    private void rehash()
    {
        capacity *= 2;
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;
        fixedRecordChunks = Arrays.copyOf(fixedRecordChunks, recordGroupsRequiredForCapacity(capacity));

        control = new byte[capacity + VECTOR_LENGTH];
        groupIdsByHash = new int[capacity];
        recordIndexesByHash = new int[capacity];
        Arrays.fill(groupIdsByHash, -1);
        Arrays.fill(recordIndexesByHash, -1);

        for (int groupId = 0; groupId < recordIndexByGroupId.length; groupId++) {
            int recordIndex = recordIndexByGroupId[groupId];
            if (recordIndex < 0) {
                continue;
            }

            long hash = (long) LONG_HANDLE.get(fixedChunk(recordIndex), fixedOffset(recordIndex));
            byte hashPrefix = (byte) (hash & 0x7F | 0x80);
            int bucket = bucket((int) (hash >> 7));
            int step = 1;
            while (true) {
                long controlVector = (long) LONG_HANDLE.get(control, bucket);
                long emptyMatches = match(controlVector, 0L);
                if (emptyMatches != 0) {
                    int index = bucket(bucket + (Long.numberOfTrailingZeros(emptyMatches) >>> 3));
                    setControl(index, hashPrefix);
                    groupIdsByHash[index] = groupId;
                    recordIndexesByHash[index] = recordIndex;
                    break;
                }
                bucket = bucket(bucket + step);
                step += VECTOR_LENGTH;
            }
        }
    }

    public boolean fieldNull(int recordIndex, int fieldIndex)
    {
        return layout.fieldNull(fixedChunk(recordIndex), fixedOffset(recordIndex) + Long.BYTES, fieldIndex);
    }

    private BooleanVector materializeNulls(int fieldIndex, int size, Mask mask, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector result = VectorAccess.writableBooleanVector(allocator, allocationContext, output, size);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            int recordIndex = recordIndex(index);
            result.values()[index] = recordIndex < 0 || fieldNull(recordIndex, fieldIndex);
        }
        return result;
    }

    int recordIndex(long groupId)
    {
        return groupId >= 0 && groupId < recordIndexByGroupId.length ? recordIndexByGroupId[(int) groupId] : -1;
    }

    byte[] fixedChunk(int recordIndex)
    {
        int groupIndex = recordIndex >> RECORDS_PER_GROUP_SHIFT;
        byte[] chunk = fixedRecordChunks[groupIndex];
        if (chunk == null) {
            chunk = new byte[RECORDS_PER_GROUP * fixedRecordSize];
            fixedRecordChunks[groupIndex] = chunk;
        }
        return chunk;
    }

    int fixedOffset(int recordIndex)
    {
        return (recordIndex & RECORDS_PER_GROUP_MASK) * fixedRecordSize;
    }

    FlatVariableWidthArena variableWidthArena()
    {
        return variableWidthArena;
    }

    private int bucket(int hash)
    {
        return hash & mask;
    }

    private void setControl(int index, byte hashPrefix)
    {
        control[index] = hashPrefix;
        if (index < VECTOR_LENGTH) {
            control[index + capacity] = hashPrefix;
        }
    }

    private static int computeCapacity(int maxSize, double loadFactor)
    {
        int capacity = (int) (maxSize / loadFactor);
        return max((int) (1L << (64 - Long.numberOfLeadingZeros(capacity - 1))), 16);
    }

    private static int calculateMaxFill(int capacity)
    {
        return (int) (capacity * 15L / 16);
    }

    private static long repeat(byte value)
    {
        return ((value & 0xFFL) * 0x01_01_01_01_01_01_01_01L);
    }

    private static long match(long vector, long repeatedValue)
    {
        long comparison = vector ^ repeatedValue;
        return (comparison - 0x01_01_01_01_01_01_01_01L) & ~comparison & 0x80_80_80_80_80_80_80_80L;
    }

    private static int recordGroupsRequiredForCapacity(int capacity)
    {
        return max(1, (capacity + 1) >> RECORDS_PER_GROUP_SHIFT);
    }

    static final class FlatVariableWidthArena
    {
        private static final int CHUNK_SIZE = 1 << 20;

        private byte[][] chunks = new byte[][] {new byte[CHUNK_SIZE]};
        private int chunkIndex;
        private int chunkOffset;

        public long append(byte[] source, int sourceOffset, int length)
        {
            if (length == 0) {
                return pointer(chunkIndex, chunkOffset);
            }

            if (chunkOffset + length > CHUNK_SIZE) {
                chunkIndex++;
                chunkOffset = 0;
                if (chunkIndex >= chunks.length) {
                    chunks = Arrays.copyOf(chunks, chunks.length * 2);
                }
                if (chunks[chunkIndex] == null) {
                    chunks[chunkIndex] = new byte[CHUNK_SIZE];
                }
            }

            byte[] chunk = chunks[chunkIndex];
            int offset = chunkOffset;
            System.arraycopy(source, sourceOffset, chunk, offset, length);
            chunkOffset += length;
            return pointer(chunkIndex, offset);
        }

        public byte[] chunk(int index)
        {
            return chunks[index];
        }

        public static long pointer(int chunkIndex, int chunkOffset)
        {
            return (((long) chunkIndex) << 32) | (chunkOffset & 0xFFFF_FFFFL);
        }

        public static int chunkIndex(long pointer)
        {
            return (int) (pointer >>> 32);
        }

        public static int chunkOffset(long pointer)
        {
            return (int) pointer;
        }
    }
}
