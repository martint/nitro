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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.LongSupplier;

import static java.lang.Math.max;
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
    private byte[][] fixedRecordChunks;
    private int[] recordIndexByGroupId = new int[16];
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
        Arrays.fill(groupIdsByHash, -1);
        this.fixedRecordChunks = new byte[recordGroupsRequiredForCapacity(capacity)][];
        Arrays.fill(recordIndexByGroupId, -1);
    }

    public long assignGroup(Vector[] values, int position, LongSupplier nextGroupIdSupplier)
    {
        long hash = layout.hash(values, position);
        int index = getIndex(values, position, hash);
        if (index >= 0) {
            return groupIdsByHash[index];
        }

        long groupId = nextGroupIdSupplier.getAsLong();
        addNewGroup(-index - 1, values, position, hash, groupId);
        if (nextRecordIndex >= maxFill) {
            rehash();
        }
        return groupId;
    }

    public Streams groupedValues(int groupedColumnIndex, Mask mask, long nullGroup, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        int size = mask.none() ? 0 : mask.maxPosition() + 1;
        FlatKeyLayout.Field field = layout.field(groupedColumnIndex);
        return switch (field.handler().kind()) {
            case LONG -> Streams.ofValuesAndNulls(
                    materializeLongValues(size, mask, field, nullGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, nullGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case BOOLEAN -> Streams.ofValuesAndNulls(
                    materializeBooleanValues(size, mask, field, nullGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, nullGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case DOUBLE -> Streams.ofValuesAndNulls(
                    materializeDoubleValues(size, mask, field, nullGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, nullGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
            case BINARY -> Streams.ofValuesAndNulls(
                    materializeBinaryValues(size, mask, field, nullGroup, output == null ? null : output.values(), allocator, allocationContext),
                    materializeNulls(size, mask, nullGroup, output == null ? null : output.getOrNull(Stream.NULLS), allocator, allocationContext));
        };
    }

    private int getIndex(Vector[] values, int position, long hash)
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
                long groupId = groupIdsByHash[index];
                int recordIndex = recordIndex(groupId);
                if (recordIndex >= 0 && identical(recordIndex, hash, values, position)) {
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

    private boolean identical(int recordIndex, long hash, Vector[] values, int position)
    {
        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        long storedHash = (long) LONG_HANDLE.get(fixedChunk, fixedOffset);
        if (storedHash != hash) {
            return false;
        }
        return layout.identicalRecordToInput(fixedChunk, fixedOffset + Long.BYTES, variableWidthArena, values, position);
    }

    private void addNewGroup(int index, Vector[] values, int position, long hash, long groupId)
    {
        setControl(index, (byte) (hash & 0x7F | 0x80));
        groupIdsByHash[index] = Math.toIntExact(groupId);
        int recordIndex = nextRecordIndex++;
        ensureGroupIdCapacity(Math.toIntExact(groupId));
        recordIndexByGroupId[Math.toIntExact(groupId)] = recordIndex;

        byte[] fixedChunk = fixedChunk(recordIndex);
        int fixedOffset = fixedOffset(recordIndex);
        LONG_HANDLE.set(fixedChunk, fixedOffset, hash);
        layout.writeRecord(fixedChunk, fixedOffset + Long.BYTES, variableWidthArena, values, position);
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
        Arrays.fill(groupIdsByHash, -1);

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
                    break;
                }
                bucket = bucket(bucket + step);
                step += VECTOR_LENGTH;
            }
        }
    }

    private I64Vector materializeLongValues(int size, Mask mask, FlatKeyLayout.Field field, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index == nullGroup) {
                continue;
            }
            int recordIndex = recordIndex(index);
            if (recordIndex >= 0) {
                result.values()[index] = field.handler().readLong(fixedChunk(recordIndex), fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset());
            }
        }
        return result;
    }

    private BooleanVector materializeBooleanValues(int size, Mask mask, FlatKeyLayout.Field field, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector result = allocator.allocateOrGrow(allocationContext, (BooleanVector) output, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(result.values(), false);
        for (int index : mask) {
            if (index == nullGroup) {
                continue;
            }
            int recordIndex = recordIndex(index);
            if (recordIndex >= 0) {
                result.values()[index] = field.handler().readBoolean(fixedChunk(recordIndex), fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset());
            }
        }
        return result;
    }

    private F64Vector materializeDoubleValues(int size, Mask mask, FlatKeyLayout.Field field, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        F64Vector result = allocator.allocateOrGrow(allocationContext, (F64Vector) output, F64Vector.class, size, F64Vector::new);
        Arrays.fill(result.values(), 0);
        for (int index : mask) {
            if (index == nullGroup) {
                continue;
            }
            int recordIndex = recordIndex(index);
            if (recordIndex >= 0) {
                result.values()[index] = Double.longBitsToDouble(field.handler().readDoubleBits(fixedChunk(recordIndex), fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset()));
            }
        }
        return result;
    }

    private BinaryVector materializeBinaryValues(int size, Mask mask, FlatKeyLayout.Field field, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        long totalBytes = 0;
        for (int index : mask) {
            if (index == nullGroup) {
                continue;
            }
            int recordIndex = recordIndex(index);
            if (recordIndex >= 0) {
                totalBytes += field.handler().binaryLength(fixedChunk(recordIndex), fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset());
            }
        }
        if (totalBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Grouped binary output exceeds maximum byte capacity: " + totalBytes);
        }

        BinaryVector result = allocator.allocateOrGrowBinary(allocationContext, (BinaryVector) output, size, (int) totalBytes);
        Arrays.fill(result.offsets(), 0);
        result.clearTraits();
        result.addTraits(field.binaryTraits());

        int previousIndex = 0;
        for (int index : mask) {
            while (previousIndex < index) {
                result.setNull(previousIndex++);
            }
            if (index == nullGroup) {
                result.setNull(index);
            }
            else {
                int recordIndex = recordIndex(index);
                if (recordIndex >= 0) {
                    field.handler().copyBinaryTo(fixedChunk(recordIndex), fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset(), variableWidthArena, result, index);
                }
                else {
                    result.setNull(index);
                }
            }
            previousIndex = index + 1;
        }
        while (previousIndex < size) {
            result.setNull(previousIndex++);
        }
        return result;
    }

    private BooleanVector materializeNulls(int size, Mask mask, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector result = allocator.allocateOrGrow(allocationContext, (BooleanVector) output, BooleanVector.class, size, BooleanVector::new);
        Arrays.fill(result.values(), true);
        for (int index : mask) {
            result.values()[index] = index == nullGroup || recordIndex(index) < 0;
        }
        return result;
    }

    private int recordIndex(long groupId)
    {
        return groupId >= 0 && groupId < recordIndexByGroupId.length ? recordIndexByGroupId[(int) groupId] : -1;
    }

    private byte[] fixedChunk(int recordIndex)
    {
        int groupIndex = recordIndex >> RECORDS_PER_GROUP_SHIFT;
        byte[] chunk = fixedRecordChunks[groupIndex];
        if (chunk == null) {
            chunk = new byte[RECORDS_PER_GROUP * fixedRecordSize];
            fixedRecordChunks[groupIndex] = chunk;
        }
        return chunk;
    }

    private int fixedOffset(int recordIndex)
    {
        return (recordIndex & RECORDS_PER_GROUP_MASK) * fixedRecordSize;
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

        public Pointer append(Vector vector, int position)
        {
            int length = OperatorVectorSupport.binaryLength(vector, position);
            if (length == 0) {
                return new Pointer(chunkIndex, chunkOffset);
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
            switch (vector) {
                case BinaryVector binary -> System.arraycopy(binary.data(), binary.startOffset(position), chunk, offset, length);
                case org.weakref.nitro.data.DictionaryVector dictionary -> copy(dictionary.values(), dictionary.ids()[position], chunk, offset, length);
                case org.weakref.nitro.data.RleVector rle -> copy(rle.values(), OperatorVectorSupport.runIndex(rle, position), chunk, offset, length);
                default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
            }
            chunkOffset += length;
            return new Pointer(chunkIndex, offset);
        }

        public byte[] chunk(int index)
        {
            return chunks[index];
        }

        private void copy(Vector vector, int position, byte[] chunk, int offset, int length)
        {
            switch (vector) {
                case BinaryVector binary -> System.arraycopy(binary.data(), binary.startOffset(position), chunk, offset, length);
                case org.weakref.nitro.data.DictionaryVector dictionary -> copy(dictionary.values(), dictionary.ids()[position], chunk, offset, length);
                case org.weakref.nitro.data.RleVector rle -> copy(rle.values(), OperatorVectorSupport.runIndex(rle, position), chunk, offset, length);
                default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
            }
        }

        record Pointer(int chunkIndex, int chunkOffset)
        {
        }
    }
}
