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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

final class FlatTypeHandlers
{
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final FlatTypeHandler LONG = new FlatTypeHandler()
    {
        @Override
        public Kind kind()
        {
            return Kind.LONG;
        }

        @Override
        public int fixedSize()
        {
            return Long.BYTES;
        }

        @Override
        public boolean variableWidth()
        {
            return false;
        }

        @Override
        public long hashInput(Vector vector, int position)
        {
            return Long.hashCode(OperatorVectorSupport.longValue(vector, position));
        }

        @Override
        public void writeFlat(Vector vector, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena)
        {
            LONG_HANDLE.set(fixedChunk, fixedOffset, OperatorVectorSupport.longValue(vector, position));
        }

        @Override
        public boolean identicalFlatToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector vector, int position)
        {
            return ((long) LONG_HANDLE.get(fixedChunk, fixedOffset)) == OperatorVectorSupport.longValue(vector, position);
        }

        @Override
        public long readLong(byte[] fixedChunk, int fixedOffset)
        {
            return (long) LONG_HANDLE.get(fixedChunk, fixedOffset);
        }

        @Override
        public Vector materializeValues(FlatGroupingTable table, FlatKeyLayout.Field field, int size, Mask mask, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
        {
            I64Vector result = allocator.allocateOrGrow(allocationContext, (I64Vector) output, I64Vector.class, size, I64Vector::new);
            Arrays.fill(result.values(), 0);
            for (int index : mask) {
                if (index == nullGroup) {
                    continue;
                }
                int recordIndex = table.recordIndex(index);
                if (recordIndex >= 0) {
                    result.values()[index] = readLong(table.fixedChunk(recordIndex), table.fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset());
                }
            }
            return result;
        }
    };

    private static final FlatTypeHandler BOOLEAN = new FlatTypeHandler()
    {
        @Override
        public Kind kind()
        {
            return Kind.BOOLEAN;
        }

        @Override
        public int fixedSize()
        {
            return 1;
        }

        @Override
        public boolean variableWidth()
        {
            return false;
        }

        @Override
        public long hashInput(Vector vector, int position)
        {
            return Boolean.hashCode(OperatorVectorSupport.booleanValue(vector, position));
        }

        @Override
        public void writeFlat(Vector vector, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena)
        {
            fixedChunk[fixedOffset] = (byte) (OperatorVectorSupport.booleanValue(vector, position) ? 1 : 0);
        }

        @Override
        public boolean identicalFlatToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector vector, int position)
        {
            return (fixedChunk[fixedOffset] != 0) == OperatorVectorSupport.booleanValue(vector, position);
        }

        @Override
        public boolean readBoolean(byte[] fixedChunk, int fixedOffset)
        {
            return fixedChunk[fixedOffset] != 0;
        }

        @Override
        public Vector materializeValues(FlatGroupingTable table, FlatKeyLayout.Field field, int size, Mask mask, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
        {
            BooleanVector result = allocator.allocateOrGrow(allocationContext, (BooleanVector) output, BooleanVector.class, size, BooleanVector::new);
            Arrays.fill(result.values(), false);
            for (int index : mask) {
                if (index == nullGroup) {
                    continue;
                }
                int recordIndex = table.recordIndex(index);
                if (recordIndex >= 0) {
                    result.values()[index] = readBoolean(table.fixedChunk(recordIndex), table.fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset());
                }
            }
            return result;
        }
    };

    private static final FlatTypeHandler DOUBLE = new FlatTypeHandler()
    {
        @Override
        public Kind kind()
        {
            return Kind.DOUBLE;
        }

        @Override
        public int fixedSize()
        {
            return Long.BYTES;
        }

        @Override
        public boolean variableWidth()
        {
            return false;
        }

        @Override
        public long hashInput(Vector vector, int position)
        {
            long bits = Double.doubleToLongBits(OperatorVectorSupport.doubleValue(vector, position));
            return Long.hashCode(bits);
        }

        @Override
        public void writeFlat(Vector vector, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena)
        {
            LONG_HANDLE.set(fixedChunk, fixedOffset, Double.doubleToLongBits(OperatorVectorSupport.doubleValue(vector, position)));
        }

        @Override
        public boolean identicalFlatToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector vector, int position)
        {
            return ((long) LONG_HANDLE.get(fixedChunk, fixedOffset)) == Double.doubleToLongBits(OperatorVectorSupport.doubleValue(vector, position));
        }

        @Override
        public long readDoubleBits(byte[] fixedChunk, int fixedOffset)
        {
            return (long) LONG_HANDLE.get(fixedChunk, fixedOffset);
        }

        @Override
        public Vector materializeValues(FlatGroupingTable table, FlatKeyLayout.Field field, int size, Mask mask, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
        {
            F64Vector result = allocator.allocateOrGrow(allocationContext, (F64Vector) output, F64Vector.class, size, F64Vector::new);
            Arrays.fill(result.values(), 0);
            for (int index : mask) {
                if (index == nullGroup) {
                    continue;
                }
                int recordIndex = table.recordIndex(index);
                if (recordIndex >= 0) {
                    result.values()[index] = Double.longBitsToDouble(readDoubleBits(table.fixedChunk(recordIndex), table.fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset()));
                }
            }
            return result;
        }
    };

    private static final FlatTypeHandler BINARY = new FlatTypeHandler()
    {
        @Override
        public Kind kind()
        {
            return Kind.BINARY;
        }

        @Override
        public int fixedSize()
        {
            return Integer.BYTES * 3;
        }

        @Override
        public boolean variableWidth()
        {
            return true;
        }

        @Override
        public long hashInput(Vector vector, int position)
        {
            return hashBinary(vector, position);
        }

        @Override
        public void writeFlat(Vector vector, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena)
        {
            writeBinaryFlat(vector, position, fixedChunk, fixedOffset, variableWidthArena);
        }

        @Override
        public boolean identicalFlatToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector vector, int position)
        {
            int length = binaryLength(fixedChunk, fixedOffset);
            byte[] chunk = variableWidthArena.chunk(readChunkIndex(fixedChunk, fixedOffset));
            int offset = readChunkOffset(fixedChunk, fixedOffset);
            return binaryEquals(vector, position, chunk, offset, length);
        }

        @Override
        public int binaryLength(byte[] fixedChunk, int fixedOffset)
        {
            return (int) INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES * 2);
        }

        @Override
        public void copyBinaryTo(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, BinaryVector output, int outputPosition)
        {
            int length = binaryLength(fixedChunk, fixedOffset);
            byte[] chunk = variableWidthArena.chunk(readChunkIndex(fixedChunk, fixedOffset));
            int offset = readChunkOffset(fixedChunk, fixedOffset);
            output.setBytes(outputPosition, chunk, offset, length);
        }

        private int readChunkIndex(byte[] fixedChunk, int fixedOffset)
        {
            return (int) INT_HANDLE.get(fixedChunk, fixedOffset);
        }

        private int readChunkOffset(byte[] fixedChunk, int fixedOffset)
        {
            return (int) INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES);
        }

        private long hashBinary(Vector vector, int position)
        {
            return switch (vector) {
                case BinaryVector binary -> OperatorVectorSupport.binaryHash(binary.data(), binary.startOffset(position), binary.length(position));
                case DictionaryVector dictionary -> hashBinary(dictionary.values(), dictionary.ids()[position]);
                case RleVector rle -> hashBinary(rle.values(), OperatorVectorSupport.runIndex(rle, position));
                default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
            };
        }

        private boolean binaryEquals(Vector vector, int position, byte[] right, int rightOffset, int rightLength)
        {
            return switch (vector) {
                case BinaryVector binary -> binary.length(position) == rightLength &&
                        OperatorVectorSupport.binaryEquals(binary.data(), binary.startOffset(position), right, rightOffset, rightLength);
                case DictionaryVector dictionary -> binaryEquals(dictionary.values(), dictionary.ids()[position], right, rightOffset, rightLength);
                case RleVector rle -> binaryEquals(rle.values(), OperatorVectorSupport.runIndex(rle, position), right, rightOffset, rightLength);
                default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
            };
        }

        private void writeBinaryFlat(Vector vector, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena)
        {
            switch (vector) {
                case BinaryVector binary -> writeFlat(binary.data(), binary.startOffset(position), binary.length(position), fixedChunk, fixedOffset, variableWidthArena);
                case DictionaryVector dictionary -> writeBinaryFlat(dictionary.values(), dictionary.ids()[position], fixedChunk, fixedOffset, variableWidthArena);
                case RleVector rle -> writeBinaryFlat(rle.values(), OperatorVectorSupport.runIndex(rle, position), fixedChunk, fixedOffset, variableWidthArena);
                default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
            }
        }

        private void writeFlat(byte[] source, int sourceOffset, int length, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena)
        {
            long pointer = variableWidthArena.append(source, sourceOffset, length);
            INT_HANDLE.set(fixedChunk, fixedOffset, FlatGroupingTable.FlatVariableWidthArena.chunkIndex(pointer));
            INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES, FlatGroupingTable.FlatVariableWidthArena.chunkOffset(pointer));
            INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES * 2, length);
        }

        @Override
        public Vector materializeValues(FlatGroupingTable table, FlatKeyLayout.Field field, int size, Mask mask, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
        {
            long totalBytes = 0;
            for (int index : mask) {
                if (index == nullGroup) {
                    continue;
                }
                int recordIndex = table.recordIndex(index);
                if (recordIndex >= 0) {
                    totalBytes += binaryLength(table.fixedChunk(recordIndex), table.fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset());
                }
            }
            if (totalBytes > Integer.MAX_VALUE) {
                throw new IllegalStateException("Grouped binary output exceeds maximum byte capacity: " + totalBytes);
            }

            BinaryVector result = BinaryVector.allocateOrGrow(allocator, allocationContext, (BinaryVector) output, size, (int) totalBytes);
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
                    int recordIndex = table.recordIndex(index);
                    if (recordIndex >= 0) {
                        copyBinaryTo(table.fixedChunk(recordIndex), table.fixedOffset(recordIndex) + Long.BYTES + field.fixedOffset(), table.variableWidthArena(), result, index);
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
    };

    private FlatTypeHandlers() {}

    public static FlatTypeHandler forVector(Vector vector)
    {
        return switch (OperatorVectorSupport.flatten(vector)) {
            case I32Vector _, I64Vector _ -> LONG;
            case BooleanVector _ -> BOOLEAN;
            case F64Vector _ -> DOUBLE;
            case BinaryVector _ -> BINARY;
            default -> null;
        };
    }
}
