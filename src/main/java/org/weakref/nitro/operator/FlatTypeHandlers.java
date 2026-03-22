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

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

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
            return OperatorVectorSupport.binaryHash(vector, position);
        }

        @Override
        public void writeFlat(Vector vector, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena)
        {
            int length = OperatorVectorSupport.binaryLength(vector, position);
            FlatGroupingTable.FlatVariableWidthArena.Pointer pointer = variableWidthArena.append(vector, position);
            INT_HANDLE.set(fixedChunk, fixedOffset, pointer.chunkIndex());
            INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES, pointer.chunkOffset());
            INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES * 2, length);
        }

        @Override
        public boolean identicalFlatToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector vector, int position)
        {
            int length = binaryLength(fixedChunk, fixedOffset);
            byte[] chunk = variableWidthArena.chunk(readChunkIndex(fixedChunk, fixedOffset));
            int offset = readChunkOffset(fixedChunk, fixedOffset);
            return OperatorVectorSupport.binaryEquals(vector, position, chunk, offset, length);
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
