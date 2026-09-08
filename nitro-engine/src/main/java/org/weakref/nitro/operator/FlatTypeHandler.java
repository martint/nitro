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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.List;
import java.util.Set;

interface FlatTypeHandler
{
    enum Kind
    {
        LONG,
        BOOLEAN,
        DOUBLE,
        BINARY,
        CANONICAL,
    }

    Kind kind();

    int fixedSize();

    boolean variableWidth();

    long hashInput(Vector vector, int position);

    void writeFlat(Vector vector, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena);

    boolean identicalFlatToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector vector, int position);

    default long retainedBytes()
    {
        return 0;
    }

    default void writeLong(byte[] fixedChunk, int fixedOffset, long value)
    {
        throw new UnsupportedOperationException();
    }

    default long readLong(byte[] fixedChunk, int fixedOffset)
    {
        throw new UnsupportedOperationException();
    }

    default boolean readBoolean(byte[] fixedChunk, int fixedOffset)
    {
        throw new UnsupportedOperationException();
    }

    default long readDoubleBits(byte[] fixedChunk, int fixedOffset)
    {
        throw new UnsupportedOperationException();
    }

    default int binaryLength(byte[] fixedChunk, int fixedOffset)
    {
        throw new UnsupportedOperationException();
    }

    default void copyBinaryTo(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, BinaryVector output, int outputPosition)
    {
        throw new UnsupportedOperationException();
    }

    default Vector materializeValues(FlatGroupingTable table, FlatKeyLayout.Field field, int fieldIndex, int size, Mask mask, long nullGroup, Vector output, Allocator allocator, Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException();
    }

    default Vector copyFlatValue(
            FlatKeyLayout.Field field,
            byte[] fixedChunk,
            int fixedOffset,
            FlatGroupingTable.FlatVariableWidthArena variableWidthArena,
            Vector output,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException();
    }

    default OperatorKeySemantics.Key reusableProbeKey()
    {
        throw new UnsupportedOperationException();
    }

    default OperatorKeySemantics.Key probeKey(Vector values, int position, OperatorKeySemantics.Key reusable)
    {
        throw new UnsupportedOperationException();
    }

    default OperatorKeySemantics.Key ownedKey(OperatorKeySemantics.Key key)
    {
        throw new UnsupportedOperationException();
    }

    default Streams materializeFallbackValues(int size, Mask mask, List<OperatorKeySemantics.Key> keysByGroup, Vector output, Allocator allocator, Allocator.Context allocationContext, Set<BinaryVector.Trait> binaryTraits)
    {
        throw new UnsupportedOperationException();
    }
}
