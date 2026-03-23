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
import org.weakref.nitro.data.Vector;

import java.util.Set;

final class FlatKeyLayout
{
    private final Field[] fields;
    private final int[] inputChannels;
    private final FlatTypeHandler[] handlers;
    private final int[] fixedOffsets;
    private final boolean singleField;
    private final int singleInputChannel;
    private final FlatTypeHandler singleHandler;
    private final int singleFixedOffset;
    private final int fixedRecordSize;
    private final boolean anyVariableWidth;

    private FlatKeyLayout(Field[] fields, int[] inputChannels, FlatTypeHandler[] handlers, int[] fixedOffsets, int fixedRecordSize, boolean anyVariableWidth)
    {
        this.fields = fields;
        this.inputChannels = inputChannels;
        this.handlers = handlers;
        this.fixedOffsets = fixedOffsets;
        this.singleField = handlers.length == 1;
        this.singleInputChannel = singleField ? inputChannels[0] : -1;
        this.singleHandler = singleField ? handlers[0] : null;
        this.singleFixedOffset = singleField ? fixedOffsets[0] : -1;
        this.fixedRecordSize = fixedRecordSize;
        this.anyVariableWidth = anyVariableWidth;
    }

    public static FlatKeyLayout tryCreate(Vector[] values)
    {
        Field[] fields = new Field[values.length];
        int[] inputChannels = new int[values.length];
        FlatTypeHandler[] handlers = new FlatTypeHandler[values.length];
        int[] fixedOffsets = new int[values.length];
        int fixedOffset = 0;
        boolean anyVariableWidth = false;
        for (int index = 0; index < values.length; index++) {
            FlatTypeHandler handler = FlatTypeHandlers.forVector(values[index]);
            if (handler == null) {
                return null;
            }
            fields[index] = new Field(index, handler, fixedOffset, OperatorVectorSupport.binaryTraits(values[index]));
            inputChannels[index] = index;
            handlers[index] = handler;
            fixedOffsets[index] = fixedOffset;
            fixedOffset += handler.fixedSize();
            anyVariableWidth |= handler.variableWidth();
        }
        return new FlatKeyLayout(fields, inputChannels, handlers, fixedOffsets, fixedOffset, anyVariableWidth);
    }

    public int fieldCount()
    {
        return fields.length;
    }

    public int fixedRecordSize()
    {
        return fixedRecordSize;
    }

    public boolean anyVariableWidth()
    {
        return anyVariableWidth;
    }

    public Field field(int index)
    {
        return fields[index];
    }

    public long hash(Vector[] values, int position)
    {
        if (singleField) {
            return 31 + singleHandler.hashInput(values[singleInputChannel], position);
        }
        long result = 1;
        for (int index = 0; index < handlers.length; index++) {
            result = 31 * result + handlers[index].hashInput(values[inputChannels[index]], position);
        }
        return result;
    }

    public void writeRecord(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, int position)
    {
        if (singleField) {
            singleHandler.writeFlat(values[singleInputChannel], position, fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena);
            return;
        }
        for (int index = 0; index < handlers.length; index++) {
            handlers[index].writeFlat(values[inputChannels[index]], position, fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena);
        }
    }

    public boolean identicalRecordToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, int position)
    {
        if (singleField) {
            return singleHandler.identicalFlatToInput(fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena, values[singleInputChannel], position);
        }
        for (int index = 0; index < handlers.length; index++) {
            if (!handlers[index].identicalFlatToInput(fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena, values[inputChannels[index]], position)) {
                return false;
            }
        }
        return true;
    }

    public record Field(int inputChannel, FlatTypeHandler handler, int fixedOffset, Set<BinaryVector.Trait> binaryTraits)
    {
    }
}
