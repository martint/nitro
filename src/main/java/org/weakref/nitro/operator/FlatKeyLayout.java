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
    private final int fixedRecordSize;
    private final boolean anyVariableWidth;

    private FlatKeyLayout(Field[] fields, int fixedRecordSize, boolean anyVariableWidth)
    {
        this.fields = fields;
        this.fixedRecordSize = fixedRecordSize;
        this.anyVariableWidth = anyVariableWidth;
    }

    public static FlatKeyLayout tryCreate(Vector[] values)
    {
        Field[] fields = new Field[values.length];
        int fixedOffset = 0;
        boolean anyVariableWidth = false;
        for (int index = 0; index < values.length; index++) {
            FlatTypeHandler handler = FlatTypeHandlers.forVector(values[index]);
            if (handler == null) {
                return null;
            }
            fields[index] = new Field(index, handler, fixedOffset, OperatorVectorSupport.binaryTraits(values[index]));
            fixedOffset += handler.fixedSize();
            anyVariableWidth |= handler.variableWidth();
        }
        return new FlatKeyLayout(fields, fixedOffset, anyVariableWidth);
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
        long result = 1;
        for (Field field : fields) {
            result = 31 * result + field.handler().hashInput(values[field.inputChannel()], position);
        }
        return result;
    }

    public void writeRecord(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, int position)
    {
        for (Field field : fields) {
            field.handler().writeFlat(values[field.inputChannel()], position, fixedChunk, fixedOffset + field.fixedOffset(), variableWidthArena);
        }
    }

    public boolean identicalRecordToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, int position)
    {
        for (Field field : fields) {
            if (!field.handler().identicalFlatToInput(fixedChunk, fixedOffset + field.fixedOffset(), variableWidthArena, values[field.inputChannel()], position)) {
                return false;
            }
        }
        return true;
    }

    public record Field(int inputChannel, FlatTypeHandler handler, int fixedOffset, Set<BinaryVector.Trait> binaryTraits)
    {
    }
}
