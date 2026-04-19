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

import java.util.Arrays;
import java.util.Set;

class FlatKeyLayout
{
    private final Field[] fields;
    private final int[] inputChannels;
    private final FlatTypeHandler[] handlers;
    private final int[] fixedOffsets;
    private final int[] comparisonOrder;
    private final int nullByteCount;
    private final boolean singleField;
    private final int singleInputChannel;
    private final FlatTypeHandler singleHandler;
    private final int singleFixedOffset;
    private final int fixedRecordSize;
    private final boolean anyVariableWidth;

    FlatKeyLayout(Field[] fields, int[] inputChannels, FlatTypeHandler[] handlers, int[] fixedOffsets, int[] comparisonOrder, int nullByteCount, int fixedRecordSize, boolean anyVariableWidth)
    {
        this.fields = fields;
        this.inputChannels = inputChannels;
        this.handlers = handlers;
        this.fixedOffsets = fixedOffsets;
        this.comparisonOrder = comparisonOrder;
        this.nullByteCount = nullByteCount;
        this.singleField = handlers.length == 1;
        this.singleInputChannel = singleField ? inputChannels[0] : -1;
        this.singleHandler = singleField ? handlers[0] : null;
        this.singleFixedOffset = singleField ? fixedOffsets[0] : -1;
        this.fixedRecordSize = fixedRecordSize;
        this.anyVariableWidth = anyVariableWidth;
    }

    public static FlatKeyLayout tryCreate(Vector[] values, boolean nullable)
    {
        Field[] fields = new Field[values.length];
        int[] inputChannels = new int[values.length];
        FlatTypeHandler[] handlers = new FlatTypeHandler[values.length];
        int[] fixedOffsets = new int[values.length];
        int nullByteCount = nullable ? Math.max(1, (values.length + Byte.SIZE - 1) / Byte.SIZE) : 0;
        int fixedOffset = nullByteCount;
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
        return new FlatKeyLayout(fields, inputChannels, handlers, fixedOffsets, comparisonOrder(handlers), nullByteCount, fixedOffset, anyVariableWidth);
    }

    public static FlatKeyLayout tryCreate(Vector[] values)
    {
        return tryCreate(values, false);
    }

    public int fieldCount()
    {
        return fields.length;
    }

    public int fixedRecordSize()
    {
        return fixedRecordSize;
    }

    public int nullByteCount()
    {
        return nullByteCount;
    }

    public boolean anyVariableWidth()
    {
        return anyVariableWidth;
    }

    public Field field(int index)
    {
        return fields[index];
    }

    /**
     * Hook called by {@link FlatGroupingTable} / {@link HashJoinOperator.FlatJoinIndex} at the
     * start of a batch, before a sequence of per-position {@link #hash}/{@link #writeRecord}/
     * {@link #identicalRecordToInput} calls over the same {@code values}/{@code nulls} arrays.
     * Specializing subclasses override this to hoist typed accessors (e.g. extract the
     * {@code long[]} from a flat {@link org.weakref.nitro.data.I64Vector}) so that the per-position
     * hot methods can read directly without a type dispatch. The architectural rule is that Vector
     * type checks must be done here, outside the per-position probe loop, and the per-position
     * methods must stay tight (no dispatch, no branching on Vector shape).
     *
     * <p>Default implementation is a no-op; the base {@link #hash}/{@link #writeRecord}/
     * {@link #identicalRecordToInput} keep their per-position {@link FlatTypeHandler} dispatch for
     * any layout that hasn't chosen to specialize.
     */
    public void beginBatch(Vector[] values, Vector[] nulls) {}

    /**
     * Hook called by {@link FlatGroupingTable} after a batch completes. Mirror of
     * {@link #beginBatch}; subclasses release cached references here.
     */
    public void endBatch() {}

    public long hash(Vector[] values, Vector[] nulls, int position)
    {
        if (singleField) {
            if (fieldNull(nulls, singleInputChannel, position)) {
                return 31;
            }
            return 31 + singleHandler.hashInput(values[singleInputChannel], position);
        }
        long result = 1;
        for (int index = 0; index < handlers.length; index++) {
            if (fieldNull(nulls, inputChannels[index], position)) {
                result = 31 * result + 1;
            }
            else {
                result = 31 * result + handlers[index].hashInput(values[inputChannels[index]], position);
            }
        }
        return result;
    }

    public void writeRecord(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position)
    {
        if (nullByteCount > 0) {
            Arrays.fill(fixedChunk, fixedOffset, fixedOffset + nullByteCount, (byte) 0);
        }
        if (singleField) {
            if (fieldNull(nulls, singleInputChannel, position)) {
                fixedChunk[fixedOffset] = 1;
            }
            else {
                singleHandler.writeFlat(values[singleInputChannel], position, fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena);
            }
            return;
        }
        for (int index = 0; index < handlers.length; index++) {
            if (fieldNull(nulls, inputChannels[index], position)) {
                setNullBit(fixedChunk, fixedOffset, index);
            }
            else {
                handlers[index].writeFlat(values[inputChannels[index]], position, fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena);
            }
        }
    }

    public boolean identicalRecordToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position)
    {
        if (singleField) {
            if (fieldNull(nulls, singleInputChannel, position)) {
                return isNull(fixedChunk, fixedOffset, 0);
            }
            if (isNull(fixedChunk, fixedOffset, 0)) {
                return false;
            }
            return singleHandler.identicalFlatToInput(fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena, values[singleInputChannel], position);
        }
        for (int index : comparisonOrder) {
            boolean inputNull = fieldNull(nulls, inputChannels[index], position);
            boolean recordNull = isNull(fixedChunk, fixedOffset, index);
            if (inputNull != recordNull) {
                return false;
            }
            if (!inputNull && !handlers[index].identicalFlatToInput(fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena, values[inputChannels[index]], position)) {
                return false;
            }
        }
        return true;
    }

    public boolean fieldNull(byte[] fixedChunk, int fixedOffset, int fieldIndex)
    {
        return isNull(fixedChunk, fixedOffset, fieldIndex);
    }

    private static boolean fieldNull(Vector[] nulls, int inputChannel, int position)
    {
        return nulls != null && nulls.length > inputChannel && OperatorVectorSupport.isNull(nulls[inputChannel], position);
    }

    private static int[] comparisonOrder(FlatTypeHandler[] handlers)
    {
        int[] order = new int[handlers.length];
        int next = 0;
        for (int index = 0; index < handlers.length; index++) {
            if (!handlers[index].variableWidth()) {
                order[next++] = index;
            }
        }
        for (int index = 0; index < handlers.length; index++) {
            if (handlers[index].variableWidth()) {
                order[next++] = index;
            }
        }
        return order;
    }

    private void setNullBit(byte[] fixedChunk, int fixedOffset, int fieldIndex)
    {
        fixedChunk[fixedOffset + fieldIndex / Byte.SIZE] |= (byte) (1 << (fieldIndex % Byte.SIZE));
    }

    private boolean isNull(byte[] fixedChunk, int fixedOffset, int fieldIndex)
    {
        if (nullByteCount == 0) {
            return false;
        }
        return (fixedChunk[fixedOffset + fieldIndex / Byte.SIZE] & (1 << (fieldIndex % Byte.SIZE))) != 0;
    }

    public record Field(int inputChannel, FlatTypeHandler handler, int fixedOffset, Set<BinaryVector.Trait> binaryTraits)
    {
    }
}
