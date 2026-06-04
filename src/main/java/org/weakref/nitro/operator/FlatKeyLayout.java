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
import org.weakref.nitro.data.DictionaryVector;
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

    // Per-batch hash cache for dictionary-encoded key columns. For each key column that arrives as a
    // DictionaryVector, the hash of every distinct dictionary entry is computed once at beginBatch and
    // indexed by dictionary id; the per-position hash() then reads it through ids[position] instead of
    // re-hashing the underlying (often variable-width) value on every probe. This only accelerates
    // hash COMPUTATION — equality is still settled by identicalRecordToInput against the stored value,
    // so two distinct values can never collapse into one group regardless of dictionary contents.
    private int[][] dictionaryHashedIds;
    private long[][] dictionaryEntryHashes;
    private Vector[] dictionaryHashedValues;

    // Id-based equality for dictionary-encoded key columns. Each field binds to the first dictionary
    // identity it sees; while a batch presents that same dictionary instance for the field, the record
    // stores the dictionary id and equality compares ids directly instead of the underlying bytes. A
    // record written under the bound dictionary stores id >= 0; any record written when the field was
    // not id-comparable stores -1 and always falls back to value comparison. Because an id is only ever
    // compared against another id from the SAME bound dictionary identity, two distinct values can never
    // be treated as equal, and equal values reached through a different dictionary instance fall back to
    // the value path. The stored value is always retained for that fallback and for materialization.
    private Vector[] boundDictionary;
    private int[][] batchDictionaryIds;
    private boolean[] fieldIdComparable;
    private boolean anyFieldIdComparable;
    private int[][] recordDictionaryIds;

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
    public void beginBatch(Vector[] values, Vector[] nulls)
    {
        if (dictionaryHashedIds == null) {
            dictionaryHashedIds = new int[handlers.length][];
            dictionaryEntryHashes = new long[handlers.length][];
            dictionaryHashedValues = new Vector[handlers.length];
            boundDictionary = new Vector[handlers.length];
            batchDictionaryIds = new int[handlers.length][];
            fieldIdComparable = new boolean[handlers.length];
        }
        anyFieldIdComparable = false;
        for (int index = 0; index < handlers.length; index++) {
            fieldIdComparable[index] = false;
            batchDictionaryIds[index] = null;
            int channel = inputChannels[index];
            if (channel >= values.length || !(values[channel] instanceof DictionaryVector dictionary)) {
                dictionaryHashedIds[index] = null;
                dictionaryEntryHashes[index] = null;
                dictionaryHashedValues[index] = null;
                continue;
            }
            Vector dictionaryValues = dictionary.values();
            dictionaryHashedIds[index] = dictionary.ids();
            // The dictionary base is shared across grouping-set expansions and probe batches, so its
            // per-entry hashes are computed once per distinct dictionary identity and reused. Index by
            // the dictionary id at hash() time instead of re-hashing the (variable-width) value.
            if (dictionaryHashedValues[index] != dictionaryValues || dictionaryEntryHashes[index] == null) {
                int distinctCount = dictionaryValues.length();
                FlatTypeHandler handler = handlers[index];
                long[] entryHashes = new long[distinctCount];
                for (int id = 0; id < distinctCount; id++) {
                    entryHashes[id] = handler.hashInput(dictionaryValues, id);
                }
                dictionaryEntryHashes[index] = entryHashes;
                dictionaryHashedValues[index] = dictionaryValues;
            }

            // Bind the field to the first dictionary identity it sees; id-based equality only applies
            // for variable-width fields (the byte-compare those would otherwise pay is what we avoid).
            if (handlers[index].variableWidth()) {
                if (boundDictionary[index] == null) {
                    boundDictionary[index] = dictionaryValues;
                }
                if (boundDictionary[index] == dictionaryValues) {
                    fieldIdComparable[index] = true;
                    batchDictionaryIds[index] = dictionary.ids();
                    anyFieldIdComparable = true;
                }
            }
        }
    }

    /**
     * Hook called by {@link FlatGroupingTable} after a batch completes. Mirror of
     * {@link #beginBatch}; subclasses release cached references here.
     */
    public void endBatch()
    {
        if (dictionaryHashedIds == null) {
            return;
        }
        // Release only the per-batch id arrays; the per-dictionary entry-hash caches are keyed by the
        // shared dictionary identity and intentionally survive across batches.
        for (int index = 0; index < dictionaryHashedIds.length; index++) {
            dictionaryHashedIds[index] = null;
        }
    }

    public long hash(Vector[] values, Vector[] nulls, int position)
    {
        if (singleField) {
            if (fieldNull(nulls, singleInputChannel, position)) {
                return 31;
            }
            return 31 + fieldHash(0, singleInputChannel, values[singleInputChannel], position);
        }
        long result = 1;
        for (int index = 0; index < handlers.length; index++) {
            if (fieldNull(nulls, inputChannels[index], position)) {
                result = 31 * result + 1;
            }
            else {
                result = 31 * result + fieldHash(index, inputChannels[index], values[inputChannels[index]], position);
            }
        }
        return result;
    }

    private long fieldHash(int fieldIndex, int channel, Vector value, int position)
    {
        if (dictionaryHashedIds != null) {
            int[] ids = dictionaryHashedIds[fieldIndex];
            if (ids != null) {
                return dictionaryEntryHashes[fieldIndex][ids[position]];
            }
        }
        return handlers[fieldIndex].hashInput(value, position);
    }

    public void writeRecord(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
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
            storeRecordDictionaryIds(recordIndex, position);
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
        storeRecordDictionaryIds(recordIndex, position);
    }

    public boolean identicalRecordToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
    {
        if (singleField) {
            if (fieldNull(nulls, singleInputChannel, position)) {
                return isNull(fixedChunk, fixedOffset, 0);
            }
            if (isNull(fixedChunk, fixedOffset, 0)) {
                return false;
            }
            if (idComparable(0, recordIndex)) {
                return recordDictionaryIds[0][recordIndex] == batchDictionaryIds[0][position];
            }
            return singleHandler.identicalFlatToInput(fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena, values[singleInputChannel], position);
        }
        for (int index : comparisonOrder) {
            boolean inputNull = fieldNull(nulls, inputChannels[index], position);
            boolean recordNull = isNull(fixedChunk, fixedOffset, index);
            if (inputNull != recordNull) {
                return false;
            }
            if (inputNull) {
                continue;
            }
            if (idComparable(index, recordIndex)) {
                if (recordDictionaryIds[index][recordIndex] != batchDictionaryIds[index][position]) {
                    return false;
                }
                continue;
            }
            if (!handlers[index].identicalFlatToInput(fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena, values[inputChannels[index]], position)) {
                return false;
            }
        }
        return true;
    }

    private boolean idComparable(int fieldIndex, int recordIndex)
    {
        return anyFieldIdComparable
                && fieldIdComparable[fieldIndex]
                && recordDictionaryIds != null
                && recordDictionaryIds[fieldIndex] != null
                && recordIndex < recordDictionaryIds[fieldIndex].length
                && recordDictionaryIds[fieldIndex][recordIndex] >= 0;
    }

    private void storeRecordDictionaryIds(int recordIndex, int position)
    {
        if (!anyFieldIdComparable) {
            return;
        }
        ensureRecordDictionaryIdCapacity(recordIndex + 1);
        for (int index = 0; index < handlers.length; index++) {
            if (recordDictionaryIds[index] == null) {
                continue;
            }
            recordDictionaryIds[index][recordIndex] = fieldIdComparable[index] ? batchDictionaryIds[index][position] : -1;
        }
    }

    private void ensureRecordDictionaryIdCapacity(int required)
    {
        if (recordDictionaryIds == null) {
            recordDictionaryIds = new int[handlers.length][];
        }
        for (int index = 0; index < handlers.length; index++) {
            // Allocate per-record id storage lazily, only for fields that have ever been id-comparable.
            if (boundDictionary[index] == null || !handlers[index].variableWidth()) {
                continue;
            }
            if (recordDictionaryIds[index] == null) {
                int initial = Math.max(16, required);
                int[] ids = new int[initial];
                Arrays.fill(ids, -1);
                recordDictionaryIds[index] = ids;
            }
            else if (recordDictionaryIds[index].length < required) {
                int previousLength = recordDictionaryIds[index].length;
                int newLength = previousLength;
                while (newLength < required) {
                    newLength *= 2;
                }
                int[] ids = Arrays.copyOf(recordDictionaryIds[index], newLength);
                Arrays.fill(ids, previousLength, newLength, -1);
                recordDictionaryIds[index] = ids;
            }
        }
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
