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
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Set;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

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

    // Per-batch null-free flags: true for a key field whose null stream is all-false this batch, so the per-row
    // input null read (a megamorphic OperatorVectorSupport.isNull) is skipped. The layout stays nullable (a later
    // batch may carry nulls), but a batch of declared-nullable-yet-null-free keys -- the common case, since the
    // Parquet writer marks columns optional even when they are semantically NOT NULL -- pays no per-row null check.
    private boolean[] batchFieldNullFree;

    // Global value-id equality for variable-width dictionary keys (Velox VectorHasher technique). Each field's
    // interner maps every distinct value to a dense id that is stable across batches AND across per-page
    // dictionary identities -- unlike the dictionary-identity binding this replaces, whose ids were valid only
    // within one dictionary instance, so a persistent group table fell back to byte comparison on every later
    // batch. The record stores the GLOBAL id and equality compares ids; a value that overflows the interner
    // ceiling (or a non-dictionary input) stores/sees id -1 and falls back to value comparison, which is always
    // available because the value itself is still stored. batchEntryGlobalId[field][dictId] is this batch's
    // dictionary entry -> global id map, cached by dictionary identity in batchEntryGlobalIdDict.
    // Interning a string/binary key column to a dense id only pays when the column is low-cardinality enough that
    // ids are reused: the dense-id grouping then beats per-row byte comparison. A high-cardinality key (distinct
    // count approaching the row/group count, e.g. customer name/address) gets no reuse, so interning is pure
    // overhead vs the value-comparison fallback. Overflow such columns early: above this ceiling the field drops to
    // value comparison. 65536 keeps low/mid-card keys (categories, dimension names, and keys whose interned ids
    // still enable the compact/array-mode group path) interned, while overflowing genuinely wide keys. Measured:
    // TPC-H q10 7031->3448ms (2.0x), no regression on q18/q03/q67/q65/q37; a lower ceiling (1<<14) regressed q18/q03.
    private static final int VALUE_ID_CEILING = Integer.getInteger("nitro.group.valueIdCeiling", 1 << 16);
    private ValueIdInterner[] fieldInterners;
    private int[][] batchEntryGlobalId;
    private Vector[] batchEntryGlobalIdDict;

    // Composite value-id for array-mode grouping (Velox's normalized-key technique): when every key field is an
    // interned, null-free, low-cardinality dictionary column, this batch's per-field global ids pack into a
    // single composite id (id0 + id1*STRIDE + …) that uniquely identifies the key and stays stable across
    // batches. FlatGroupingTable indexes a direct array by it, skipping the hash + probe + record machinery
    // entirely; out-of-range composites (a field above STRIDE distinct, too many fields, or a nullable/non-dict
    // batch) yield -1 and fall back to the hash table. STRIDE bounds the per-field cardinality so the packing is
    // a bijection; COMPOSITE_MAX bounds the indexable range.
    private static final int COMPOSITE_STRIDE = 1 << 10;
    private static final long COMPOSITE_MAX = 1 << 20;
    private boolean batchCompositeEligible;

    // Per-field concrete handler kind, resolved once at construction. The per-position hot methods switch on this
    // (a tableswitch) and call the concrete handler singleton (monomorphic, inlined) instead of dispatching through
    // handlers[index] (a megamorphic FlatTypeHandler[] whose 4 anonymous element types defeat inlining and mispredict
    // per field per row).
    private final FlatTypeHandler.Kind[] fieldKinds;

    // Layer-2 monomorphization: resolve each field's Vector shape ONCE per batch in beginBatch into a typed accessor,
    // so the per-position hot methods read the value with no Vector-type dispatch (the second megamorphic layer,
    // inside the handlers' longValue/binary access). LONG fields use a VectorAccess.LongValues accessor (resolves
    // flat/dict/rle); BINARY fields resolve to a base BinaryVector (final -> monomorphic) plus a dictionary id map
    // (null when flat). batchAccessorsReady is set only after beginBatch resolves them and cleared in endBatch: some
    // callers (DistinctKeySet / MarkDistinctOperator) drive FlatGroupingTable WITHOUT a beginBatch, so the hot methods
    // fall back to the layer-1 handler-kind switch when the accessors are not current.
    private static final VarHandle GROUP_LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final VarHandle GROUP_INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    private boolean batchAccessorsReady;
    private VectorAccess.LongValues[] fieldLong;
    private BinaryVector[] fieldBinaryBase;
    private int[][] fieldBinaryIds;
    private VectorAccess.BooleanValues[] fieldNullAccess;

    FlatKeyLayout(Field[] fields, int[] inputChannels, FlatTypeHandler[] handlers, int[] fixedOffsets, int[] comparisonOrder, int nullByteCount, int fixedRecordSize, boolean anyVariableWidth)
    {
        this.fields = fields;
        this.inputChannels = inputChannels;
        this.handlers = handlers;
        this.fieldKinds = new FlatTypeHandler.Kind[handlers.length];
        for (int index = 0; index < handlers.length; index++) {
            this.fieldKinds[index] = handlers[index].kind();
        }
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
            batchFieldNullFree = new boolean[handlers.length];
            fieldInterners = new ValueIdInterner[handlers.length];
            batchEntryGlobalId = new int[handlers.length][];
            batchEntryGlobalIdDict = new Vector[handlers.length];
            fieldLong = new VectorAccess.LongValues[handlers.length];
            fieldBinaryBase = new BinaryVector[handlers.length];
            fieldBinaryIds = new int[handlers.length][];
            fieldNullAccess = new VectorAccess.BooleanValues[handlers.length];
        }
        anyFieldIdComparable = false;
        for (int index = 0; index < handlers.length; index++) {
            fieldIdComparable[index] = false;
            batchDictionaryIds[index] = null;
            int channel = inputChannels[index];
            batchFieldNullFree[index] = VectorAccess.isAllFalseNulls((nulls != null && channel < nulls.length) ? nulls[channel] : null);
            // Resolve this field's typed value/null accessors once for the batch (layer-2 monomorphization).
            Vector fieldValue = channel < values.length ? values[channel] : null;
            fieldLong[index] = fieldKinds[index] == FlatTypeHandler.Kind.LONG && fieldValue != null ? VectorAccess.longValues(fieldValue) : null;
            fieldBinaryBase[index] = null;
            fieldBinaryIds[index] = null;
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY) {
                if (fieldValue instanceof BinaryVector base) {
                    fieldBinaryBase[index] = base;
                }
                else if (fieldValue instanceof DictionaryVector dict && dict.values() instanceof BinaryVector base) {
                    fieldBinaryBase[index] = base;
                    fieldBinaryIds[index] = dict.ids();
                }
            }
            fieldNullAccess[index] = (nulls != null && channel < nulls.length && nulls[channel] != null) ? VectorAccess.booleanValues(nulls[channel]) : null;
            if (channel >= values.length || !(values[channel] instanceof DictionaryVector dictionary)) {
                dictionaryHashedIds[index] = null;
                dictionaryEntryHashes[index] = null;
                dictionaryHashedValues[index] = null;
                continue;
            }
            Vector dictionaryValues = dictionary.values();
            int distinctCount = dictionaryValues.length();
            // Pre-hashing every distinct dictionary entry pays off only when the dictionary is smaller than the batch
            // it describes (low-card key, values repeat across rows): each entry is hashed once and reused by id. A
            // join-output key instead wraps the whole build column in a dictionary -- far more entries than a probe
            // batch references -- so pre-hashing all of them every batch is mostly wasted. When the dictionary has
            // more entries than the batch has positions, skip the pre-hash and let fieldHash() hash the referenced
            // entries per row (the same path a non-dictionary field uses). This also sidesteps the per-batch stale
            // cache problem below, since the per-row path always reads the current batch's bytes.
            if (distinctCount > dictionary.ids().length) {
                dictionaryHashedIds[index] = null;
                dictionaryEntryHashes[index] = null;
                dictionaryHashedValues[index] = null;
            }
            else {
                dictionaryHashedIds[index] = dictionary.ids();
                // Compute the per-entry hash for every distinct dictionary value once and index it by dictionary id at
                // hash() time, instead of re-hashing the (variable-width) value per row. This is recomputed every batch:
                // the value vector cannot be cached by identity across batches because the allocator pools vector
                // instances, so the same instance can carry different content in a later batch (e.g. a hash-join probe
                // passthrough wrapped in a dictionary) -- a stale cache would hash equal keys differently and split groups.
                if (dictionaryEntryHashes[index] == null || dictionaryEntryHashes[index].length < distinctCount) {
                    dictionaryEntryHashes[index] = new long[distinctCount];
                }
                FlatTypeHandler.Kind kind = fieldKinds[index];
                long[] entryHashes = dictionaryEntryHashes[index];
                for (int id = 0; id < distinctCount; id++) {
                    entryHashes[id] = hashByKind(kind, dictionaryValues, id);
                }
                dictionaryHashedValues[index] = dictionaryValues;
            }

            // Intern this dictionary's entries to GLOBAL value ids (stable across batches and dictionary
            // identities) so id-based equality works for the whole persistent group table, not just within one
            // dictionary instance. Id equality only applies to variable-width fields -- the byte-compare those
            // would otherwise pay is what we avoid.
            if (handlers[index].variableWidth()) {
                int[] entryGlobalIds = internDictionaryEntries(index, dictionaryValues);
                if (entryGlobalIds != null) {
                    boundDictionary[index] = dictionaryValues;
                    batchDictionaryIds[index] = dictionary.ids();
                    fieldIdComparable[index] = true;
                    anyFieldIdComparable = true;
                }
            }
        }

        // Decide once per batch whether the key is array-mode eligible: every field interned, null-free, with at
        // most STRIDE distinct ids (so the packing is a bijection), and few enough fields that the composite
        // range stays indexable. Checked here so the per-position compositeValueId stays a tight pack loop.
        batchCompositeEligible = handlers.length > 0;
        long compositeMultiplier = 1;
        for (int index = 0; batchCompositeEligible && index < handlers.length; index++) {
            if (!fieldIdComparable[index] || !batchFieldNullFree[index]
                    || fieldInterners[index] == null || fieldInterners[index].distinctCount() > COMPOSITE_STRIDE) {
                batchCompositeEligible = false;
                break;
            }
            compositeMultiplier *= COMPOSITE_STRIDE;
            if (compositeMultiplier > COMPOSITE_MAX) {
                batchCompositeEligible = false;
                break;
            }
        }
        batchAccessorsReady = true;
    }

    /**
     * The composite value id for the key at {@code position} (id0 + id1*STRIDE + …) when the batch is array-mode
     * eligible, else -1. A bijection of the per-field global ids, stable across batches, in {@code [0, STRIDE^k)}
     * with the range bounded below COMPOSITE_MAX; FlatGroupingTable uses it as a direct array index.
     */
    /**
     * Whether this batch resolves group keys by composite (array-mode) id without hashing. The decoupled
     * hash-then-probe driver skips its precompute pass when this holds.
     */
    boolean batchArrayModeEligible()
    {
        return batchCompositeEligible;
    }

    long compositeValueId(int position)
    {
        if (!batchCompositeEligible) {
            return -1;
        }
        long composite = 0;
        long multiplier = 1;
        for (int index = 0; index < handlers.length; index++) {
            composite += batchEntryGlobalId[index][batchDictionaryIds[index][position]] * multiplier;
            multiplier *= COMPOSITE_STRIDE;
        }
        return composite;
    }

    /**
     * This batch's dictionary entry -> global value id map for a variable-width field, interning any entries not
     * seen before. Cached by dictionary identity. Returns {@code null} when the dictionary's values are not a
     * byte-string vector (the field then keeps the value-comparison path). An entry that overflows the interner
     * ceiling maps to -1, and equality for that value falls back to the byte compare.
     */
    private int[] internDictionaryEntries(int fieldIndex, Vector dictionaryValues)
    {
        if (batchEntryGlobalIdDict[fieldIndex] == dictionaryValues && batchEntryGlobalId[fieldIndex] != null) {
            return batchEntryGlobalId[fieldIndex];
        }
        if (!(dictionaryValues instanceof BinaryVector dictionary)) {
            return null;
        }
        ValueIdInterner interner = fieldInterners[fieldIndex];
        if (interner == null) {
            interner = new ValueIdInterner(VALUE_ID_CEILING);
            fieldInterners[fieldIndex] = interner;
        }
        int entryCount = dictionary.length();
        int[] globalIds = new int[entryCount];
        byte[] data = dictionary.data();
        for (int entry = 0; entry < entryCount; entry++) {
            int so = dictionary.startOffset(entry);
            int len = dictionary.length(entry);
            // A dictionary may carry more value slots than its ids reference (over-allocated/phantom entries whose
            // offsets are unset); those are never indexed by a live id, so map them to the byte-compare fallback (-1)
            // rather than interning out-of-range bytes.
            if (len < 0 || so < 0 || so + len > data.length) {
                globalIds[entry] = -1;
                continue;
            }
            globalIds[entry] = interner.intern(data, so, len);
        }
        batchEntryGlobalId[fieldIndex] = globalIds;
        batchEntryGlobalIdDict[fieldIndex] = dictionaryValues;
        return globalIds;
    }

    /**
     * Hook called by {@link FlatGroupingTable} after a batch completes. Mirror of
     * {@link #beginBatch}; subclasses release cached references here.
     */
    public void endBatch()
    {
        batchAccessorsReady = false;
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
            if (inputFieldNull(0, nulls, position)) {
                return 31;
            }
            return 31 + fieldHash(0, singleInputChannel, values[singleInputChannel], position);
        }
        long result = 1;
        for (int index = 0; index < handlers.length; index++) {
            if (inputFieldNull(index, nulls, position)) {
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
        if (!batchAccessorsReady) {
            return hashByKind(fieldKinds[fieldIndex], value, position);
        }
        return switch (fieldKinds[fieldIndex]) {
            case LONG -> Long.hashCode(fieldLong[fieldIndex].value(position));
            case BINARY -> binaryFieldHash(fieldIndex, value, position);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.hashInput(value, position);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.hashInput(value, position);
        };
    }

    private long binaryFieldHash(int fieldIndex, Vector value, int position)
    {
        BinaryVector base = fieldBinaryBase[fieldIndex];
        if (base == null) {
            return FlatTypeHandlers.BINARY.hashInput(value, position);
        }
        int[] ids = fieldBinaryIds[fieldIndex];
        int entry = ids == null ? position : ids[position];
        return OperatorVectorSupport.binaryHash(base.data(), base.startOffset(entry), base.length(entry));
    }

    private void writeFieldFlat(int fieldIndex, Vector value, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena)
    {
        if (!batchAccessorsReady) {
            writeFlatByKind(fieldKinds[fieldIndex], value, position, fixedChunk, fixedOffset, arena);
            return;
        }
        switch (fieldKinds[fieldIndex]) {
            case LONG -> GROUP_LONG_HANDLE.set(fixedChunk, fixedOffset, fieldLong[fieldIndex].value(position));
            case BINARY -> writeBinaryField(fieldIndex, value, position, fixedChunk, fixedOffset, arena);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.writeFlat(value, position, fixedChunk, fixedOffset, arena);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.writeFlat(value, position, fixedChunk, fixedOffset, arena);
        }
    }

    private void writeBinaryField(int fieldIndex, Vector value, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena)
    {
        BinaryVector base = fieldBinaryBase[fieldIndex];
        if (base == null) {
            FlatTypeHandlers.BINARY.writeFlat(value, position, fixedChunk, fixedOffset, arena);
            return;
        }
        int[] ids = fieldBinaryIds[fieldIndex];
        int entry = ids == null ? position : ids[position];
        long pointer = arena.append(base.data(), base.startOffset(entry), base.length(entry));
        GROUP_INT_HANDLE.set(fixedChunk, fixedOffset, FlatGroupingTable.FlatVariableWidthArena.chunkIndex(pointer));
        GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES, FlatGroupingTable.FlatVariableWidthArena.chunkOffset(pointer));
        GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES * 2, base.length(entry));
    }

    private boolean identicalField(int fieldIndex, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, Vector value, int position)
    {
        if (!batchAccessorsReady) {
            return identicalByKind(fieldKinds[fieldIndex], fixedChunk, fixedOffset, arena, value, position);
        }
        return switch (fieldKinds[fieldIndex]) {
            case LONG -> (long) GROUP_LONG_HANDLE.get(fixedChunk, fixedOffset) == fieldLong[fieldIndex].value(position);
            case BINARY -> identicalBinaryField(fieldIndex, fixedChunk, fixedOffset, arena, value, position);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
        };
    }

    private boolean identicalBinaryField(int fieldIndex, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, Vector value, int position)
    {
        BinaryVector base = fieldBinaryBase[fieldIndex];
        if (base == null) {
            return FlatTypeHandlers.BINARY.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
        }
        int length = (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES * 2);
        int[] ids = fieldBinaryIds[fieldIndex];
        int entry = ids == null ? position : ids[position];
        if (base.length(entry) != length) {
            return false;
        }
        byte[] chunk = arena.chunk((int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset));
        int offset = (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES);
        return OperatorVectorSupport.binaryEquals(base.data(), base.startOffset(entry), chunk, offset, length);
    }

    private static long hashByKind(FlatTypeHandler.Kind kind, Vector value, int position)
    {
        return switch (kind) {
            case LONG -> FlatTypeHandlers.LONG.hashInput(value, position);
            case BINARY -> FlatTypeHandlers.BINARY.hashInput(value, position);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.hashInput(value, position);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.hashInput(value, position);
        };
    }

    private static void writeFlatByKind(FlatTypeHandler.Kind kind, Vector value, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena)
    {
        switch (kind) {
            case LONG -> FlatTypeHandlers.LONG.writeFlat(value, position, fixedChunk, fixedOffset, arena);
            case BINARY -> FlatTypeHandlers.BINARY.writeFlat(value, position, fixedChunk, fixedOffset, arena);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.writeFlat(value, position, fixedChunk, fixedOffset, arena);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.writeFlat(value, position, fixedChunk, fixedOffset, arena);
        }
    }

    private static boolean identicalByKind(FlatTypeHandler.Kind kind, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, Vector value, int position)
    {
        return switch (kind) {
            case LONG -> FlatTypeHandlers.LONG.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
            case BINARY -> FlatTypeHandlers.BINARY.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
        };
    }

    public void writeRecord(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
    {
        if (nullByteCount > 0) {
            Arrays.fill(fixedChunk, fixedOffset, fixedOffset + nullByteCount, (byte) 0);
        }
        if (singleField) {
            if (inputFieldNull(0, nulls, position)) {
                fixedChunk[fixedOffset] = 1;
            }
            else {
                singleHandler.writeFlat(values[singleInputChannel], position, fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena);
            }
            storeRecordDictionaryIds(recordIndex, position);
            return;
        }
        for (int index = 0; index < handlers.length; index++) {
            if (inputFieldNull(index, nulls, position)) {
                setNullBit(fixedChunk, fixedOffset, index);
            }
            else {
                writeFieldFlat(index, values[inputChannels[index]], position, fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena);
            }
        }
        storeRecordDictionaryIds(recordIndex, position);
    }

    public boolean identicalRecordToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
    {
        if (singleField) {
            if (inputFieldNull(0, nulls, position)) {
                return isNull(fixedChunk, fixedOffset, 0);
            }
            if (isNull(fixedChunk, fixedOffset, 0)) {
                return false;
            }
            if (idComparable(0, recordIndex)) {
                int probeId = batchEntryGlobalId[0][batchDictionaryIds[0][position]];
                if (probeId >= 0) {
                    return recordDictionaryIds[0][recordIndex] == probeId;
                }
                // probe value overflowed the interner: fall through to the value comparison.
            }
            return singleHandler.identicalFlatToInput(fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena, values[singleInputChannel], position);
        }
        for (int index : comparisonOrder) {
            boolean inputNull = inputFieldNull(index, nulls, position);
            boolean recordNull = isNull(fixedChunk, fixedOffset, index);
            if (inputNull != recordNull) {
                return false;
            }
            if (inputNull) {
                continue;
            }
            if (idComparable(index, recordIndex)) {
                int probeId = batchEntryGlobalId[index][batchDictionaryIds[index][position]];
                if (probeId >= 0) {
                    if (recordDictionaryIds[index][recordIndex] != probeId) {
                        return false;
                    }
                    continue;
                }
                // probe value overflowed the interner: fall through to the value comparison.
            }
            if (!identicalField(index, fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena, values[inputChannels[index]], position)) {
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
            recordDictionaryIds[index][recordIndex] = fieldIdComparable[index] ? batchEntryGlobalId[index][batchDictionaryIds[index][position]] : -1;
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

    /**
     * The input null flag for a key field, skipping the per-row read when this batch's null stream for the field
     * is all-false (recorded in {@link #beginBatch}). Falls back to the real read when {@link #beginBatch} was
     * not called (the flags are absent) or the field may carry nulls this batch.
     */
    private boolean inputFieldNull(int fieldIndex, Vector[] nulls, int position)
    {
        if (batchFieldNullFree != null && batchFieldNullFree[fieldIndex]) {
            return false;
        }
        if (batchAccessorsReady) {
            VectorAccess.BooleanValues accessor = fieldNullAccess[fieldIndex];
            return accessor != null && accessor.value(position);
        }
        return fieldNull(nulls, inputChannels[fieldIndex], position);
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
