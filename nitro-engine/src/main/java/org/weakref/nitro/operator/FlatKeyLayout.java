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

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

class FlatKeyLayout
{
    private static final int COMPACT_BINARY_SAMPLE_SIZE = 64;
    private final PrimitiveArrayPool arrayPool;
    private final OperatorCodeGenerationResources codeGeneration;
    private final FlatKeyTablePolicy keyTablePolicy;
    private final FlatKeyTablePolicy.Layout policy;
    private final Field[] fields;
    private final int[] inputChannels;
    private final FlatTypeHandler[] handlers;
    private final int[] fixedOffsets;
    private final int[] comparisonOrder;
    private final int nullByteCount;
    private final boolean singleField;
    private final boolean embedIdOnlyBinaryIds;
    private final boolean compactEmbeddedBinaryRecords;
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
    private long[] dictionaryHashedGenerations;

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
    private int[] packedRecordDictionaryIds;
    private final int[] packedDictionaryFieldIndex;
    private final int packedDictionaryFieldCount;

    // Per-batch null-free flags: true for a key field whose null stream is all-false this batch, so the per-row
    // input null read (a megamorphic OperatorVectorSupport.isNull) is skipped. The layout stays nullable (a later
    // batch may carry nulls), but a batch of declared-nullable-yet-null-free keys -- the common case, since the
    // Parquet writer marks columns optional even when they are semantically NOT NULL -- pays no per-row null check.
    private boolean[] batchFieldNullFree;
    private boolean[] batchFieldAllNull;

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
    private ValueIdInterner[] fieldInterners;
    private int[][] batchEntryGlobalId;
    private Vector[] batchEntryGlobalIdDict;
    private long[] batchEntryGlobalIdGeneration;
    private int[][] batchPositionGlobalId;
    private DictionaryVector[] fieldDictionaryMapping;
    private DictionaryVector[] batchPositionDictionaryMapping;
    private boolean debugCompactBinaryPositionIdsPrinted;
    // Sentinel in batchEntryGlobalId for a dictionary entry not yet interned. A join wraps a grouping-key
    // column over the whole build column, so its dictionary carries far more entries than any batch
    // references. Interning every entry eagerly on the first batch that presents a new dictionary identity
    // is mostly wasted (only the referenced entries ever reach a record). Instead, for a base larger than
    // the array-mode stride (so never array-mode eligible), fill the map with this sentinel and intern each
    // entry lazily on first reference, caching the result -- each distinct entry is interned once across the
    // query, over the same base-identity cache the eager path already used, so ids are unchanged.
    private static final int GLOBAL_ID_NOT_INTERNED = Integer.MIN_VALUE;
    private boolean[] fieldLazyIntern;
    // Once a binary value has a query-stable interned id, the id is sufficient both for equality and for
    // reconstructing the grouped output. Avoid copying the same low-cardinality bytes into every group record.
    // A -1 record length marks this representation; fallback probes compare against the interner's owned bytes.
    private boolean[] fieldUsesIdOnlyRecords;
    // Wide composite layouts usually carry several low-cardinality dictionary-backed BINARY fields. Once a
    // query-stable value id exists, their ordinary 12-byte {chunk, offset, length} record wastes eight bytes per
    // group. Eligible layouts instead store one 32-bit token: a non-negative stable value id, or ~ordinal for an
    // exact variable-width fallback. Fallback metadata is dense and field-local, so an overflowing/high-cardinality
    // field costs the same 12 bytes as the ordinary record while every id-backed field costs four. The fallback
    // word packs arena chunk, offset, and length exactly; the arena's 1 MiB chunk invariant gives offset and length
    // 21 bits each (including the exact end boundary), leaving 22 bits for the chunk index.
    private long[][] compactBinaryFallbacks;
    private int[] compactBinaryFallbackCounts;

    // Composite value-id for array-mode grouping (Velox's normalized-key technique): when every key field is an
    // interned, null-free, low-cardinality dictionary column, this batch's per-field global ids pack into a
    // single composite id (id0 + id1*STRIDE + …) that uniquely identifies the key and stays stable across
    // batches. FlatGroupingTable indexes a direct array by it, skipping the hash + probe + record machinery
    // entirely; out-of-range composites (a field above STRIDE distinct, too many fields, or a nullable/non-dict
    // batch) yield -1 and fall back to the hash table. STRIDE bounds the per-field cardinality so the packing is
    // a bijection; COMPOSITE_MAX bounds the indexable range.
    private static final int COMPOSITE_STRIDE = 1 << 10;
    private static final int BINARY_COMPOSITE_RADIX = COMPOSITE_STRIDE + 1; // final digit represents null
    private static final int LONG_COMPOSITE_CARDINALITY = 1 << 4;
    private static final int LONG_COMPOSITE_RADIX = LONG_COMPOSITE_CARDINALITY + 1; // final digit represents null
    private static final long COMPOSITE_MAX = 1 << 25;
    private final int[] compositeOrder;
    private boolean batchMixedComposite;
    private int[] compactCompositeRadix;
    private long[] compactLongBase;
    private boolean[] compactLongBaseSet;
    private boolean[] compactLongDomainRejected;
    private boolean batchCompositeEncodable;
    private boolean batchCompositeEligible;
    private boolean debugMixedCompositePrinted;
    // Concrete three-field mixed-composite state, resolved once per batch. A GroupId-style producer commonly makes
    // every key channel either all-null or null-free for the whole batch. Keep that invariant, field order, and
    // compact radices out of the per-row composite-id loop; -1 means at least one channel has mixed nullness and
    // retains the ordinary exact path.
    private int batchMixedComposite3ConstantNullMask = -1;
    private int batchMixedComposite3LongIndex;
    private int batchMixedComposite3FirstBinary;
    private int batchMixedComposite3SecondBinary;
    private int batchMixedComposite3FirstRadix;
    private int batchMixedComposite3SecondRadix;
    private boolean batchMixedComposite3Prepared;
    private boolean debugConstantNullMixedComposite3Printed;
    private int debugConstantNullMixedComposite3BatchMask = Integer.MIN_VALUE;
    private int debugConstantNullMixedComposite3BatchTransitions;

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
    private boolean batchNullFreeLongBinary;
    private boolean batchNullFreeSingleBinaryCandidate;
    private boolean batchNullFreeSingleBinary;
    private boolean dictionarySingleBinaryFastPathDecided;
    private boolean dictionarySingleBinaryFastPathAdmitted;
    private boolean debugDictionarySingleBinaryPrinted;
    private boolean debugGeneratedDictionaryHashBatchPrinted;
    private boolean debugGeneratedDictionaryHashBatchRejectedPrinted;
    private DictionaryRecordEqualityKernel generatedRecordEqualityKernel;
    private VectorAccess.LongValues[] fieldLong;
    private DictionaryHashBatchKernel.BinaryHashes[] fieldBinaryHashes;
    private BinaryVector[] fieldBinaryBase;
    private int[][] fieldBinaryIds;
    private long batchSingleRunBinaryFields;
    private long[] fieldBinaryConstantHash;
    private int[] fieldBinaryConstantGlobalId;
    // Reusable, per-field logical-position -> leaf-position maps for nested dictionaries. Join output commonly
    // wraps an already dictionary-encoded dimension column. Resolving that chain once per batch lets all grouping
    // operations use the concrete leaf and a single id lookup instead of recursively walking the encoding in every
    // hash/equality/write call. These buffers are high-water retained for the query and returned by releaseBuffers.
    private int[][] composedDictionaryIds;
    private int[][] composedNullDictionaryIds;
    private VectorAccess.BooleanValues[] fieldNullAccess;
    // A composite hash table needs a deterministic discriminator, not necessarily every key field. When one field
    // is already highly selective, hashing the remaining wide fields only adds loads and TLB pressure; equality
    // still compares the complete record, so equal hashes can never merge distinct composite keys.
    private boolean discriminatingHashFieldDecided;
    private int discriminatingHashField = -1;
    private final boolean normalizedIntKeyShape;
    private boolean batchNormalizedIntKeyEligible;
    private long preparedNormalizedFirst;
    private long preparedNormalizedSecond;

    FlatKeyLayout(
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy keyTablePolicy,
            Field[] fields,
            int[] inputChannels,
            FlatTypeHandler[] handlers,
            int[] fixedOffsets,
            int[] comparisonOrder,
            int nullByteCount,
            int fixedRecordSize,
            boolean anyVariableWidth,
            boolean compactEmbeddedBinaryRecords)
    {
        this.arrayPool = arrayPool;
        this.codeGeneration = codeGeneration;
        this.keyTablePolicy = keyTablePolicy;
        this.policy = keyTablePolicy.layout();
        this.fields = fields;
        this.inputChannels = inputChannels;
        this.handlers = handlers;
        this.fieldKinds = new FlatTypeHandler.Kind[handlers.length];
        this.packedDictionaryFieldIndex = new int[handlers.length];
        Arrays.fill(packedDictionaryFieldIndex, -1);
        int packedFields = 0;
        for (int index = 0; index < handlers.length; index++) {
            this.fieldKinds[index] = handlers[index].kind();
            if (handlers[index].variableWidth()) {
                packedDictionaryFieldIndex[index] = packedFields++;
            }
        }
        this.packedDictionaryFieldCount = packedFields;
        this.fixedOffsets = fixedOffsets;
        this.comparisonOrder = comparisonOrder;
        this.compositeOrder = compositeOrder(fieldKinds);
        this.nullByteCount = nullByteCount;
        this.singleField = handlers.length == 1;
        this.embedIdOnlyBinaryIds = policy.embedIdOnlyBinaryIds() && !singleField;
        this.compactEmbeddedBinaryRecords = compactEmbeddedBinaryRecords;
        this.singleInputChannel = singleField ? inputChannels[0] : -1;
        this.singleHandler = singleField ? handlers[0] : null;
        this.singleFixedOffset = singleField ? fixedOffsets[0] : -1;
        this.fixedRecordSize = fixedRecordSize;
        this.anyVariableWidth = anyVariableWidth;
        int normalizedKeyBits = Math.multiplyExact(handlers.length, Integer.SIZE);
        boolean normalizedShape = policy.normalizedIntKey() &&
                normalizedKeyBits > Long.SIZE &&
                normalizedKeyBits <= policy.normalizedIntKeyMaxBits();
        for (FlatTypeHandler.Kind kind : fieldKinds) {
            normalizedShape &= kind == FlatTypeHandler.Kind.LONG || kind == FlatTypeHandler.Kind.BINARY;
        }
        this.normalizedIntKeyShape = normalizedShape;
        // Grouped output can be requested by callers that do not drive beginBatch (for example an empty or
        // pre-materialized grouping path). This flag describes record-lifetime state, not batch scratch, so keep it
        // available for the layout's entire lifetime rather than initializing it with the per-batch caches.
        this.fieldUsesIdOnlyRecords = new boolean[handlers.length];
    }

    public static FlatKeyLayout tryCreate(
            Vector[] values,
            boolean nullable,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy policy)
    {
        return tryCreate(values, nullable, arrayPool, codeGeneration, policy, List.of());
    }

    public static FlatKeyLayout tryCreate(
            Vector[] values,
            boolean nullable,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy policy,
            List<TypeBinding> types)
    {
        FlatKeyTablePolicy.Layout layoutPolicy = policy.layout();
        Field[] fields = new Field[values.length];
        int[] inputChannels = new int[values.length];
        FlatTypeHandler[] handlers = new FlatTypeHandler[values.length];
        int[] fixedOffsets = new int[values.length];
        int nullByteCount = nullable ? Math.max(1, (values.length + Byte.SIZE - 1) / Byte.SIZE) : 0;
        boolean anyVariableWidth = false;
        int binaryFields = 0;
        int idBackedBinaryFields = 0;
        int dictionaryBackedBinaryFields = dictionaryBackedBinaryFields(values);
        boolean adaptiveCompactLongRecords = values.length >= 3 &&
                dictionaryBackedBinaryFields > 0 &&
                Arrays.stream(values).anyMatch(I32Vector.class::isInstance);
        for (int index = 0; index < values.length; index++) {
            TypeBinding type = index < types.size() ? types.get(index) : null;
            FlatTypeHandler handler = FlatTypeHandlers.forVector(
                    values[index],
                    type,
                    layoutPolicy,
                    adaptiveCompactLongRecords);
            if (handler == null) {
                return null;
            }
            inputChannels[index] = index;
            handlers[index] = handler;
            anyVariableWidth |= handler.variableWidth();
            binaryFields += handler.kind() == FlatTypeHandler.Kind.BINARY ? 1 : 0;
            idBackedBinaryFields += handler.kind() == FlatTypeHandler.Kind.BINARY &&
                    values[index] instanceof DictionaryVector ? 1 : 0;
        }
        // A compact token is useful only when the first physical batch proves both reusable ids and enough possible
        // groups to amortize its fallback sidecar. Flat binary fields have no stable value ids: representing all of
        // them as fallback ordinals merely moves the ordinary 12-byte metadata out of line, without reducing it.
        // Require the same minimum reusable cohort to be physically id-backed before selecting this immutable
        // layout. Sampled per-field distinctness then requires one discriminating field and a large product of
        // distinct counts, so a tiny geographical cube does not optimize ten records.
        boolean compactEmbeddedBinaryRecords = layoutPolicy.compactEmbeddedBinaryRecords() &&
                layoutPolicy.idOnlyBinaryRecords() &&
                layoutPolicy.embedIdOnlyBinaryIds() &&
                binaryFields >= layoutPolicy.compactBinaryMinFields() &&
                idBackedBinaryFields >= layoutPolicy.compactBinaryMinReusableFields() &&
                values.length > 0 &&
                values[0].length() >= layoutPolicy.compactBinaryMinRows() &&
                admitsCompactBinaryRecords(values, handlers, layoutPolicy);
        int fixedOffset = nullByteCount;
        for (int index = 0; index < values.length; index++) {
            FlatTypeHandler handler = handlers[index];
            fields[index] = new Field(index, handler, fixedOffset, OperatorVectorSupport.binaryTraits(values[index]));
            fixedOffsets[index] = fixedOffset;
            fixedOffset += compactEmbeddedBinaryRecords && handler.kind() == FlatTypeHandler.Kind.BINARY
                    ? Integer.BYTES
                    : handler.fixedSize();
        }
        if (layoutPolicy.precomputeCompactBinaryPositionIds() && compactEmbeddedBinaryRecords) {
            return new PositionIdFlatKeyLayout(
                    arrayPool,
                    codeGeneration,
                    policy,
                    fields,
                    inputChannels,
                    handlers,
                    fixedOffsets,
                    comparisonOrder(handlers),
                    nullByteCount,
                    fixedOffset,
                    anyVariableWidth);
        }
        return new FlatKeyLayout(arrayPool, codeGeneration, policy, fields, inputChannels, handlers, fixedOffsets, comparisonOrder(handlers), nullByteCount, fixedOffset, anyVariableWidth, compactEmbeddedBinaryRecords);
    }

    private static int dictionaryBackedBinaryFields(Vector[] values)
    {
        int fields = 0;
        for (Vector value : values) {
            if (value instanceof DictionaryVector && OperatorVectorSupport.flatten(value) instanceof BinaryVector) {
                fields++;
            }
        }
        return fields;
    }

    private static boolean admitsCompactBinaryRecords(
            Vector[] values,
            FlatTypeHandler[] handlers,
            FlatKeyTablePolicy.Layout policy)
    {
        int reusableFields = 0;
        int maximumDistinct = 0;
        long distinctProduct = 1;
        long[] hashes = new long[COMPACT_BINARY_SAMPLE_SIZE];
        for (int index = 0; index < handlers.length; index++) {
            if (handlers[index].kind() != FlatTypeHandler.Kind.BINARY) {
                continue;
            }
            int samples = Math.min(values[index].length(), COMPACT_BINARY_SAMPLE_SIZE);
            int distinct = sampledBinaryDistinct(values[index], samples, hashes);
            maximumDistinct = Math.max(maximumDistinct, distinct);
            if ((long) distinct * 100 <= (long) samples * policy.compactBinaryReusePercent()) {
                reusableFields++;
            }
            if (distinctProduct < policy.compactBinaryMinDistinctProduct()) {
                distinctProduct = Math.min(
                        policy.compactBinaryMinDistinctProduct(),
                        Math.multiplyExact(distinctProduct, Math.max(1, distinct)));
            }
        }
        return reusableFields >= policy.compactBinaryMinReusableFields() &&
                maximumDistinct >= policy.compactBinaryMinDiscriminatingDistinct() &&
                distinctProduct >= policy.compactBinaryMinDistinctProduct();
    }

    private static int sampledBinaryDistinct(Vector vector, int samples, long[] hashes)
    {
        for (int sample = 0; sample < samples; sample++) {
            int position = samples == 1 ? 0 : (int) ((long) sample * (vector.length() - 1) / (samples - 1));
            hashes[sample] = FlatTypeHandlers.BINARY.hashInput(vector, position);
        }
        Arrays.sort(hashes, 0, samples);
        int distinct = samples == 0 ? 0 : 1;
        for (int sample = 1; sample < samples; sample++) {
            distinct += hashes[sample] != hashes[sample - 1] ? 1 : 0;
        }
        return distinct;
    }

    public static FlatKeyLayout tryCreate(
            Vector[] values,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy policy)
    {
        return tryCreate(values, false, arrayPool, codeGeneration, policy);
    }

    PrimitiveArrayPool primitiveArrays()
    {
        return arrayPool;
    }

    FlatKeyTablePolicy.Table tablePolicy()
    {
        return keyTablePolicy.table();
    }

    public int fieldCount()
    {
        return fields.length;
    }

    public int fixedRecordSize()
    {
        return fixedRecordSize;
    }

    boolean usesCompactEmbeddedBinaryRecords()
    {
        return compactEmbeddedBinaryRecords;
    }

    boolean batchSupportsNormalizedIntKey()
    {
        return batchNormalizedIntKeyEligible;
    }

    boolean supportsNormalizedIntKeyShape()
    {
        return normalizedIntKeyShape;
    }

    boolean usesGeneratedDictionaryRecordEquality()
    {
        return generatedRecordEqualityKernel != null;
    }

    /**
     * Exposes the concrete dictionary identity for a null-free, single-field binary batch. A grouping table can
     * use this to memoize the exact group assigned to a dictionary entry, but only for the lifetime of this
     * identity and content generation. The ordinary hash table remains authoritative for the first occurrence.
     */
    boolean batchSupportsSingleDictionaryGroupCache()
    {
        return singleField &&
                fieldKinds[0] == FlatTypeHandler.Kind.BINARY &&
                batchFieldNullFree[0] &&
                fieldBinaryBase[0] != null &&
                fieldBinaryBase[0].contentImmutable() &&
                fieldBinaryIds[0] != null &&
                fieldBinaryBase[0].contentGeneration() >= 0;
    }

    Vector batchSingleDictionaryGroupIdentity()
    {
        return fieldBinaryBase[0];
    }

    long batchSingleDictionaryGroupGeneration()
    {
        return fieldBinaryBase[0].contentGeneration();
    }

    int batchSingleDictionaryGroupCardinality()
    {
        return fieldBinaryBase[0].length();
    }

    int[] batchSingleDictionaryGroupIds()
    {
        return fieldBinaryIds[0];
    }

    /**
     * Packs up to four nullable, non-negative int-domain key fields into two exact primitive lanes. Zero encodes
     * null and every non-null value is shifted by one, so the pair is a bijection for the admitted domain. Binary
     * fields use their query-stable interned value id; a flat/overflowing binary or an out-of-range long simply
     * declines the accelerator and retains the complete record-equality path.
     */
    boolean tryPrepareNormalizedIntKey(Vector[] values, Vector[] nulls, int position)
    {
        if (!normalizedIntKeyShape || !batchAccessorsReady) {
            return false;
        }
        long first = 0;
        long second = 0;
        for (int index = 0; index < fieldKinds.length; index++) {
            int encoded;
            if (inputFieldNull(index, nulls, position)) {
                encoded = 0;
            }
            else if (fieldKinds[index] == FlatTypeHandler.Kind.LONG) {
                long value = fieldLong[index].value(position);
                if (value < 0 || value >= Integer.MAX_VALUE) {
                    return false;
                }
                encoded = (int) value + 1;
            }
            else {
                int valueId = normalizedBinaryValueId(index, position);
                if (valueId < 0 || valueId >= Integer.MAX_VALUE) {
                    return false;
                }
                encoded = valueId + 1;
            }
            if (index < 2) {
                first |= Integer.toUnsignedLong(encoded) << (index * Integer.SIZE);
            }
            else {
                second |= Integer.toUnsignedLong(encoded) << ((index - 2) * Integer.SIZE);
            }
        }
        preparedNormalizedFirst = first;
        preparedNormalizedSecond = second;
        return true;
    }

    private int normalizedBinaryValueId(int fieldIndex, int position)
    {
        int[] ids = batchDictionaryIds[fieldIndex];
        int valueId;
        if (fieldIdComparable[fieldIndex] && ids != null) {
            valueId = globalIdFor(fieldIndex, ids[position]);
        }
        else {
            BinaryVector binary = fieldBinaryBase[fieldIndex];
            if (binary == null) {
                return -1;
            }
            int physicalPosition = binaryEntry(fieldIndex, position);
            ValueIdInterner interner = fieldInterners[fieldIndex];
            if (interner == null) {
                interner = new ValueIdInterner(policy.valueIdCeiling(), keyTablePolicy.valueIds());
                fieldInterners[fieldIndex] = interner;
            }
            valueId = interner.intern(binary.data(), binary.startOffset(physicalPosition), binary.length(physicalPosition));
        }
        if (valueId >= 0) {
            return valueId;
        }
        BinaryVector binary = fieldBinaryBase[fieldIndex];
        if (binary == null) {
            return -1;
        }
        int physicalPosition = binaryEntry(fieldIndex, position);
        return fieldInterners[fieldIndex].find(
                binary.data(),
                binary.startOffset(physicalPosition),
                binary.length(physicalPosition));
    }

    long assignNormalizedIntBatch(
            FlatGroupingTable table,
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            long nextGroupId,
            long[] output)
    {
        if (!batchNormalizedIntKeyEligible || mask.none()) {
            return -1;
        }
        int requiredSize = mask.maxPosition() + 1;
        for (int field = 0; field < fieldKinds.length; field++) {
            if (fieldKinds[field] != FlatTypeHandler.Kind.BINARY || batchFieldAllNull[field]) {
                continue;
            }
            int[] positioned = batchPositionGlobalId[field];
            if (positioned == null || positioned.length < requiredSize) {
                int[] previous = positioned;
                positioned = borrowInts(requiredSize);
                batchPositionGlobalId[field] = positioned;
                release(previous);
            }
            for (int position : mask) {
                if (!batchFieldNullFree[field] && fieldNullAccess[field].value(position)) {
                    continue;
                }
                positioned[position] = normalizedBinaryValueId(field, position);
            }
        }

        long shape = NormalizedIntGroupingKernelGenerator.shape(fieldKinds, batchFieldNullFree, batchFieldAllNull);
        return codeGeneration.normalizedIntGrouping().create(shape).assign(
                mask.selectedPositions(),
                mask.selectedCount(),
                fieldLong,
                batchPositionGlobalId,
                fieldNullAccess,
                table,
                values,
                nulls,
                nextGroupId,
                output);
    }

    long preparedNormalizedFirst()
    {
        return preparedNormalizedFirst;
    }

    long preparedNormalizedSecond()
    {
        return preparedNormalizedSecond;
    }

    boolean supportsNormalizedRecordWrite()
    {
        // Query-stable binary ids are exact record values: idOnlyBinaryEquals compares a later flat/overflowing
        // probe directly with the interner-owned bytes, and grouped output reconstructs the value from the same
        // owner. A later row may therefore decline normalization without forcing normalized records to retain a
        // second variable-width copy. All-long layouts use the same writer for their provider-owned field widths.
        return normalizedIntKeyShape &&
                policy.idOnlyBinaryRecords() &&
                embedIdOnlyBinaryIds;
    }

    /**
     * Writes the exact normalized key without copying variable-width values into the record arena. Binary lanes
     * contain query-stable value ids, so the ordinary id-only record representation remains readable by fallback
     * equality and grouped-output materialization. Long and null fields retain their ordinary flat encoding.
     */
    void writeNormalizedRecord(byte[] fixedChunk, int fixedOffset, long first, long second)
    {
        if (!supportsNormalizedRecordWrite()) {
            throw new IllegalStateException("Normalized record writes are not supported by this layout");
        }
        if (nullByteCount > 0) {
            Arrays.fill(fixedChunk, fixedOffset, fixedOffset + nullByteCount, (byte) 0);
        }
        for (int field = 0; field < fieldKinds.length; field++) {
            long packed = field < 2 ? first : second;
            int encoded = (int) (packed >>> ((field & 1) * Integer.SIZE));
            if (encoded == 0) {
                setNullBit(fixedChunk, fixedOffset, field);
                continue;
            }
            int fieldOffset = fixedOffset + fixedOffsets[field];
            switch (fieldKinds[field]) {
                case LONG -> handlers[field].writeLong(fixedChunk, fieldOffset, (long) encoded - 1);
                case BINARY -> {
                    GROUP_INT_HANDLE.set(fixedChunk, fieldOffset, encoded - 1);
                    if (!compactBinaryRecord(field)) {
                        GROUP_INT_HANDLE.set(fixedChunk, fieldOffset + Integer.BYTES, 0);
                        GROUP_INT_HANDLE.set(fixedChunk, fieldOffset + Integer.BYTES * 2, -1);
                    }
                    fieldUsesIdOnlyRecords[field] = true;
                }
                case BOOLEAN, DOUBLE -> throw new IllegalStateException("Normalized key contains an unsupported field");
            }
        }
    }

    static long normalizedIntKeyHash(long first, long second)
    {
        long hash = first * 0x9E37_79B9_7F4A_7C15L + second * 0xC2B2_AE3D_27D4_EB4FL;
        hash ^= hash >>> 33;
        hash *= 0xFF51_AFD7_ED55_8CCDL;
        return hash ^ (hash >>> 33);
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
        generatedRecordEqualityKernel = null;
        if (dictionaryHashedIds == null) {
            dictionaryHashedIds = new int[handlers.length][];
            dictionaryEntryHashes = new long[handlers.length][];
            dictionaryHashedValues = new Vector[handlers.length];
            dictionaryHashedGenerations = new long[handlers.length];
            Arrays.fill(dictionaryHashedGenerations, -1);
            boundDictionary = new Vector[handlers.length];
            batchDictionaryIds = new int[handlers.length][];
            fieldIdComparable = new boolean[handlers.length];
            batchFieldNullFree = new boolean[handlers.length];
            batchFieldAllNull = new boolean[handlers.length];
            fieldInterners = new ValueIdInterner[handlers.length];
            batchEntryGlobalId = new int[handlers.length][];
            batchEntryGlobalIdDict = new Vector[handlers.length];
            batchEntryGlobalIdGeneration = new long[handlers.length];
            batchPositionGlobalId = new int[handlers.length][];
            fieldDictionaryMapping = new DictionaryVector[handlers.length];
            batchPositionDictionaryMapping = new DictionaryVector[handlers.length];
            fieldLazyIntern = new boolean[handlers.length];
            fieldLong = new VectorAccess.LongValues[handlers.length];
            fieldBinaryHashes = new DictionaryHashBatchKernel.BinaryHashes[handlers.length];
            fieldBinaryBase = new BinaryVector[handlers.length];
            fieldBinaryIds = new int[handlers.length][];
            fieldBinaryConstantHash = new long[handlers.length];
            fieldBinaryConstantGlobalId = new int[handlers.length];
            composedDictionaryIds = new int[handlers.length][];
            composedNullDictionaryIds = new int[handlers.length][];
            fieldNullAccess = new VectorAccess.BooleanValues[handlers.length];
            compactCompositeRadix = new int[handlers.length];
            compactLongBase = new long[handlers.length];
            compactLongBaseSet = new boolean[handlers.length];
            compactLongDomainRejected = new boolean[handlers.length];
        }
        // A narrow mixed key can use compact radices whether its dictionaries are tiny or large. Tiny bases are
        // already eagerly interned by the normal policy; only a large base needs the extra eager scan so its final
        // radix is known. Keep those two decisions separate: otherwise a tiny county/state key falls back to the
        // 1025x1025 conservative radices and creates a huge sparse direct cache, while a wide rollup pays setup even
        // though its composite can never fit.
        boolean mixedCompositeShape = policy.mixedCompositeIds() &&
                handlers.length <= policy.mixedCompositeMaxFields() &&
                hasLongField() &&
                (handlers.length <= 3 || hasCompactDirectBinaryDictionary(values));
        // Narrow mixed-composite packing reserves only four bits for every long lane. Reject that representation
        // before eagerly interning a large binary dictionary: otherwise a plainly out-of-domain long (dates are a
        // common example) can make us scan an entire join-build dictionary merely to discover radices for a direct
        // cache that cannot be used. Wider shapes retain their compact-range discovery below.
        if (mixedCompositeShape &&
                handlers.length <= 3 &&
                policy.earlyRejectMixedComposite() &&
                hasEarlyOutOfRangeLong(values)) {
            mixedCompositeShape = false;
        }
        // Compact mixed keys need the final binary radices before their generated batch kernel can run. Eagerly
        // discover those radices only when the dictionary base is dense for this batch. Join output commonly wraps
        // a small output batch over an entire build column; scanning that sparse base makes grouping cost scale with
        // unreferenced build rows, so leave it on the existing lazy, referenced-entry path.
        boolean eagerMixedComposite = mixedCompositeShape;
        batchMixedComposite = mixedCompositeShape;
        if (policy.debugMixedComposite() && mixedCompositeShape && handlers.length > 3 && !debugMixedCompositePrinted) {
            System.err.printf("[mixed-composite-shape] fields=%d max=%d has-long=%s mixed=%s eager=%s%n",
                    handlers.length, policy.mixedCompositeMaxFields(), hasLongField(), mixedCompositeShape, eagerMixedComposite);
        }
        anyFieldIdComparable = false;
        batchSingleRunBinaryFields = 0;
        for (int index = 0; index < handlers.length; index++) {
            fieldIdComparable[index] = false;
            batchDictionaryIds[index] = null;
            int channel = inputChannels[index];
            Vector fieldNulls = (nulls != null && channel < nulls.length) ? nulls[channel] : null;
            batchFieldNullFree[index] = VectorAccess.isAllFalseNulls(fieldNulls);
            batchFieldAllNull[index] = policy.allNullBatchMetadata() && VectorAccess.isAllTrueNulls(fieldNulls);
            // Resolve this field's typed value/null accessors once for the batch (layer-2 monomorphization).
            Vector fieldValue = channel < values.length ? values[channel] : null;
            fieldLong[index] = fieldKinds[index] == FlatTypeHandler.Kind.LONG && fieldValue != null ? VectorAccess.longValues(fieldValue) : null;
            fieldBinaryBase[index] = null;
            fieldBinaryIds[index] = null;
            fieldBinaryConstantHash[index] = 0;
            fieldBinaryConstantGlobalId[index] = -1;
            fieldDictionaryMapping[index] = fieldValue instanceof DictionaryVector dictionary ? dictionary : null;
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY) {
                if (fieldValue instanceof BinaryVector base) {
                    fieldBinaryBase[index] = base;
                }
                else if (fieldValue instanceof DictionaryVector dict && dict.values() instanceof BinaryVector base) {
                    fieldBinaryBase[index] = base;
                    fieldBinaryIds[index] = dict.ids();
                }
                else if (policy.singleRunBinaryAccessor() &&
                        index < Long.SIZE &&
                        fieldValue instanceof org.weakref.nitro.data.RleVector rle &&
                        rle.counts().length == 1 &&
                        rle.values() instanceof BinaryVector base) {
                    fieldBinaryBase[index] = base;
                    batchSingleRunBinaryFields |= 1L << index;
                    fieldBinaryConstantHash[index] = OperatorVectorSupport.binaryHash(
                            base.data(), base.startOffset(0), base.length(0));
                    if (policy.singleRunBinaryIdOnly() && policy.idOnlyBinaryRecords()) {
                        ValueIdInterner interner = fieldInterners[index];
                        if (interner == null) {
                            interner = new ValueIdInterner(policy.valueIdCeiling(), keyTablePolicy.valueIds());
                            fieldInterners[index] = interner;
                        }
                        fieldBinaryConstantGlobalId[index] = interner.intern(
                                base.data(), base.startOffset(0), base.length(0));
                    }
                }
            }
            fieldNullAccess[index] = fieldNulls != null && !batchFieldNullFree[index] && !batchFieldAllNull[index]
                    ? resolveNullAccessor(index, fieldNulls)
                    : null;
            if (channel >= values.length || !(values[channel] instanceof DictionaryVector dictionary)) {
                dictionaryHashedIds[index] = null;
                release(dictionaryEntryHashes[index]);
                dictionaryEntryHashes[index] = null;
                dictionaryHashedValues[index] = null;
                dictionaryHashedGenerations[index] = -1;
                continue;
            }
            Vector dictionaryValues = dictionary.values();
            int[] dictionaryIds = dictionary.ids();
            if (policy.composeNestedDictionaries() && dictionaryValues instanceof DictionaryVector) {
                int length = dictionary.length();
                int[] composed = composedDictionaryIds[index];
                if (composed == null || composed.length < length) {
                    int[] previous = composed;
                    composed = borrowInts(length);
                    composedDictionaryIds[index] = composed;
                    release(previous);
                }
                System.arraycopy(dictionaryIds, 0, composed, 0, length);
                do {
                    DictionaryVector nested = (DictionaryVector) dictionaryValues;
                    int[] nestedIds = nested.ids();
                    for (int position = 0; position < length; position++) {
                        composed[position] = nestedIds[composed[position]];
                    }
                    dictionaryValues = nested.values();
                }
                while (dictionaryValues instanceof DictionaryVector);
                dictionaryIds = composed;

                if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY && dictionaryValues instanceof BinaryVector base) {
                    fieldBinaryBase[index] = base;
                    fieldBinaryIds[index] = composed;
                }
                else if (fieldKinds[index] == FlatTypeHandler.Kind.LONG) {
                    int[] positions = composed;
                    fieldLong[index] = switch (dictionaryValues) {
                        case I64Vector base -> {
                            long[] leaf = base.values();
                            yield position -> leaf[positions[position]];
                        }
                        case I32Vector base -> {
                            int[] leaf = base.values();
                            yield position -> leaf[positions[position]];
                        }
                        default -> fieldLong[index];
                    };
                }
            }
            int distinctCount = dictionaryValues.length();
            // Cross-batch entry-hash reuse is retained for the variable-width payload it was designed to
            // amortize. Numeric vectors now expose generations for other derived-state consumers, but enabling a
            // second hash-cache cohort here would require its own activation and whole-query controls.
            long contentGeneration = handlers[index].variableWidth() ? dictionaryValues.contentGeneration() : -1;
            boolean sameDictionaryGeneration = policy.reuseDictionaryEntryHashes() &&
                    dictionaryValues instanceof BinaryVector binary &&
                    binary.contentImmutable() &&
                    contentGeneration >= 0 &&
                    dictionaryHashedValues[index] == dictionaryValues &&
                    dictionaryHashedGenerations[index] == contentGeneration;
            boolean cachedEntryHashes = sameDictionaryGeneration &&
                    dictionaryEntryHashes[index] != null &&
                    dictionaryEntryHashes[index].length >= distinctCount;
            boolean oversizedDictionary = distinctCount > dictionary.length();
            // A wide generated hash kernel can mix cached low-cardinality lanes with resolved accessors for an
            // oversized join-output lane. Eagerly traversing that large base on its second appearance defeats the
            // hybrid: the one complete pass costs more than hashing only referenced rows. Narrow layouts lack that
            // mixed-lane amortization opportunity and retain the established recurring-generation admission.
            boolean reusableEntryHashes = sameDictionaryGeneration &&
                    shouldReuseDictionaryEntryHashes(
                            handlers.length,
                            oversizedDictionary,
                            cachedEntryHashes,
                            policy);
            // Pre-hashing every distinct dictionary entry pays off only when the dictionary is smaller than the batch
            // it describes (low-card key, values repeat across rows): each entry is hashed once and reused by id. A
            // join-output key instead wraps the whole build column in a dictionary -- far more entries than a probe
            // batch references -- so pre-hashing all of them every batch is mostly wasted. When the dictionary has
            // more entries than the batch has positions, skip the first pre-hash and let fieldHash() hash the
            // referenced entries per row. If a later batch presents the same vector generation, the repeated use
            // proves that one complete entry-hash pass can be amortized; vectors without a generation retain the
            // conservative per-batch policy.
            if (oversizedDictionary && !reusableEntryHashes) {
                dictionaryHashedIds[index] = null;
                release(dictionaryEntryHashes[index]);
                dictionaryEntryHashes[index] = null;
                dictionaryHashedValues[index] = contentGeneration >= 0 ? dictionaryValues : null;
                dictionaryHashedGenerations[index] = contentGeneration;
            }
            else {
                dictionaryHashedIds[index] = dictionaryIds;
                // Compute the per-entry hash for every distinct dictionary value once and index it by dictionary id at
                // hash() time, instead of re-hashing the (variable-width) value per row. A concrete value vector may
                // let the cache survive across batches by exposing a stable content generation. Object identity alone
                // is never sufficient because the allocator pools vector instances; a generation change recomputes the
                // complete cache before it can influence hash placement.
                if (dictionaryEntryHashes[index] == null || dictionaryEntryHashes[index].length < distinctCount) {
                    long[] previous = dictionaryEntryHashes[index];
                    dictionaryEntryHashes[index] = borrowLongs(distinctCount);
                    release(previous);
                }
                if (!cachedEntryHashes) {
                    FlatTypeHandler.Kind kind = fieldKinds[index];
                    long[] entryHashes = dictionaryEntryHashes[index];
                    for (int id = 0; id < distinctCount; id++) {
                        entryHashes[id] = hashByKind(kind, dictionaryValues, id);
                    }
                }
                dictionaryHashedValues[index] = dictionaryValues;
                dictionaryHashedGenerations[index] = contentGeneration;
            }

            // Intern this dictionary's entries to GLOBAL value ids (stable across batches and dictionary
            // identities) so id-based equality works for the whole persistent group table, not just within one
            // dictionary instance. Id equality only applies to variable-width fields -- the byte-compare those
            // would otherwise pay is what we avoid.
            if (handlers[index].variableWidth()) {
                int[] entryGlobalIds = internDictionaryEntries(
                        index,
                        dictionaryValues,
                        eagerMixedComposite && dictionaryValues.length() <= dictionary.length());
                if (entryGlobalIds != null) {
                    boundDictionary[index] = dictionaryValues;
                    batchDictionaryIds[index] = dictionaryIds;
                    fieldIdComparable[index] = true;
                    anyFieldIdComparable = true;
                    if (!fieldLazyIntern[index] && compactCompositeRadix[index] == 0) {
                        compactCompositeRadix[index] = compactRadix(fieldInterners[index].distinctCount());
                    }
                }
            }
        }
        prepareCompactBinaryPositionIds(values);
        prepareBinaryHashAccessors(values);
        batchNullFreeLongBinary = policy.fastNullFreeLongBinary() &&
                handlers.length == 2 &&
                fieldKinds[0] == FlatTypeHandler.Kind.LONG &&
                fieldKinds[1] == FlatTypeHandler.Kind.BINARY &&
                batchFieldNullFree[0] &&
                batchFieldNullFree[1];
        // Version this concrete physical shape once at the batch boundary. Set-like and grouping operators then use
        // the same layout API without carrying flat/dictionary/null/type checks through every hash, insert, and
        // equality probe. This is a layout specialization, not an operator- or query-specific implementation.
        batchNullFreeSingleBinaryCandidate = policy.fastNullFreeSingleBinary() && singleField &&
                fieldKinds[0] == FlatTypeHandler.Kind.BINARY &&
                batchFieldNullFree[0] &&
                fieldBinaryBase[0] != null;
        // Flat binary input has always used this concrete layout. Dictionary input is admitted later by the
        // set-like consumer only when its active mask proves that a frequent sentinel makes the monomorphic
        // path worthwhile. A structural match alone is insufficient: SQL filters often leave the same physical
        // dictionary shape after removing the sentinel, and those batches do not amortize the specialized path.
        batchNullFreeSingleBinary = batchNullFreeSingleBinaryCandidate &&
                (fieldBinaryIds[0] == null || dictionarySingleBinaryFastPathAdmitted);

        if (batchMixedComposite && handlers.length > 3) {
            prepareWideMixedLongDomains(values);
        }

        // Decide once per batch whether the key can attempt array mode. Binary fields use query-stable interned
        // ids; small non-negative LONG fields use their value directly. Each radix reserves one digit for null,
        // which makes rollup grouping sets directly indexable instead of forcing their null-extended rows through
        // the hash table. A lazy dictionary may discover an id outside the binary radix later; compositeValueId
        // rejects just that position and the caller falls back to the normal hash table.
        batchCompositeEncodable = handlers.length > 0;
        batchCompositeEligible = batchCompositeEncodable;
        long compositeMultiplier = 1;
        for (int orderIndex = 0; batchCompositeEncodable && orderIndex < compositeOrder.length; orderIndex++) {
            int index = compositeOrder[orderIndex];
            int radix;
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY) {
                if (!fieldIdComparable[index] || fieldInterners[index] == null || fieldInterners[index].distinctCount() > COMPOSITE_STRIDE) {
                    batchCompositeEncodable = false;
                    batchCompositeEligible = false;
                    break;
                }
                radix = compositeRadix(index);
            }
            else if (fieldKinds[index] == FlatTypeHandler.Kind.LONG && fieldLong[index] != null) {
                if (!policy.mixedCompositeIds()) {
                    batchCompositeEncodable = false;
                    batchCompositeEligible = false;
                    break;
                }
                boolean compactWideLong = handlers.length > 3 && batchMixedComposite;
                if (compactWideLong
                        ? compactLongDomainRejected[index] || (!compactLongBaseSet[index] && !batchFieldAllNull[index])
                        : policy.earlyRejectMixedComposite() && hasEarlyOutOfRangeLong(index, values[index].length())) {
                    batchCompositeEncodable = false;
                    batchCompositeEligible = false;
                    break;
                }
                radix = LONG_COMPOSITE_RADIX;
            }
            else {
                batchCompositeEncodable = false;
                batchCompositeEligible = false;
                break;
            }
            try {
                compositeMultiplier = Math.multiplyExact(compositeMultiplier, radix);
            }
            catch (ArithmeticException _) {
                batchCompositeEncodable = false;
                batchCompositeEligible = false;
                break;
            }
            if (compositeMultiplier > COMPOSITE_MAX) {
                batchCompositeEligible = false;
            }
        }
        if (policy.debugMixedComposite() && batchMixedComposite && !debugMixedCompositePrinted) {
            debugMixedCompositePrinted = true;
            int[] radices = new int[compositeOrder.length];
            for (int orderIndex = 0; orderIndex < compositeOrder.length; orderIndex++) {
                int index = compositeOrder[orderIndex];
                radices[orderIndex] = fieldKinds[index] == FlatTypeHandler.Kind.BINARY
                        ? compositeRadix(index)
                        : LONG_COMPOSITE_RADIX;
            }
            System.err.printf("[mixed-composite] fields=%d eligible=%s product=%d radices=%s%n",
                    handlers.length, batchCompositeEligible, compositeMultiplier, Arrays.toString(radices));
        }
        batchNormalizedIntKeyEligible = normalizedIntKeyShape && sampledNormalizedIntKeyDomain(values, nulls);
        prepareConstantNullMixedComposite3();
        batchAccessorsReady = true;
        prepareGeneratedDictionaryRecordEquality();
        decideDiscriminatingHashField(values, nulls);
    }

    private void prepareBinaryHashAccessors(Vector[] values)
    {
        Arrays.fill(fieldBinaryHashes, null);
        for (int field = 0; field < handlers.length; field++) {
            BinaryVector base = fieldBinaryBase[field];
            if (fieldKinds[field] != FlatTypeHandler.Kind.BINARY) {
                continue;
            }
            if (base != null && (batchSingleRunBinaryFields & (1L << field)) != 0) {
                long hash = fieldBinaryConstantHash[field];
                fieldBinaryHashes[field] = _ -> hash;
                continue;
            }
            if (base != null) {
                int[] ids = fieldBinaryIds[field];
                byte[] data = base.data();
                int[] offsets = base.offsets();
                if (ids == null) {
                    fieldBinaryHashes[field] = position ->
                            OperatorVectorSupport.binaryHash(data, offsets[position], offsets[position + 1] - offsets[position]);
                }
                else {
                    fieldBinaryHashes[field] = position -> {
                        int entry = ids[position];
                        return OperatorVectorSupport.binaryHash(data, offsets[entry], offsets[entry + 1] - offsets[entry]);
                    };
                }
                continue;
            }
            int channel = inputChannels[field];
            if (channel >= values.length || values[channel] == null) {
                continue;
            }
            VectorAccess.BinaryRegions regions = VectorAccess.binaryRegions(values[channel]);
            fieldBinaryHashes[field] = position -> OperatorVectorSupport.binaryHash(
                    regions.data(position), regions.offset(position), regions.length(position));
        }
    }

    static boolean shouldReuseDictionaryEntryHashes(
            int fieldCount,
            boolean oversizedDictionary,
            boolean cachedEntryHashes,
            FlatKeyTablePolicy.Layout policy)
    {
        return cachedEntryHashes ||
                !oversizedDictionary ||
                fieldCount < policy.generatedHybridHashBatchMinFields();
    }

    private void prepareGeneratedDictionaryRecordEquality()
    {
        if (!policy.generatedDictionaryRecordEquality() ||
                !embedIdOnlyBinaryIds ||
                handlers.length < 5) {
            return;
        }
        List<DictionaryRecordEqualityKernelGenerator.FieldShape> fields = new ArrayList<>(handlers.length);
        for (int field = 0; field < handlers.length; field++) {
            if (fieldKinds[field] == FlatTypeHandler.Kind.BINARY) {
                if (!fieldIdComparable[field] || batchDictionaryIds[field] == null) {
                    return;
                }
            }
            else if (fieldKinds[field] != FlatTypeHandler.Kind.LONG ||
                    fieldLong[field] == null) {
                return;
            }
        }
        for (int field : comparisonOrder) {
            int kind = fieldKinds[field] == FlatTypeHandler.Kind.LONG
                    ? DictionaryRecordEqualityKernelGenerator.LONG
                    : compactEmbeddedBinaryRecords
                            ? DictionaryRecordEqualityKernelGenerator.COMPACT_BINARY_ID
                            : DictionaryRecordEqualityKernelGenerator.EMBEDDED_BINARY_ID;
            fields.add(new DictionaryRecordEqualityKernelGenerator.FieldShape(
                    field,
                    batchNullShape(field),
                    kind,
                    fixedOffsets[field],
                    handlers[field].fixedSize()));
        }
        generatedRecordEqualityKernel = codeGeneration.dictionaryRecordEquality().create(
                new DictionaryRecordEqualityKernelGenerator.Shape(
                        nullByteCount != 0,
                        fields));
    }

    private void prepareConstantNullMixedComposite3()
    {
        batchMixedComposite3ConstantNullMask = -1;
        batchMixedComposite3Prepared = false;
        if (!policy.fastConstantNullMixedComposite3() ||
                !policy.fastMixedComposite3() ||
                !batchCompositeEligible ||
                !batchMixedComposite ||
                compositeOrder.length != 3) {
            return;
        }

        int longIndex = compositeOrder[0];
        int firstBinary = compositeOrder[1];
        int secondBinary = compositeOrder[2];
        if (fieldKinds[longIndex] != FlatTypeHandler.Kind.LONG ||
                fieldKinds[firstBinary] != FlatTypeHandler.Kind.BINARY ||
                fieldKinds[secondBinary] != FlatTypeHandler.Kind.BINARY ||
                fieldLazyIntern[firstBinary] ||
                fieldLazyIntern[secondBinary]) {
            return;
        }

        batchMixedComposite3LongIndex = longIndex;
        batchMixedComposite3FirstBinary = firstBinary;
        batchMixedComposite3SecondBinary = secondBinary;
        batchMixedComposite3FirstRadix = compositeRadix(firstBinary);
        batchMixedComposite3SecondRadix = compositeRadix(secondBinary);
        batchMixedComposite3Prepared = true;
        if (!constantNullShape(longIndex) ||
                !constantNullShape(firstBinary) ||
                !constantNullShape(secondBinary)) {
            return;
        }
        int nullMask = (batchFieldAllNull[longIndex] ? 1 : 0) |
                (batchFieldAllNull[firstBinary] ? 2 : 0) |
                (batchFieldAllNull[secondBinary] ? 4 : 0);
        batchMixedComposite3ConstantNullMask = nullMask;
        if (policy.debugConstantNullMixedComposite3() && !debugConstantNullMixedComposite3Printed) {
            debugConstantNullMixedComposite3Printed = true;
            System.err.printf("[constant-null-mixed-composite-3] nullMask=%d radices=%d/%d%n",
                    nullMask,
                    batchMixedComposite3FirstRadix,
                    batchMixedComposite3SecondRadix);
        }
    }

    private boolean constantNullShape(int fieldIndex)
    {
        return batchFieldAllNull[fieldIndex] || batchFieldNullFree[fieldIndex];
    }

    /**
     * Normalized scratch is reserved before the position loop, so reject a batch up front when a representative
     * sample shows that its integer keys are outside the exact packed domain. Individual positions are still
     * checked by {@link #tryPrepareNormalizedIntKey}; this admission only avoids reserving dense scratch for a path
     * that would almost always fall back.
     */
    private boolean sampledNormalizedIntKeyDomain(Vector[] values, Vector[] nulls)
    {
        if (handlers.length == 0) {
            return false;
        }
        int length = values[inputChannels[0]].length();
        int sampleSize = Math.min(length, 32);
        for (int index = 0; index < fieldKinds.length; index++) {
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY) {
                if (fieldBinaryBase[index] == null) {
                    return false;
                }
                continue;
            }
            if (fieldLong[index] == null) {
                return false;
            }
            for (int sample = 0; sample < sampleSize; sample++) {
                int position = sampleSize == 1 ? 0 : (int) ((long) sample * (length - 1) / (sampleSize - 1));
                if (inputFieldNull(index, nulls, position)) {
                    continue;
                }
                long value = fieldLong[index].value(position);
                if (value < 0 || value >= Integer.MAX_VALUE) {
                    return false;
                }
            }
        }
        return true;
    }

    private void decideDiscriminatingHashField(Vector[] values, Vector[] nulls)
    {
        if (discriminatingHashFieldDecided) {
            return;
        }
        discriminatingHashFieldDecided = true;
        if (!policy.adaptiveDiscriminatingFieldHash() ||
                handlers.length < policy.discriminatingFieldHashMinFields() ||
                policy.discriminatingFieldHashSampleSize() <= 0 ||
                !hasSharedNestedDictionaryRecord(values)) {
            return;
        }

        int sampleSize = Math.min(values[0].length(), policy.discriminatingFieldHashSampleSize());
        if (sampleSize == 0) {
            return;
        }
        long[] hashes = arrayPool.borrowLongs(sampleSize);
        try {
            int bestField = -1;
            int bestDistinct = 0;
            for (int field = 0; field < handlers.length; field++) {
                // A nullable discriminator remains correct, but a common null value creates an avoidable collision
                // cluster. Admit only a null-free first batch; the selected hash remains correctness-equivalent if
                // a later batch happens to carry nulls.
                if (!batchFieldNullFree[field]) {
                    continue;
                }
                int channel = inputChannels[field];
                for (int position = 0; position < sampleSize; position++) {
                    hashes[position] = fieldHash(field, channel, values[channel], position);
                }
                Arrays.sort(hashes, 0, sampleSize);
                int distinct = 1;
                for (int position = 1; position < sampleSize; position++) {
                    if (hashes[position] != hashes[position - 1]) {
                        distinct++;
                    }
                }
                if (distinct > bestDistinct) {
                    bestDistinct = distinct;
                    bestField = field;
                }
            }
            if (bestField >= 0 &&
                    (long) bestDistinct * 100 >=
                            (long) sampleSize * policy.discriminatingFieldHashMinDistinctPercent()) {
                discriminatingHashField = bestField;
            }
        }
        finally {
            arrayPool.release(hashes);
        }
    }

    private boolean hasSharedNestedDictionaryRecord(Vector[] values)
    {
        Vector firstValue = values[inputChannels[0]];
        if (!(firstValue instanceof DictionaryVector first) || !(first.values() instanceof DictionaryVector firstNested)) {
            return false;
        }
        int[] outerIds = first.ids();
        int[] nestedIds = firstNested.ids();
        for (int field = 1; field < handlers.length; field++) {
            Vector value = values[inputChannels[field]];
            if (!(value instanceof DictionaryVector dictionary) ||
                    dictionary.length() != first.length() ||
                    !(dictionary.values() instanceof DictionaryVector nested) ||
                    nested.length() != firstNested.length() ||
                    !sameIds(outerIds, dictionary.ids(), first.length()) ||
                    !sameIds(nestedIds, nested.ids(), firstNested.length())) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameIds(int[] left, int[] right, int length)
    {
        if (left == right) {
            return true;
        }
        for (int index = 0; index < length; index++) {
            if (left[index] != right[index]) {
                return false;
            }
        }
        return true;
    }

    private boolean hasEarlyOutOfRangeLong(int fieldIndex, int length)
    {
        int sampleSize = Math.min(length, 32);
        for (int position = 0; position < sampleSize; position++) {
            if (inputFieldNull(fieldIndex, null, position)) {
                continue;
            }
            long value = fieldLong[fieldIndex].value(position);
            if (value < 0 || value >= LONG_COMPOSITE_CARDINALITY) {
                return true;
            }
        }
        return false;
    }

    private boolean hasEarlyOutOfRangeLong(Vector[] values)
    {
        for (int fieldIndex = 0; fieldIndex < fieldKinds.length; fieldIndex++) {
            if (fieldKinds[fieldIndex] != FlatTypeHandler.Kind.LONG) {
                continue;
            }
            int channel = inputChannels[fieldIndex];
            VectorAccess.LongValues longValues = VectorAccess.longValues(values[channel]);
            int sampleSize = Math.min(values[channel].length(), 32);
            for (int position = 0; position < sampleSize; position++) {
                long value = longValues.value(position);
                if (value < 0 || value >= LONG_COMPOSITE_CARDINALITY) {
                    return true;
                }
            }
        }
        return false;
    }

    int internedValueCount(int fieldIndex)
    {
        ValueIdInterner interner = fieldInterners == null ? null : fieldInterners[fieldIndex];
        return interner == null ? 0 : interner.distinctCount();
    }

    private void prepareWideMixedLongDomains(Vector[] values)
    {
        for (int index = 0; index < handlers.length; index++) {
            if (fieldKinds[index] != FlatTypeHandler.Kind.LONG || compactLongBaseSet[index] || compactLongDomainRejected[index]) {
                continue;
            }
            if (batchFieldAllNull[index]) {
                continue;
            }
            int channel = inputChannels[index];
            int length = values[channel].length();
            VectorAccess.BooleanValues nulls = fieldNullAccess[index];
            long minimum = Long.MAX_VALUE;
            long maximum = Long.MIN_VALUE;
            for (int position = 0; position < length; position++) {
                if (nulls != null && nulls.value(position)) {
                    continue;
                }
                long value = fieldLong[index].value(position);
                minimum = Math.min(minimum, value);
                maximum = Math.max(maximum, value);
            }
            if (minimum == Long.MAX_VALUE) {
                continue;
            }
            long range;
            try {
                range = Math.subtractExact(maximum, minimum);
            }
            catch (ArithmeticException _) {
                range = Long.MAX_VALUE;
            }
            if (range >= LONG_COMPOSITE_CARDINALITY) {
                compactLongDomainRejected[index] = true;
                continue;
            }
            compactLongBase[index] = minimum;
            compactLongBaseSet[index] = true;
        }
    }

    /**
     * Whether this batch exposes stable composite ids. A grouping table may resolve them through either a direct
     * array or a sparse primitive cache; the table decides whether that representation makes eager hash
     * precomputation unnecessary.
     */
    boolean batchArrayModeEligible()
    {
        return batchCompositeEncodable;
    }

    boolean batchDirectCompositeEligible()
    {
        return batchCompositeEligible;
    }

    /**
     * Whether this concrete layout exposes a cheap, stable sentinel that a set-like consumer may handle outside
     * its hash table.  The operator consumes only this structural contract; concrete type recognition stays in
     * the layout and can move into a generated shape without changing the consumer.
     */
    boolean hasTrackedSentinel(Vector[] values)
    {
        return batchNullFreeSingleBinary || (singleField &&
                fieldKinds[0] == FlatTypeHandler.Kind.BINARY &&
                values[singleInputChannel] instanceof BinaryVector);
    }

    /**
     * Admits the dictionary-backed single-binary concrete layout only when the rows the consumer will actually
     * process contain a frequent empty sentinel. Sampling logical mask ranks instead of source positions keeps the
     * decision representative for dense, sparse, and all-except masks without allocating a position list.
     */
    boolean admitFrequentDictionarySentinel(Vector[] values, Mask mask)
    {
        if (!batchNullFreeSingleBinaryCandidate || fieldBinaryIds[0] == null || mask.none()) {
            return batchNullFreeSingleBinary;
        }

        int samples = 0;
        int sentinelCount = 0;
        if (!dictionarySingleBinaryFastPathDecided) {
            dictionarySingleBinaryFastPathDecided = true;
            if (policy.trackDictionaryEmptySentinel() && policy.dictionarySentinelSampleSize() > 0) {
                samples = Math.min(mask.selectedCount(), policy.dictionarySentinelSampleSize());
                int[] ids = fieldBinaryIds[0];
                BinaryVector base = fieldBinaryBase[0];
                for (int sample = 0; sample < samples; sample++) {
                    int rank = samples == 1 ? 0 : (int) ((long) sample * (mask.selectedCount() - 1) / (samples - 1));
                    int position = mask.position(rank);
                    if (base.length(ids[position]) == 0) {
                        sentinelCount++;
                    }
                }
                dictionarySingleBinaryFastPathAdmitted =
                        (long) sentinelCount * 100 >=
                                (long) samples * policy.dictionarySentinelMinPercent();
            }
        }
        batchNullFreeSingleBinary = dictionarySingleBinaryFastPathAdmitted;
        if (policy.debugNullFreeSingleBinary() &&
                batchNullFreeSingleBinary &&
                samples > 0 &&
                !debugDictionarySingleBinaryPrinted) {
            debugDictionarySingleBinaryPrinted = true;
            System.err.printf("[null-free-single-binary] dictionary=true entries=%d positions=%d selected=%d sentinels=%d/%d%n",
                    fieldBinaryBase[0].length(), values[singleInputChannel].length(), mask.selectedCount(), sentinelCount, samples);
        }
        return batchNullFreeSingleBinary;
    }

    boolean isTrackedSentinel(Vector[] values, int position)
    {
        if (batchNullFreeSingleBinary) {
            int[] ids = fieldBinaryIds[0];
            return fieldBinaryBase[0].length(ids == null ? position : ids[position]) == 0;
        }
        if (!hasTrackedSentinel(values)) {
            return false;
        }
        return ((BinaryVector) values[singleInputChannel]).length(position) == 0;
    }

    long compositeValueId(int position)
    {
        if (!batchCompositeEncodable) {
            return -1;
        }
        if (policy.fastNullFreeLongBinaryComposite() && batchNullFreeLongBinary && !fieldLazyIntern[1]) {
            long longDigit = fieldLong[0].value(position);
            if (longDigit < 0 || longDigit >= LONG_COMPOSITE_CARDINALITY) {
                return -1;
            }
            int binaryRadix = compositeRadix(1);
            long binaryDigit = batchEntryGlobalId[1][batchDictionaryIds[1][position]];
            if (binaryDigit < 0 || binaryDigit >= binaryRadix - 1L) {
                return -1;
            }
            if (compositeOrder[0] == 0) {
                return longDigit + LONG_COMPOSITE_RADIX * binaryDigit;
            }
            return binaryDigit + (long) binaryRadix * longDigit;
        }
        if (batchMixedComposite3ConstantNullMask >= 0) {
            int nullMask = batchMixedComposite3ConstantNullMask;
            long longDigit = (nullMask & 1) != 0
                    ? LONG_COMPOSITE_CARDINALITY
                    : fieldLong[batchMixedComposite3LongIndex].value(position);
            long longLimit = (nullMask & 1) != 0 ? LONG_COMPOSITE_RADIX : LONG_COMPOSITE_CARDINALITY;
            if (longDigit < 0 || longDigit >= longLimit) {
                return -1;
            }

            int firstRadix = batchMixedComposite3FirstRadix;
            long firstDigit = (nullMask & 2) != 0
                    ? firstRadix - 1L
                    : batchEntryGlobalId[batchMixedComposite3FirstBinary][batchDictionaryIds[batchMixedComposite3FirstBinary][position]];
            long firstLimit = (nullMask & 2) != 0 ? firstRadix : firstRadix - 1L;
            if (firstDigit < 0 || firstDigit >= firstLimit) {
                return -1;
            }

            int secondRadix = batchMixedComposite3SecondRadix;
            long secondDigit = (nullMask & 4) != 0
                    ? secondRadix - 1L
                    : batchEntryGlobalId[batchMixedComposite3SecondBinary][batchDictionaryIds[batchMixedComposite3SecondBinary][position]];
            long secondLimit = (nullMask & 4) != 0 ? secondRadix : secondRadix - 1L;
            if (secondDigit < 0 || secondDigit >= secondLimit) {
                return -1;
            }
            return longDigit + LONG_COMPOSITE_RADIX * (firstDigit + (long) firstRadix * secondDigit);
        }
        if (policy.fastMixedComposite3() && batchMixedComposite && compositeOrder.length == 3) {
            // compositeOrder puts the LONG digit first. The mixed-composite policy eagerly interns both
            // dictionary bases, so their entry->global-id maps contain no lazy sentinels and can be read
            // directly. Keep this common narrow rollup monomorphic instead of walking handler metadata and
            // calling globalIdFor for every field of every row.
            int longIndex = compositeOrder[0];
            int firstBinary = compositeOrder[1];
            int secondBinary = compositeOrder[2];
            if (fieldKinds[longIndex] == FlatTypeHandler.Kind.LONG &&
                    fieldKinds[firstBinary] == FlatTypeHandler.Kind.BINARY &&
                    fieldKinds[secondBinary] == FlatTypeHandler.Kind.BINARY &&
                    !fieldLazyIntern[firstBinary] && !fieldLazyIntern[secondBinary]) {
                boolean longNull = inputFieldNull(longIndex, null, position);
                long longDigit = longNull ? LONG_COMPOSITE_CARDINALITY : fieldLong[longIndex].value(position);
                if (longDigit < 0 || (!longNull && longDigit >= LONG_COMPOSITE_CARDINALITY)) {
                    return -1;
                }

                int firstRadix = compositeRadix(firstBinary);
                boolean firstNull = inputFieldNull(firstBinary, null, position);
                long firstDigit = firstNull ? firstRadix - 1L : batchEntryGlobalId[firstBinary][batchDictionaryIds[firstBinary][position]];
                if (firstDigit < 0 || (!firstNull && firstDigit >= firstRadix - 1L)) {
                    return -1;
                }

                int secondRadix = compositeRadix(secondBinary);
                boolean secondNull = inputFieldNull(secondBinary, null, position);
                long secondDigit = secondNull ? secondRadix - 1L : batchEntryGlobalId[secondBinary][batchDictionaryIds[secondBinary][position]];
                if (secondDigit < 0 || (!secondNull && secondDigit >= secondRadix - 1L)) {
                    return -1;
                }
                return longDigit + LONG_COMPOSITE_RADIX * (firstDigit + (long) firstRadix * secondDigit);
            }
        }
        long composite = 0;
        long multiplier = 1;
        for (int orderIndex = 0; orderIndex < compositeOrder.length; orderIndex++) {
            int index = compositeOrder[orderIndex];
            long digit;
            int radix;
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY) {
                radix = compositeRadix(index);
                if (inputFieldNull(index, null, position)) {
                    digit = radix - 1L;
                }
                else {
                    digit = globalIdFor(index, batchDictionaryIds[index][position]);
                    if (digit < 0 || digit >= radix - 1L) {
                        return -1;
                    }
                }
            }
            else {
                radix = LONG_COMPOSITE_RADIX;
                if (inputFieldNull(index, null, position)) {
                    digit = LONG_COMPOSITE_CARDINALITY;
                }
                else {
                    digit = fieldLong[index].value(position);
                    if (handlers.length > 3 && batchMixedComposite && compactLongBaseSet[index]) {
                        digit -= compactLongBase[index];
                    }
                    if (digit < 0 || digit >= LONG_COMPOSITE_CARDINALITY) {
                        return -1;
                    }
                }
            }
            composite += digit * multiplier;
            multiplier *= radix;
        }
        return composite;
    }

    /**
     * Assign a complete batch through the prepared constant-null three-field recipe. The ordinary per-position
     * driver has to cross both the table and layout abstraction for every row even though {@link #beginBatch}
     * already resolved the physical arrays, field order, radices, and null shape. Keep that abstraction at the
     * batch boundary: the loop below uses the exact same composite id and falls back to the ordinary hash table for
     * an out-of-domain digit or the first occurrence of a composite. A negative return means this batch does not
     * expose the concrete recipe and the caller must use its existing driver.
     */
    long assignMixedComposite3Batch(
            FlatGroupingTable table,
            Vector[] values,
            Vector[] nulls,
            Mask mask,
            I64Vector result,
            long nextGroupId)
    {
        if (policy.debugConstantNullMixedComposite3() &&
                batchMixedComposite &&
                compositeOrder.length == 3 &&
                debugConstantNullMixedComposite3BatchMask != batchMixedComposite3ConstantNullMask &&
                debugConstantNullMixedComposite3BatchTransitions++ < 12) {
            debugConstantNullMixedComposite3BatchMask = batchMixedComposite3ConstantNullMask;
            System.err.printf("[constant-null-mixed-composite-3-batch] mask=%d rows=%d enabled=%s%n",
                    batchMixedComposite3ConstantNullMask,
                    mask.selectedCount(),
                    policy.fastMixedComposite3Batch());
        }
        if (!policy.fastMixedComposite3Batch() || !batchMixedComposite3Prepared) {
            return -1;
        }

        int longIndex = batchMixedComposite3LongIndex;
        int firstBinary = batchMixedComposite3FirstBinary;
        int secondBinary = batchMixedComposite3SecondBinary;
        VectorAccess.LongValues longValues = fieldLong[batchMixedComposite3LongIndex];
        int[] firstIds = batchDictionaryIds[firstBinary];
        int[] firstGlobalIds = batchEntryGlobalId[firstBinary];
        int[] secondIds = batchDictionaryIds[secondBinary];
        int[] secondGlobalIds = batchEntryGlobalId[secondBinary];
        VectorAccess.BooleanValues longNulls = fieldNullAccess[longIndex];
        VectorAccess.BooleanValues firstNulls = fieldNullAccess[firstBinary];
        VectorAccess.BooleanValues secondNulls = fieldNullAccess[secondBinary];
        int firstRadix = batchMixedComposite3FirstRadix;
        int secondRadix = batchMixedComposite3SecondRadix;
        int shape = MixedComposite3GroupingKernelGenerator.shape(
                batchNullShape(longIndex),
                batchNullShape(firstBinary),
                batchNullShape(secondBinary));
        MixedComposite3GroupingKernel kernel = codeGeneration.mixedComposite3Grouping().create(shape);
        int[] compositeCache = table.prepareCompositeCache(Math.multiplyExact(
                LONG_COMPOSITE_RADIX,
                Math.multiplyExact(firstRadix, secondRadix)));
        return kernel.assign(
                mask.selectedPositions(),
                mask.selectedCount(),
                longValues,
                longNulls,
                firstIds,
                firstGlobalIds,
                firstNulls,
                secondIds,
                secondGlobalIds,
                secondNulls,
                firstRadix,
                secondRadix,
                table,
                values,
                nulls,
                nextGroupId,
                result.values(),
                compositeCache);
    }

    private int batchNullShape(int fieldIndex)
    {
        if (batchFieldAllNull[fieldIndex]) {
            return MixedComposite3GroupingKernelGenerator.ALL_NULL;
        }
        if (batchFieldNullFree[fieldIndex]) {
            return MixedComposite3GroupingKernelGenerator.NULL_FREE;
        }
        return MixedComposite3GroupingKernelGenerator.MIXED;
    }

    /**
     * This batch's dictionary entry -> global value id map for a variable-width field, interning any entries not
     * seen before. Cached by dictionary identity. Returns {@code null} when the dictionary's values are not a
     * byte-string vector (the field then keeps the value-comparison path). An entry that overflows the interner
     * ceiling maps to -1, and equality for that value falls back to the byte compare.
     */
    private int[] internDictionaryEntries(int fieldIndex, Vector dictionaryValues, boolean eagerForComposite)
    {
        if (!(dictionaryValues instanceof BinaryVector dictionary)) {
            return null;
        }
        if (dictionary.contentImmutable() &&
                batchEntryGlobalIdDict[fieldIndex] == dictionaryValues &&
                batchEntryGlobalIdGeneration[fieldIndex] == dictionary.contentGeneration() &&
                batchEntryGlobalId[fieldIndex] != null) {
            return batchEntryGlobalId[fieldIndex];
        }
        ValueIdInterner interner = fieldInterners[fieldIndex];
        if (interner == null) {
            interner = new ValueIdInterner(policy.valueIdCeiling(), keyTablePolicy.valueIds());
            fieldInterners[fieldIndex] = interner;
        }
        int entryCount = dictionary.length();
        int[] previous = batchEntryGlobalId[fieldIndex];
        int[] globalIds = borrowInts(entryCount);
        release(previous);
        // A base larger than the array-mode stride can never be array-mode eligible, and a join wraps the
        // whole build column so most entries are never referenced: intern lazily on first reference instead
        // of eagerly here. Small bases stay eager so their distinctCount is known for the array-mode decision.
        if (entryCount > COMPOSITE_STRIDE && !eagerForComposite) {
            Arrays.fill(globalIds, GLOBAL_ID_NOT_INTERNED);
            fieldLazyIntern[fieldIndex] = true;
            batchEntryGlobalId[fieldIndex] = globalIds;
            batchEntryGlobalIdDict[fieldIndex] = dictionaryValues;
            batchEntryGlobalIdGeneration[fieldIndex] = dictionary.contentGeneration();
            return globalIds;
        }
        fieldLazyIntern[fieldIndex] = false;
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
            long[] entryHashes = dictionaryEntryHashes[fieldIndex];
            globalIds[entry] = entryHashes == null
                    ? interner.intern(data, so, len)
                    : interner.intern(data, so, len, (int) entryHashes[entry]);
        }
        batchEntryGlobalId[fieldIndex] = globalIds;
        batchEntryGlobalIdDict[fieldIndex] = dictionaryValues;
        batchEntryGlobalIdGeneration[fieldIndex] = dictionary.contentGeneration();
        return globalIds;
    }

    private int compositeRadix(int fieldIndex)
    {
        if (!batchMixedComposite) {
            return BINARY_COMPOSITE_RADIX;
        }
        int radix = compactCompositeRadix[fieldIndex];
        return radix == 0 ? BINARY_COMPOSITE_RADIX : radix;
    }

    private static int compactRadix(int distinctCount)
    {
        int required = Math.max(2, distinctCount + 1); // one digit is reserved for null
        int radix = Integer.highestOneBit(required - 1) << 1;
        return Math.min(radix, BINARY_COMPOSITE_RADIX);
    }

    private boolean hasLongField()
    {
        for (FlatTypeHandler.Kind kind : fieldKinds) {
            if (kind == FlatTypeHandler.Kind.LONG) {
                return true;
            }
        }
        return false;
    }

    private boolean hasCompactDirectBinaryDictionary(Vector[] values)
    {
        for (int index = 0; index < fieldKinds.length; index++) {
            int channel = inputChannels[index];
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY &&
                    channel < values.length &&
                    values[channel] instanceof DictionaryVector dictionary &&
                    dictionary.values() instanceof BinaryVector base &&
                    base.length() <= COMPOSITE_STRIDE) {
                return true;
            }
        }
        return false;
    }

    /**
     * The global value id for dictionary entry {@code dictId} of {@code fieldIndex}, interning it on first
     * reference for a lazily-populated (large) base. Eager fields short-circuit on the first read.
     */
    private int globalIdFor(int fieldIndex, int dictId)
    {
        int[] ids = batchEntryGlobalId[fieldIndex];
        int cached = ids[dictId];
        if (cached != GLOBAL_ID_NOT_INTERNED) {
            return cached;
        }
        BinaryVector dictionary = (BinaryVector) boundDictionary[fieldIndex];
        int so = dictionary.startOffset(dictId);
        int len = dictionary.length(dictId);
        int globalId;
        if (len < 0 || so < 0 || so + len > dictionary.data().length) {
            globalId = -1;
        }
        else {
            long[] entryHashes = dictionaryEntryHashes[fieldIndex];
            globalId = entryHashes == null
                    ? fieldInterners[fieldIndex].intern(dictionary.data(), so, len)
                    : fieldInterners[fieldIndex].intern(dictionary.data(), so, len, (int) entryHashes[dictId]);
        }
        ids[dictId] = globalId;
        return globalId;
    }

    int globalIdAtPosition(int fieldIndex, int position)
    {
        return globalIdFor(fieldIndex, batchDictionaryIds[fieldIndex][position]);
    }

    final int preparedGlobalIdAtPosition(int fieldIndex, int position)
    {
        return batchPositionGlobalId[fieldIndex][position];
    }

    private void prepareCompactBinaryPositionIds(Vector[] values)
    {
        if (!policy.precomputeCompactBinaryPositionIds() || !compactEmbeddedBinaryRecords || values.length == 0) {
            return;
        }
        int positions = values[0].length();
        int activeFields = 0;
        for (int index = 0; index < handlers.length; index++) {
            if (batchFieldAllNull[index] || !fieldIdComparable[index] || batchDictionaryIds[index] == null) {
                continue;
            }
            int[] dictionaryIds = batchDictionaryIds[index];
            int[] positioned = batchPositionGlobalId[index];
            boolean reusable = positioned != null &&
                    positioned.length >= positions &&
                    fieldDictionaryMapping[index] != null &&
                    fieldDictionaryMapping[index].baseValues() instanceof BinaryVector binary &&
                    binary.contentImmutable() &&
                    fieldDictionaryMapping[index].hasSameMapping(batchPositionDictionaryMapping[index]);
            if (reusable) {
                activeFields++;
                continue;
            }
            if (positioned == null || positioned.length < positions) {
                int[] previous = positioned;
                positioned = borrowInts(positions);
                batchPositionGlobalId[index] = positioned;
                release(previous);
            }
            int[] entryGlobalIds = batchEntryGlobalId[index];
            for (int position = 0; position < positions; position++) {
                int dictionaryId = dictionaryIds[position];
                int globalId = entryGlobalIds[dictionaryId];
                positioned[position] = globalId == GLOBAL_ID_NOT_INTERNED
                        ? globalIdFor(index, dictionaryId)
                        : globalId;
            }
            batchPositionDictionaryMapping[index] = fieldDictionaryMapping[index];
            activeFields++;
        }
        if (policy.debugCompactBinaryPositionIds() && activeFields > 0 && !debugCompactBinaryPositionIdsPrinted) {
            debugCompactBinaryPositionIdsPrinted = true;
            System.err.printf("[compact-binary-position-ids] fields=%d active=%d positions=%d%n",
                    handlers.length, activeFields, positions);
        }
    }

    /**
     * Emits a grouped BINARY key column as a {@link DictionaryVector} over the field's interned distinct values
     * (base indexed by global id) using each group's stored id, when every group carried a valid interned id and
     * the values repeat enough that the dictionary is smaller than a flat per-group copy. A downstream re-group or
     * join over the key then sees a compact dictionary rather than one entry per row. Returns {@code null} to fall
     * back to the flat {@code materializeValues} path. Nulls ride the separate NULLS stream, so a null or absent
     * group can point at any base entry.
     */
    Vector tryGroupedValuesAsDictionary(FlatGroupingTable table, int fieldIndex, int size, org.weakref.nitro.data.Mask mask, org.weakref.nitro.data.Allocator allocator, org.weakref.nitro.data.Allocator.Context allocationContext)
    {
        if (fieldKinds[fieldIndex] != FlatTypeHandler.Kind.BINARY) {
            return null;
        }
        ValueIdInterner interner = fieldInterners == null ? null : fieldInterners[fieldIndex];
        int[] recordIds = recordDictionaryIds == null ? null : recordDictionaryIds[fieldIndex];
        if (interner == null ||
                (!normalizedIntKeyShape && !embedIdOnlyBinaryIds &&
                        (policy.packedRecordDictionaryIds() ? packedRecordDictionaryIds == null : recordIds == null))) {
            return null;
        }
        int distinct = interner.distinctCount();
        // Only when the distinct values are at most half the groups: the dictionary base is then smaller than
        // the flat per-group copy would be, so emitting it never costs more than materializing flat even when
        // nothing downstream re-groups the key, while a downstream re-group/join sees a compact dictionary.
        if (distinct == 0 || (!fieldUsesIdOnlyRecords[fieldIndex] && distinct * 2 > size)) {
            return null;
        }
        I32Vector ownedDictionaryIds = policy.ownGroupedDictionaryIds()
                ? allocator.allocate(allocationContext, I32Vector.class, size, I32Vector::new)
                : null;
        int[] dictionaryIds = ownedDictionaryIds == null ? new int[size] : ownedDictionaryIds.values();
        for (int index : mask) {
            int recordIndex = table.recordIndex(index);
            if (recordIndex < 0) {
                return null;
            }
            if (table.fieldNull(recordIndex, fieldIndex)) {
                dictionaryIds[index] = 0;
                continue;
            }
            int globalId = groupedDictionaryId(table, fieldIndex, recordIndex, recordIds);
            if (globalId < 0 || globalId >= distinct) {
                return null;
            }
            dictionaryIds[index] = globalId;
        }
        BinaryVector base = interner.toBinaryVector(allocator, allocationContext);
        base.clearTraits();
        base.addTraits(field(fieldIndex).binaryTraits());
        if (ownedDictionaryIds != null) {
            return allocator.adopt(allocationContext, DictionaryVector.wrapOwnedIds(ownedDictionaryIds, size, base));
        }
        return DictionaryVector.wrap(dictionaryIds, base);
    }

    Vector tryGroupedValueRangeAsDictionary(
            FlatGroupingTable table,
            int fieldIndex,
            int sourceStart,
            int size,
            org.weakref.nitro.data.Mask outputMask,
            org.weakref.nitro.data.Allocator allocator,
            org.weakref.nitro.data.Allocator.Context allocationContext)
    {
        if (fieldKinds[fieldIndex] != FlatTypeHandler.Kind.BINARY) {
            return null;
        }
        ValueIdInterner interner = fieldInterners == null ? null : fieldInterners[fieldIndex];
        int[] recordIds = recordDictionaryIds == null ? null : recordDictionaryIds[fieldIndex];
        if (interner == null ||
                (!normalizedIntKeyShape && !embedIdOnlyBinaryIds &&
                        (policy.packedRecordDictionaryIds() ? packedRecordDictionaryIds == null : recordIds == null))) {
            return null;
        }
        int distinct = interner.distinctCount();
        // A ranged output must not retain the generation-wide dictionary when that base dwarfs the range. ID-only
        // records can still materialize their values through the interner-backed copy path below this admission.
        if (distinct == 0 || distinct * 2 > outputMask.count()) {
            return null;
        }

        I32Vector ownedDictionaryIds = policy.ownGroupedDictionaryIds()
                ? allocator.allocate(allocationContext, I32Vector.class, size, I32Vector::new)
                : null;
        int[] dictionaryIds = ownedDictionaryIds == null ? new int[size] : ownedDictionaryIds.values();
        long flatValueBytes = 0;
        for (int outputPosition : outputMask) {
            int recordIndex = table.recordIndex(sourceStart + outputPosition);
            if (recordIndex < 0) {
                return null;
            }
            if (table.fieldNull(recordIndex, fieldIndex)) {
                dictionaryIds[outputPosition] = 0;
                continue;
            }
            int globalId = groupedDictionaryId(table, fieldIndex, recordIndex, recordIds);
            if (globalId < 0 || globalId >= distinct) {
                return null;
            }
            dictionaryIds[outputPosition] = globalId;
            flatValueBytes += interner.valueLength(globalId);
        }

        long dictionaryBytes = (long) interner.valueBytes() +
                ((long) distinct + 1) * Integer.BYTES +
                (long) size * Integer.BYTES;
        long flatBytes = flatValueBytes + ((long) size + 1) * Integer.BYTES;
        if (dictionaryBytes > flatBytes) {
            if (ownedDictionaryIds != null) {
                allocator.release(allocationContext, ownedDictionaryIds);
            }
            return null;
        }

        BinaryVector base = interner.toBinaryVector(allocator, allocationContext);
        base.clearTraits();
        base.addTraits(field(fieldIndex).binaryTraits());
        if (ownedDictionaryIds != null) {
            return allocator.adopt(allocationContext, DictionaryVector.wrapOwnedIds(ownedDictionaryIds, size, base));
        }
        return DictionaryVector.wrap(dictionaryIds, base);
    }

    private int groupedDictionaryId(FlatGroupingTable table, int fieldIndex, int recordIndex, int[] recordIds)
    {
        int normalizedId = table.normalizedBinaryId(recordIndex, fieldIndex);
        if (normalizedId >= 0) {
            return normalizedId;
        }
        if (compactBinaryRecord(fieldIndex)) {
            byte[] chunk = table.fixedChunk(recordIndex);
            int offset = table.keyOffset(table.fixedOffset(recordIndex)) + fixedOffsets[fieldIndex];
            return (int) GROUP_INT_HANDLE.get(chunk, offset);
        }
        if (embedIdOnlyBinaryIds) {
            byte[] chunk = table.fixedChunk(recordIndex);
            int offset = table.keyOffset(table.fixedOffset(recordIndex)) + fixedOffsets[fieldIndex];
            if ((int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2) >= 0) {
                return -1;
            }
            return recordDictionaryId(fieldIndex, chunk, offset, recordIndex);
        }
        if (policy.packedRecordDictionaryIds()) {
            int packedIndex = recordIndex * packedDictionaryFieldCount + packedDictionaryFieldIndex[fieldIndex];
            return packedIndex >= packedRecordDictionaryIds.length ? -1 : packedRecordDictionaryIds[packedIndex];
        }
        return recordIndex >= recordIds.length ? -1 : recordIds[recordIndex];
    }

    Vector tryMaterializeIdBackedBinaryValues(
            FlatGroupingTable table,
            int fieldIndex,
            int size,
            org.weakref.nitro.data.Mask mask,
            Vector output,
            org.weakref.nitro.data.Allocator allocator,
            org.weakref.nitro.data.Allocator.Context allocationContext)
    {
        if (fieldKinds[fieldIndex] != FlatTypeHandler.Kind.BINARY || (!compactBinaryRecord(fieldIndex) && !fieldUsesIdOnlyRecords[fieldIndex])) {
            return null;
        }
        long totalBytes = 0;
        for (int groupId : mask) {
            int recordIndex = table.recordIndex(groupId);
            if (recordIndex < 0 || table.fieldNull(recordIndex, fieldIndex)) {
                continue;
            }
            byte[] chunk = table.fixedChunk(recordIndex);
            int offset = table.keyOffset(table.fixedOffset(recordIndex)) + fixedOffsets[fieldIndex];
            if (compactBinaryRecord(fieldIndex)) {
                int token = (int) GROUP_INT_HANDLE.get(chunk, offset);
                totalBytes += token >= 0
                        ? fieldInterners[fieldIndex].valueLength(token)
                        : compactBinaryLength(compactBinaryFallback(fieldIndex, ~token));
            }
            else {
                int length = (int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2);
                totalBytes += length < 0 ? fieldInterners[fieldIndex].valueLength(recordDictionaryId(fieldIndex, chunk, offset, recordIndex)) : length;
            }
        }
        if (totalBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Grouped binary output exceeds maximum byte capacity: " + totalBytes);
        }

        BinaryVector existing = output instanceof BinaryVector binary ? binary : null;
        BinaryVector result = BinaryVector.allocateOrGrow(allocator, allocationContext, existing, size, (int) totalBytes);
        Arrays.fill(result.offsets(), 0);
        result.clearTraits();
        result.addTraits(field(fieldIndex).binaryTraits());
        int previous = 0;
        for (int groupId : mask) {
            while (previous < groupId) {
                result.setNull(previous++);
            }
            int recordIndex = table.recordIndex(groupId);
            if (recordIndex < 0 || table.fieldNull(recordIndex, fieldIndex)) {
                result.setNull(groupId);
            }
            else {
                byte[] chunk = table.fixedChunk(recordIndex);
                int offset = table.keyOffset(table.fixedOffset(recordIndex)) + fixedOffsets[fieldIndex];
                if (compactBinaryRecord(fieldIndex)) {
                    int token = (int) GROUP_INT_HANDLE.get(chunk, offset);
                    if (token >= 0) {
                        fieldInterners[fieldIndex].copyValue(token, result, groupId);
                    }
                    else {
                        long fallback = compactBinaryFallback(fieldIndex, ~token);
                        result.setBytes(
                                groupId,
                                table.variableWidthArena().chunk(compactBinaryChunkIndex(fallback)),
                                compactBinaryChunkOffset(fallback),
                                compactBinaryLength(fallback));
                    }
                }
                else {
                    int length = (int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2);
                    if (length < 0) {
                        fieldInterners[fieldIndex].copyValue(recordDictionaryId(fieldIndex, chunk, offset, recordIndex), result, groupId);
                    }
                    else {
                        FlatTypeHandlers.BINARY.copyBinaryTo(chunk, offset, table.variableWidthArena(), result, groupId);
                    }
                }
            }
            previous = groupId + 1;
        }
        while (previous < size) {
            result.setNull(previous++);
        }
        return result;
    }

    Vector tryCopyIdBackedBinaryValue(
            FlatGroupingTable table,
            int fieldIndex,
            int recordIndex,
            Vector output,
            int outputPosition,
            int size,
            org.weakref.nitro.data.Allocator allocator,
            org.weakref.nitro.data.Allocator.Context allocationContext)
    {
        if (fieldKinds[fieldIndex] != FlatTypeHandler.Kind.BINARY || (!compactBinaryRecord(fieldIndex) && !fieldUsesIdOnlyRecords[fieldIndex])) {
            return null;
        }

        byte[] chunk = table.fixedChunk(recordIndex);
        int offset = table.keyOffset(table.fixedOffset(recordIndex)) + fixedOffsets[fieldIndex];
        int length;
        if (compactBinaryRecord(fieldIndex)) {
            int token = (int) GROUP_INT_HANDLE.get(chunk, offset);
            length = token >= 0
                    ? fieldInterners[fieldIndex].valueLength(token)
                    : compactBinaryLength(compactBinaryFallback(fieldIndex, ~token));
        }
        else {
            int storedLength = (int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2);
            length = storedLength < 0
                    ? fieldInterners[fieldIndex].valueLength(recordDictionaryId(fieldIndex, chunk, offset, recordIndex))
                    : storedLength;
        }

        BinaryVector existing = output instanceof BinaryVector binary ? binary : null;
        int outputOffset = existing == null ? 0 : existing.offsets()[outputPosition];
        BinaryVector result = BinaryVector.allocateOrGrow(
                allocator,
                allocationContext,
                existing,
                size,
                Math.addExact(outputOffset, length));
        if (outputPosition == 0) {
            Arrays.fill(result.offsets(), 0);
            result.clearTraits();
            result.addTraits(field(fieldIndex).binaryTraits());
        }
        else if (result.traits().isEmpty()) {
            result.addTraits(field(fieldIndex).binaryTraits());
        }

        if (compactBinaryRecord(fieldIndex)) {
            int token = (int) GROUP_INT_HANDLE.get(chunk, offset);
            if (token >= 0) {
                fieldInterners[fieldIndex].copyValue(token, result, outputPosition);
            }
            else {
                long fallback = compactBinaryFallback(fieldIndex, ~token);
                result.setBytes(
                        outputPosition,
                        table.variableWidthArena().chunk(compactBinaryChunkIndex(fallback)),
                        compactBinaryChunkOffset(fallback),
                        compactBinaryLength(fallback));
            }
        }
        else {
            int storedLength = (int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2);
            if (storedLength < 0) {
                fieldInterners[fieldIndex].copyValue(
                        recordDictionaryId(fieldIndex, chunk, offset, recordIndex),
                        result,
                        outputPosition);
            }
            else {
                FlatTypeHandlers.BINARY.copyBinaryTo(chunk, offset, table.variableWidthArena(), result, outputPosition);
            }
        }
        return result;
    }

    BinaryVector tryPrepareIdBackedBinaryOutput(
            FlatGroupingTable table,
            int fieldIndex,
            int[] sourcePositions,
            int sourceStart,
            int sourceCount,
            int size,
            BinaryVector output,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        if (fieldKinds[fieldIndex] != FlatTypeHandler.Kind.BINARY || (!compactBinaryRecord(fieldIndex) && !fieldUsesIdOnlyRecords[fieldIndex])) {
            return null;
        }
        long totalBytes = 0;
        for (int index = 0; index < sourceCount; index++) {
            int recordIndex = table.recordIndex(sourcePositions[sourceStart + index]);
            if (recordIndex < 0 || table.fieldNull(recordIndex, fieldIndex)) {
                continue;
            }
            byte[] chunk = table.fixedChunk(recordIndex);
            int offset = table.keyOffset(table.fixedOffset(recordIndex)) + fixedOffsets[fieldIndex];
            if (compactBinaryRecord(fieldIndex)) {
                int token = (int) GROUP_INT_HANDLE.get(chunk, offset);
                totalBytes += token >= 0
                        ? fieldInterners[fieldIndex].valueLength(token)
                        : compactBinaryLength(compactBinaryFallback(fieldIndex, ~token));
            }
            else {
                int length = (int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2);
                totalBytes += length < 0 ? fieldInterners[fieldIndex].valueLength(recordDictionaryId(fieldIndex, chunk, offset, recordIndex)) : length;
            }
        }
        if (totalBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Grouped binary output exceeds maximum byte capacity: " + totalBytes);
        }
        BinaryVector result = BinaryVector.allocateOrGrow(allocator, allocationContext, output, size, (int) totalBytes);
        Arrays.fill(result.offsets(), 0);
        result.clearTraits();
        result.addTraits(field(fieldIndex).binaryTraits());
        return result;
    }

    /**
     * Hook called by {@link FlatGroupingTable} after a batch completes. Mirror of
     * {@link #beginBatch}; subclasses release cached references here.
     */
    public void endBatch()
    {
        batchAccessorsReady = false;
        generatedRecordEqualityKernel = null;
        batchNullFreeLongBinary = false;
        batchNullFreeSingleBinaryCandidate = false;
        batchNullFreeSingleBinary = false;
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
        if (batchNullFreeSingleBinary) {
            BinaryVector binary = fieldBinaryBase[0];
            int entry = binaryEntry(0, position);
            return 31 + OperatorVectorSupport.binaryHash(binary.data(), binary.startOffset(entry), binary.length(entry));
        }
        if (batchNullFreeLongBinary) {
            long result = 31L + Long.hashCode(fieldLong[0].value(position));
            return 31 * result + binaryFieldHash(1, values[inputChannels[1]], position);
        }
        if (singleField) {
            if (inputFieldNull(0, nulls, position)) {
                return 31;
            }
            return 31 + fieldHash(0, singleInputChannel, values[singleInputChannel], position);
        }
        if (discriminatingHashField >= 0) {
            int field = discriminatingHashField;
            int channel = inputChannels[field];
            return inputFieldNull(field, nulls, position)
                    ? 31
                    : 31 + fieldHash(field, channel, values[channel], position);
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

    /**
     * Hashes a dense physical batch when each key field either exposes dictionary-id/exact-entry-hash arrays or a
     * resolved integer accessor prepared by {@link #beginBatch}. The latter preserves the oversized-dictionary cost
     * guard: only referenced rows are read, rather than eagerly hashing a large join-output base. The generated loop
     * specializes only physical hash/null shapes; query identities are absent, and the hashes are byte-for-byte
     * identical to {@link #hash} so fallback batches remain in the same authoritative table domain.
     */
    boolean prepareGeneratedDictionaryBatchHashes(int count, long[] output)
    {
        DictionaryHashBatchKernel kernel = generatedDictionaryHashKernel(count);
        if (kernel == null) {
            return false;
        }
        kernel.hash(
                count,
                dictionaryHashedIds,
                dictionaryEntryHashes,
                fieldLong,
                fieldBinaryHashes,
                fieldNullAccess,
                output);
        return true;
    }

    long assignGeneratedDictionaryBatch(
            int count,
            FlatGroupingTable table,
            Vector[] values,
            Vector[] nulls,
            long nextGroupId,
            long[] output)
    {
        DictionaryHashBatchKernel kernel = generatedDictionaryHashKernel(count);
        if (kernel == null) {
            return -1;
        }
        return kernel.assign(
                count,
                dictionaryHashedIds,
                dictionaryEntryHashes,
                fieldLong,
                fieldBinaryHashes,
                fieldNullAccess,
                table,
                values,
                nulls,
                nextGroupId,
                output);
    }

    private DictionaryHashBatchKernel generatedDictionaryHashKernel(int count)
    {
        if (!policy.generatedDictionaryHashBatch() ||
                count < policy.generatedDictionaryHashBatchMinRows() ||
                handlers.length < 2 ||
                handlers.length > 15 ||
                dictionaryHashedIds == null) {
            return null;
        }
        long shape = handlers.length;
        int accessorHashedFields = 0;
        boolean allFieldsNullFree = true;
        for (int field = 0; field < handlers.length; field++) {
            int nullShape = batchNullShape(field);
            shape |= (long) nullShape << (4 + field * 2);
            allFieldsNullFree &= nullShape == DictionaryHashBatchKernelGenerator.NULL_FREE;
            if (nullShape == DictionaryHashBatchKernelGenerator.ALL_NULL) {
                continue;
            }
            if (dictionaryHashedIds[field] != null && dictionaryEntryHashes[field] != null) {
                continue;
            }
            if (handlers.length >= policy.generatedHybridHashBatchMinFields() &&
                    fieldKinds[field] == FlatTypeHandler.Kind.LONG &&
                    fieldLong[field] != null) {
                shape |= (long) DictionaryHashBatchKernelGenerator.LONG_ACCESSOR_HASH <<
                        (DictionaryHashBatchKernelGenerator.HASH_MODE_SHIFT + field * 2);
                accessorHashedFields++;
                continue;
            }
            if (handlers.length >= policy.generatedHybridHashBatchMinFields() &&
                    fieldKinds[field] == FlatTypeHandler.Kind.BINARY &&
                    fieldBinaryHashes[field] != null) {
                shape |= (long) DictionaryHashBatchKernelGenerator.BINARY_ACCESSOR_HASH <<
                        (DictionaryHashBatchKernelGenerator.HASH_MODE_SHIFT + field * 2);
                accessorHashedFields++;
                continue;
            }
            if (policy.debugGeneratedDictionaryHashBatch() && !debugGeneratedDictionaryHashBatchRejectedPrinted) {
                debugGeneratedDictionaryHashBatchRejectedPrinted = true;
                System.err.printf("[generated-dictionary-hash-batch-rejected] fields=%d field=%d ids=%s hashes=%s rows=%d%n",
                        handlers.length,
                        field,
                        dictionaryHashedIds[field] != null,
                        dictionaryEntryHashes[field] != null,
                        count);
            }
            return null;
        }
        if (accessorHashedFields > 0 &&
                accessorHashedFields < policy.generatedHybridHashBatchMinAccessorFields() &&
                handlers.length - accessorHashedFields <
                        policy.generatedHybridHashBatchMinFields() + policy.generatedHybridHashBatchMinAccessorFields()) {
            return null;
        }
        if (handlers.length == 2 &&
                accessorHashedFields == 0 &&
                allFieldsNullFree &&
                count < policy.generatedDictionaryHashBatchNullFreePairMinRows()) {
            return null;
        }
        DictionaryHashBatchKernel kernel = codeGeneration.dictionaryHash().create(
                shape,
                policy.generatedDictionaryHashProbeTileRows());
        if (policy.debugGeneratedDictionaryHashBatch() && !debugGeneratedDictionaryHashBatchPrinted) {
            debugGeneratedDictionaryHashBatchPrinted = true;
            System.err.printf("[generated-dictionary-hash-batch] fields=%d shape=%d rows=%d compact=%s kinds=%s ids=%s offsets=%s order=%s%n",
                    handlers.length,
                    shape,
                    count,
                    compactEmbeddedBinaryRecords,
                    Arrays.toString(fieldKinds),
                    Arrays.toString(fieldIdComparable),
                    Arrays.toString(fixedOffsets),
                    Arrays.toString(comparisonOrder));
        }
        return kernel;
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
        // Dictionary equality already resolves through a query-stable value id. Reuse the value's
        // operator-compatible hash by that id as well: the first referenced dictionary entry pays
        // the byte scan while every subsequent fact row is an id lookup. The cached hash is exactly
        // OperatorVectorSupport.binaryHash, so flat inputs and values that overflow the interner
        // remain hash-compatible with records created through this path.
        if (policy.stableDictionaryValueHash() &&
                fieldIdComparable != null &&
                fieldIdComparable[fieldIndex] &&
                batchDictionaryIds[fieldIndex] != null) {
            int globalId = globalIdAtPosition(fieldIndex, position);
            if (globalId >= 0) {
                return fieldInterners[fieldIndex].groupingHash(globalId);
            }
        }
        BinaryVector base = fieldBinaryBase[fieldIndex];
        if (base == null) {
            return FlatTypeHandlers.BINARY.hashInput(value, position);
        }
        if (isSingleRunBinaryField(fieldIndex)) {
            return fieldBinaryConstantHash[fieldIndex];
        }
        int entry = binaryEntry(fieldIndex, position);
        return OperatorVectorSupport.binaryHash(base.data(), base.startOffset(entry), base.length(entry));
    }

    private int binaryEntry(int fieldIndex, int position)
    {
        if (isSingleRunBinaryField(fieldIndex)) {
            return 0;
        }
        int[] ids = fieldBinaryIds[fieldIndex];
        return ids == null ? position : ids[position];
    }

    private void writeFieldFlat(int fieldIndex, Vector value, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, int recordIndex)
    {
        if (!batchAccessorsReady) {
            handlers[fieldIndex].writeFlat(value, position, fixedChunk, fixedOffset, arena);
            return;
        }
        switch (fieldKinds[fieldIndex]) {
            case LONG -> handlers[fieldIndex].writeLong(fixedChunk, fixedOffset, fieldLong[fieldIndex].value(position));
            case BINARY -> writeBinaryField(fieldIndex, value, position, fixedChunk, fixedOffset, arena, recordIndex);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.writeFlat(value, position, fixedChunk, fixedOffset, arena);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.writeFlat(value, position, fixedChunk, fixedOffset, arena);
        }
    }

    private void writeBinaryField(int fieldIndex, Vector value, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, int recordIndex)
    {
        if (policy.idOnlyBinaryRecords() && isSingleRunBinaryField(fieldIndex)) {
            int globalId = fieldBinaryConstantGlobalId[fieldIndex];
            if (globalId >= 0) {
                GROUP_INT_HANDLE.set(fixedChunk, fixedOffset, embedIdOnlyBinaryIds ? globalId : 0);
                if (!compactBinaryRecord(fieldIndex)) {
                    GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES, 0);
                    GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES * 2, -1);
                }
                fieldUsesIdOnlyRecords[fieldIndex] = true;
                return;
            }
        }
        if (policy.idOnlyBinaryRecords() &&
                fieldIdComparable != null &&
                fieldIdComparable[fieldIndex] &&
                batchDictionaryIds[fieldIndex] != null) {
            int globalId = globalIdAtPosition(fieldIndex, position);
            if (globalId >= 0) {
                GROUP_INT_HANDLE.set(fixedChunk, fixedOffset, embedIdOnlyBinaryIds ? globalId : 0);
                if (!compactBinaryRecord(fieldIndex)) {
                    GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES, 0);
                    GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES * 2, -1);
                }
                fieldUsesIdOnlyRecords[fieldIndex] = true;
                return;
            }
        }
        BinaryVector base = fieldBinaryBase == null ? null : fieldBinaryBase[fieldIndex];
        if (compactBinaryRecord(fieldIndex)) {
            if (base == null) {
                writeCompactBinaryFallback(fieldIndex, value, position, fixedChunk, fixedOffset, arena);
                return;
            }
            int entry = binaryEntry(fieldIndex, position);
            writeCompactBinaryFallback(fieldIndex, base.data(), base.startOffset(entry), base.length(entry), fixedChunk, fixedOffset, arena);
            return;
        }
        if (base == null) {
            FlatTypeHandlers.BINARY.writeFlat(value, position, fixedChunk, fixedOffset, arena);
            return;
        }
        int entry = binaryEntry(fieldIndex, position);
        long pointer = arena.append(base.data(), base.startOffset(entry), base.length(entry));
        GROUP_INT_HANDLE.set(fixedChunk, fixedOffset, FlatGroupingTable.FlatVariableWidthArena.chunkIndex(pointer));
        GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES, FlatGroupingTable.FlatVariableWidthArena.chunkOffset(pointer));
        GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES * 2, base.length(entry));
    }

    private boolean identicalField(int fieldIndex, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, Vector value, int position, int recordIndex)
    {
        if (!batchAccessorsReady) {
            return handlers[fieldIndex].identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
        }
        return switch (fieldKinds[fieldIndex]) {
            case LONG -> handlers[fieldIndex].readLong(fixedChunk, fixedOffset) == fieldLong[fieldIndex].value(position);
            case BINARY -> identicalBinaryField(fieldIndex, fixedChunk, fixedOffset, arena, value, position, recordIndex);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
        };
    }

    private boolean identicalBinaryField(int fieldIndex, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, Vector value, int position, int recordIndex)
    {
        if (compactBinaryRecord(fieldIndex)) {
            int token = (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset);
            if (token >= 0) {
                return idOnlyBinaryEquals(fieldIndex, token, value, position);
            }
            long fallback = compactBinaryFallback(fieldIndex, ~token);
            int length = compactBinaryLength(fallback);
            BinaryVector base = fieldBinaryBase[fieldIndex];
            if (base != null) {
                int entry = binaryEntry(fieldIndex, position);
                if (base.length(entry) != length) {
                    return false;
                }
                return OperatorVectorSupport.binaryEquals(
                        base.data(),
                        base.startOffset(entry),
                        arena.chunk(compactBinaryChunkIndex(fallback)),
                        compactBinaryChunkOffset(fallback),
                        length);
            }
            return OperatorVectorSupport.binaryEquals(
                    value,
                    position,
                    arena.chunk(compactBinaryChunkIndex(fallback)),
                    compactBinaryChunkOffset(fallback),
                    length);
        }
        int length = (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES * 2);
        if (length < 0) {
            return idOnlyBinaryEquals(
                    fieldIndex,
                    recordDictionaryId(fieldIndex, fixedChunk, fixedOffset, recordIndex),
                    value,
                    position);
        }
        BinaryVector base = fieldBinaryBase[fieldIndex];
        if (base == null) {
            return FlatTypeHandlers.BINARY.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
        }
        int entry = binaryEntry(fieldIndex, position);
        if (base.length(entry) != length) {
            return false;
        }
        byte[] chunk = arena.chunk((int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset));
        int offset = (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES);
        return OperatorVectorSupport.binaryEquals(base.data(), base.startOffset(entry), chunk, offset, length);
    }

    private boolean idOnlyBinaryEquals(int fieldIndex, int recordId, Vector value, int position)
    {
        if (isSingleRunBinaryField(fieldIndex) && fieldBinaryConstantGlobalId[fieldIndex] >= 0) {
            return recordId == fieldBinaryConstantGlobalId[fieldIndex];
        }
        return switch (value) {
            case BinaryVector binary -> fieldInterners[fieldIndex].valueEquals(
                    recordId,
                    binary.data(),
                    binary.startOffset(position),
                    binary.length(position));
            case DictionaryVector dictionary -> idOnlyBinaryEquals(fieldIndex, recordId, dictionary.values(), dictionary.ids()[position]);
            case org.weakref.nitro.data.RleVector rle -> idOnlyBinaryEquals(fieldIndex, recordId, rle.values(), OperatorVectorSupport.runIndex(rle, position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + value.getClass().getSimpleName());
        };
    }

    private boolean isSingleRunBinaryField(int fieldIndex)
    {
        return fieldIndex < Long.SIZE && (batchSingleRunBinaryFields & (1L << fieldIndex)) != 0;
    }

    private boolean compactBinaryRecord(int fieldIndex)
    {
        return compactEmbeddedBinaryRecords && fieldKinds[fieldIndex] == FlatTypeHandler.Kind.BINARY;
    }

    private void writeCompactBinaryFallback(int fieldIndex, Vector value, int position, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena)
    {
        switch (value) {
            case BinaryVector binary -> writeCompactBinaryFallback(fieldIndex, binary.data(), binary.startOffset(position), binary.length(position), fixedChunk, fixedOffset, arena);
            case DictionaryVector dictionary -> writeCompactBinaryFallback(fieldIndex, dictionary.values(), dictionary.ids()[position], fixedChunk, fixedOffset, arena);
            case org.weakref.nitro.data.RleVector rle -> writeCompactBinaryFallback(fieldIndex, rle.values(), OperatorVectorSupport.runIndex(rle, position), fixedChunk, fixedOffset, arena);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + value.getClass().getSimpleName());
        }
    }

    private void writeCompactBinaryFallback(int fieldIndex, byte[] source, int sourceOffset, int length, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena)
    {
        long pointer = arena.append(source, sourceOffset, length);
        int ordinal = appendCompactBinaryFallback(fieldIndex, pointer, length);
        GROUP_INT_HANDLE.set(fixedChunk, fixedOffset, ~ordinal);
    }

    private int appendCompactBinaryFallback(int fieldIndex, long pointer, int length)
    {
        if (compactBinaryFallbacks == null) {
            compactBinaryFallbacks = new long[handlers.length][];
            compactBinaryFallbackCounts = new int[handlers.length];
        }
        int count = compactBinaryFallbackCounts[fieldIndex];
        long[] fallbacks = compactBinaryFallbacks[fieldIndex];
        if (fallbacks == null) {
            fallbacks = borrowLongs(16);
            compactBinaryFallbacks[fieldIndex] = fallbacks;
        }
        else if (count == fallbacks.length) {
            long[] previous = fallbacks;
            fallbacks = borrowLongs(Math.multiplyExact(previous.length, 2));
            System.arraycopy(previous, 0, fallbacks, 0, previous.length);
            compactBinaryFallbacks[fieldIndex] = fallbacks;
            release(previous);
        }
        fallbacks[count] = packCompactBinaryFallback(pointer, length);
        compactBinaryFallbackCounts[fieldIndex] = count + 1;
        return count;
    }

    private long compactBinaryFallback(int fieldIndex, int ordinal)
    {
        if (compactBinaryFallbacks == null || ordinal < 0 || ordinal >= compactBinaryFallbackCounts[fieldIndex]) {
            throw new IllegalStateException("Invalid compact binary fallback ordinal " + ordinal + " for field " + fieldIndex);
        }
        return compactBinaryFallbacks[fieldIndex][ordinal];
    }

    private static long packCompactBinaryFallback(long pointer, int length)
    {
        int chunkIndex = FlatGroupingTable.FlatVariableWidthArena.chunkIndex(pointer);
        int chunkOffset = FlatGroupingTable.FlatVariableWidthArena.chunkOffset(pointer);
        if (chunkIndex < 0 || chunkIndex >= (1 << 22) || chunkOffset < 0 || chunkOffset > (1 << 20) || length < 0 || length > (1 << 20)) {
            throw new IllegalStateException("Compact binary fallback exceeds arena encoding: chunk=" + chunkIndex + ", offset=" + chunkOffset + ", length=" + length);
        }
        return ((long) chunkIndex << 42) | ((long) chunkOffset << 21) | length;
    }

    private static int compactBinaryChunkIndex(long fallback)
    {
        return (int) (fallback >>> 42);
    }

    private static int compactBinaryChunkOffset(long fallback)
    {
        return (int) ((fallback >>> 21) & ((1 << 21) - 1));
    }

    private static int compactBinaryLength(long fallback)
    {
        return (int) (fallback & ((1 << 21) - 1));
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

    public void writeRecord(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
    {
        if (batchNullFreeSingleBinary) {
            if (nullByteCount > 0) {
                fixedChunk[fixedOffset] = 0;
            }
            writeBinaryField(0, values[singleInputChannel], position, fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena, recordIndex);
            // Dictionary-backed single fields keep their stable value id in the ordinary sidecar because the
            // single-field record layout deliberately does not embed it. Flat input has no comparable id, so this
            // is a no-op there. Keeping the ownership in the layout makes the same monomorphic write path exact for
            // both physical encodings without exposing a type branch to the operator.
            storeRecordDictionaryIds(recordIndex, position);
            return;
        }
        if (batchNullFreeLongBinary) {
            if (nullByteCount > 0) {
                fixedChunk[fixedOffset] = 0;
            }
            handlers[0].writeLong(fixedChunk, fixedOffset + fixedOffsets[0], fieldLong[0].value(position));
            writeBinaryField(1, values[inputChannels[1]], position, fixedChunk, fixedOffset + fixedOffsets[1], variableWidthArena, recordIndex);
            storeRecordDictionaryIds(recordIndex, position);
            return;
        }
        if (nullByteCount > 0) {
            Arrays.fill(fixedChunk, fixedOffset, fixedOffset + nullByteCount, (byte) 0);
        }
        if (singleField) {
            if (inputFieldNull(0, nulls, position)) {
                fixedChunk[fixedOffset] = 1;
            }
            else {
                writeFieldFlat(
                        0,
                        values[singleInputChannel],
                        position,
                        fixedChunk,
                        fixedOffset + singleFixedOffset,
                        variableWidthArena,
                        recordIndex);
            }
            storeRecordDictionaryIds(recordIndex, position);
            return;
        }
        for (int index = 0; index < handlers.length; index++) {
            if (inputFieldNull(index, nulls, position)) {
                setNullBit(fixedChunk, fixedOffset, index);
            }
            else {
                writeFieldFlat(index, values[inputChannels[index]], position, fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena, recordIndex);
            }
        }
        storeRecordDictionaryIds(recordIndex, position);
    }

    public boolean identicalRecordToInput(byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena variableWidthArena, Vector[] values, Vector[] nulls, int position, int recordIndex)
    {
        int generatedResult = generatedDictionaryRecordEquality(
                fixedChunk,
                fixedOffset,
                variableWidthArena,
                values,
                position,
                recordIndex);
        if (generatedResult != DictionaryRecordEqualityKernel.FALLBACK) {
            return generatedResult == DictionaryRecordEqualityKernel.IDENTICAL;
        }
        if (batchNullFreeSingleBinary) {
            return identicalBinaryField(0, fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena, values[singleInputChannel], position, recordIndex);
        }
        if (batchNullFreeLongBinary) {
            if (handlers[0].readLong(fixedChunk, fixedOffset + fixedOffsets[0]) != fieldLong[0].value(position)) {
                return false;
            }
            int fieldOffset = fixedOffset + fixedOffsets[1];
            if (idComparable(1, fixedChunk, fieldOffset, recordIndex)) {
                int probeId = globalIdAtPosition(1, position);
                if (probeId >= 0) {
                    return recordDictionaryId(1, fixedChunk, fieldOffset, recordIndex) == probeId;
                }
            }
            return identicalBinaryField(1, fixedChunk, fieldOffset, variableWidthArena, values[inputChannels[1]], position, recordIndex);
        }
        if (singleField) {
            if (inputFieldNull(0, nulls, position)) {
                return isNull(fixedChunk, fixedOffset, 0);
            }
            if (isNull(fixedChunk, fixedOffset, 0)) {
                return false;
            }
            if (idComparable(0, fixedChunk, fixedOffset + singleFixedOffset, recordIndex)) {
                int probeId = globalIdAtPosition(0, position);
                if (probeId >= 0) {
                    return recordDictionaryId(0, fixedChunk, fixedOffset + singleFixedOffset, recordIndex) == probeId;
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
            int fieldOffset = fixedOffset + fixedOffsets[index];
            if (idComparable(index, fixedChunk, fieldOffset, recordIndex)) {
                int probeId = globalIdAtPosition(index, position);
                if (probeId >= 0) {
                    if (recordDictionaryId(index, fixedChunk, fieldOffset, recordIndex) != probeId) {
                        return false;
                    }
                    continue;
                }
                // probe value overflowed the interner: fall through to the value comparison.
            }
            if (!identicalField(index, fixedChunk, fixedOffset + fixedOffsets[index], variableWidthArena, values[inputChannels[index]], position, recordIndex)) {
                return false;
            }
        }
        return true;
    }

    int generatedDictionaryRecordEquality(
            byte[] fixedChunk,
            int fixedOffset,
            FlatGroupingTable.FlatVariableWidthArena variableWidthArena,
            Vector[] values,
            int position,
            int recordIndex)
    {
        return generatedRecordEqualityKernel == null
                ? DictionaryRecordEqualityKernel.FALLBACK
                : generatedRecordEqualityKernel.identical(
                        this,
                        fixedChunk,
                        fixedOffset,
                        variableWidthArena,
                        values,
                        position,
                        recordIndex);
    }

    boolean generatedEqualityExactBinary(
            int field,
            byte[] fixedChunk,
            int fixedOffset,
            FlatGroupingTable.FlatVariableWidthArena variableWidthArena,
            Vector[] values,
            int position,
            int recordIndex)
    {
        return identicalBinaryField(
                field,
                fixedChunk,
                fixedOffset,
                variableWidthArena,
                values[inputChannels[field]],
                position,
                recordIndex);
    }

    boolean generatedEqualityInputNull(int field, int position)
    {
        VectorAccess.BooleanValues nulls = fieldNullAccess[field];
        return nulls != null && nulls.value(position);
    }

    long generatedEqualityInputLong(int field, int position)
    {
        return fieldLong[field].value(position);
    }

    int generatedEqualityInputGlobalId(int field, int position)
    {
        return globalIdAtPosition(field, position);
    }

    static int generatedEqualityRecordInt(byte[] fixedChunk, int fixedOffset)
    {
        return (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset);
    }

    static long generatedEqualityRecordLong(byte[] fixedChunk, int fixedOffset)
    {
        return (long) GROUP_LONG_HANDLE.get(fixedChunk, fixedOffset);
    }

    long generatedEqualityRecordLong(int field, byte[] fixedChunk, int fixedOffset)
    {
        return handlers[field].readLong(fixedChunk, fixedOffset);
    }

    private boolean idComparable(int fieldIndex, byte[] fixedChunk, int fixedOffset, int recordIndex)
    {
        if (compactBinaryRecord(fieldIndex)) {
            return anyFieldIdComparable
                    && fieldIdComparable[fieldIndex]
                    && (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset) >= 0;
        }
        if (embedIdOnlyBinaryIds && fieldKinds[fieldIndex] == FlatTypeHandler.Kind.BINARY) {
            return anyFieldIdComparable
                    && fieldIdComparable[fieldIndex]
                    && (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES * 2) < 0;
        }
        if (policy.packedRecordDictionaryIds()) {
            int packedIndex = recordIndex * packedDictionaryFieldCount + packedDictionaryFieldIndex[fieldIndex];
            return anyFieldIdComparable
                    && fieldIdComparable[fieldIndex]
                    && packedRecordDictionaryIds != null
                    && packedIndex < packedRecordDictionaryIds.length
                    && packedRecordDictionaryIds[packedIndex] >= 0;
        }
        return anyFieldIdComparable
                && fieldIdComparable[fieldIndex]
                && recordDictionaryIds != null
                && recordDictionaryIds[fieldIndex] != null
                && recordIndex < recordDictionaryIds[fieldIndex].length
                && recordDictionaryIds[fieldIndex][recordIndex] >= 0;
    }

    private int recordDictionaryId(int fieldIndex, byte[] fixedChunk, int fixedOffset, int recordIndex)
    {
        if (compactBinaryRecord(fieldIndex)) {
            return (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset);
        }
        if (embedIdOnlyBinaryIds && fieldKinds[fieldIndex] == FlatTypeHandler.Kind.BINARY) {
            return (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset);
        }
        return policy.packedRecordDictionaryIds()
                ? packedRecordDictionaryIds[recordIndex * packedDictionaryFieldCount + packedDictionaryFieldIndex[fieldIndex]]
                : recordDictionaryIds[fieldIndex][recordIndex];
    }

    private void storeRecordDictionaryIds(int recordIndex, int position)
    {
        if (!anyFieldIdComparable || embedIdOnlyBinaryIds) {
            return;
        }
        ensureRecordDictionaryIdCapacity(recordIndex + 1);
        if (policy.packedRecordDictionaryIds()) {
            int recordOffset = recordIndex * packedDictionaryFieldCount;
            for (int index = 0; index < handlers.length; index++) {
                int packedField = packedDictionaryFieldIndex[index];
                if (packedField >= 0) {
                    packedRecordDictionaryIds[recordOffset + packedField] = fieldIdComparable[index]
                            ? globalIdAtPosition(index, position)
                            : -1;
                }
            }
            return;
        }
        for (int index = 0; index < handlers.length; index++) {
            if (recordDictionaryIds[index] == null) {
                continue;
            }
            recordDictionaryIds[index][recordIndex] = fieldIdComparable[index] ? globalIdAtPosition(index, position) : -1;
        }
    }

    private void ensureRecordDictionaryIdCapacity(int required)
    {
        if (policy.packedRecordDictionaryIds()) {
            int requiredEntries = Math.multiplyExact(required, packedDictionaryFieldCount);
            if (packedRecordDictionaryIds == null) {
                int initialRecords = Math.max(16, required);
                packedRecordDictionaryIds = borrowInts(Math.multiplyExact(initialRecords, packedDictionaryFieldCount));
                Arrays.fill(packedRecordDictionaryIds, -1);
            }
            else if (packedRecordDictionaryIds.length < requiredEntries) {
                int newLength = packedRecordDictionaryIds.length;
                while (newLength < requiredEntries) {
                    newLength = Math.multiplyExact(newLength, 2);
                }
                int[] previous = packedRecordDictionaryIds;
                packedRecordDictionaryIds = borrowInts(newLength);
                System.arraycopy(previous, 0, packedRecordDictionaryIds, 0, previous.length);
                Arrays.fill(packedRecordDictionaryIds, previous.length, newLength, -1);
                release(previous);
            }
            return;
        }
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
                int[] ids = borrowInts(initial);
                Arrays.fill(ids, -1);
                recordDictionaryIds[index] = ids;
            }
            else if (recordDictionaryIds[index].length < required) {
                int previousLength = recordDictionaryIds[index].length;
                int newLength = previousLength;
                while (newLength < required) {
                    newLength *= 2;
                }
                int[] previous = recordDictionaryIds[index];
                int[] ids = borrowInts(newLength);
                System.arraycopy(previous, 0, ids, 0, previousLength);
                Arrays.fill(ids, previousLength, newLength, -1);
                recordDictionaryIds[index] = ids;
                release(previous);
            }
        }
    }

    long retainedBytes()
    {
        long bytes = referenceArrayBytes(fields);
        bytes += intArrayBytes(inputChannels);
        bytes += referenceArrayBytes(handlers);
        bytes += intArrayBytes(fixedOffsets);
        bytes += intArrayBytes(comparisonOrder);
        bytes += nestedIntArrayBytes(dictionaryHashedIds);
        bytes += nestedLongArrayBytes(dictionaryEntryHashes);
        for (FlatTypeHandler handler : handlers) {
            bytes += handler.retainedBytes();
        }
        bytes += referenceArrayBytes(dictionaryHashedValues);
        bytes += longArrayBytes(dictionaryHashedGenerations);
        bytes += referenceArrayBytes(boundDictionary);
        bytes += nestedIntArrayBytes(batchDictionaryIds);
        bytes += booleanArrayBytes(fieldIdComparable);
        bytes += nestedIntArrayBytes(recordDictionaryIds);
        bytes += intArrayBytes(packedRecordDictionaryIds);
        bytes += intArrayBytes(packedDictionaryFieldIndex);
        bytes += booleanArrayBytes(batchFieldNullFree);
        bytes += booleanArrayBytes(batchFieldAllNull);
        bytes += referenceArrayBytes(fieldInterners);
        if (fieldInterners != null) {
            for (ValueIdInterner interner : fieldInterners) {
                bytes += interner == null ? 0 : interner.retainedBytes();
            }
        }
        bytes += nestedIntArrayBytes(batchEntryGlobalId);
        bytes += referenceArrayBytes(batchEntryGlobalIdDict);
        bytes += longArrayBytes(batchEntryGlobalIdGeneration);
        bytes += nestedIntArrayBytes(batchPositionGlobalId);
        bytes += referenceArrayBytes(fieldDictionaryMapping);
        bytes += referenceArrayBytes(batchPositionDictionaryMapping);
        bytes += booleanArrayBytes(fieldLazyIntern);
        bytes += booleanArrayBytes(fieldUsesIdOnlyRecords);
        bytes += nestedLongArrayBytes(compactBinaryFallbacks);
        bytes += intArrayBytes(compactBinaryFallbackCounts);
        bytes += intArrayBytes(compositeOrder);
        bytes += intArrayBytes(compactCompositeRadix);
        bytes += longArrayBytes(compactLongBase);
        bytes += booleanArrayBytes(compactLongBaseSet);
        bytes += booleanArrayBytes(compactLongDomainRejected);
        bytes += referenceArrayBytes(fieldKinds);
        bytes += referenceArrayBytes(fieldLong);
        bytes += referenceArrayBytes(fieldBinaryHashes);
        bytes += referenceArrayBytes(fieldBinaryBase);
        bytes += nestedIntArrayBytes(fieldBinaryIds);
        bytes += longArrayBytes(fieldBinaryConstantHash);
        bytes += intArrayBytes(fieldBinaryConstantGlobalId);
        bytes += nestedIntArrayBytes(composedDictionaryIds);
        bytes += nestedIntArrayBytes(composedNullDictionaryIds);
        bytes += referenceArrayBytes(fieldNullAccess);
        return bytes;
    }

    void releaseBuffers()
    {
        if (dictionaryEntryHashes != null) {
            for (long[] hashes : dictionaryEntryHashes) {
                release(hashes);
            }
            Arrays.fill(dictionaryEntryHashes, null);
        }
        if (batchEntryGlobalId != null) {
            for (int[] ids : batchEntryGlobalId) {
                release(ids);
            }
            Arrays.fill(batchEntryGlobalId, null);
        }
        if (batchPositionGlobalId != null) {
            for (int[] ids : batchPositionGlobalId) {
                release(ids);
            }
            Arrays.fill(batchPositionGlobalId, null);
        }
        if (recordDictionaryIds != null) {
            for (int[] ids : recordDictionaryIds) {
                release(ids);
            }
            Arrays.fill(recordDictionaryIds, null);
        }
        if (compactBinaryFallbacks != null) {
            for (long[] fallbacks : compactBinaryFallbacks) {
                release(fallbacks);
            }
            Arrays.fill(compactBinaryFallbacks, null);
            Arrays.fill(compactBinaryFallbackCounts, 0);
        }
        if (composedDictionaryIds != null) {
            for (int[] ids : composedDictionaryIds) {
                release(ids);
            }
            Arrays.fill(composedDictionaryIds, null);
        }
        if (composedNullDictionaryIds != null) {
            for (int[] ids : composedNullDictionaryIds) {
                release(ids);
            }
            Arrays.fill(composedNullDictionaryIds, null);
        }
        release(packedRecordDictionaryIds);
        packedRecordDictionaryIds = null;
        if (dictionaryHashedIds != null) {
            Arrays.fill(dictionaryHashedIds, null);
            Arrays.fill(dictionaryHashedValues, null);
            Arrays.fill(dictionaryHashedGenerations, -1);
            Arrays.fill(boundDictionary, null);
            Arrays.fill(batchDictionaryIds, null);
            Arrays.fill(batchEntryGlobalIdDict, null);
            Arrays.fill(fieldDictionaryMapping, null);
            Arrays.fill(batchPositionDictionaryMapping, null);
            Arrays.fill(fieldBinaryBase, null);
            Arrays.fill(fieldBinaryIds, null);
            Arrays.fill(fieldLong, null);
            Arrays.fill(fieldBinaryHashes, null);
            Arrays.fill(fieldNullAccess, null);
        }
        batchAccessorsReady = false;
        batchNormalizedIntKeyEligible = false;
        dictionarySingleBinaryFastPathDecided = false;
        dictionarySingleBinaryFastPathAdmitted = false;
    }

    /**
     * Resolves a dictionary-backed null stream once at the batch boundary. Join output commonly wraps the value
     * and NULLS streams in the same dictionary chain; walking that chain for every hash and equality comparison
     * duplicates work that is invariant for the whole batch. A single dictionary layer reads its mapping directly.
     * Deeper chains compose into pooled, high-water scratch and then perform one mapping lookup per row.
     */
    private VectorAccess.BooleanValues resolveNullAccessor(int fieldIndex, Vector nulls)
    {
        if (!policy.resolveDictionaryNulls() ||
                handlers.length < policy.dictionaryNullResolutionMinFields() ||
                !(nulls instanceof DictionaryVector dictionary)) {
            return VectorAccess.booleanValues(nulls);
        }

        int[] positions = dictionary.ids();
        Vector leaf = dictionary.values();
        if (leaf instanceof DictionaryVector) {
            if (!policy.composeNestedDictionaryNulls()) {
                return VectorAccess.booleanValues(nulls);
            }
            int length = dictionary.length();
            int[] composed = composedNullDictionaryIds[fieldIndex];
            if (composed == null || composed.length < length) {
                int[] previous = composed;
                composed = borrowInts(length);
                composedNullDictionaryIds[fieldIndex] = composed;
                release(previous);
            }
            System.arraycopy(positions, 0, composed, 0, length);
            do {
                DictionaryVector nested = (DictionaryVector) leaf;
                int[] nestedIds = nested.ids();
                for (int position = 0; position < length; position++) {
                    composed[position] = nestedIds[composed[position]];
                }
                leaf = nested.values();
            }
            while (leaf instanceof DictionaryVector);
            positions = composed;
        }

        int[] resolvedPositions = positions;
        if (leaf instanceof BooleanVector values) {
            boolean[] resolvedValues = values.values();
            return position -> resolvedValues[resolvedPositions[position]];
        }
        VectorAccess.BooleanValues resolvedValues = VectorAccess.booleanValues(leaf);
        return position -> resolvedValues.value(resolvedPositions[position]);
    }

    private int[] borrowInts(int length)
    {
        long bytes = (long) length * Integer.BYTES;
        return policy.poolScratch() && arrayPool.isRetainable(bytes) ? arrayPool.borrowInts(length) : new int[length];
    }

    private long[] borrowLongs(int length)
    {
        long bytes = (long) length * Long.BYTES;
        return policy.poolScratch() && arrayPool.isRetainable(bytes) ? arrayPool.borrowLongs(length) : new long[length];
    }

    private static long booleanArrayBytes(boolean[] values)
    {
        return values == null ? 0 : values.length;
    }

    private static long intArrayBytes(int[] values)
    {
        return values == null ? 0 : (long) values.length * Integer.BYTES;
    }

    private static long longArrayBytes(long[] values)
    {
        return values == null ? 0 : (long) values.length * Long.BYTES;
    }

    private static long referenceArrayBytes(Object[] values)
    {
        return values == null ? 0 : (long) values.length * Long.BYTES;
    }

    private static long nestedIntArrayBytes(int[][] values)
    {
        long bytes = referenceArrayBytes(values);
        if (values != null) {
            for (int[] value : values) {
                bytes += intArrayBytes(value);
            }
        }
        return bytes;
    }

    private static long nestedLongArrayBytes(long[][] values)
    {
        long bytes = referenceArrayBytes(values);
        if (values != null) {
            for (long[] value : values) {
                bytes += longArrayBytes(value);
            }
        }
        return bytes;
    }

    private void release(int[] array)
    {
        if (policy.poolScratch() && array != null && arrayPool.isRetainable((long) array.length * Integer.BYTES)) {
            arrayPool.release(array);
        }
    }

    private void release(long[] array)
    {
        if (policy.poolScratch() && array != null && arrayPool.isRetainable((long) array.length * Long.BYTES)) {
            arrayPool.release(array);
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
        if (batchFieldAllNull != null && batchFieldAllNull[fieldIndex]) {
            return true;
        }
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

    private static int[] compositeOrder(FlatTypeHandler.Kind[] kinds)
    {
        int[] order = new int[kinds.length];
        int next = 0;
        // Put the smaller radix first. This does not change the bijection, but keeps the largest live direct-cache
        // index compact for the common case where small integer values occupy only the first few digits.
        for (int index = 0; index < kinds.length; index++) {
            if (kinds[index] == FlatTypeHandler.Kind.LONG) {
                order[next++] = index;
            }
        }
        for (int index = 0; index < kinds.length; index++) {
            if (kinds[index] != FlatTypeHandler.Kind.LONG) {
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

    private static final class PositionIdFlatKeyLayout
            extends FlatKeyLayout
    {
        private PositionIdFlatKeyLayout(
                PrimitiveArrayPool arrayPool,
                OperatorCodeGenerationResources codeGeneration,
                FlatKeyTablePolicy policy,
                Field[] fields,
                int[] inputChannels,
                FlatTypeHandler[] handlers,
                int[] fixedOffsets,
                int[] comparisonOrder,
                int nullByteCount,
                int fixedRecordSize,
                boolean anyVariableWidth)
        {
            super(
                    arrayPool,
                    codeGeneration,
                    policy,
                    fields,
                    inputChannels,
                    handlers,
                    fixedOffsets,
                    comparisonOrder,
                    nullByteCount,
                    fixedRecordSize,
                    anyVariableWidth,
                    true);
        }

        @Override
        int globalIdAtPosition(int fieldIndex, int position)
        {
            return preparedGlobalIdAtPosition(fieldIndex, position);
        }
    }

    public record Field(int inputChannel, FlatTypeHandler handler, int fixedOffset, Set<BinaryVector.Trait> binaryTraits)
    {
    }
}
