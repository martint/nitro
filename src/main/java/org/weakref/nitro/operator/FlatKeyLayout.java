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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Set;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

class FlatKeyLayout
{
    private static final boolean POOL_SCRATCH = Boolean.parseBoolean(System.getProperty("nitro.flatKeyLayout.poolScratch", "true"));
    private static final boolean STABLE_DICTIONARY_VALUE_HASH =
            Boolean.parseBoolean(System.getProperty("nitro.group.stableDictionaryValueHash", "true"));
    private static final boolean MIXED_COMPOSITE_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.group.mixedCompositeIds", "true"));
    private static final boolean FAST_MIXED_COMPOSITE_3 =
            Boolean.parseBoolean(System.getProperty("nitro.group.fastMixedComposite3", "true"));
    private static final boolean EARLY_REJECT_MIXED_COMPOSITE =
            Boolean.parseBoolean(System.getProperty("nitro.group.earlyRejectMixedComposite", "true"));
    private static final boolean FAST_NULL_FREE_LONG_BINARY =
            Boolean.parseBoolean(System.getProperty("nitro.group.fastNullFreeLongBinary", "true"));
    private static final boolean FAST_NULL_FREE_SINGLE_BINARY =
            Boolean.parseBoolean(System.getProperty("nitro.group.fastNullFreeSingleBinary", "true"));
    private static final boolean DEBUG_NULL_FREE_SINGLE_BINARY =
            Boolean.getBoolean("nitro.debug.nullFreeSingleBinary");
    private static final boolean TRACK_DICTIONARY_EMPTY_SENTINEL =
            Boolean.parseBoolean(System.getProperty("nitro.group.trackDictionaryEmptySentinel", "true"));
    private static final int DICTIONARY_SENTINEL_SAMPLE_SIZE =
            Integer.getInteger("nitro.group.dictionarySentinelSampleSize", 128);
    private static final int DICTIONARY_SENTINEL_MIN_PERCENT =
            Integer.getInteger("nitro.group.dictionarySentinelMinPercent", 25);
    private static final boolean FAST_NULL_FREE_LONG_BINARY_COMPOSITE =
            Boolean.parseBoolean(System.getProperty("nitro.group.fastNullFreeLongBinaryComposite", "true"));
    private static final boolean PACKED_RECORD_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.group.packedRecordDictionaryIds", "true"));
    private static final boolean ID_ONLY_BINARY_RECORDS =
            Boolean.parseBoolean(System.getProperty("nitro.group.idOnlyBinaryRecords", "true"));
    private static final boolean EMBED_ID_ONLY_BINARY_IDS_CONFIG =
            Boolean.parseBoolean(System.getProperty("nitro.group.embedIdOnlyBinaryIds", "true"));
    private static final boolean OWN_GROUPED_DICTIONARY_IDS =
            Boolean.parseBoolean(System.getProperty("nitro.group.ownedGroupedDictionaryIds", "true"));
    private static final boolean COMPOSE_NESTED_DICTIONARIES =
            Boolean.parseBoolean(System.getProperty("nitro.group.composeNestedDictionaries", "true"));
    private static final boolean ADAPTIVE_DISCRIMINATING_FIELD_HASH =
            Boolean.parseBoolean(System.getProperty("nitro.group.adaptiveDiscriminatingFieldHash", "true"));
    private static final boolean NORMALIZED_INT_KEY =
            Boolean.parseBoolean(System.getProperty("nitro.group.normalizedIntKey", "true"));
    private static final int DISCRIMINATING_FIELD_HASH_MIN_FIELDS =
            Integer.getInteger("nitro.group.discriminatingFieldHashMinFields", 4);
    private static final int DISCRIMINATING_FIELD_HASH_SAMPLE_SIZE =
            Integer.getInteger("nitro.group.discriminatingFieldHashSampleSize", 128);
    private static final int DISCRIMINATING_FIELD_HASH_MIN_DISTINCT_PERCENT =
            Integer.getInteger("nitro.group.discriminatingFieldHashMinDistinctPercent", 90);
    private final PrimitiveArrayPool arrayPool = PrimitiveArrayPool.shared();
    private final Field[] fields;
    private final int[] inputChannels;
    private final FlatTypeHandler[] handlers;
    private final int[] fixedOffsets;
    private final int[] comparisonOrder;
    private final int nullByteCount;
    private final boolean singleField;
    private final boolean embedIdOnlyBinaryIds;
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
    private int[] packedRecordDictionaryIds;
    private final int[] packedDictionaryFieldIndex;
    private final int packedDictionaryFieldCount;

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
    private long[] batchEntryGlobalIdGeneration;
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
    private boolean batchNullFreeLongBinary;
    private boolean batchNullFreeSingleBinaryCandidate;
    private boolean batchNullFreeSingleBinary;
    private boolean dictionarySingleBinaryFastPathDecided;
    private boolean dictionarySingleBinaryFastPathAdmitted;
    private boolean debugDictionarySingleBinaryPrinted;
    private VectorAccess.LongValues[] fieldLong;
    private BinaryVector[] fieldBinaryBase;
    private int[][] fieldBinaryIds;
    // Reusable, per-field logical-position -> leaf-position maps for nested dictionaries. Join output commonly
    // wraps an already dictionary-encoded dimension column. Resolving that chain once per batch lets all grouping
    // operations use the concrete leaf and a single id lookup instead of recursively walking the encoding in every
    // hash/equality/write call. These buffers are high-water retained for the query and returned by releaseBuffers.
    private int[][] composedDictionaryIds;
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

    FlatKeyLayout(Field[] fields, int[] inputChannels, FlatTypeHandler[] handlers, int[] fixedOffsets, int[] comparisonOrder, int nullByteCount, int fixedRecordSize, boolean anyVariableWidth)
    {
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
        this.embedIdOnlyBinaryIds = EMBED_ID_ONLY_BINARY_IDS_CONFIG && !singleField;
        this.singleInputChannel = singleField ? inputChannels[0] : -1;
        this.singleHandler = singleField ? handlers[0] : null;
        this.singleFixedOffset = singleField ? fixedOffsets[0] : -1;
        this.fixedRecordSize = fixedRecordSize;
        this.anyVariableWidth = anyVariableWidth;
        boolean normalizedShape = NORMALIZED_INT_KEY && handlers.length >= 3 && handlers.length <= 4;
        for (FlatTypeHandler.Kind kind : fieldKinds) {
            normalizedShape &= kind == FlatTypeHandler.Kind.LONG || kind == FlatTypeHandler.Kind.BINARY;
        }
        this.normalizedIntKeyShape = normalizedShape;
        // Grouped output can be requested by callers that do not drive beginBatch (for example an empty or
        // pre-materialized grouping path). This flag describes record-lifetime state, not batch scratch, so keep it
        // available for the layout's entire lifetime rather than initializing it with the per-batch caches.
        this.fieldUsesIdOnlyRecords = new boolean[handlers.length];
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

    boolean batchSupportsNormalizedIntKey()
    {
        return batchNormalizedIntKeyEligible;
    }

    /**
     * Packs up to four nullable, non-negative int-domain key fields into two exact primitive lanes. Zero encodes
     * null and every non-null value is shifted by one, so the pair is a bijection for the admitted domain. Binary
     * fields use their query-stable interned value id; a flat/overflowing binary or an out-of-range long simply
     * declines the accelerator and retains the complete record-equality path.
     */
    boolean tryPrepareNormalizedIntKey(Vector[] values, Vector[] nulls, int position)
    {
        if (!batchNormalizedIntKeyEligible || !batchAccessorsReady) {
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
                int[] ids = batchDictionaryIds[index];
                int valueId;
                if (fieldIdComparable[index] && ids != null) {
                    valueId = globalIdFor(index, ids[position]);
                }
                else {
                    BinaryVector binary = fieldBinaryBase[index];
                    if (binary == null) {
                        return false;
                    }
                    int physicalPosition = fieldBinaryIds[index] == null ? position : fieldBinaryIds[index][position];
                    ValueIdInterner interner = fieldInterners[index];
                    if (interner == null) {
                        interner = new ValueIdInterner(VALUE_ID_CEILING);
                        fieldInterners[index] = interner;
                    }
                    valueId = interner.intern(binary.data(), binary.startOffset(physicalPosition), binary.length(physicalPosition));
                }
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

    long preparedNormalizedFirst()
    {
        return preparedNormalizedFirst;
    }

    long preparedNormalizedSecond()
    {
        return preparedNormalizedSecond;
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
            batchEntryGlobalIdGeneration = new long[handlers.length];
            fieldLazyIntern = new boolean[handlers.length];
            fieldLong = new VectorAccess.LongValues[handlers.length];
            fieldBinaryBase = new BinaryVector[handlers.length];
            fieldBinaryIds = new int[handlers.length][];
            composedDictionaryIds = new int[handlers.length][];
            fieldNullAccess = new VectorAccess.BooleanValues[handlers.length];
            compactCompositeRadix = new int[handlers.length];
        }
        // A narrow mixed key can use compact radices whether its dictionaries are tiny or large. Tiny bases are
        // already eagerly interned by the normal policy; only a large base needs the extra eager scan so its final
        // radix is known. Keep those two decisions separate: otherwise a tiny county/state key falls back to the
        // 1025x1025 conservative radices and creates a huge sparse direct cache, while a wide rollup pays setup even
        // though its composite can never fit.
        boolean mixedCompositeShape = MIXED_COMPOSITE_IDS && handlers.length <= 3 && hasLongField();
        boolean eagerMixedComposite = mixedCompositeShape && hasLargeBinaryDictionary(values);
        batchMixedComposite = mixedCompositeShape;
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
                release(dictionaryEntryHashes[index]);
                dictionaryEntryHashes[index] = null;
                dictionaryHashedValues[index] = null;
                continue;
            }
            Vector dictionaryValues = dictionary.values();
            int[] dictionaryIds = dictionary.ids();
            if (COMPOSE_NESTED_DICTIONARIES && dictionaryValues instanceof DictionaryVector) {
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
            // Pre-hashing every distinct dictionary entry pays off only when the dictionary is smaller than the batch
            // it describes (low-card key, values repeat across rows): each entry is hashed once and reused by id. A
            // join-output key instead wraps the whole build column in a dictionary -- far more entries than a probe
            // batch references -- so pre-hashing all of them every batch is mostly wasted. When the dictionary has
            // more entries than the batch has positions, skip the pre-hash and let fieldHash() hash the referenced
            // entries per row (the same path a non-dictionary field uses). This also sidesteps the per-batch stale
            // cache problem below, since the per-row path always reads the current batch's bytes.
            if (distinctCount > dictionary.length()) {
                dictionaryHashedIds[index] = null;
                release(dictionaryEntryHashes[index]);
                dictionaryEntryHashes[index] = null;
                dictionaryHashedValues[index] = null;
            }
            else {
                dictionaryHashedIds[index] = dictionaryIds;
                // Compute the per-entry hash for every distinct dictionary value once and index it by dictionary id at
                // hash() time, instead of re-hashing the (variable-width) value per row. This is recomputed every batch:
                // the value vector cannot be cached by identity across batches because the allocator pools vector
                // instances, so the same instance can carry different content in a later batch (e.g. a hash-join probe
                // passthrough wrapped in a dictionary) -- a stale cache would hash equal keys differently and split groups.
                if (dictionaryEntryHashes[index] == null || dictionaryEntryHashes[index].length < distinctCount) {
                    long[] previous = dictionaryEntryHashes[index];
                    dictionaryEntryHashes[index] = borrowLongs(distinctCount);
                    release(previous);
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
                int[] entryGlobalIds = internDictionaryEntries(index, dictionaryValues, eagerMixedComposite);
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
        batchNullFreeLongBinary = FAST_NULL_FREE_LONG_BINARY &&
                handlers.length == 2 &&
                fieldKinds[0] == FlatTypeHandler.Kind.LONG &&
                fieldKinds[1] == FlatTypeHandler.Kind.BINARY &&
                batchFieldNullFree[0] &&
                batchFieldNullFree[1];
        // Version this concrete physical shape once at the batch boundary. Set-like and grouping operators then use
        // the same layout API without carrying flat/dictionary/null/type checks through every hash, insert, and
        // equality probe. This is a layout specialization, not an operator- or query-specific implementation.
        batchNullFreeSingleBinaryCandidate = FAST_NULL_FREE_SINGLE_BINARY && singleField &&
                fieldKinds[0] == FlatTypeHandler.Kind.BINARY &&
                batchFieldNullFree[0] &&
                fieldBinaryBase[0] != null;
        // Flat binary input has always used this concrete layout. Dictionary input is admitted later by the
        // set-like consumer only when its active mask proves that a frequent sentinel makes the monomorphic
        // path worthwhile. A structural match alone is insufficient: SQL filters often leave the same physical
        // dictionary shape after removing the sentinel, and those batches do not amortize the specialized path.
        batchNullFreeSingleBinary = batchNullFreeSingleBinaryCandidate &&
                (fieldBinaryIds[0] == null || dictionarySingleBinaryFastPathAdmitted);

        // Decide once per batch whether the key can attempt array mode. Binary fields use query-stable interned
        // ids; small non-negative LONG fields use their value directly. Each radix reserves one digit for null,
        // which makes rollup grouping sets directly indexable instead of forcing their null-extended rows through
        // the hash table. A lazy dictionary may discover an id outside the binary radix later; compositeValueId
        // rejects just that position and the caller falls back to the normal hash table.
        batchCompositeEligible = handlers.length > 0;
        long compositeMultiplier = 1;
        for (int orderIndex = 0; batchCompositeEligible && orderIndex < compositeOrder.length; orderIndex++) {
            int index = compositeOrder[orderIndex];
            int radix;
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY) {
                if (!fieldIdComparable[index] || fieldInterners[index] == null || fieldInterners[index].distinctCount() > COMPOSITE_STRIDE) {
                    batchCompositeEligible = false;
                    break;
                }
                radix = compositeRadix(index);
            }
            else if (fieldKinds[index] == FlatTypeHandler.Kind.LONG && fieldLong[index] != null) {
                if (!MIXED_COMPOSITE_IDS) {
                    batchCompositeEligible = false;
                    break;
                }
                if (EARLY_REJECT_MIXED_COMPOSITE && hasEarlyOutOfRangeLong(index, values[index].length())) {
                    batchCompositeEligible = false;
                    break;
                }
                radix = LONG_COMPOSITE_RADIX;
            }
            else {
                batchCompositeEligible = false;
                break;
            }
            compositeMultiplier *= radix;
            if (compositeMultiplier > COMPOSITE_MAX) {
                batchCompositeEligible = false;
                break;
            }
        }
        batchNormalizedIntKeyEligible = normalizedIntKeyShape && sampledNormalizedIntKeyDomain(values, nulls);
        batchAccessorsReady = true;
        decideDiscriminatingHashField(values, nulls);
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
        if (!ADAPTIVE_DISCRIMINATING_FIELD_HASH ||
                handlers.length < DISCRIMINATING_FIELD_HASH_MIN_FIELDS ||
                DISCRIMINATING_FIELD_HASH_SAMPLE_SIZE <= 0 ||
                !hasSharedNestedDictionaryRecord(values)) {
            return;
        }

        int sampleSize = Math.min(values[0].length(), DISCRIMINATING_FIELD_HASH_SAMPLE_SIZE);
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
                    (long) bestDistinct * 100 >= (long) sampleSize * DISCRIMINATING_FIELD_HASH_MIN_DISTINCT_PERCENT) {
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
            if (!batchFieldNullFree[fieldIndex] && fieldNullAccess[fieldIndex].value(position)) {
                continue;
            }
            long value = fieldLong[fieldIndex].value(position);
            if (value < 0 || value >= LONG_COMPOSITE_CARDINALITY) {
                return true;
            }
        }
        return false;
    }

    /**
     * The composite value id for the key at {@code position} (id0 + id1*STRIDE + …) when the batch is array-mode
     * eligible, else -1. Fields are packed in ascending-radix order so a small grouping-id field occupies the low
     * digit instead of sparsely multiplying the common binary ids. The mapping is a stable bijection across batches;
     * FlatGroupingTable uses it as a direct array index.
     */
    /**
     * Whether this batch resolves group keys by composite (array-mode) id without hashing. The decoupled
     * hash-then-probe driver skips its precompute pass when this holds.
     */
    boolean batchArrayModeEligible()
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
            if (TRACK_DICTIONARY_EMPTY_SENTINEL && DICTIONARY_SENTINEL_SAMPLE_SIZE > 0) {
                samples = Math.min(mask.selectedCount(), DICTIONARY_SENTINEL_SAMPLE_SIZE);
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
                        (long) sentinelCount * 100 >= (long) samples * DICTIONARY_SENTINEL_MIN_PERCENT;
            }
        }
        batchNullFreeSingleBinary = dictionarySingleBinaryFastPathAdmitted;
        if (DEBUG_NULL_FREE_SINGLE_BINARY && batchNullFreeSingleBinary && samples > 0 && !debugDictionarySingleBinaryPrinted) {
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
        if (!batchCompositeEligible) {
            return -1;
        }
        if (FAST_NULL_FREE_LONG_BINARY_COMPOSITE && batchNullFreeLongBinary && !fieldLazyIntern[1]) {
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
        if (FAST_MIXED_COMPOSITE_3 && batchMixedComposite && compositeOrder.length == 3) {
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
                boolean longNull = !batchFieldNullFree[longIndex] && fieldNullAccess[longIndex].value(position);
                long longDigit = longNull ? LONG_COMPOSITE_CARDINALITY : fieldLong[longIndex].value(position);
                if (longDigit < 0 || (!longNull && longDigit >= LONG_COMPOSITE_CARDINALITY)) {
                    return -1;
                }

                int firstRadix = compositeRadix(firstBinary);
                boolean firstNull = !batchFieldNullFree[firstBinary] && fieldNullAccess[firstBinary].value(position);
                long firstDigit = firstNull ? firstRadix - 1L : batchEntryGlobalId[firstBinary][batchDictionaryIds[firstBinary][position]];
                if (firstDigit < 0 || (!firstNull && firstDigit >= firstRadix - 1L)) {
                    return -1;
                }

                int secondRadix = compositeRadix(secondBinary);
                boolean secondNull = !batchFieldNullFree[secondBinary] && fieldNullAccess[secondBinary].value(position);
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
        if (batchEntryGlobalIdDict[fieldIndex] == dictionaryValues &&
                batchEntryGlobalIdGeneration[fieldIndex] == dictionary.contentGeneration() &&
                batchEntryGlobalId[fieldIndex] != null) {
            return batchEntryGlobalId[fieldIndex];
        }
        ValueIdInterner interner = fieldInterners[fieldIndex];
        if (interner == null) {
            interner = new ValueIdInterner(VALUE_ID_CEILING);
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
            globalIds[entry] = interner.intern(data, so, len);
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

    private boolean hasLargeBinaryDictionary(Vector[] values)
    {
        for (int index = 0; index < fieldKinds.length; index++) {
            int channel = inputChannels[index];
            if (fieldKinds[index] == FlatTypeHandler.Kind.BINARY &&
                    channel < values.length &&
                    values[channel] instanceof DictionaryVector dictionary &&
                    dictionary.values().length() > COMPOSITE_STRIDE) {
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
            globalId = fieldInterners[fieldIndex].intern(dictionary.data(), so, len);
        }
        ids[dictId] = globalId;
        return globalId;
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
        if (interner == null || (!embedIdOnlyBinaryIds && (PACKED_RECORD_DICTIONARY_IDS ? packedRecordDictionaryIds == null : recordIds == null))) {
            return null;
        }
        int distinct = interner.distinctCount();
        // Only when the distinct values are at most half the groups: the dictionary base is then smaller than
        // the flat per-group copy would be, so emitting it never costs more than materializing flat even when
        // nothing downstream re-groups the key, while a downstream re-group/join sees a compact dictionary.
        if (distinct == 0 || (!fieldUsesIdOnlyRecords[fieldIndex] && distinct * 2 > size)) {
            return null;
        }
        I32Vector ownedDictionaryIds = OWN_GROUPED_DICTIONARY_IDS
                ? allocator.allocate(allocationContext, I32Vector.class, size, I32Vector::new)
                : null;
        int[] dictionaryIds = ownedDictionaryIds == null ? new int[size] : ownedDictionaryIds.values();
        for (int index : mask) {
            int recordIndex = table.recordIndex(index);
            if (recordIndex < 0) {
                return null;
            }
            int globalId;
            if (embedIdOnlyBinaryIds) {
                byte[] chunk = table.fixedChunk(recordIndex);
                int offset = table.keyOffset(table.fixedOffset(recordIndex)) + fixedOffsets[fieldIndex];
                if ((int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2) >= 0) {
                    return null;
                }
                globalId = recordDictionaryId(fieldIndex, chunk, offset, recordIndex);
            }
            else if (PACKED_RECORD_DICTIONARY_IDS) {
                int packedIndex = recordIndex * packedDictionaryFieldCount + packedDictionaryFieldIndex[fieldIndex];
                if (packedIndex >= packedRecordDictionaryIds.length) {
                    return null;
                }
                globalId = packedRecordDictionaryIds[packedIndex];
            }
            else {
                if (recordIndex >= recordIds.length) {
                    return null;
                }
                globalId = recordIds[recordIndex];
            }
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

    Vector tryMaterializeIdBackedBinaryValues(
            FlatGroupingTable table,
            int fieldIndex,
            int size,
            org.weakref.nitro.data.Mask mask,
            Vector output,
            org.weakref.nitro.data.Allocator allocator,
            org.weakref.nitro.data.Allocator.Context allocationContext)
    {
        if (fieldKinds[fieldIndex] != FlatTypeHandler.Kind.BINARY || !fieldUsesIdOnlyRecords[fieldIndex]) {
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
            int length = (int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2);
            totalBytes += length < 0 ? fieldInterners[fieldIndex].valueLength(recordDictionaryId(fieldIndex, chunk, offset, recordIndex)) : length;
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
                int length = (int) GROUP_INT_HANDLE.get(chunk, offset + Integer.BYTES * 2);
                if (length < 0) {
                    fieldInterners[fieldIndex].copyValue(recordDictionaryId(fieldIndex, chunk, offset, recordIndex), result, groupId);
                }
                else {
                    FlatTypeHandlers.BINARY.copyBinaryTo(chunk, offset, table.variableWidthArena(), result, groupId);
                }
            }
            previous = groupId + 1;
        }
        while (previous < size) {
            result.setNull(previous++);
        }
        return result;
    }

    /**
     * Hook called by {@link FlatGroupingTable} after a batch completes. Mirror of
     * {@link #beginBatch}; subclasses release cached references here.
     */
    public void endBatch()
    {
        batchAccessorsReady = false;
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
            int[] ids = fieldBinaryIds[0];
            int entry = ids == null ? position : ids[position];
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
        if (STABLE_DICTIONARY_VALUE_HASH &&
                fieldIdComparable != null &&
                fieldIdComparable[fieldIndex] &&
                batchDictionaryIds[fieldIndex] != null) {
            int globalId = globalIdFor(fieldIndex, batchDictionaryIds[fieldIndex][position]);
            if (globalId >= 0) {
                return fieldInterners[fieldIndex].groupingHash(globalId);
            }
        }
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
        if (ID_ONLY_BINARY_RECORDS && fieldIdComparable[fieldIndex] && batchDictionaryIds[fieldIndex] != null) {
            int globalId = globalIdFor(fieldIndex, batchDictionaryIds[fieldIndex][position]);
            if (globalId >= 0) {
                GROUP_INT_HANDLE.set(fixedChunk, fixedOffset, embedIdOnlyBinaryIds ? globalId : 0);
                GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES, 0);
                GROUP_INT_HANDLE.set(fixedChunk, fixedOffset + Integer.BYTES * 2, -1);
                fieldUsesIdOnlyRecords[fieldIndex] = true;
                return;
            }
        }
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

    private boolean identicalField(int fieldIndex, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, Vector value, int position, int recordIndex)
    {
        if (!batchAccessorsReady) {
            return identicalByKind(fieldKinds[fieldIndex], fixedChunk, fixedOffset, arena, value, position);
        }
        return switch (fieldKinds[fieldIndex]) {
            case LONG -> (long) GROUP_LONG_HANDLE.get(fixedChunk, fixedOffset) == fieldLong[fieldIndex].value(position);
            case BINARY -> identicalBinaryField(fieldIndex, fixedChunk, fixedOffset, arena, value, position, recordIndex);
            case BOOLEAN -> FlatTypeHandlers.BOOLEAN.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
            case DOUBLE -> FlatTypeHandlers.DOUBLE.identicalFlatToInput(fixedChunk, fixedOffset, arena, value, position);
        };
    }

    private boolean identicalBinaryField(int fieldIndex, byte[] fixedChunk, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, Vector value, int position, int recordIndex)
    {
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
        int[] ids = fieldBinaryIds[fieldIndex];
        int entry = ids == null ? position : ids[position];
        if (base.length(entry) != length) {
            return false;
        }
        byte[] chunk = arena.chunk((int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset));
        int offset = (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES);
        return OperatorVectorSupport.binaryEquals(base.data(), base.startOffset(entry), chunk, offset, length);
    }

    private boolean idOnlyBinaryEquals(int fieldIndex, int recordId, Vector value, int position)
    {
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
        if (batchNullFreeSingleBinary) {
            writeBinaryField(0, values[singleInputChannel], position, fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena);
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
            GROUP_LONG_HANDLE.set(fixedChunk, fixedOffset + fixedOffsets[0], fieldLong[0].value(position));
            writeBinaryField(1, values[inputChannels[1]], position, fixedChunk, fixedOffset + fixedOffsets[1], variableWidthArena);
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
        if (batchNullFreeSingleBinary) {
            return identicalBinaryField(0, fixedChunk, fixedOffset + singleFixedOffset, variableWidthArena, values[singleInputChannel], position, recordIndex);
        }
        if (batchNullFreeLongBinary) {
            if ((long) GROUP_LONG_HANDLE.get(fixedChunk, fixedOffset + fixedOffsets[0]) != fieldLong[0].value(position)) {
                return false;
            }
            int fieldOffset = fixedOffset + fixedOffsets[1];
            if (idComparable(1, fixedChunk, fieldOffset, recordIndex)) {
                int probeId = globalIdFor(1, batchDictionaryIds[1][position]);
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
                int probeId = globalIdFor(0, batchDictionaryIds[0][position]);
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
                int probeId = globalIdFor(index, batchDictionaryIds[index][position]);
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

    private boolean idComparable(int fieldIndex, byte[] fixedChunk, int fixedOffset, int recordIndex)
    {
        if (embedIdOnlyBinaryIds && fieldKinds[fieldIndex] == FlatTypeHandler.Kind.BINARY) {
            return anyFieldIdComparable
                    && fieldIdComparable[fieldIndex]
                    && (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset + Integer.BYTES * 2) < 0;
        }
        if (PACKED_RECORD_DICTIONARY_IDS) {
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
        if (embedIdOnlyBinaryIds && fieldKinds[fieldIndex] == FlatTypeHandler.Kind.BINARY) {
            return (int) GROUP_INT_HANDLE.get(fixedChunk, fixedOffset);
        }
        return PACKED_RECORD_DICTIONARY_IDS
                ? packedRecordDictionaryIds[recordIndex * packedDictionaryFieldCount + packedDictionaryFieldIndex[fieldIndex]]
                : recordDictionaryIds[fieldIndex][recordIndex];
    }

    private void storeRecordDictionaryIds(int recordIndex, int position)
    {
        if (!anyFieldIdComparable || embedIdOnlyBinaryIds) {
            return;
        }
        ensureRecordDictionaryIdCapacity(recordIndex + 1);
        if (PACKED_RECORD_DICTIONARY_IDS) {
            int recordOffset = recordIndex * packedDictionaryFieldCount;
            for (int index = 0; index < handlers.length; index++) {
                int packedField = packedDictionaryFieldIndex[index];
                if (packedField >= 0) {
                    packedRecordDictionaryIds[recordOffset + packedField] = fieldIdComparable[index]
                            ? globalIdFor(index, batchDictionaryIds[index][position])
                            : -1;
                }
            }
            return;
        }
        for (int index = 0; index < handlers.length; index++) {
            if (recordDictionaryIds[index] == null) {
                continue;
            }
            recordDictionaryIds[index][recordIndex] = fieldIdComparable[index] ? globalIdFor(index, batchDictionaryIds[index][position]) : -1;
        }
    }

    private void ensureRecordDictionaryIdCapacity(int required)
    {
        if (PACKED_RECORD_DICTIONARY_IDS) {
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
        if (recordDictionaryIds != null) {
            for (int[] ids : recordDictionaryIds) {
                release(ids);
            }
            Arrays.fill(recordDictionaryIds, null);
        }
        if (composedDictionaryIds != null) {
            for (int[] ids : composedDictionaryIds) {
                release(ids);
            }
            Arrays.fill(composedDictionaryIds, null);
        }
        release(packedRecordDictionaryIds);
        packedRecordDictionaryIds = null;
        if (dictionaryHashedIds != null) {
            Arrays.fill(dictionaryHashedIds, null);
            Arrays.fill(dictionaryHashedValues, null);
            Arrays.fill(boundDictionary, null);
            Arrays.fill(batchDictionaryIds, null);
            Arrays.fill(batchEntryGlobalIdDict, null);
            Arrays.fill(fieldBinaryBase, null);
            Arrays.fill(fieldBinaryIds, null);
            Arrays.fill(fieldLong, null);
            Arrays.fill(fieldNullAccess, null);
        }
        batchAccessorsReady = false;
        batchNormalizedIntKeyEligible = false;
        dictionarySingleBinaryFastPathDecided = false;
        dictionarySingleBinaryFastPathAdmitted = false;
    }

    private int[] borrowInts(int length)
    {
        long bytes = (long) length * Integer.BYTES;
        return POOL_SCRATCH && arrayPool.isRetainable(bytes) ? arrayPool.borrowInts(length) : new int[length];
    }

    private long[] borrowLongs(int length)
    {
        long bytes = (long) length * Long.BYTES;
        return POOL_SCRATCH && arrayPool.isRetainable(bytes) ? arrayPool.borrowLongs(length) : new long[length];
    }

    private void release(int[] array)
    {
        if (POOL_SCRATCH && array != null && arrayPool.isRetainable((long) array.length * Integer.BYTES)) {
            arrayPool.release(array);
        }
    }

    private void release(long[] array)
    {
        if (POOL_SCRATCH && array != null && arrayPool.isRetainable((long) array.length * Long.BYTES)) {
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

    public record Field(int inputChannel, FlatTypeHandler handler, int fixedOffset, Set<BinaryVector.Trait> binaryTraits)
    {
    }
}
