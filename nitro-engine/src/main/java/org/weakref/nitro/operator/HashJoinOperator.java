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

import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.source.ExternallyScheduledSource;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HashJoinOperator
        implements Operator
{
    @FunctionalInterface
    public interface JoinFilterFunction
    {
        boolean test(Vector outer, int outerPosition, Vector inner, int innerPosition);
    }

    /** A non-equi predicate evaluated for an equi-join candidate before an output mapping is emitted. */
    public record JoinFilter(int outerColumn, int innerColumn, JoinFilterFunction function, boolean encodedBinaryEquals, boolean longNotEqual, boolean longBitwiseOverlap)
    {
        public JoinFilter(int outerColumn, int innerColumn, JoinFilterFunction function)
        {
            this(outerColumn, innerColumn, requireNonNull(function, "function is null"), false, false, false);
        }

        public JoinFilter
        {
            if (outerColumn < 0 || innerColumn < 0) {
                throw new IllegalArgumentException("Join filter columns must be non-negative");
            }
            if (!encodedBinaryEquals && !longNotEqual && !longBitwiseOverlap) {
                requireNonNull(function, "function is null");
            }
        }

        public static JoinFilter binaryEquals(int outerColumn, int innerColumn)
        {
            return new JoinFilter(outerColumn, innerColumn, null, true, false, false);
        }

        public static JoinFilter binaryNotEquals(int outerColumn, int innerColumn)
        {
            return new JoinFilter(
                    outerColumn,
                    innerColumn,
                    (outer, outerPosition, inner, innerPosition) ->
                            !OperatorVectorSupport.binaryEquals(outer, outerPosition, inner, innerPosition));
        }

        public static JoinFilter longNotEqual(int outerColumn, int innerColumn)
        {
            return new JoinFilter(
                    outerColumn,
                    innerColumn,
                    (outer, outerPosition, inner, innerPosition) ->
                            OperatorVectorSupport.longValue(outer, outerPosition) != OperatorVectorSupport.longValue(inner, innerPosition),
                    false,
                    true,
                    false);
        }

        public static JoinFilter longBitwiseOverlap(int outerColumn, int innerColumn)
        {
            return new JoinFilter(
                    outerColumn,
                    innerColumn,
                    (outer, outerPosition, inner, innerPosition) ->
                            (OperatorVectorSupport.longValue(outer, outerPosition) & OperatorVectorSupport.longValue(inner, innerPosition)) != 0,
                    false,
                    false,
                    true);
        }
    }

    private static final long NO_MATCH_ROW_REFERENCE = -1L;
    private static final int NO_MATCH_COMPACT_ROW_REFERENCE = -1;
    private static final int VALUES_FLAG = 1;
    private static final int NULLS_FLAG = 1 << 1;
    private static final int ERRORS_FLAG = 1 << 2;
    private final Allocator allocator;
    private final OperatorResources operatorResources;
    private final GenericJoinIndexFactory genericJoinIndexes;
    private final HashJoinIndexPolicy joinIndexPolicy;
    private final HashJoinDynamicFilterPolicy dynamicFilterPolicy;
    private final DynamicFilterPolicy dynamicFilterRepresentationPolicy;
    private final HashJoinBuildPolicy buildPolicy;
    private final HashJoinOutputPolicy outputPolicy;
    private final HashJoinFilterPolicy filterPolicy;
    private final HashJoinExecutionPolicy executionPolicy;
    private final FlatKeyTablePolicy.ValueIds valueIdPolicy;
    private final HashJoinMaterializationListener materializationListener;
    // Build buffers outlive every individual result batch. Keep their ownership separate from result wrappers:
    // a dictionary result may borrow a build vector, and closing that result must release only the wrapper rather
    // than returning the still-live build vector to the shared pool. The two contexts deliberately share a pool so
    // their compatible buffers can still be recycled once the owning lifetime has ended. Independent join instances
    // use the same engine-owner compatibility key by default: a released sibling/subtree generation can feed the next
    // join without either context gaining authority over the other's live buffers. The Allocator itself owns the pool,
    // so the compatibility key never shares storage between queries or Allocator instances.
    private final Object allocationPoolGroup = new Object();
    private final Object allocationCompatibilityGroup;
    // Keep the established profile name for output buffers; allocation telemetry and external diagnostics aggregate
    // contexts by this stable name. Build ownership is still isolated below by a distinct scope and name.
    private final Allocator.Context allocationContext;
    private final Allocator.Context buildAllocationContext;
    private final Allocator.Context indexAllocationContext;
    private final VectorAllocator typeVectorAllocator;
    private final Operator outer;
    private Operator probeSource;
    private final Operator inner;
    private final int outerOutputCount;
    private final int innerOutputCount;
    private final int totalOutputCount;
    private final Schema fullOutputSchema;
    // Public output ordinal -> physical concatenated [outer..., inner...] column. Keeping projection inside the join
    // preserves lazy materialization: columns omitted by the plan never get an Output wrapper or a payload borrow.
    private int[] outputChannels;
    private Schema outputSchema;
    private int composeEncodedOuterDictionaryDepth;
    private boolean lazyDuplicateSlotState;
    private boolean implicitSequentialBuildRowReferences;
    private final boolean probeOuterJoin;
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;
    private final List<Optional<TypeBinding>> joinKeyTypes;
    private final List<TypeBinding> flatJoinKeyTypes;
    private final StructuralKeyKernel[] structuralKeyKernels;
    private final boolean allowsLegacyKeyShortcuts;
    private final JoinFilter[] joinFilters;
    private final Vector[] currentOuterFilterValues;
    private final Vector[] currentOuterFilterNulls;
    private final VectorAccess.BooleanValues[] currentOuterFilterNullAccess;
    private final Vector[] currentOuterFilterBaseValues;
    private final int[] currentOuterFilterDictionaryDepths;
    private Vector[][] innerFilterValues;
    private Vector[][] innerFilterNulls;
    private VectorAccess.BooleanValues[][] innerFilterNullAccess;
    private Vector[][] innerFilterBaseValues;
    private int[][] innerFilterDictionaryDepths;
    // Lazily resolved per-build-batch NULLS accessors for stream-only outer-join materialization. A null accessor
    // denotes an absent or all-false source stream; the parallel resolved map distinguishes it from not-yet-read.
    private VectorAccess.BooleanValues[][] directInnerNullAccess;
    private boolean[][] directInnerNullAccessResolved;
    private final boolean singleEncodedBinaryJoinFilter;
    private final boolean promotedBinaryEqualityFilter;
    private final boolean singleLongNotEqualJoinFilter;
    private final boolean singleLongBitwiseOverlapJoinFilter;
    private VectorAccess.LongValues fastOuterFilterLongs;
    private VectorAccess.LongValues fastInnerFilterLongs;
    private VectorAccess.LongValues[] directInnerFilterLongs;
    private long[] fastOuterFilterLongArray;
    private long[] fastInnerFilterLongArray;
    private long[][] directInnerFilterLongArrays;
    private int[] fastInnerFilterPositions;
    private int[] fastInnerOrderedIntFilterValues;
    private boolean fastInnerOrderedFilterAttempted;
    private boolean currentFastOuterFilterNull;
    private long currentFastOuterFilterLong;
    private DictionaryVector fastOuterFilterDictionary;
    private BinaryVector fastOuterFilterBase;
    private int fastOuterFilterDepth;
    private int currentFastOuterFilterBasePosition;
    private VectorAccess.BooleanValues fastOuterFilterNulls;
    private BufferedJoinInput.InnerBatch fastInnerFilterBatch;
    private DictionaryVector fastInnerFilterDictionary;
    private BinaryVector fastInnerFilterBase;
    private int fastInnerFilterDepth;
    private VectorAccess.BooleanValues fastInnerFilterNulls;
    private final JoinBufferSupport buffers;
    private final PrimitiveArrayPool arrayPool;
    private final BufferedJoinInput bufferedInner;
    private final HashJoinBuild preparedBuild;
    private final Streams[] outerSchema;
    private final Streams[] innerSchema;
    private final Vector[] currentOuterJoinValues;
    private final Vector[] currentOuterJoinNulls;
    private final JoinScratch joinScratch;
    private final int[] outputOuterPositions;
    private final long[] outputInnerRows;
    private final int[] outputInnerLogicalPositions;
    private final int[] outputInnerRunStarts;
    private final int[] outputInnerRunLengths;
    private final int[] outputInnerRunBatchIndexes;
    private final int[] outputInnerRunUniqueStarts;
    private final int[] outputInnerRunUniqueCounts;
    private final int[] preparedOuterPositions;
    private final LongList[] preparedOuterMatches;
    private SingleLongList[] preparedSingleMatches;
    // Flat single-match output: when the build is unique, the probe writes one build row reference per
    // outer row here and produceBatch emits from it without a LongList or per-row virtual dispatch.
    private final long[] preparedSingleRefs;
    private final int[] preparedSingleRefs32;
    private final int[] preparedRangeStarts;
    private final int[] preparedRangeCounts;
    private boolean joinScratchReleased;
    private boolean singleMatchProbe;
    private boolean singleMatchPositionProbe;
    private boolean compactSingleMatchProbe;
    private boolean outputSingleMatch;
    private int currentMatchRangeStart;
    private long currentMatchRef;
    private int currentMatchPosition;
    private final Streams[] currentOutputs;
    private int[] retainedConstraintCountsByBatch = new int[16];
    private int[][] retainedConstraintPositionsByBatch = new int[16][];
    private Mask[] retainedConstraintMasksByBatch = new Mask[16];
    private int[] preparedInnerBatchPositionCounts = new int[16];
    private int[] preparedInnerBatchCursors = new int[16];
    // Lazily built, per (inner batch, inner column) unified dictionary view for non-retained build
    // BinaryVector columns. Built once over the (small) build side; reused to emit every probe-output
    // batch's matched rows as a DictionaryVector over the shared dictionary instead of flattening the
    // bytes per output row. Keyed by batchIndex * innerOutputCount + innerOutputIndex.
    private final Map<Integer, BuildDictionary> buildDictionaries = new HashMap<>();
    private final BuildDictionary notDictionary = new BuildDictionary(new int[0], null);
    private final long[] multiRunBinaryOutputRows;
    private final Map<Integer, UnifiedBuildBinaryColumn> unifiedBuildBinaryColumns = new HashMap<>();
    private final UnifiedBuildBinaryColumn unsupportedUnifiedBuildBinaryColumn = new UnifiedBuildBinaryColumn(null, new int[0]);
    private long unifiedBuildBinaryRetainedBytes;
    private JoinIndex joinIndex;

    private Mask currentOuterMask;
    private Batch currentOuterBatch;
    private int currentOuterMaskIndex;
    private int outerRemaining;
    private int currentOuterPosition;
    private boolean currentOuterPositionReady;
    private LongList currentMatches = LongLists.emptyList();
    private int currentMatchCount;
    private boolean currentOuterJoinHasNulls;
    private int currentMatchIndex;
    private boolean currentOuterMatchEmitted;
    private int currentOutputCount;
    // 0 = not inspected for this output batch, 1 = contains a match, 2 = entirely unmatched.
    private byte allRowsNoMatchState;
    // 0 = uninitialized, 1 = ordinary producer, 2 = compacted-range producer, 3 = empty inner join.
    private int probeOutputMode;
    private Mask currentOutputMask;
    private int[] currentOuterDictionaryIds;
    // When encoded outer columns share the same dictionary/RLE mapping chain, composing that chain separately for
    // every VALUES/NULLS/ERRORS stream repeats the same random gathers and allocates one int[] per stream. Retain one
    // representative chain and its composed ids for the current output batch; columns with the same mapping identity
    // reuse those ids over their own leaf vector. This is representation-driven and independent of logical type.
    private final Vector[] composedOuterMappingSources;
    private final int[][] composedOuterMappingIds;
    private int composedOuterMappingCount;
    private int[] currentInnerLogicalDictionaryIds;
    private int[] currentInnerSourceDictionaryIds;
    private JoinBufferSupport.PositionMappingCache innerPositionMappingCache;
    private int preparedOuterCount;
    private int preparedOuterIndex;
    private boolean preparedOuterRange;
    private int preparedOuterRangeStart;
    private boolean outputInnerLogicalPositionsReady;
    private int outputInnerLogicalPositionsBatchIndex;
    private boolean done;
    private boolean waitingForProbeInput;
    private boolean outerConstrained;
    private boolean outerConstraintApplied;
    private int[] matchedOuterPositions = new int[0];
    private boolean outerSupportsReborrow;
    private int preparedInnerRunCount = -1;
    private String profileName;
    // Dynamic-filter state: distinct single-column build keys collected during index build, and whether collection
    // is still viable (single long key, inner join, readable vector, under the cap). Pushed to the probe once.
    private final boolean buildKeysViable;
    // Per join-key column: the distinct build-side values, for a dynamic filter pushed to the matching probe column.
    // A multi-key join contributes one filter per column — each is a necessary condition (a row joins only if EVERY
    // key matches), so the per-column membership sets over-approximate the joinable set; the join still does the
    // exact tuple match. A column is abandoned on its own when its encoding is unsupported or it exceeds the cap.
    private LongOpenHashSet[] buildKeyValues;
    private boolean[] buildKeyColumnAbandoned;
    private long[] buildKeyMins;
    private long[] buildKeyMaxs;
    private long[] collectedBuildKeys;
    private int collectedBuildKeyCount;
    private byte collectedBuildKeyAdmission;
    private boolean buildKeysAbandoned;
    private boolean streamedInnerLoaded;
    private long streamedInnerRows;
    private boolean dynamicFilterPushed;
    private int expectedIndexedInnerRows = -1;

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, false, new JoinFilter[0]);
    }

    public HashJoinOperator(OperatorResources operatorResources, Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn)
    {
        this(operatorResources, allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, false, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, JoinFilter... joinFilters)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, false, joinFilters);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int outerJoinColumn, Operator inner, int innerJoinColumn, boolean probeOuterJoin)
    {
        this(allocator, outer, new int[] {outerJoinColumn}, inner, new int[] {innerJoinColumn}, probeOuterJoin, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns)
    {
        this(allocator, outer, outerJoinColumns, inner, innerJoinColumns, false, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns, boolean probeOuterJoin)
    {
        this(allocator, outer, outerJoinColumns, inner, innerJoinColumns, probeOuterJoin, new JoinFilter[0]);
    }

    public HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns, JoinFilter... joinFilters)
    {
        this(allocator, outer, outerJoinColumns, inner, innerJoinColumns, false, joinFilters);
    }

    private HashJoinOperator(Allocator allocator, Operator outer, int[] outerJoinColumns, Operator inner, int[] innerJoinColumns, boolean probeOuterJoin, JoinFilter[] joinFilters)
    {
        this(
                EngineResources.from(allocator).operatorResources(),
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns,
                probeOuterJoin,
                joinFilters);
    }

    public HashJoinOperator(
            OperatorResources operatorResources,
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            boolean probeOuterJoin,
            JoinFilter... joinFilters)
    {
        this(
                operatorResources,
                allocator,
                outer,
                outerJoinColumns,
                inner,
                innerJoinColumns,
                probeOuterJoin,
                null,
                joinFilters);
    }

    HashJoinOperator(
            OperatorResources operatorResources,
            Allocator allocator,
            Operator outer,
            int[] outerJoinColumns,
            Operator inner,
            int[] innerJoinColumns,
            boolean probeOuterJoin,
            HashJoinBuild preparedBuild,
            JoinFilter... joinFilters)
    {
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("Hash join requires at least one join key");
        }
        Schema outerSchema = outer.outputSchema();
        Schema innerSchema = inner.outputSchema();
        List<Optional<TypeBinding>> equiJoinKeyTypes = joinKeyTypes(
                outerSchema,
                outerJoinColumns,
                innerSchema,
                innerJoinColumns);
        this.outerOutputCount = outer.outputCount();
        this.innerOutputCount = inner.outputCount();
        this.totalOutputCount = outerOutputCount + innerOutputCount;
        this.fullOutputSchema = outputSchema(
                outerSchema,
                outerOutputCount,
                innerSchema,
                innerOutputCount,
                probeOuterJoin);
        this.outputSchema = fullOutputSchema;
        this.outputChannels = new int[totalOutputCount];
        java.util.Arrays.setAll(outputChannels, index -> index);

        this.allocator = allocator;
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.genericJoinIndexes = operatorResources.genericJoinIndexes();
        this.joinIndexPolicy = operatorResources.hashJoin().indexPolicy();
        this.dynamicFilterPolicy = operatorResources.hashJoin().dynamicFilterPolicy();
        this.dynamicFilterRepresentationPolicy = operatorResources.dynamicFilterPolicy();
        this.buildPolicy = operatorResources.hashJoin().buildPolicy();
        this.outputPolicy = operatorResources.hashJoin().outputPolicy();
        this.filterPolicy = operatorResources.hashJoin().filterPolicy();
        this.executionPolicy = operatorResources.hashJoin().executionPolicy();
        this.valueIdPolicy = operatorResources.flatKeyTablePolicy().valueIds();
        this.composeEncodedOuterDictionaryDepth = outputPolicy.composeEncodedOuterDictionaryDepth();
        this.lazyDuplicateSlotState = executionPolicy.lazyDuplicateSlotState();
        this.materializationListener = operatorResources.hashJoin().materializationListener();
        this.allocationCompatibilityGroup = operatorResources.hashJoin()
                .bufferPoolCompatibilityGroup(allocationPoolGroup);
        this.allocationContext = new Allocator.Context(
                "HashJoinOperator",
                allocationPoolGroup,
                allocationCompatibilityGroup);
        this.buildAllocationContext = new Allocator.Context(
                "HashJoinOperatorBuild",
                allocationPoolGroup,
                allocationCompatibilityGroup);
        this.indexAllocationContext = new Allocator.Context(
                "HashJoinOperatorIndex",
                allocationPoolGroup,
                allocationCompatibilityGroup);
        this.arrayPool = allocator.primitiveArrays();
        allocator.register(allocationContext);
        allocator.register(buildAllocationContext);
        allocator.register(indexAllocationContext);
        this.typeVectorAllocator = allocator.vectorAllocator(allocationContext);
        this.outer = outer;
        this.probeSource = outer;
        this.inner = inner;
        this.composedOuterMappingSources = new Vector[totalOutputCount * 3];
        this.composedOuterMappingIds = new int[totalOutputCount * 3][];
        this.probeOuterJoin = probeOuterJoin;
        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.joinFilters = joinFilters.clone();
        this.currentOuterFilterValues = new Vector[joinFilters.length];
        this.currentOuterFilterNulls = new Vector[joinFilters.length];
        this.currentOuterFilterNullAccess = new VectorAccess.BooleanValues[joinFilters.length];
        this.currentOuterFilterBaseValues = new Vector[joinFilters.length];
        this.currentOuterFilterDictionaryDepths = new int[joinFilters.length];
        this.singleEncodedBinaryJoinFilter = joinFilters.length == 1 && joinFilters[0].encodedBinaryEquals();
        this.promotedBinaryEqualityFilter = filterPolicy.promoteBinaryEquality() && singleEncodedBinaryJoinFilter;
        if (promotedBinaryEqualityFilter) {
            java.util.ArrayList<Optional<TypeBinding>> effectiveJoinKeyTypes = new java.util.ArrayList<>(equiJoinKeyTypes);
            effectiveJoinKeyTypes.addAll(joinKeyTypes(
                    outerSchema,
                    new int[] {joinFilters[0].outerColumn()},
                    innerSchema,
                    new int[] {joinFilters[0].innerColumn()}));
            this.joinKeyTypes = List.copyOf(effectiveJoinKeyTypes);
        }
        else {
            this.joinKeyTypes = equiJoinKeyTypes;
        }
        this.structuralKeyKernels = new StructuralKeyKernel[joinKeyTypes.size()];
        java.util.ArrayList<TypeBinding> flatJoinKeyTypes = new java.util.ArrayList<>(joinKeyTypes.size());
        boolean allowsLegacyKeyShortcuts = true;
        StructuralTypeKernelFactory structuralTypes = operatorResources.codeGeneration().structuralTypes();
        for (int keyIndex = 0; keyIndex < joinKeyTypes.size(); keyIndex++) {
            int index = keyIndex;
            TypeBinding keyType = joinKeyTypes.get(keyIndex)
                    .orElseGet(() -> Schema.unspecified(index + 1).field(index).type());
            flatJoinKeyTypes.add(keyType);
            structuralKeyKernels[keyIndex] = structuralTypes.key(keyType);
            allowsLegacyKeyShortcuts &= structuralKeyKernels[keyIndex].allowsLegacyPhysicalShortcuts();
        }
        this.flatJoinKeyTypes = List.copyOf(flatJoinKeyTypes);
        this.allowsLegacyKeyShortcuts = allowsLegacyKeyShortcuts;
        this.singleLongNotEqualJoinFilter = joinFilters.length == 1 && joinFilters[0].longNotEqual();
        this.singleLongBitwiseOverlapJoinFilter = joinFilters.length == 1 && joinFilters[0].longBitwiseOverlap();
        for (JoinFilter filter : joinFilters) {
            if (filter.outerColumn() >= outerOutputCount || filter.innerColumn() >= innerOutputCount) {
                throw new IllegalArgumentException("Join filter column is out of bounds");
            }
        }
        JoinBufferPolicy joinBufferPolicy = operatorResources.joinBufferPolicy();
        this.buffers = new JoinBufferSupport(joinBufferPolicy, allocator, allocationContext);
        this.bufferedInner = new BufferedJoinInput(
                operatorResources.bufferedJoinInputPolicy(),
                new JoinBufferSupport(joinBufferPolicy, allocator, buildAllocationContext),
                innerOutputCount);
        this.preparedBuild = preparedBuild;
        this.outerSchema = new Streams[outerOutputCount];
        this.innerSchema = new Streams[innerOutputCount];
        this.multiRunBinaryOutputRows = new long[innerOutputCount];
        int effectiveJoinKeyCount = outerJoinColumns.length + (promotedBinaryEqualityFilter ? 1 : 0);
        this.currentOuterJoinValues = new Vector[effectiveJoinKeyCount];
        this.currentOuterJoinNulls = new Vector[effectiveJoinKeyCount];
        int maxBatchRows = executionPolicy.maxBatchRows();
        JoinScratch pooledScratch = executionPolicy.poolScratch()
                ? arrayPool.borrow(JoinScratch.class, maxBatchRows, JoinScratch.class)
                : null;
        this.joinScratch = pooledScratch != null ? pooledScratch : new JoinScratch(maxBatchRows);
        this.outputOuterPositions = joinScratch.outputOuterPositions;
        this.outputInnerRows = joinScratch.outputInnerRows;
        this.outputInnerLogicalPositions = joinScratch.outputInnerLogicalPositions;
        this.outputInnerRunStarts = joinScratch.outputInnerRunStarts;
        this.outputInnerRunLengths = joinScratch.outputInnerRunLengths;
        this.outputInnerRunBatchIndexes = joinScratch.outputInnerRunBatchIndexes;
        this.outputInnerRunUniqueStarts = joinScratch.outputInnerRunUniqueStarts;
        this.outputInnerRunUniqueCounts = joinScratch.outputInnerRunUniqueCounts;
        this.preparedOuterPositions = joinScratch.preparedOuterPositions;
        this.preparedSingleRefs = joinScratch.preparedSingleRefs;
        this.preparedSingleRefs32 = joinScratch.preparedSingleRefs32;
        this.preparedRangeStarts = joinScratch.preparedRangeStarts;
        this.preparedRangeCounts = joinScratch.preparedRangeCounts;
        accountJoinScratch();
        this.preparedOuterMatches = new LongList[maxBatchRows];
        this.currentOutputs = new Streams[totalOutputCount];
        this.buildKeysViable = preparedBuild == null && allowsLegacyKeyShortcuts && dynamicFilterPolicy.enabled() && !probeOuterJoin
                && (innerJoinColumns.length == 1 || dynamicFilterPolicy.multiKey());
        Arrays.fill(retainedConstraintCountsByBatch, -1);
    }

    private static List<Optional<TypeBinding>> joinKeyTypes(
            Schema outerSchema,
            int[] outerJoinColumns,
            Schema innerSchema,
            int[] innerJoinColumns)
    {
        java.util.ArrayList<Optional<TypeBinding>> types = new java.util.ArrayList<>(outerJoinColumns.length);
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Optional<TypeBinding> outerType = typeAt(outerSchema, outerJoinColumns[keyIndex]);
            Optional<TypeBinding> innerType = typeAt(innerSchema, innerJoinColumns[keyIndex]);
            if (outerType.filter(TypeBinding::isSpecified).isPresent() &&
                    innerType.filter(TypeBinding::isSpecified).isPresent() &&
                    !outerType.orElseThrow().identity().equals(innerType.orElseThrow().identity())) {
                throw new IllegalArgumentException("Hash-join key types do not match at index " + keyIndex);
            }
            types.add(innerType.filter(TypeBinding::isSpecified)
                    .or(() -> outerType.filter(TypeBinding::isSpecified))
                    .or(() -> innerType)
                    .or(() -> outerType));
        }
        return List.copyOf(types);
    }

    private static Optional<TypeBinding> typeAt(Schema schema, int column)
    {
        if (column < 0 || column >= schema.size()) {
            return Optional.empty();
        }
        return Optional.of(schema.field(column).type());
    }

    private void validateJoinKeyVectors(Vector[] values, String side)
    {
        for (int keyIndex = 0; keyIndex < joinKeyTypes.size(); keyIndex++) {
            int index = keyIndex;
            joinKeyTypes.get(keyIndex)
                    .filter(TypeBinding::isSpecified)
                    .filter(type -> !type.supportsVector(values[index]))
                    .ifPresent(type -> {
                        throw new IllegalArgumentException(
                                "Hash-join " + side + " key vector at index " + index +
                                        " is incompatible with plan-time type " + type.identity());
                    });
        }
    }

    @Override
    public int outputCount()
    {
        return outputChannels.length;
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    private static Schema outputSchema(
            Schema outerSchema,
            int outerOutputCount,
            Schema innerSchema,
            int innerOutputCount,
            boolean probeOuterJoin)
    {
        if (outerSchema.size() != outerOutputCount) {
            throw new IllegalArgumentException("outer schema size does not match output count");
        }
        if (innerSchema.size() != innerOutputCount) {
            throw new IllegalArgumentException("inner schema size does not match output count");
        }
        java.util.ArrayList<Field> fields = new java.util.ArrayList<>(outerOutputCount + innerOutputCount);
        fields.addAll(outerSchema.fields());
        for (Field field : innerSchema.fields()) {
            fields.add(probeOuterJoin ? nullable(field) : field);
        }
        return new Schema(fields);
    }

    private static Schema selectOutputs(Schema fullOutputSchema, int[] outputChannels)
    {
        java.util.ArrayList<Field> fields = new java.util.ArrayList<>(outputChannels.length);
        for (int outputChannel : outputChannels) {
            fields.add(fullOutputSchema.field(outputChannel));
        }
        return new Schema(fields);
    }

    private static Field nullable(Field field)
    {
        return field.nullable() ? field : new Field(field.name(), field.type(), true);
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return true;
    }

    @Override
    public Batch next()
    {
        long start = System.nanoTime();
        Mask batchMask = produceBatch();
        long afterProduceBatch = System.nanoTime();
        preparedInnerRunCount = -1;
        currentOutputMask = batchMask;
        outerConstrained = false;
        outerConstraintApplied = false;
        currentOuterDictionaryIds = null;
        clearComposedOuterMappings();
        currentInnerLogicalDictionaryIds = null;
        currentInnerSourceDictionaryIds = null;
        innerPositionMappingCache = buffers.newPositionMappingCache();
        allRowsNoMatchState = 0;
        java.util.Arrays.fill(currentOutputs, null);
        Output[] outputs = new Output[outputChannels.length];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            outputs[outputIndex] = resultOutput(outputChannels[outputIndex]);
        }
        long afterBuildOutputs = System.nanoTime();
        return new Batch(
                batchMask,
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                releasedMask -> allocator.release(allocationContext, releasedMask),
                () -> {},
                outputs);
    }

    private Mask produceBatch()
    {
        outputInnerLogicalPositionsReady = false;
        outputInnerLogicalPositionsBatchIndex = -1;
        if (probeOutputMode == 0) {
            prepareProbeFirstBuildFilter();
            loadInnerIfNecessary();
            pushDynamicFilterIfReady();
            if ((joinIndex == null || joinIndex.isEmpty()) && !probeOuterJoin) {
                captureOuterSchemaIfAvailable();
                done = true;
                probeOutputMode = 3;
            }
            else if (!probeOuterJoin && joinFilters.length == 0 && !outputSingleMatch &&
                    !joinIndex.supportsSingleMatchRefs() && joinIndex.supportsRowRanges()) {
                probeOutputMode = 2;
            }
            else {
                probeOutputMode = 1;
            }
        }
        return switch (probeOutputMode) {
            case 1 -> produceOrdinaryBatch();
            case 2 -> produceRangeBatch();
            case 3 -> {
                currentOutputCount = 0;
                yield allocator.allocateAllMask(allocationContext, 0);
            }
            default -> throw new IllegalStateException("Unexpected probe output mode: " + probeOutputMode);
        };
    }

    private Mask produceOrdinaryBatch()
    {
        int outputPosition = 0;
        boolean logicalPositionsReady = false;
        boolean rowReferencesWritten = false;
        Batch outputOuterBatch = currentOuterBatch;
        while (outputPosition < executionPolicy.maxBatchRows()) {
            if (outerRemaining == 0) {
                if (outputPosition > 0) {
                    break;
                }
                if (!loadNextOuterBatch()) {
                    if (!waitingForProbeInput) {
                        done = true;
                    }
                    break;
                }
                outputOuterBatch = currentOuterBatch;
            }

            if (!currentOuterPositionReady) {
                int emitted = tryEmitDirectSingleMatchPositionRange(outputPosition);
                if (emitted >= 0) {
                    outputPosition += emitted;
                    if (emitted > 0 && !rowReferencesWritten) {
                        logicalPositionsReady = true;
                    }
                    continue;
                }
                if (preparedOuterIndex >= preparedOuterCount) {
                    if (currentOuterMaskIndex >= currentOuterMask.count()) {
                        outerRemaining = 0;
                        continue;
                    }
                    prepareOuterProbeChunk();
                }
                if (preparedOuterIndex >= preparedOuterCount) {
                    outerRemaining = 0;
                    continue;
                }
                currentOuterPosition = preparedOuterRange ? preparedOuterRangeStart + preparedOuterIndex : preparedOuterPositions[preparedOuterIndex];
                if (singleMatchProbe) {
                    if (singleMatchPositionProbe) {
                        currentMatchPosition = preparedSingleRefs32[preparedOuterIndex];
                        currentMatchCount = currentMatchPosition == NO_MATCH_COMPACT_ROW_REFERENCE ? 0 : 1;
                    }
                    else {
                        currentMatchRef = compactSingleMatchProbe
                                ? joinIndex.unpackCompactSingleMatchRef(preparedSingleRefs32[preparedOuterIndex])
                                : preparedSingleRefs[preparedOuterIndex];
                        currentMatchCount = currentMatchRef == NO_MATCH_ROW_REFERENCE ? 0 : 1;
                    }
                }
                else {
                    currentMatches = preparedOuterMatches[preparedOuterIndex];
                    currentMatchCount = currentMatches.size();
                }
                preparedOuterIndex++;
                currentOuterPositionReady = true;
                currentMatchIndex = 0;
                currentOuterMatchEmitted = false;
                cacheCurrentOuterFilterValue();
            }

            if (currentMatchCount == 0) {
                if (probeOuterJoin) {
                    outputOuterPositions[outputPosition] = currentOuterPosition;
                    outputInnerRows[outputPosition] = NO_MATCH_ROW_REFERENCE;
                    outputPosition++;
                }
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
                currentMatchCount = 0;
                continue;
            }

            while (currentMatchIndex < currentMatchCount && outputPosition < executionPolicy.maxBatchRows()) {
                int matchIndex = currentMatchIndex;
                long rowReference;
                if (singleMatchPositionProbe) {
                    rowReference = JoinRowReference.pack(joinIndex.singleMatchPositionBatchIndex(), currentMatchPosition);
                }
                else {
                    rowReference = singleMatchProbe ? currentMatchRef : currentMatches.getLong(currentMatchIndex);
                }
                currentMatchIndex++;
                boolean passes = promotedBinaryEqualityFilter ||
                        (filterPolicy.directBinaryDispatch() && singleEncodedBinaryJoinFilter
                                ? passesBinaryJoinFilter(currentOuterPosition, rowReference)
                                : passesJoinFilters(currentOuterPosition, rowReference, matchIndex));
                if (!passes) {
                    continue;
                }
                outputOuterPositions[outputPosition] = currentOuterPosition;
                if (singleMatchPositionProbe) {
                    outputInnerLogicalPositions[outputPosition] = currentMatchPosition;
                    if (!rowReferencesWritten) {
                        logicalPositionsReady = true;
                    }
                }
                else {
                    outputInnerRows[outputPosition] = rowReference;
                    rowReferencesWritten = true;
                    logicalPositionsReady = false;
                }
                outputPosition++;
                currentOuterMatchEmitted = true;
                if (outputSingleMatch) {
                    currentMatchIndex = currentMatchCount;
                }
            }

            if (currentMatchIndex == currentMatchCount) {
                if (probeOuterJoin && !currentOuterMatchEmitted) {
                    outputOuterPositions[outputPosition] = currentOuterPosition;
                    outputInnerRows[outputPosition] = NO_MATCH_ROW_REFERENCE;
                    outputPosition++;
                }
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatches = LongLists.emptyList();
                currentMatchCount = 0;
            }
        }

        if (outputPosition == 0) {
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
        }
        currentOutputCount = outputPosition;
        if (logicalPositionsReady && !rowReferencesWritten) {
            outputInnerLogicalPositionsReady = true;
            outputInnerLogicalPositionsBatchIndex = joinIndex.singleMatchPositionBatchIndex();
        }
        return allocator.allocateRangeMask(allocationContext, 0, outputPosition);
    }

    /**
     * Filter-free inner join over a compacted one-to-many build. The index exposes each probe match as a contiguous
     * slice of pooled row references, so output production bulk-copies that slice and fills the repeated probe
     * position. This method is selected once per output batch; the ordinary per-row loop contains no range feature
     * branch.
     */
    private Mask produceRangeBatch()
    {
        int outputPosition = 0;
        while (outputPosition < executionPolicy.maxBatchRows()) {
            if (outerRemaining == 0) {
                if (outputPosition > 0) {
                    break;
                }
                if (!loadNextOuterBatch()) {
                    if (!waitingForProbeInput) {
                        done = true;
                    }
                    break;
                }
            }

            if (!currentOuterPositionReady) {
                if (preparedOuterIndex >= preparedOuterCount) {
                    if (currentOuterMaskIndex >= currentOuterMask.count()) {
                        outerRemaining = 0;
                        continue;
                    }
                    preparedOuterCount = Math.min(currentOuterMask.count() - currentOuterMaskIndex, executionPolicy.maxBatchRows());
                    preparedOuterIndex = 0;
                    for (int index = 0; index < preparedOuterCount; index++) {
                        preparedOuterPositions[index] = currentOuterMask.position(currentOuterMaskIndex++);
                    }
                    if (!joinIndex.matchRowRanges(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls,
                            preparedOuterPositions, preparedOuterCount, preparedRangeStarts, preparedRangeCounts)) {
                        throw new IllegalStateException("Compacted range index stopped supporting row ranges");
                    }
                    accountJoinIndex();
                }
                currentOuterPosition = preparedOuterPositions[preparedOuterIndex];
                currentMatchRangeStart = preparedRangeStarts[preparedOuterIndex];
                currentMatchCount = preparedRangeCounts[preparedOuterIndex];
                preparedOuterIndex++;
                currentOuterPositionReady = true;
                currentMatchIndex = 0;
            }

            if (currentMatchCount == 0) {
                outerRemaining--;
                currentOuterPositionReady = false;
                continue;
            }

            int emitted = Math.min(currentMatchCount - currentMatchIndex, executionPolicy.maxBatchRows() - outputPosition);
            Arrays.fill(outputOuterPositions, outputPosition, outputPosition + emitted, currentOuterPosition);
            joinIndex.copyRowRange(currentMatchRangeStart + currentMatchIndex, outputInnerRows, outputPosition, emitted);
            currentMatchIndex += emitted;
            outputPosition += emitted;

            if (currentMatchIndex == currentMatchCount) {
                outerRemaining--;
                currentOuterPositionReady = false;
                currentMatchCount = 0;
            }
        }

        if (outputPosition == 0) {
            currentOutputCount = 0;
            return allocator.allocateAllMask(allocationContext, 0);
        }
        currentOutputCount = outputPosition;
        return allocator.allocateRangeMask(allocationContext, 0, outputPosition);
    }

    private int tryEmitDirectSingleMatchPositionRange(int outputPosition)
    {
        if (!outputPolicy.directDenseSingleMatchRangeOutput() ||
                joinFilters.length != 0 ||
                probeOuterJoin ||
                preparedOuterIndex < preparedOuterCount ||
                !currentOuterMask.all() ||
                currentOuterMaskIndex >= currentOuterMask.count()) {
            return -1;
        }
        int probeCount = Math.min(
                Math.min(currentOuterMask.count() - currentOuterMaskIndex, outerRemaining),
                executionPolicy.maxBatchRows() - outputPosition);
        if (probeCount <= 0) {
            return -1;
        }
        if (!joinIndex.supportsSingleMatchRefs() ||
                !joinIndex.supportsSingleMatchPositions() ||
                !joinIndex.supportsSingleMatchPositionRange() ||
                !joinIndex.supportsDirectSingleMatchPositionRangeOutput()) {
            return -1;
        }
        int probeStart = currentOuterMaskIndex;
        int emitted = joinIndex.emitSingleRowsPositionsRange(
                currentOuterJoinValues,
                currentOuterJoinNulls,
                currentOuterJoinHasNulls,
                probeStart,
                probeCount,
                outputOuterPositions,
                outputInnerLogicalPositions,
                outputPosition);
        accountJoinIndex();
        currentOuterMaskIndex += probeCount;
        outerRemaining -= probeCount;
        return emitted;
    }

    private void prepareOuterProbeChunk()
    {
        long start = System.nanoTime();
        preparedOuterCount = Math.min(currentOuterMask.count() - currentOuterMaskIndex, executionPolicy.maxBatchRows());
        preparedOuterIndex = 0;
        singleMatchProbe = joinIndex.supportsSingleMatchRefs();
        singleMatchPositionProbe = singleMatchProbe && !probeOuterJoin && joinIndex.supportsSingleMatchPositions();
        preparedOuterRange = singleMatchPositionProbe && currentOuterMask.all() && joinIndex.supportsSingleMatchPositionRange();
        if (preparedOuterRange) {
            preparedOuterRangeStart = currentOuterMaskIndex;
            currentOuterMaskIndex += preparedOuterCount;
        }
        else {
            for (int index = 0; index < preparedOuterCount; index++) {
                preparedOuterPositions[index] = currentOuterMask.position(currentOuterMaskIndex++);
            }
        }
        compactSingleMatchProbe = singleMatchProbe && !singleMatchPositionProbe && joinIndex.supportsCompactSingleMatchRefs();
        if (preparedOuterRange) {
            joinIndex.matchSingleRowsPositionsRange(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterRangeStart, preparedOuterCount, preparedSingleRefs32);
        }
        else if (singleMatchPositionProbe) {
            joinIndex.matchSingleRowsPositions(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedSingleRefs32);
        }
        else if (compactSingleMatchProbe) {
            joinIndex.matchSingleRowsCompact(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedSingleRefs32);
        }
        else if (singleMatchProbe) {
            joinIndex.matchSingleRows(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedSingleRefs);
        }
        else {
            joinIndex.matchRows(currentOuterJoinValues, currentOuterJoinNulls, currentOuterJoinHasNulls, preparedOuterPositions, preparedOuterCount, preparedOuterMatches, preparedSingleMatches());
        }
        accountJoinIndex();
        cacheOrderedInnerFilterPayload();
    }

    private SingleLongList[] preparedSingleMatches()
    {
        if (preparedSingleMatches == null) {
            preparedSingleMatches = createSingleLongLists(executionPolicy.maxBatchRows());
        }
        return preparedSingleMatches;
    }

    private String genericProbeKind()
    {
        return joinIndex == null ? "object" : joinIndex.probeKind();
    }

    private boolean loadNextOuterBatch()
    {
        waitingForProbeInput = false;
        // The previous probe batch has been fully consumed before this method is reached. Closing it releases lazy
        // projection results and evaluator scratch back to their shared pools before the source advances. Without
        // this boundary, ProjectOperator loses its old BatchState on next() and retains one full set of vectors per
        // input batch until query teardown.
        if (currentOuterBatch != null) {
            currentOuterBatch.close();
            currentOuterBatch = null;
        }
        while (probeSource.hasNext()) {
            currentOuterBatch = probeSource.next();
            // Re-borrow is a property of the current open batch, not necessarily of every batch a source can emit.
            // A scan may offer it only for a selective filtered window that remains wholly live in one batch.
            outerSupportsReborrow = probeSource.supportsConstrainedReborrow();
            // When the outer can satisfy a constrained re-borrow, skip eager outer schema capture:
            // it borrows a representative VALUES vector for every outer column, forcing lazy
            // projected payloads to materialize. The schema is then derived on demand in
            // outputSchema() from currentOuterBatch. When the outer's reader advances irreversibly,
            // capture the schema eagerly now while the batch is live (deferring past the advance
            // would read an already-advanced source).
            if (!outerSupportsReborrow) {
                BufferedJoinInput.captureSchema(currentOuterBatch, outerSchema);
            }
            currentOuterMask = currentOuterBatch.borrowMask();
            if (!currentOuterMask.none()) {
                cacheOuterJoinInputs();
                cacheOuterFilterInputs();
                currentOuterMaskIndex = 0;
                outerRemaining = currentOuterMask.count();
                currentOuterPositionReady = false;
                preparedOuterCount = 0;
                preparedOuterIndex = 0;
                return true;
            }
        }
        waitingForProbeInput = probeSource instanceof ExternallyScheduledSource source && !source.isFinished();
        return false;
    }

    boolean isWaitingForProbeInput()
    {
        return waitingForProbeInput;
    }

    private LongList matchesForOuterPosition()
    {
        return matchesForOuterPosition(currentOuterPosition);
    }

    private LongList matchesForOuterPosition(int outerPosition)
    {
        if (joinIndex == null) {
            return LongLists.emptyList();
        }
        String operatorName = profileName != null ? profileName : "hash_join";
        String probeKind = genericProbeKind();
        if (!currentOuterJoinHasNulls) {
            return joinIndex.matchesNoNulls(currentOuterJoinValues, outerPosition);
        }
        return joinIndex.matches(currentOuterJoinValues, currentOuterJoinNulls, outerPosition);
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        // Forward a downstream join's filter on a probe-side output column toward the probe source. This join's
        // projected public ordinal is first remapped to the physical concatenated join output.
        if (filter.column() < 0 || filter.column() >= outputChannels.length) {
            return;
        }
        int physicalColumn = outputChannels[filter.column()];
        if (physicalColumn < outerOutputCount) {
            outer.pushDynamicFilter(filter.withColumn(physicalColumn));
        }
    }

    /** Once the build side is materialized, push this join's own key membership down to the probe. Runs once. */
    private void pushDynamicFilterIfReady()
    {
        // A probe-side outer join preserves probe rows that have no build match. Filtering that side by build-key
        // membership would discard precisely those rows before the join can null-extend them.
        if (dynamicFilterPushed || !buildKeysViable || probeOuterJoin) {
            return;
        }
        dynamicFilterPushed = true;
        if (dynamicFilterPolicy.debug()) {
            System.err.printf("[dynamic-filter] join=%s viable=%s abandoned=%s keys=%s collected=%d%n",
                    profileName, buildKeysViable, buildKeysAbandoned,
                    buildKeyValues == null ? "null" : java.util.Arrays.toString(java.util.Arrays.stream(buildKeyValues)
                            .mapToInt(values -> values == null ? -1 : values.size()).toArray()),
                    collectedBuildKeyCount);
        }
        if (buildKeysAbandoned) {
            // A large single-long build may still have an exact bounded-range membership bitset owned by its join
            // index. Share that immutable representation with the probe scan instead of rebuilding a huge hash set.
            if (dynamicFilterPolicy.shareSparseLongRange() && joinIndex != null) {
                DynamicFilter filter = joinIndex.buildDynamicFilter(outerJoinColumns[0]);
                if (filter != null) {
                    outer.pushDynamicFilter(filter);
                }
            }
            return;
        }
        if (collectedBuildKeys != null) {
            if (collectedBuildKeyCount > 0) {
                outer.pushDynamicFilter(DynamicFilter.fromCollectedValues(
                        outerJoinColumns[0],
                        collectedBuildKeys,
                        collectedBuildKeyCount,
                        buildKeyMins[0],
                        buildKeyMaxs[0],
                        dynamicFilterRepresentationPolicy));
            }
            releaseCollectedBuildKeys();
            return;
        }
        if (buildKeyValues == null) {
            return;
        }
        for (int column = 0; column < buildKeyValues.length; column++) {
            if (!buildKeyColumnAbandoned[column] && buildKeyValues[column] != null && !buildKeyValues[column].isEmpty()) {
                DynamicFilter filter = dynamicFilterPolicy.trackValueRange() && buildKeyMins != null
                        ? DynamicFilter.fromValues(
                                outerJoinColumns[column],
                                buildKeyValues[column],
                                buildKeyMins[column],
                                buildKeyMaxs[column],
                                dynamicFilterRepresentationPolicy)
                        : DynamicFilter.fromValues(
                                outerJoinColumns[column],
                                buildKeyValues[column],
                                dynamicFilterRepresentationPolicy);
                outer.pushDynamicFilter(filter);
            }
        }
    }

    private void prepareProbeFirstBuildFilter()
    {
        if (!allowsLegacyKeyShortcuts ||
                !dynamicFilterPolicy.probeFirstBuildFilter() ||
                probeOuterJoin ||
                !supportsInnerDynamicFilterPushdown() ||
                inner.exactOutputRows() < dynamicFilterPolicy.probeFirstMinBuildRows()) {
            return;
        }
        ProbeSpool spool = new ProbeSpool(allocator, outer);
        probeSource = spool;
        it.unimi.dsi.fastutil.longs.LongSet[] values = spool.prepare(
                outerJoinColumns,
                dynamicFilterPolicy.probeFirstMaxProbeRows());
        if (dynamicFilterPolicy.debug()) {
            System.err.printf("[probe-first-build-filter] join=%s probeRows=%d complete=%s keySizes=%s%n",
                    profileName != null ? profileName : "hash_join",
                    spool.bufferedRows(),
                    values != null,
                    values == null ? "-" : java.util.Arrays.toString(java.util.Arrays.stream(values).mapToInt(it.unimi.dsi.fastutil.longs.LongSet::size).toArray()));
        }
        if (values == null) {
            return;
        }
        for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
            inner.pushDynamicFilter(DynamicFilter.fromValues(
                    innerJoinColumns[keyIndex],
                    values[keyIndex],
                    dynamicFilterRepresentationPolicy));
        }
    }

    private boolean supportsInnerDynamicFilterPushdown()
    {
        for (int column : innerJoinColumns) {
            if (!inner.supportsDynamicFilterPushdown(column)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Record one build row's join-key values into the per-column membership sets. A row with any null key can never
     * match an inner join, so it contributes nothing. Called once per build row while collection is still viable.
     */
    private void collectBuildKeys(VectorAccess.LongValues[] joinValues, Vector[] joinNulls, boolean hasNulls, int position)
    {
        if (buildKeysAbandoned) {
            return;
        }
        int keyCount = joinValues.length;
        if (usesCollectedBuildKeys(keyCount)) {
            collectBuildKey(joinValues[0], joinNulls[0], hasNulls, position);
            return;
        }
        if (buildKeyValues == null) {
            buildKeyValues = new LongOpenHashSet[keyCount];
            buildKeyColumnAbandoned = new boolean[keyCount];
            initializeBuildKeyRanges(keyCount);
        }
        if (hasNulls) {
            for (int column = 0; column < keyCount; column++) {
                if (joinNulls[column] != null && VectorAccess.isNull(joinNulls[column], position)) {
                    return;   // any null key => this row cannot join; omit all its values
                }
            }
        }
        boolean allAbandoned = true;
        for (int column = 0; column < keyCount; column++) {
            if (buildKeyColumnAbandoned[column]) {
                continue;
            }
            long value = joinValues[column].value(position);
            if (buildKeyValues[column] == null) {
                int expectedValues = dynamicFilterPolicy.preSizeValueSets()
                        ? Math.min(expectedInnerRowCount(), dynamicFilterPolicy.maxValues() + 1)
                        : 16;
                buildKeyValues[column] = new LongOpenHashSet(expectedValues);
            }
            buildKeyValues[column].add(value);
            if (dynamicFilterPolicy.trackValueRange()) {
                buildKeyMins[column] = Math.min(buildKeyMins[column], value);
                buildKeyMaxs[column] = Math.max(buildKeyMaxs[column], value);
            }
            if (buildKeyValues[column].size() > dynamicFilterPolicy.maxValues()) {
                buildKeyColumnAbandoned[column] = true;   // not selective enough to be worth a runtime filter
                buildKeyValues[column] = null;
            }
            else if (column == 0 && keyCount == 1) {
                promoteBuildKeySetToCollectedIfReady();
            }
            allAbandoned &= buildKeyColumnAbandoned[column];
        }
        if (allAbandoned) {
            buildKeysAbandoned = true;   // every column dropped: stop collecting entirely
        }
    }

    private boolean usesCollectedBuildKeys(int keyCount)
    {
        return dynamicFilterPolicy.deferDeduplication() && keyCount == 1 && collectedBuildKeyAdmission > 0;
    }

    private void promoteBuildKeySetToCollectedIfReady()
    {
        if (!dynamicFilterPolicy.deferDeduplication() ||
                dynamicFilterPolicy.buildRowLimit() > dynamicFilterPolicy.maxValues() ||
                collectedBuildKeyAdmission != 0 ||
                buildKeyValues[0].size() < dynamicFilterPolicy.deferDeduplicationMinRows()) {
            return;
        }
        long span = buildKeyMaxs[0] - buildKeyMins[0] + 1;
        double density = span <= 0 ? 1 : (double) buildKeyValues[0].size() / span;
        boolean admitted = density <= dynamicFilterPolicy.deferDeduplicationMaxDensity();
        if (dynamicFilterPolicy.debug()) {
            System.err.printf("[deferred-dynamic-filter-admission] join=%s distinct=%d span=%d density=%.6f admitted=%s%n",
                    profileName,
                    buildKeyValues[0].size(),
                    span,
                    density,
                    admitted);
        }
        if (!admitted) {
            collectedBuildKeyAdmission = -1;
            return;
        }
        int expectedValues = Math.min(
                dynamicFilterPolicy.maxValues(),
                Math.max(expectedInnerRowCount(), buildKeyValues[0].size()));
        collectedBuildKeys = arrayPool.borrowLongs(expectedValues);
        for (long value : buildKeyValues[0]) {
            collectedBuildKeys[collectedBuildKeyCount++] = value;
        }
        buildKeyValues[0] = null;
        collectedBuildKeyAdmission = 1;
    }

    private void collectBuildKey(VectorAccess.LongValues joinValues, Vector joinNulls, boolean hasNulls, int position)
    {
        if (hasNulls && joinNulls != null && VectorAccess.isNull(joinNulls, position)) {
            return;
        }
        if (collectedBuildKeys == null) {
            int expectedValues = Math.min(expectedInnerRowCount(), dynamicFilterPolicy.maxValues());
            collectedBuildKeys = arrayPool.borrowLongs(Math.max(16, expectedValues));
            initializeBuildKeyRanges(1);
        }
        if (collectedBuildKeyCount == collectedBuildKeys.length) {
            int newLength = Math.min(
                    dynamicFilterPolicy.maxValues(),
                    Math.multiplyExact(collectedBuildKeys.length, 2));
            if (newLength == collectedBuildKeys.length) {
                buildKeysAbandoned = true;
                releaseCollectedBuildKeys();
                return;
            }
            long[] expanded = arrayPool.borrowLongs(newLength);
            System.arraycopy(collectedBuildKeys, 0, expanded, 0, collectedBuildKeyCount);
            arrayPool.release(collectedBuildKeys);
            collectedBuildKeys = expanded;
        }
        long value = joinValues.value(position);
        collectedBuildKeys[collectedBuildKeyCount++] = value;
        buildKeyMins[0] = Math.min(buildKeyMins[0], value);
        buildKeyMaxs[0] = Math.max(buildKeyMaxs[0], value);
    }

    private void releaseCollectedBuildKeys()
    {
        arrayPool.release(collectedBuildKeys);
        collectedBuildKeys = null;
        collectedBuildKeyCount = 0;
    }

    private void initializeBuildKeyRanges(int keyCount)
    {
        if (!dynamicFilterPolicy.trackValueRange()) {
            return;
        }
        buildKeyMins = arrayPool.borrowLongs(keyCount);
        buildKeyMaxs = arrayPool.borrowLongs(keyCount);
        Arrays.fill(buildKeyMins, Long.MAX_VALUE);
        Arrays.fill(buildKeyMaxs, Long.MIN_VALUE);
    }

    private void loadInnerIfNecessary()
    {
        if (canStreamUnusedBuildPayload()) {
            loadStreamingInner();
            return;
        }
        int batchCountBefore = bufferedInner.batches().size();
        BufferedJoinInput.BatchMaskPruner maskPruner = buildPolicy.pruneZeroBitwiseOverlapRows() && singleLongBitwiseOverlapJoinFilter
                ? this::pruneZeroBitwiseOverlapBuildMask
                : null;
        boolean sharedPreparedPayload = preparedBuild != null && preparedBuild.sharePayloadWith(bufferedInner);
        if (!sharedPreparedPayload) {
            bufferedInner.loadAll(
                    inner,
                    buildPolicy.maxBuildBatchRows(),
                    innerJoinColumns,
                    inner.supportsRetainedBatches(),
                    !inner.supportsRetainedBatches() && inner.supportsConstrainedReborrow(),
                    maskPruner);
        }
        ensureRetainedConstraintCacheCapacity(bufferedInner.batches().size());
        copySchema(bufferedInner.schema(), innerSchema);
        cacheInnerFilterInputs();
        if (preparedBuild != null) {
            joinIndex = preparedBuild.newProbeIndex();
            expectedIndexedInnerRows = (int) Math.min(Integer.MAX_VALUE, bufferedInner.rowCount());
            return;
        }
        if (buildPolicy.pruneZeroBitwiseOverlapRows() && singleLongBitwiseOverlapJoinFilter) {
            expectedIndexedInnerRows = (int) Math.min(Integer.MAX_VALUE, bufferedInner.rowCount());
        }
        // A dynamic filter caps its distinct build values. If the build side alone has more
        // rows than that, its key membership set will either overflow the cap (and be abandoned) or — for a rare
        // low-cardinality key — yield a value set so large the probe scan discards it as non-selective. Either way the
        // per-row set insertion is wasted, and it dominates large-build joins (TPC-DS q84). Skip collection up front.
        // Correctness is unaffected: the join still enforces the condition; the filter is a pure decode-pruning hint.
        if (buildKeysViable && !buildKeysAbandoned) {
            long totalInnerRows = 0;
            for (BufferedJoinInput.InnerBatch batch : bufferedInner.batches()) {
                totalInnerRows += batch.length();
            }
            if (totalInnerRows > dynamicFilterPolicy.buildRowLimit()) {
                buildKeysAbandoned = true;
            }
        }
        for (int batchIndex = batchCountBefore; batchIndex < bufferedInner.batches().size(); batchIndex++) {
            BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
            indexInnerRows(batch, 0, batch.length(), batchIndex);
        }
    }

    private void pruneZeroBitwiseOverlapBuildMask(Batch batch, Mask mask)
    {
        Output output = batch.output(joinFilters[0].innerColumn());
        Vector values = output.borrow(Stream.VALUES);
        Vector nulls = output.borrowOrNull(Stream.NULLS);
        if (values instanceof I64Vector longs && (nulls == null || nulls instanceof BooleanVector)) {
            mask.retainConstantComparison(
                    longs.values(),
                    0,
                    Mask.ComparisonOperator.NOT_EQUAL,
                    nulls == null ? null : ((BooleanVector) nulls).values());
            return;
        }
        if (values instanceof I32Vector ints && (nulls == null || nulls instanceof BooleanVector)) {
            mask.retainConstantComparison(
                    ints.values(),
                    0,
                    Mask.ComparisonOperator.NOT_EQUAL,
                    nulls == null ? null : ((BooleanVector) nulls).values());
            return;
        }
        VectorAccess.LongValues longValues = VectorAccess.longValues(values);
        VectorAccess.BooleanValues nullValues = nulls == null ? null : VectorAccess.booleanValues(nulls);
        mask.retainIf(position -> (nullValues == null || !nullValues.value(position)) && longValues.value(position) != 0);
    }

    private boolean canStreamUnusedBuildPayload()
    {
        if (!buildPolicy.streamUnusedPayload() || joinFilters.length != 0) {
            return false;
        }
        for (int outputChannel : outputChannels) {
            if (outputChannel >= outerOutputCount) {
                return false;
            }
        }
        return true;
    }

    private void loadStreamingInner()
    {
        if (streamedInnerLoaded) {
            return;
        }
        streamedInnerLoaded = true;
        int batchIndex = 0;
        while (inner.hasNext()) {
            try (Batch batch = inner.next()) {
                BufferedJoinInput.captureSchema(batch, innerSchema);
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }
                Vector[] joinValues = new Vector[innerJoinColumns.length];
                Vector[] joinNulls = new Vector[innerJoinColumns.length];
                boolean hasNulls = false;
                for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
                    Output output = batch.output(innerJoinColumns[keyIndex]);
                    joinValues[keyIndex] = output.borrow(Stream.VALUES);
                    joinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
                    hasNulls |= joinNulls[keyIndex] != null && !VectorAccess.isAllFalseNulls(joinNulls[keyIndex]);
                }
                validateJoinKeyVectors(joinValues, "build");
                if (joinIndex == null) {
                    if (joinValues.length == 1 && isSingleLongJoinCandidate(joinValues[0])) {
                        joinIndex = new LongJoinIndex(joinIndexPolicy, outputPolicy, executionPolicy, arrayPool, Math.max(16, mask.count()), true, true, true, lazyDuplicateSlotState, false, false, true, buildPolicy.batchSingleLongBuild());
                    }
                    else {
                        joinIndex = createJoinIndex(joinValues, false, true, false);
                    }
                }
                streamedInnerRows += mask.count();
                if (buildKeysViable && streamedInnerRows > dynamicFilterPolicy.buildRowLimit()) {
                    buildKeysAbandoned = true;
                    releaseCollectedBuildKeys();
                    buildKeyValues = null;
                }
                boolean collectKeys = buildKeysViable && !buildKeysAbandoned;
                VectorAccess.LongValues[] buildKeyAccessors = null;
                if (collectKeys) {
                    buildKeyAccessors = new VectorAccess.LongValues[joinValues.length];
                    for (int keyIndex = 0; keyIndex < joinValues.length; keyIndex++) {
                        try {
                            buildKeyAccessors[keyIndex] = VectorAccess.longValues(joinValues[keyIndex]);
                        }
                        catch (IllegalArgumentException _) {
                            buildKeysAbandoned = true;
                            releaseCollectedBuildKeys();
                            buildKeyValues = null;
                            collectKeys = false;
                            break;
                        }
                    }
                }
                if (!collectKeys && joinIndex.addBuildRows(
                        joinValues,
                        joinNulls,
                        hasNulls,
                        mask,
                        batchIndex)) {
                    accountJoinIndex();
                    batchIndex++;
                    continue;
                }
                for (int logicalPosition = 0; logicalPosition < mask.count(); logicalPosition++) {
                    int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
                    long rowReference = JoinRowReference.pack(batchIndex, logicalPosition);
                    if (hasNulls) {
                        joinIndex.add(joinValues, joinNulls, sourcePosition, rowReference);
                    }
                    else {
                        joinIndex.addNoNulls(joinValues, sourcePosition, rowReference);
                    }
                    if (collectKeys) {
                        collectBuildKeys(buildKeyAccessors, joinNulls, hasNulls, sourcePosition);
                        collectKeys = !buildKeysAbandoned;
                    }
                }
                accountJoinIndex();
                batchIndex++;
            }
        }
    }

    private void cacheOuterFilterInputs()
    {
        if (promotedBinaryEqualityFilter) {
            return;
        }
        for (int index = 0; index < joinFilters.length; index++) {
            Output output = currentOuterBatch.output(joinFilters[index].outerColumn());
            currentOuterFilterValues[index] = output.borrow(Stream.VALUES);
            currentOuterFilterNulls[index] = output.borrowOrNull(Stream.NULLS);
            if (currentOuterFilterNulls[index] != null && VectorAccess.isAllFalseNulls(currentOuterFilterNulls[index])) {
                currentOuterFilterNulls[index] = null;
            }
            currentOuterFilterNullAccess[index] = currentOuterFilterNulls[index] == null ? null : VectorAccess.booleanValues(currentOuterFilterNulls[index]);
            currentOuterFilterBaseValues[index] = baseValues(currentOuterFilterValues[index]);
            currentOuterFilterDictionaryDepths[index] = dictionaryDepth(currentOuterFilterValues[index]);
        }
        if (singleEncodedBinaryJoinFilter) {
            fastOuterFilterDictionary = currentOuterFilterValues[0] instanceof DictionaryVector dictionary ? dictionary : null;
            fastOuterFilterBase = (BinaryVector) currentOuterFilterBaseValues[0];
            fastOuterFilterDepth = currentOuterFilterDictionaryDepths[0];
            fastOuterFilterNulls = currentOuterFilterNullAccess[0];
        }
        else if (singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) {
            fastOuterFilterLongs = VectorAccess.longValues(currentOuterFilterValues[0]);
            fastOuterFilterLongArray = currentOuterFilterValues[0] instanceof I64Vector values ? values.values() : null;
            fastOuterFilterNulls = currentOuterFilterNullAccess[0];
        }
    }

    private void cacheInnerFilterInputs()
    {
        if (joinFilters.length == 0 || promotedBinaryEqualityFilter || innerFilterValues != null) {
            return;
        }
        int batchCount = bufferedInner.batches().size();
        innerFilterValues = new Vector[joinFilters.length][batchCount];
        innerFilterNulls = new Vector[joinFilters.length][batchCount];
        innerFilterNullAccess = new VectorAccess.BooleanValues[joinFilters.length][batchCount];
        innerFilterBaseValues = new Vector[joinFilters.length][batchCount];
        innerFilterDictionaryDepths = new int[joinFilters.length][batchCount];
        for (int filterIndex = 0; filterIndex < joinFilters.length; filterIndex++) {
            int column = joinFilters[filterIndex].innerColumn();
            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
                BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
                if (batch.retained()) {
                    Output output = batch.retainedBatch().output(column);
                    innerFilterValues[filterIndex][batchIndex] = output.borrow(Stream.VALUES);
                    innerFilterNulls[filterIndex][batchIndex] = output.borrowOrNull(Stream.NULLS);
                }
                else {
                    Streams streams = batch.columns()[column];
                    innerFilterValues[filterIndex][batchIndex] = streams.values();
                    innerFilterNulls[filterIndex][batchIndex] = streams.getOrNull(Stream.NULLS);
                }
                if (innerFilterNulls[filterIndex][batchIndex] != null && VectorAccess.isAllFalseNulls(innerFilterNulls[filterIndex][batchIndex])) {
                    innerFilterNulls[filterIndex][batchIndex] = null;
                }
                innerFilterNullAccess[filterIndex][batchIndex] = innerFilterNulls[filterIndex][batchIndex] == null
                        ? null
                        : VectorAccess.booleanValues(innerFilterNulls[filterIndex][batchIndex]);
                innerFilterBaseValues[filterIndex][batchIndex] = baseValues(innerFilterValues[filterIndex][batchIndex]);
                innerFilterDictionaryDepths[filterIndex][batchIndex] = dictionaryDepth(innerFilterValues[filterIndex][batchIndex]);
            }
        }
        if (singleEncodedBinaryJoinFilter && batchCount == 1) {
            fastInnerFilterBatch = bufferedInner.batches().getFirst();
            fastInnerFilterPositions = fastInnerFilterBatch.positions();
            fastInnerFilterDictionary = innerFilterValues[0][0] instanceof DictionaryVector dictionary ? dictionary : null;
            fastInnerFilterBase = (BinaryVector) innerFilterBaseValues[0][0];
            fastInnerFilterDepth = innerFilterDictionaryDepths[0][0];
            fastInnerFilterNulls = innerFilterNullAccess[0][0];
        }
        else if ((singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) && batchCount == 1) {
            fastInnerFilterBatch = bufferedInner.batches().getFirst();
            fastInnerFilterPositions = fastInnerFilterBatch.positions();
            fastInnerFilterLongs = VectorAccess.longValues(innerFilterValues[0][0]);
            fastInnerFilterLongArray = innerFilterValues[0][0] instanceof I64Vector values ? values.values() : null;
            fastInnerFilterNulls = innerFilterNullAccess[0][0];
        }
        else if (singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) {
            directInnerFilterLongs = new VectorAccess.LongValues[batchCount];
            directInnerFilterLongArrays = new long[batchCount][];
            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
                Vector values = innerFilterValues[0][batchIndex];
                directInnerFilterLongs[batchIndex] = VectorAccess.longValues(values);
                directInnerFilterLongArrays[batchIndex] = values instanceof I64Vector longs ? longs.values() : null;
            }
        }
    }

    private boolean passesJoinFilters(int outerPosition, long rowReference, int matchIndex)
    {
        if (joinFilters.length == 0) {
            return true;
        }
        if (fastInnerFilterBatch != null) {
            if ((singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) &&
                    fastInnerOrderedIntFilterValues != null &&
                    currentMatches instanceof ChainLongList chain) {
                int storageIndex = chain.storageIndex(matchIndex);
                if (storageIndex >= 0) {
                    boolean outerNull = filterPolicy.cacheCurrentOuterValue()
                            ? currentFastOuterFilterNull
                            : fastOuterFilterNulls != null && fastOuterFilterNulls.value(outerPosition);
                    long outerValue = filterPolicy.cacheCurrentOuterValue()
                            ? currentFastOuterFilterLong
                            : fastOuterFilterLongArray == null
                                    ? fastOuterFilterLongs.value(outerPosition)
                                    : fastOuterFilterLongArray[outerPosition];
                    long innerValue = fastInnerOrderedIntFilterValues[storageIndex];
                    return !outerNull && (singleLongNotEqualJoinFilter
                            ? outerValue != innerValue
                            : (outerValue & innerValue) != 0);
                }
            }
            return singleEncodedBinaryJoinFilter
                    ? passesFastBinaryJoinFilter(outerPosition, JoinRowReference.position(rowReference))
                    : passesFastLongJoinFilter(outerPosition, JoinRowReference.position(rowReference));
        }
        int batchIndex = JoinRowReference.batchIndex(rowReference);
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
        int innerPosition = innerBatch.sourcePosition(JoinRowReference.position(rowReference));
        if (singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) {
            boolean outerNull = filterPolicy.cacheCurrentOuterValue()
                    ? currentFastOuterFilterNull
                    : fastOuterFilterNulls != null && fastOuterFilterNulls.value(outerPosition);
            if (outerNull ||
                    (innerFilterNullAccess[0][batchIndex] != null && innerFilterNullAccess[0][batchIndex].value(innerPosition))) {
                return false;
            }
            long outerValue = filterPolicy.cacheCurrentOuterValue()
                    ? currentFastOuterFilterLong
                    : fastOuterFilterLongArray == null
                            ? fastOuterFilterLongs.value(outerPosition)
                            : fastOuterFilterLongArray[outerPosition];
            long[] innerArray = directInnerFilterLongArrays[batchIndex];
            long innerValue = innerArray == null
                    ? directInnerFilterLongs[batchIndex].value(innerPosition)
                    : innerArray[innerPosition];
            return singleLongNotEqualJoinFilter ? outerValue != innerValue : (outerValue & innerValue) != 0;
        }
        if (joinFilters.length == 1 && joinFilters[0].encodedBinaryEquals()) {
            if ((currentOuterFilterNulls[0] != null && VectorAccess.isNull(currentOuterFilterNulls[0], outerPosition)) ||
                    (innerFilterNulls[0][batchIndex] != null && VectorAccess.isNull(innerFilterNulls[0][batchIndex], innerPosition))) {
                return false;
            }
            BinaryVector outerBase = (BinaryVector) currentOuterFilterBaseValues[0];
            BinaryVector innerBase = (BinaryVector) innerFilterBaseValues[0][batchIndex];
            int outerBasePosition = basePosition(currentOuterFilterValues[0], outerPosition, currentOuterFilterDictionaryDepths[0]);
            int innerBasePosition = basePosition(innerFilterValues[0][batchIndex], innerPosition, innerFilterDictionaryDepths[0][batchIndex]);
            int length = outerBase.length(outerBasePosition);
            return length == innerBase.length(innerBasePosition) && OperatorVectorSupport.binaryEquals(
                    outerBase.data(), outerBase.startOffset(outerBasePosition),
                    innerBase.data(), innerBase.startOffset(innerBasePosition),
                    length);
        }
        for (int index = 0; index < joinFilters.length; index++) {
            if ((currentOuterFilterNullAccess[index] != null && currentOuterFilterNullAccess[index].value(outerPosition)) ||
                    (innerFilterNullAccess[index][batchIndex] != null && innerFilterNullAccess[index][batchIndex].value(innerPosition))) {
                return false;
            }
            boolean matches = joinFilters[index].encodedBinaryEquals()
                    ? OperatorVectorSupport.binaryEquals(
                            currentOuterFilterBaseValues[index],
                            basePosition(currentOuterFilterValues[index], outerPosition, currentOuterFilterDictionaryDepths[index]),
                            innerFilterBaseValues[index][batchIndex],
                            basePosition(innerFilterValues[index][batchIndex], innerPosition, innerFilterDictionaryDepths[index][batchIndex]))
                    : joinFilters[index].function().test(currentOuterFilterValues[index], outerPosition, innerFilterValues[index][batchIndex], innerPosition);
            if (!matches) {
                return false;
            }
        }
        return true;
    }

    private boolean passesBinaryJoinFilter(int outerPosition, long rowReference)
    {
        if (fastInnerFilterBatch != null) {
            return passesFastBinaryJoinFilter(outerPosition, JoinRowReference.position(rowReference));
        }
        int batchIndex = JoinRowReference.batchIndex(rowReference);
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
        int innerPosition = innerBatch.sourcePosition(JoinRowReference.position(rowReference));
        if ((currentOuterFilterNulls[0] != null && VectorAccess.isNull(currentOuterFilterNulls[0], outerPosition)) ||
                (innerFilterNulls[0][batchIndex] != null && VectorAccess.isNull(innerFilterNulls[0][batchIndex], innerPosition))) {
            return false;
        }
        BinaryVector outerBase = (BinaryVector) currentOuterFilterBaseValues[0];
        BinaryVector innerBase = (BinaryVector) innerFilterBaseValues[0][batchIndex];
        int outerBasePosition = basePosition(currentOuterFilterValues[0], outerPosition, currentOuterFilterDictionaryDepths[0]);
        int innerBasePosition = basePosition(innerFilterValues[0][batchIndex], innerPosition, innerFilterDictionaryDepths[0][batchIndex]);
        int length = outerBase.length(outerBasePosition);
        return length == innerBase.length(innerBasePosition) && OperatorVectorSupport.binaryEquals(
                outerBase.data(), outerBase.startOffset(outerBasePosition),
                innerBase.data(), innerBase.startOffset(innerBasePosition),
                length);
    }

    private boolean passesFastBinaryJoinFilter(int outerPosition, int innerLogicalPosition)
    {
        int innerPosition = fastInnerFilterPositions == null ? innerLogicalPosition : fastInnerFilterPositions[innerLogicalPosition];
        boolean outerNull = filterPolicy.cacheCurrentOuterValue()
                ? currentFastOuterFilterNull
                : fastOuterFilterNulls != null && fastOuterFilterNulls.value(outerPosition);
        if (outerNull ||
                (fastInnerFilterNulls != null && fastInnerFilterNulls.value(innerPosition))) {
            return false;
        }
        int outerBasePosition = filterPolicy.cacheCurrentOuterValue()
                ? currentFastOuterFilterBasePosition
                : fastOuterFilterDictionary == null
                        ? outerPosition
                        : fastOuterFilterDictionary.basePosition(outerPosition, fastOuterFilterDepth);
        int innerBasePosition = fastInnerFilterDictionary == null
                ? innerPosition
                : fastInnerFilterDictionary.basePosition(innerPosition, fastInnerFilterDepth);
        int length = fastOuterFilterBase.length(outerBasePosition);
        return length == fastInnerFilterBase.length(innerBasePosition) && OperatorVectorSupport.binaryEquals(
                fastOuterFilterBase.data(), fastOuterFilterBase.startOffset(outerBasePosition),
                fastInnerFilterBase.data(), fastInnerFilterBase.startOffset(innerBasePosition),
                length);
    }

    private boolean passesFastLongJoinFilter(int outerPosition, int innerLogicalPosition)
    {
        int innerPosition = fastInnerFilterPositions == null ? innerLogicalPosition : fastInnerFilterPositions[innerLogicalPosition];
        boolean outerNull = filterPolicy.cacheCurrentOuterValue()
                ? currentFastOuterFilterNull
                : fastOuterFilterNulls != null && fastOuterFilterNulls.value(outerPosition);
        if (outerNull ||
                (fastInnerFilterNulls != null && fastInnerFilterNulls.value(innerPosition))) {
            return false;
        }
        long outerValue = filterPolicy.cacheCurrentOuterValue()
                ? currentFastOuterFilterLong
                : fastOuterFilterLongArray == null
                        ? fastOuterFilterLongs.value(outerPosition)
                        : fastOuterFilterLongArray[outerPosition];
        long innerValue = fastInnerFilterLongArray == null ? fastInnerFilterLongs.value(innerPosition) : fastInnerFilterLongArray[innerPosition];
        return singleLongNotEqualJoinFilter ? outerValue != innerValue : (outerValue & innerValue) != 0;
    }

    private void cacheCurrentOuterFilterValue()
    {
        if (!filterPolicy.cacheCurrentOuterValue() ||
                (fastInnerFilterBatch == null && !singleLongNotEqualJoinFilter && !singleLongBitwiseOverlapJoinFilter)) {
            return;
        }
        currentFastOuterFilterNull = fastOuterFilterNulls != null && fastOuterFilterNulls.value(currentOuterPosition);
        if (currentFastOuterFilterNull) {
            return;
        }
        if (singleLongNotEqualJoinFilter || singleLongBitwiseOverlapJoinFilter) {
            currentFastOuterFilterLong = fastOuterFilterLongArray == null
                    ? fastOuterFilterLongs.value(currentOuterPosition)
                    : fastOuterFilterLongArray[currentOuterPosition];
        }
        else {
            currentFastOuterFilterBasePosition = fastOuterFilterDictionary == null
                    ? currentOuterPosition
                    : fastOuterFilterDictionary.basePosition(currentOuterPosition, fastOuterFilterDepth);
        }
    }

    private void cacheOrderedInnerFilterPayload()
    {
        if (fastInnerOrderedFilterAttempted ||
                !filterPolicy.orderedLongPayload() ||
                (!singleLongNotEqualJoinFilter && !singleLongBitwiseOverlapJoinFilter) ||
                fastInnerFilterBatch == null ||
                fastInnerFilterNulls != null ||
                joinIndex == null) {
            return;
        }
        fastInnerOrderedFilterAttempted = true;
        fastInnerOrderedIntFilterValues = joinIndex.buildOrderedIntPayload(
                fastInnerFilterLongs,
                fastInnerFilterLongArray,
                fastInnerFilterPositions);
    }

    private static Vector baseValues(Vector vector)
    {
        return vector instanceof DictionaryVector dictionary ? dictionary.baseValues() : vector;
    }

    private static int dictionaryDepth(Vector vector)
    {
        return vector instanceof DictionaryVector dictionary ? dictionary.dictionaryDepth() : 0;
    }

    private static int basePosition(Vector vector, int position, int dictionaryDepth)
    {
        return vector instanceof DictionaryVector dictionary ? dictionary.basePosition(position, dictionaryDepth) : position;
    }

    private void indexInnerRows(BufferedJoinInput.InnerBatch batch, int startPosition, int length, int batchIndex)
    {
        int effectiveJoinKeyCount = innerJoinColumns.length + (promotedBinaryEqualityFilter ? 1 : 0);
        Vector[] joinValues = new Vector[effectiveJoinKeyCount];
        Vector[] joinNulls = new Vector[effectiveJoinKeyCount];
        boolean hasNulls = false;
        for (int keyIndex = 0; keyIndex < innerJoinColumns.length; keyIndex++) {
            if (batch.retained()) {
                Output output = batch.retainedBatch().output(innerJoinColumns[keyIndex]);
                joinValues[keyIndex] = output.borrow(Stream.VALUES);
                joinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = batch.columns()[innerJoinColumns[keyIndex]];
                joinValues[keyIndex] = streams.values();
                joinNulls[keyIndex] = streams.getOrNull(Stream.NULLS);
            }
            // A present NULLS stream that is provably all-false (e.g. a non-nullable key surfaced through a
            // prior join's DictionaryVector/ConcatenatedBooleanVector) carries no nulls; treat the key as
            // null-free so the index build and probe skip per-row null reads.
            hasNulls = hasNulls || (joinNulls[keyIndex] != null && !VectorAccess.isAllFalseNulls(joinNulls[keyIndex]));
        }
        if (promotedBinaryEqualityFilter) {
            int keyIndex = innerJoinColumns.length;
            int column = joinFilters[0].innerColumn();
            if (batch.retained()) {
                Output output = batch.retainedBatch().output(column);
                joinValues[keyIndex] = output.borrow(Stream.VALUES);
                joinNulls[keyIndex] = output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = batch.columns()[column];
                joinValues[keyIndex] = streams.values();
                joinNulls[keyIndex] = streams.getOrNull(Stream.NULLS);
            }
            hasNulls = hasNulls || (joinNulls[keyIndex] != null && !VectorAccess.isAllFalseNulls(joinNulls[keyIndex]));
        }
        validateJoinKeyVectors(joinValues, "build");
        if (joinIndex == null) {
            int expectedRows = expectedInnerRowCount();
            boolean keyOnlyBuild = innerSchema.length == innerJoinColumns.length;
            joinIndex = createJoinIndex(
                    joinValues,
                    genericJoinIndexes.shouldCapInitialHash(batch, joinValues, expectedRows, keyOnlyBuild),
                    genericJoinIndexes.shouldUseGroupedLongHash(batch, joinValues, expectedRows),
                    genericJoinIndexes.shouldUseDirectRangeBuild(batch, joinValues, expectedRows));
        }

        boolean collectKeys = buildKeysViable && !buildKeysAbandoned;
        VectorAccess.LongValues[] buildKeyAccessors = null;
        if (collectKeys) {
            if (buildKeyValues == null) {
                buildKeyValues = new LongOpenHashSet[innerJoinColumns.length];
                buildKeyColumnAbandoned = new boolean[innerJoinColumns.length];
                initializeBuildKeyRanges(innerJoinColumns.length);
            }
            buildKeyAccessors = new VectorAccess.LongValues[innerJoinColumns.length];
            for (int column = 0; column < innerJoinColumns.length; column++) {
                try {
                    buildKeyAccessors[column] = VectorAccess.longValues(joinValues[column]);
                }
                catch (IllegalArgumentException _) {
                    buildKeyColumnAbandoned[column] = true;
                    buildKeyValues[column] = null;
                }
            }
            buildKeysAbandoned = true;
            for (boolean abandoned : buildKeyColumnAbandoned) {
                buildKeysAbandoned &= abandoned;
            }
            collectKeys = !buildKeysAbandoned;
        }
        if (!collectKeys && joinIndex.addBuildRows(
                joinValues,
                joinNulls,
                hasNulls,
                batch,
                startPosition,
                length,
                batchIndex)) {
            accountJoinIndex();
            return;
        }
        for (int position = startPosition; position < startPosition + length; position++) {
            int sourcePosition = batch.sourcePosition(position);
            if (hasNulls) {
                joinIndex.add(joinValues, joinNulls, sourcePosition, JoinRowReference.pack(batchIndex, position));
            }
            else {
                joinIndex.addNoNulls(joinValues, sourcePosition, JoinRowReference.pack(batchIndex, position));
            }
            if (collectKeys) {
                collectBuildKeys(buildKeyAccessors, joinNulls, hasNulls, sourcePosition);
                collectKeys = !buildKeysAbandoned;
            }
        }
        accountJoinIndex();
    }

    private void accountJoinIndex()
    {
        if (joinIndex != null) {
            allocator.setRetainedBytes(indexAllocationContext, joinIndex, joinIndex.retainedBytes());
        }
    }

    private void cacheOuterJoinInputs()
    {
        currentOuterJoinHasNulls = false;
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Output output = currentOuterBatch.output(outerJoinColumns[keyIndex]);
            long start = System.nanoTime();
            currentOuterJoinValues[keyIndex] = output.borrow(Stream.VALUES);
            Vector keyNulls = output.isKnownAllFalse(Stream.NULLS) ? null : output.borrowOrNull(Stream.NULLS);
            // A present-but-all-false NULLS stream (e.g. a non-nullable key surfaced through a prior join's
            // DictionaryVector/ConcatenatedBooleanVector) carries no nulls: drop it so the probe takes the
            // null-free match path instead of reading a per-row null (a binary search for that shape).
            if (keyNulls != null && VectorAccess.isAllFalseNulls(keyNulls)) {
                keyNulls = null;
            }
            currentOuterJoinNulls[keyIndex] = keyNulls;
            currentOuterJoinHasNulls = currentOuterJoinHasNulls || keyNulls != null;
        }
        if (promotedBinaryEqualityFilter) {
            int keyIndex = outerJoinColumns.length;
            Output output = currentOuterBatch.output(joinFilters[0].outerColumn());
            currentOuterJoinValues[keyIndex] = output.borrow(Stream.VALUES);
            Vector keyNulls = output.isKnownAllFalse(Stream.NULLS) ? null : output.borrowOrNull(Stream.NULLS);
            if (keyNulls != null && VectorAccess.isAllFalseNulls(keyNulls)) {
                keyNulls = null;
            }
            currentOuterJoinNulls[keyIndex] = keyNulls;
            currentOuterJoinHasNulls = currentOuterJoinHasNulls || keyNulls != null;
        }
        validateJoinKeyVectors(currentOuterJoinValues, "probe");
    }

    private void captureOuterSchemaIfAvailable()
    {
        while (probeSource.hasNext()) {
            try (Batch batch = probeSource.next()) {
                BufferedJoinInput.captureSchema(batch, outerSchema);
            }
        }
    }

    private JoinIndex createJoinIndex(
            Vector[] joinValues,
            boolean capInitialHash,
            boolean groupedLongHashTable,
            boolean directRangeBuild)
    {
        int expectedSize = expectedInnerRowCount();
        if (joinIndexPolicy.debugJoinIndex()) {
            System.err.printf("[join-index] expected=%d fields=%d shape=%s%n",
                    expectedSize,
                    joinValues.length,
                    java.util.Arrays.stream(joinValues).map(value -> value.getClass().getSimpleName() + '(' + value.length() + ')').toList());
        }
        if (!allowsLegacyKeyShortcuts) {
            return genericJoinIndexes.structural(structuralKeyKernels);
        }
        if (joinValues.length == 1 && isSingleLongJoinCandidate(joinValues[0])) {
            // When no build output or residual filter can observe a physical row, preserve duplicate multiplicity
            // without retaining each otherwise-unused reference. Payload-bearing joins retain exact references.
            return new LongJoinIndex(
                    joinIndexPolicy,
                    outputPolicy,
                    executionPolicy,
                    arrayPool,
                    expectedSize,
                    innerSchema.length == innerJoinColumns.length,
                    capInitialHash,
                    groupedLongHashTable,
                    lazyDuplicateSlotState,
                    implicitSequentialBuildRowReferences,
                    directRangeBuild,
                    canStreamUnusedBuildPayload(),
                    buildPolicy.batchSingleLongBuild());
        }
        return genericJoinIndexes.create(
                joinValues,
                flatJoinKeyTypes,
                arrayPool,
                expectedSize,
                canStreamUnusedBuildPayload(),
                capInitialHash);
    }

    @Override
    public void constrain(Mask mask)
    {
        currentOutputMask = mask;
    }

    @Override
    public void close()
    {
        if (currentOuterBatch != null) {
            currentOuterBatch.close();
            currentOuterBatch = null;
        }
        probeSource.close();
        inner.close();
        if (joinIndex != null) {
            if (preparedBuild == null) {
                joinIndex.releaseBuffers();
            }
            else {
                joinIndex.releaseProbeBuffers();
            }
            joinIndex = null;
        }
        allocator.release(indexAllocationContext);
        bufferedInner.releaseBuffers();
        for (BuildDictionary dictionary : buildDictionaries.values()) {
            releaseBuildDictionaryIds(dictionary.idByPosition());
        }
        buildDictionaries.clear();
        directInnerNullAccess = null;
        directInnerNullAccessResolved = null;
        arrayPool.release(fastInnerOrderedIntFilterValues);
        fastInnerOrderedIntFilterValues = null;
        arrayPool.release(buildKeyMins);
        buildKeyMins = null;
        arrayPool.release(buildKeyMaxs);
        buildKeyMaxs = null;
        buildKeyValues = null;
        releaseCollectedBuildKeys();
        arrayPool.release(matchedOuterPositions);
        matchedOuterPositions = null;
        typeVectorAllocator.close();
        allocator.release(buildAllocationContext);
        if (executionPolicy.poolScratch() && !joinScratchReleased) {
            joinScratchReleased = true;
            arrayPool.retain(JoinScratch.class, executionPolicy.maxBatchRows(), joinScratch.retainedBytes(), joinScratch);
        }
    }

    HashJoinBuild prepareBuild()
    {
        if (preparedBuild != null) {
            throw new IllegalStateException("Cannot prepare an already prepared hash join");
        }
        loadInnerIfNecessary();
        if (!(joinIndex instanceof LongJoinIndex) &&
                !(joinIndex instanceof LongPairJoinIndex) &&
                !(joinIndex instanceof LongTripleJoinIndex) &&
                !(joinIndex instanceof StructuralHashJoinIndex) &&
                !(joinIndex instanceof FlatJoinIndex)) {
            return null;
        }
        if (joinIndex instanceof LongJoinIndex longJoinIndex) {
            longJoinIndex.finalizeForProbe(executionPolicy.maxBatchRows());
        }
        if (joinIndex instanceof LongPairJoinIndex longPairJoinIndex) {
            longPairJoinIndex.finalizeForProbe(executionPolicy.maxBatchRows());
        }
        return new HashJoinBuild(this);
    }

    JoinIndex newPreparedProbeIndex()
    {
        if (joinIndex instanceof LongJoinIndex longJoinIndex && longJoinIndex.finalized) {
            return new LongJoinIndex(longJoinIndex);
        }
        if (joinIndex instanceof LongPairJoinIndex longPairJoinIndex) {
            return longPairJoinIndex.newProbeView();
        }
        if (joinIndex instanceof LongTripleJoinIndex longTripleJoinIndex) {
            return longTripleJoinIndex.newProbeView();
        }
        if (joinIndex instanceof StructuralHashJoinIndex structuralHashJoinIndex) {
            return structuralHashJoinIndex.newProbeView();
        }
        if (joinIndex instanceof FlatJoinIndex flatJoinIndex) {
            return flatJoinIndex.newProbeView();
        }
        throw new IllegalStateException("Hash join build is not prepared");
    }

    BufferedJoinInput bufferedInner()
    {
        return bufferedInner;
    }

    public HashJoinOperator withProfileName(String profileName)
    {
        this.profileName = profileName;
        return this;
    }

    /**
     * Selects and orders the join's public outputs using physical concatenated column ordinals
     * ({@code outer columns} followed by {@code inner columns}). Join keys and residual columns remain available to
     * execution even when omitted. This is the join-output-layout operation exposed by native plan builders, and
     * avoids paying for a separate projection operator or materializing discarded payloads.
     */
    public HashJoinOperator withOutputs(int... outputChannels)
    {
        requireNonNull(outputChannels, "outputChannels is null");
        int[] selected = outputChannels.clone();
        for (int outputChannel : selected) {
            if (outputChannel < 0 || outputChannel >= totalOutputCount) {
                throw new IllegalArgumentException("Join output column is out of bounds: " + outputChannel);
            }
        }
        this.outputChannels = selected;
        this.outputSchema = selectOutputs(fullOutputSchema, selected);
        return this;
    }

    /**
     * Emits at most one admitted build match for each probe row. This is valid when the physical
     * plan is insensitive to duplicate build matches and no projected build payload can distinguish
     * those matches.
     */
    public HashJoinOperator withOutputSingleMatch()
    {
        this.outputSingleMatch = true;
        return this;
    }

    /**
     * Bounds lazy output mapping depth at this physical join boundary. A depth of zero always composes an encoded
     * probe-side mapping; larger values preserve shallower nested mappings and compose only once the source reaches
     * the limit. Composition is shared across columns with the same mapping chain.
     */
    public HashJoinOperator withComposeEncodedOuterDictionaryDepth(int depth)
    {
        if (depth < 0) {
            throw new IllegalArgumentException("depth is negative");
        }
        this.composeEncodedOuterDictionaryDepth = depth;
        return this;
    }

    /**
     * Starts a single-long build index without duplicate-chain tail/count arrays. If a duplicate is observed, the
     * index promotes exactly by reconstructing that state from occupied slots. This is a safe physical-plan hint for
     * expected-unique build keys; it never changes duplicate semantics.
     */
    public HashJoinOperator withLazyDuplicateSlotState()
    {
        this.lazyDuplicateSlotState = true;
        return this;
    }

    /**
     * Writes a non-retained build with an exact, bounded cardinality directly into its final coalesced buffers.
     * Source batches are still closed as soon as their selected rows have been copied, so this removes an
     * intermediate copy without extending the lifetime of reader-owned buffers.
     */
    public HashJoinOperator withDirectExactBuildCoalescing()
    {
        bufferedInner.enableDirectExactCoalesce();
        implicitSequentialBuildRowReferences = executionPolicy.implicitSequentialBuildRowReferences();
        return this;
    }

    /**
     * Writes a build expected to fit the bounded coalescing limit directly into one final buffer. If the estimate is
     * wrong, the full bounded page is emitted and subsequent rows continue in another page, preserving exact output
     * without exceeding the configured capacity. Copied source batches are still closed promptly.
     */
    public HashJoinOperator withDirectBoundedBuildCoalescing()
    {
        implicitSequentialBuildRowReferences = bufferedInner.enableDirectBoundedCoalesce() && executionPolicy.implicitSequentialBuildRowReferences();
        return this;
    }

    private Output resultOutput(int outputIndex)
    {
        if (currentOutputCount == 0) {
            Streams schema = outputSchema(outputIndex);
            if (schema == null) {
                return new Output(Set.of(), stream -> {
                    throw new IllegalArgumentException("Output does not expose stream: " + stream);
                });
            }
            Streams empty = buffers.emptyLike(schema);
            return new Output(
                    empty.streams(),
                    empty::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }

        Set<Stream> streams = outputIndex < outerOutputCount
                ? currentOuterBatch.output(outputIndex).streams()
                : innerOutputStreams(outputIndex - outerOutputCount);
        Set<Stream> knownAllFalseStreams = resultKnownAllFalseStreams(outputIndex, streams);
        return new Output(
                streams,
                stream -> {
                    // A known-false side stream is independent of the corresponding value. Check it before the
                    // whole-column fallback so a NULLS-only consumer does not accidentally materialize VALUES.
                    if (knownAllFalseStreams.contains(stream)) {
                        return allFalseBooleanStream(currentOutputCount);
                    }
                    if (stream == Stream.NULLS && canResolveInnerNullStreamDirectly(outputIndex)) {
                        return materializeInnerNullStreamDirectly(outputIndex - outerOutputCount);
                    }
                    Streams materialized = materializeOutput(outputIndex);
                    if (materialized.has(stream)) {
                        return materialized.get(stream);
                    }
                    throw new IllegalArgumentException("Output does not expose stream: " + stream);
                },
                (stream, vector) -> allocator.transfer(allocationContext, vector),
                (stream, vector) -> allocator.release(allocationContext, vector),
                null,
                null)
                .withKnownAllFalse(knownAllFalseStreams);
    }

    private boolean canResolveInnerNullStreamDirectly(int outputIndex)
    {
        if (!outputPolicy.directOuterJoinNullStream() || !probeOuterJoin || outputIndex < outerOutputCount) {
            return false;
        }
        return bufferedInner.outputStreams(outputIndex - outerOutputCount) != null;
    }

    private Vector materializeInnerNullStreamDirectly(int innerOutputIndex)
    {
        long start = System.nanoTime();
        if (outputPolicy.rleAllUnmatchedOuterJoinOutput() && allRowsHaveNoMatch()) {
            Vector nulls = allTrueBooleanStream(currentOutputCount);
            if (materializationListener != null) {
                materializationListener.record(profileName != null ? profileName : "hash_join", -1, Streams.of(Stream.NULLS, nulls), currentOutputCount, System.nanoTime() - start);
            }
            return nulls;
        }
        BooleanVector nulls = allocator.allocate(allocationContext, BooleanVector.class, currentOutputCount, BooleanVector::new);
        boolean[] values = nulls.values();
        // Allocator storage is recycled. Clear the visible range before marking the unmatched probe rows.
        Arrays.fill(values, 0, currentOutputCount, false);
        if (!outputInnerLogicalPositionsReady) {
            for (int position = 0; position < currentOutputCount; position++) {
                long rowReference = outputInnerRows[position];
                if (rowReference == NO_MATCH_ROW_REFERENCE) {
                    values[position] = true;
                    continue;
                }
                int innerBatchIndex = JoinRowReference.batchIndex(rowReference);
                VectorAccess.BooleanValues sourceNulls = directInnerNullAccess(innerOutputIndex, innerBatchIndex);
                if (sourceNulls != null) {
                    BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
                    int logicalPosition = JoinRowReference.position(rowReference);
                    int sourcePosition = innerBatch.retained() ? innerBatch.sourcePosition(logicalPosition) : logicalPosition;
                    values[position] = sourceNulls.value(sourcePosition);
                }
            }
        }
        if (materializationListener != null) {
            materializationListener.record(profileName != null ? profileName : "hash_join", -1, Streams.of(Stream.NULLS, nulls), currentOutputCount, System.nanoTime() - start);
        }
        return nulls;
    }

    private VectorAccess.BooleanValues directInnerNullAccess(int innerOutputIndex, int innerBatchIndex)
    {
        if (directInnerNullAccess == null) {
            directInnerNullAccess = new VectorAccess.BooleanValues[innerOutputCount][];
            directInnerNullAccessResolved = new boolean[innerOutputCount][];
        }
        int batchCount = bufferedInner.batches().size();
        if (directInnerNullAccess[innerOutputIndex] == null) {
            directInnerNullAccess[innerOutputIndex] = new VectorAccess.BooleanValues[batchCount];
            directInnerNullAccessResolved[innerOutputIndex] = new boolean[batchCount];
        }
        if (!directInnerNullAccessResolved[innerOutputIndex][innerBatchIndex]) {
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            Vector sourceNulls;
            if (innerBatch.retained()) {
                Output output = innerBatch.retainedBatch().output(innerOutputIndex);
                sourceNulls = output.isKnownAllFalse(Stream.NULLS) ? null : output.borrowOrNull(Stream.NULLS);
            }
            else {
                Streams streams = innerBatch.columns()[innerOutputIndex];
                sourceNulls = streams == null ? null : streams.getOrNull(Stream.NULLS);
            }
            if (sourceNulls != null && !VectorAccess.isAllFalseNulls(sourceNulls)) {
                directInnerNullAccess[innerOutputIndex][innerBatchIndex] = VectorAccess.booleanValues(sourceNulls);
            }
            directInnerNullAccessResolved[innerOutputIndex][innerBatchIndex] = true;
        }
        return directInnerNullAccess[innerOutputIndex][innerBatchIndex];
    }

    private Streams outputSchema(int outputIndex)
    {
        if (outputIndex < outerOutputCount) {
            Streams schema = outerSchema[outputIndex];
            if (schema != null) {
                return schema;
            }
            if (currentOuterBatch != null) {
                Streams.Builder streams = Streams.builder();
                Output output = currentOuterBatch.output(outputIndex);
                if (output.hasValues()) {
                    streams.put(Stream.VALUES, output.borrow(Stream.VALUES));
                }
                if (output.hasNulls()) {
                    streams.put(Stream.NULLS, new BooleanVector(0));
                }
                if (output.hasErrors()) {
                    streams.put(Stream.ERRORS, new BooleanVector(0));
                }
                return streams.build();
            }
            return null;
        }
        Streams schema = innerSchema[outputIndex - outerOutputCount];
        if (schema != null) {
            return probeOuterJoin ? ensureNullStream(schema) : schema;
        }
        Streams bufferedSchema = bufferedInner.outputSchema(outputIndex - outerOutputCount);
        return probeOuterJoin && bufferedSchema != null ? ensureNullStream(bufferedSchema) : bufferedSchema;
    }

    private static void copySchema(Streams[] source, Streams[] target)
    {
        for (int index = 0; index < target.length; index++) {
            if (target[index] == null && source[index] != null) {
                target[index] = source[index];
            }
        }
    }

    private int[] innerSourcePositions()
    {
        if (joinScratch.outputInnerSourcePositions == null) {
            joinScratch.outputInnerSourcePositions = new int[executionPolicy.maxBatchRows()];
            accountJoinScratch();
        }
        return joinScratch.outputInnerSourcePositions;
    }

    private int[] innerUniqueSourcePositions()
    {
        if (joinScratch.outputInnerUniqueSourcePositions == null) {
            joinScratch.outputInnerUniqueSourcePositions = new int[executionPolicy.maxBatchRows()];
            accountJoinScratch();
        }
        return joinScratch.outputInnerUniqueSourcePositions;
    }

    private int[] retainedInnerMaskPositionsScratch()
    {
        if (joinScratch.retainedInnerMaskPositionsScratch == null) {
            joinScratch.retainedInnerMaskPositionsScratch = new int[executionPolicy.maxBatchRows()];
            accountJoinScratch();
        }
        return joinScratch.retainedInnerMaskPositionsScratch;
    }

    private void accountJoinScratch()
    {
        allocator.setRetainedBytes(allocationContext, joinScratch, joinScratch.retainedBytes());
    }

    private Streams materializeOutput(int outputIndex)
    {
        Streams existing = currentOutputs[outputIndex];
        if (existing != null) {
            return existing;
        }
        long start = System.nanoTime();
        Streams materialized = outputIndex < outerOutputCount
                ? materializeOuterOutput(outputIndex)
                : materializeInnerOutput(outputIndex - outerOutputCount);
        if (materializationListener != null) {
            materializationListener.record(profileName != null ? profileName : "hash_join", outputIndex, materialized, currentOutputCount, System.nanoTime() - start);
        }
        currentOutputs[outputIndex] = materialized;
        return materialized;
    }

    private Streams materializeOuterOutput(int outputIndex)
    {
        Output sourceOutput = currentOuterBatch.output(outputIndex);
        if (!outerSupportsReborrow) {
            // Outer cannot satisfy a constrained re-borrow (e.g. a Parquet-backed subplan or a join
            // result): keep the baseline dictionary-wrap, which borrows the live outer column once
            // and indexes it by the matched output positions. No constraint is pushed and no deferral
            // happens past the source's advance.
            return wrapOuterOutput(sourceOutput);
        }

        // Outer can satisfy a constrained re-borrow: narrow it to the matched rows so a lazy
        // projected payload computes only the rows the join emits, then flat-copy the matched
        // positions into a dense vector indexable directly by output position. Materialization stays
        // lazy: it only runs when an outer stream is borrowed.
        constrainOuterIfNecessary();
        if (currentOutputMask.all()) {
            return buffers.copyPositions(sourceOutput, null, outputOuterPositions, currentOutputCount, 0, currentOutputCount);
        }

        Streams result = null;
        for (int index = 0; index < currentOutputMask.count(); index++) {
            int outputPosition = currentOutputMask.position(index);
            result = buffers.copySinglePosition(sourceOutput, result, currentOutputCount, outputPosition, outputOuterPositions[outputPosition]);
        }
        return result == null ? buffers.emptyLike(outputSchema(outputIndex)) : result;
    }

    private Streams wrapOuterOutput(Output sourceOutput)
    {
        if (sourceOutput.isValuesOnly()) {
            return Streams.ofValues(allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.VALUES))));
        }
        Streams.Builder streams = Streams.builder();
        if (sourceOutput.hasValues()) {
            streams.put(Stream.VALUES, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.VALUES))));
        }
        if (sourceOutput.hasNulls() && !sourceOutput.isKnownAllFalse(Stream.NULLS)) {
            streams.put(Stream.NULLS, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.NULLS))));
        }
        if (sourceOutput.hasErrors() && !sourceOutput.isKnownAllFalse(Stream.ERRORS)) {
            streams.put(Stream.ERRORS, allocator.adopt(allocationContext, buildOuterDictionaryStream(sourceOutput.borrow(Stream.ERRORS))));
        }
        return streams.build();
    }

    private int[] outerDictionaryIds()
    {
        if (currentOuterDictionaryIds == null) {
            // Result vectors can be taken from the batch and outlive this call to next(). The join's position
            // arrays are reusable operator scratch and are overwritten while producing the following batch, so a
            // dictionary mapping must own an immutable snapshot even when the current output fills the scratch.
            currentOuterDictionaryIds = Arrays.copyOf(outputOuterPositions, currentOutputCount);
        }
        return currentOuterDictionaryIds;
    }

    private Vector buildOuterDictionaryStream(Vector source)
    {
        if (source instanceof DictionaryVector || source instanceof org.weakref.nitro.data.RleVector) {
            int composeDepth = composeEncodedOuterDictionaryDepth;
            if (composeDepth == Integer.MAX_VALUE && outputPolicy.adaptiveTinyDictionaryComposition() && currentOutputCount <= outputPolicy.adaptiveComposeMaxRows()) {
                composeDepth = outputPolicy.adaptiveComposeDepth();
            }
            if (outputPolicy.wrapEncodedOuterDictionaries() && encodingDepth(source) < composeDepth) {
                return DictionaryVector.wrapNested(outerDictionaryIds(), currentOutputCount, source);
            }
            if (outputPolicy.cacheComposedOuterDictionaryIds()) {
                for (int index = 0; index < composedOuterMappingCount; index++) {
                    if (sameEncodingMapping(source, composedOuterMappingSources[index])) {
                        return DictionaryVector.wrap(
                                composedOuterMappingIds[index],
                                currentOutputCount,
                                encodingLeaf(source));
                    }
                }
            }
            // wrapComposedDictionary rewrites the ids in place while collapsing nested encodings, so an
            // encoded source needs a private copy it can mutate. The outer positions ascend (one output row per
            // match, in probe order), so an RLE level resolves run indices with a forward hint instead of binary search.
            int[] ids = Arrays.copyOf(outerDictionaryIds(), currentOutputCount);
            DictionaryVector composed = wrapComposedDictionary(ids, source, true);
            if (outputPolicy.cacheComposedOuterDictionaryIds()) {
                composedOuterMappingSources[composedOuterMappingCount] = source;
                composedOuterMappingIds[composedOuterMappingCount] = composed.ids();
                composedOuterMappingCount++;
            }
            return composed;
        }
        // Flat source: wrapComposedDictionary leaves the ids untouched, so every flat outer column can share
        // the single cached id snapshot instead of allocating a per-column copy.
        return DictionaryVector.wrap(outerDictionaryIds(), currentOutputCount, source);
    }

    private void clearComposedOuterMappings()
    {
        Arrays.fill(composedOuterMappingSources, 0, composedOuterMappingCount, null);
        Arrays.fill(composedOuterMappingIds, 0, composedOuterMappingCount, null);
        composedOuterMappingCount = 0;
    }

    /** Returns true when two encoded columns apply the identical position mapping to different leaf vectors. */
    private static boolean sameEncodingMapping(Vector left, Vector right)
    {
        Vector leftLevel = left;
        Vector rightLevel = right;
        while (true) {
            if (leftLevel instanceof DictionaryVector leftDictionary && rightLevel instanceof DictionaryVector rightDictionary) {
                if (leftDictionary.ids() != rightDictionary.ids() || leftDictionary.length() != rightDictionary.length()) {
                    return false;
                }
                leftLevel = leftDictionary.values();
                rightLevel = rightDictionary.values();
                continue;
            }
            if (leftLevel instanceof org.weakref.nitro.data.RleVector leftRle && rightLevel instanceof org.weakref.nitro.data.RleVector rightRle) {
                if (leftRle.counts() != rightRle.counts() || leftRle.length() != rightRle.length()) {
                    return false;
                }
                leftLevel = leftRle.values();
                rightLevel = rightRle.values();
                continue;
            }
            return !(leftLevel instanceof DictionaryVector || leftLevel instanceof org.weakref.nitro.data.RleVector) &&
                    !(rightLevel instanceof DictionaryVector || rightLevel instanceof org.weakref.nitro.data.RleVector);
        }
    }

    private static Vector encodingLeaf(Vector vector)
    {
        Vector level = vector;
        while (true) {
            if (level instanceof DictionaryVector dictionary) {
                level = dictionary.values();
                continue;
            }
            if (level instanceof org.weakref.nitro.data.RleVector rle) {
                level = rle.values();
                continue;
            }
            return level;
        }
    }

    private static int encodingDepth(Vector vector)
    {
        int depth = 0;
        Vector level = vector;
        while (true) {
            if (level instanceof DictionaryVector dictionary) {
                depth++;
                level = dictionary.values();
                continue;
            }
            if (level instanceof org.weakref.nitro.data.RleVector rle) {
                depth++;
                level = rle.values();
                continue;
            }
            return depth;
        }
    }

    private int[] currentBatchPositions(int[] positions)
    {
        // See outerDictionaryIds(): output ownership cannot be represented by an alias to reusable join scratch.
        return Arrays.copyOf(positions, currentOutputCount);
    }

    private int[] copyCurrentBatchPositions(int[] positions)
    {
        return Arrays.copyOf(positions, currentOutputCount);
    }

    private int[] innerLogicalDictionaryIds()
    {
        if (!outputPolicy.cacheInnerDictionaryIds()) {
            return currentBatchPositions(outputInnerLogicalPositions);
        }
        if (currentInnerLogicalDictionaryIds == null) {
            currentInnerLogicalDictionaryIds = currentBatchPositions(outputInnerLogicalPositions);
        }
        return currentInnerLogicalDictionaryIds;
    }

    private int[] innerSourceDictionaryIds()
    {
        if (!outputPolicy.cacheInnerDictionaryIds()) {
            return currentBatchPositions(innerSourcePositions());
        }
        if (currentInnerSourceDictionaryIds == null) {
            currentInnerSourceDictionaryIds = currentBatchPositions(innerSourcePositions());
        }
        return currentInnerSourceDictionaryIds;
    }

    private Vector wrapInnerLogicalDictionary(Vector source)
    {
        if (source instanceof DictionaryVector || source instanceof org.weakref.nitro.data.RleVector) {
            return wrapComposedDictionary(copyCurrentBatchPositions(outputInnerLogicalPositions), source);
        }
        return DictionaryVector.wrap(innerLogicalDictionaryIds(), currentOutputCount, source);
    }

    private Vector wrapInnerSourceDictionary(Vector source)
    {
        if (source instanceof DictionaryVector || source instanceof org.weakref.nitro.data.RleVector) {
            return wrapComposedDictionary(copyCurrentBatchPositions(innerSourcePositions()), source);
        }
        return DictionaryVector.wrap(innerSourceDictionaryIds(), currentOutputCount, source);
    }

    private void constrainOuterIfNecessary()
    {
        // The first time an outer column of this output batch is materialized, narrow the outer
        // operator to exactly the outer positions that survived the join for this batch. This lets a
        // lazy outer subplan (a deferred projected payload) compute only the rows the join emits.
        // Only push a constraint to an outer that can satisfy a constrained re-borrow; a source
        // whose reader advances irreversibly (a Parquet scan) must not be constrained and re-borrowed
        // - for those, the flat copy above borrows the live batch column once without deferral.
        if (!outerSupportsReborrow || outerConstrained || currentOuterBatch == null || !currentOuterBatchFullyConsumed()) {
            return;
        }
        outerConstrained = true;
        outerConstraintApplied = true;
        probeSource.constrain(matchedOuterMask());
    }

    /**
     * Whether this output batch is the last one drawing from the current outer batch — i.e. the
     * outer batch's probe positions are fully exhausted, so no later output batch will reference
     * {@code currentOuterBatch}. Only then is it safe to push a narrowing constraint to the outer:
     * its borrowed columns would otherwise be shared by later output batches drawing from the same
     * outer batch.
     */
    private boolean currentOuterBatchFullyConsumed()
    {
        return outerRemaining == 0
                && !currentOuterPositionReady
                && preparedOuterIndex >= preparedOuterCount
                && currentOuterMaskIndex >= currentOuterMask.count();
    }

    private Mask matchedOuterMask()
    {
        int totalPositions = currentOuterMask.size();
        if (currentOutputCount == 0 || currentOutputMask.none()) {
            return allocator.allocateEmptyMask(allocationContext, totalPositions);
        }

        int count = currentOutputMask.count();
        int capacity = Math.min(count, currentOuterMask.count());
        if (matchedOuterPositions.length < capacity) {
            int[] previousPositions = matchedOuterPositions;
            matchedOuterPositions = arrayPool.borrowInts(capacity);
            arrayPool.release(previousPositions);
        }
        int selectedCount = 0;
        int previous = -1;
        for (int index = 0; index < count; index++) {
            int outputPosition = currentOutputMask.position(index);
            int outerPosition = outputOuterPositions[outputPosition];
            if (outerPosition != previous) {
                matchedOuterPositions[selectedCount++] = outerPosition;
                previous = outerPosition;
            }
        }
        return allocator.allocateSparseMask(allocationContext, matchedOuterPositions, selectedCount, totalPositions);
    }

    private static Set<Stream> sideStreams(Output output)
    {
        EnumSet<Stream> streams = EnumSet.noneOf(Stream.class);
        if (output.hasNulls()) {
            streams.add(Stream.NULLS);
        }
        if (output.hasErrors()) {
            streams.add(Stream.ERRORS);
        }
        return streams;
    }

    private Streams materializeInnerOutput(int innerOutputIndex)
    {
        if (outputPolicy.rleAllUnmatchedOuterJoinOutput() && allRowsHaveNoMatch()) {
            Streams schema = outputSchema(innerOutputIndex + outerOutputCount);
            if (schema == null) {
                throw new IllegalStateException("Unable to determine inner output schema for left join");
            }
            return allNullInnerOutput(innerOutputIndex, schema, currentOutputCount);
        }

        Streams preMaterialized = null;
        if (!hasNoMatchRows()) {
            // The existing dictionary-wrap shortcuts produce a full-length, position-indexed vector, so they are
            // correct whether or not the output mask is sparse (a downstream constraint). They also drop
            // the per-position byte copy that otherwise dominates high-fan-out joins. Deferred build
            // batches still fall through (the wrap methods bail on them) so their lazy payloads keep
            // materializing only the constrained rows.
            prepareInnerOutputRuns();
            Streams wrapped = tryWrapSingleBatchInnerOutput(innerOutputIndex);
            if (wrapped != null) {
                return wrapped;
            }
            Vector dictionaryValues = tryWrapNonRetainedValues(innerOutputIndex);
            if (dictionaryValues != null) {
                // VALUES are carried as a unified dictionary (no byte copy). The boolean side streams
                // are wrapped the same way -- a dictionary over the matched logical positions -- rather
                // than flattened per output row, mirroring the retained and multi-run wrap paths and
                // matching a build row's whole record (values + nulls + errors) by index. This is a
                // single-run path (tryWrapNonRetainedValues requires preparedInnerRunCount ==
                // 1), so the whole output draws from one non-retained build batch.
                Streams.Builder result = Streams.builder();
                result.put(Stream.VALUES, allocator.adopt(allocationContext, dictionaryValues));
                Streams column = bufferedInner.batches().get(outputInnerRunBatchIndexes[0]).columns()[innerOutputIndex];
                if (column != null && column.hasNulls()) {
                    result.put(Stream.NULLS, wrapInnerSideStream(column.get(Stream.NULLS)));
                }
                if (column != null && column.hasErrors()) {
                    result.put(Stream.ERRORS, wrapInnerSideStream(column.get(Stream.ERRORS)));
                }
                return result.build();
            }
            // Unifying non-retained variable-width values pays for a build-owned copy. Restrict that representation
            // to sparse consumers, where it also avoids the per-position materialization forced by the constraint;
            // the full-mask bulk copier is already cheaper for consumers that immediately flatten the result.
            Vector multiRunDictionaryValues = currentOutputMask.all() ? null : tryWrapMultiRunNonRetainedBinaryValues(innerOutputIndex);
            if (multiRunDictionaryValues != null) {
                preMaterialized = Streams.ofValues(multiRunDictionaryValues);
            }
            // No zero-copy wrap applies. The remaining per-run bulk copy materializes the full output
            // range, so only take it when the mask is full; a sparse mask falls through to the
            // per-position copy below, which honors the constraint (and any deferred-batch laziness).
            if (currentOutputMask.all()) {
                Streams wrappedSideStreams = tryWrapMultiRunInnerBooleanSideStreams(innerOutputIndex);
                Streams result = wrappedSideStreams;
                Set<Stream> exposedStreams = innerOutputStreams(innerOutputIndex);
                boolean copyNulls = exposedStreams.contains(Stream.NULLS) && (wrappedSideStreams == null || !wrappedSideStreams.hasNulls());
                boolean copyErrors = exposedStreams.contains(Stream.ERRORS) && (wrappedSideStreams == null || !wrappedSideStreams.hasErrors());
                Vector gatheredValues = tryGatherMultiRunRetainedFixedWidthValues(innerOutputIndex);
                if (gatheredValues != null) {
                    result = result == null ? Streams.ofValues(gatheredValues) : result.with(Stream.VALUES, gatheredValues);
                }
                if (gatheredValues != null && !copyNulls && !copyErrors) {
                    return result;
                }
                for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                    int outputStart = outputInnerRunStarts[runIndex];
                    int runLength = outputInnerRunLengths[runIndex];
                    BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(outputInnerRunBatchIndexes[runIndex]);
                    result = copyInnerPositions(result, innerBatch, runIndex, outputInnerRunBatchIndexes[runIndex], innerOutputIndex, outputStart, runLength, currentOutputCount, gatheredValues == null, copyNulls, copyErrors);
                }
                return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outerOutputCount)) : result;
            }
        }

        Streams result = preMaterialized;
        boolean valuesMaterialized = preMaterialized != null;
        boolean exposeNulls = probeOuterJoin || innerOutputStreams(innerOutputIndex).contains(Stream.NULLS);
        Streams nullInnerSchema = null;
        // The constraint mask can select no rows while a downstream consumer still borrows this column
        // and indexes a sample position (for example ProjectOperator probes one row to discover which
        // streams a column exposes). Outer-side columns are always materialized over the full output
        // range, so an inner-side column must do the same when its constraint is empty; otherwise the
        // borrowed column has length zero and indexing the sample position throws.
        boolean materializeFullRange = currentOutputMask.none();
        int positionCount = materializeFullRange ? currentOutputCount : currentOutputMask.count();
        long[] innerRows = outputInnerRows;
        for (int index = 0; index < positionCount; index++) {
            int outputPosition = materializeFullRange ? index : currentOutputMask.position(index);
            long rowReference = innerRows[outputPosition];
            if (rowReference == NO_MATCH_ROW_REFERENCE) {
                if (nullInnerSchema == null) {
                    nullInnerSchema = outputSchema(innerOutputIndex + outerOutputCount);
                    if (nullInnerSchema == null) {
                        throw new IllegalStateException("Unable to determine inner output schema for left join");
                    }
                }
                result = copyNullInnerPosition(innerOutputIndex, result, nullInnerSchema, currentOutputCount, outputPosition);
                continue;
            }
            int innerBatchIndex = JoinRowReference.batchIndex(rowReference);
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            result = copyInnerSinglePosition(
                    result,
                    innerBatch,
                    innerBatchIndex,
                    innerOutputIndex,
                    currentOutputCount,
                    outputPosition,
                    JoinRowReference.position(rowReference),
                    !valuesMaterialized,
                    true,
                    true,
                    exposeNulls);
        }
        return result == null ? buffers.emptyLike(outputSchema(innerOutputIndex + outerOutputCount)) : result;
    }

    private boolean allRowsHaveNoMatch()
    {
        if (!probeOuterJoin || outputInnerLogicalPositionsReady || currentOutputCount == 0) {
            return false;
        }
        if (allRowsNoMatchState != 0) {
            return allRowsNoMatchState == 2;
        }
        // Mixed batches overwhelmingly expose a match at one of their boundaries. Reject those with
        // two predictable loads; only a plausible all-unmatched batch pays the full verification scan.
        if (outputInnerRows[0] != NO_MATCH_ROW_REFERENCE ||
                outputInnerRows[currentOutputCount - 1] != NO_MATCH_ROW_REFERENCE) {
            allRowsNoMatchState = 1;
            return false;
        }
        for (int position = 1; position < currentOutputCount - 1; position++) {
            if (outputInnerRows[position] != NO_MATCH_ROW_REFERENCE) {
                allRowsNoMatchState = 1;
                return false;
            }
        }
        allRowsNoMatchState = 2;
        if (outputPolicy.debugRleAllUnmatchedOuterJoinOutput()) {
            System.err.printf("[rle-all-unmatched-outer-join] join=%s rows=%d%n",
                    profileName != null ? profileName : "hash_join",
                    currentOutputCount);
        }
        return true;
    }

    private Streams allNullInnerOutput(int innerOutputIndex, Streams schema, int size)
    {
        Streams.Builder result = Streams.builder();
        Vector value = nullValuesForInnerOutput(innerOutputIndex, schema.values(), 1);
        result.put(Stream.VALUES, allocator.allocateSingleRunRle(allocationContext, size, value));

        result.put(Stream.NULLS, allTrueBooleanStream(size));

        if (schema.has(Stream.ERRORS)) {
            BooleanVector noError = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            result.put(Stream.ERRORS, allocator.allocateSingleRunRle(allocationContext, size, noError));
        }
        return result.build();
    }

    private Vector allTrueBooleanStream(int size)
    {
        BooleanVector value = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
        value.values()[0] = true;
        return allocator.allocateSingleRunRle(allocationContext, size, value);
    }

    private Streams tryWrapMultiRunInnerBooleanSideStreams(int innerOutputIndex)
    {
        if (currentOutputCount == 0 || preparedInnerRunCount <= 1) {
            return null;
        }

        Streams.Builder wrapped = Streams.builder();
        Vector nulls = tryWrapMultiRunInnerBooleanStream(innerOutputIndex, Stream.NULLS);
        if (nulls != null) {
            wrapped.put(Stream.NULLS, allocator.adopt(allocationContext, nulls));
        }
        Vector errors = tryWrapMultiRunInnerBooleanStream(innerOutputIndex, Stream.ERRORS);
        if (errors != null) {
            wrapped.put(Stream.ERRORS, allocator.adopt(allocationContext, errors));
        }

        Streams wrappedStreams = wrapped.build();
        return wrappedStreams.streams().isEmpty() ? null : wrappedStreams;
    }

    /**
     * Gathers a fixed-width VALUES stream from many retained build batches in one pass over the output rows.
     *
     * <p>A hash probe can alternate build batches on nearly every match. Processing those matches as consecutive
     * batch runs turns a dense result into thousands of tiny copy calls. Resolve each retained batch's flat backing
     * once and scatter directly into one dense output instead. This is a physical vector specialization only; the
     * join remains independent of logical types, columns, and query shapes.
     */
    private Vector tryGatherMultiRunRetainedFixedWidthValues(int innerOutputIndex)
    {
        if (!outputPolicy.gatherMultiRunRetainedFixedWidthValues() || preparedInnerRunCount <= 1) {
            return null;
        }

        Vector[] sources = new Vector[bufferedInner.batches().size()];
        int vectorKind = 0;
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int batchIndex = outputInnerRunBatchIndexes[runIndex];
            if (sources[batchIndex] != null) {
                continue;
            }
            BufferedJoinInput.InnerBatch batch = bufferedInner.batches().get(batchIndex);
            if (!batch.retained() || batch.deferred()) {
                return null;
            }
            Output output = batch.retainedBatch().output(innerOutputIndex);
            if (!output.hasValues()) {
                return null;
            }
            Vector values = output.borrow(Stream.VALUES);
            int sourceKind = fixedWidthVectorKind(values);
            if (sourceKind == 0) {
                return null;
            }
            if (vectorKind == 0) {
                vectorKind = sourceKind;
            }
            else if (sourceKind != vectorKind) {
                return null;
            }
            sources[batchIndex] = values;
        }

        if (vectorKind == 1 || vectorKind == 2) {
            VectorAccess.LongValues[] accessors = new VectorAccess.LongValues[sources.length];
            for (int index = 0; index < sources.length; index++) {
                if (sources[index] != null) {
                    accessors[index] = VectorAccess.longValues(sources[index]);
                }
            }
            if (vectorKind == 2) {
                I32Vector result = allocator.allocate(allocationContext, I32Vector.class, currentOutputCount, I32Vector::new);
                int[] output = result.values();
                int[] sourcePositions = innerSourcePositions();
                for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                    int batchIndex = outputInnerRunBatchIndexes[runIndex];
                    VectorAccess.LongValues accessor = accessors[batchIndex];
                    int positionStart = outputInnerRunStarts[runIndex];
                    int positionEnd = positionStart + outputInnerRunLengths[runIndex];
                    for (int position = positionStart; position < positionEnd; position++) {
                        output[position] = toIntExact(accessor.value(sourcePositions[position]));
                    }
                }
                return result;
            }
            I64Vector result = allocator.allocate(allocationContext, I64Vector.class, currentOutputCount, I64Vector::new);
            long[] output = result.values();
            int[] sourcePositions = innerSourcePositions();
            for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                int batchIndex = outputInnerRunBatchIndexes[runIndex];
                VectorAccess.LongValues accessor = accessors[batchIndex];
                int positionStart = outputInnerRunStarts[runIndex];
                int positionEnd = positionStart + outputInnerRunLengths[runIndex];
                for (int position = positionStart; position < positionEnd; position++) {
                    output[position] = accessor.value(sourcePositions[position]);
                }
            }
            return result;
        }
        if (vectorKind == 3) {
            VectorAccess.DoubleValues[] accessors = new VectorAccess.DoubleValues[sources.length];
            for (int index = 0; index < sources.length; index++) {
                if (sources[index] != null) {
                    accessors[index] = VectorAccess.doubleValues(sources[index]);
                }
            }
            F64Vector result = allocator.allocate(allocationContext, F64Vector.class, currentOutputCount, F64Vector::new);
            double[] output = result.values();
            int[] sourcePositions = innerSourcePositions();
            for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                int batchIndex = outputInnerRunBatchIndexes[runIndex];
                VectorAccess.DoubleValues accessor = accessors[batchIndex];
                int positionStart = outputInnerRunStarts[runIndex];
                int positionEnd = positionStart + outputInnerRunLengths[runIndex];
                for (int position = positionStart; position < positionEnd; position++) {
                    output[position] = accessor.value(sourcePositions[position]);
                }
            }
            return result;
        }
        VectorAccess.BooleanValues[] accessors = new VectorAccess.BooleanValues[sources.length];
        for (int index = 0; index < sources.length; index++) {
            if (sources[index] != null) {
                accessors[index] = VectorAccess.booleanValues(sources[index]);
            }
        }
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, currentOutputCount, BooleanVector::new);
        boolean[] output = result.values();
        int[] sourcePositions = innerSourcePositions();
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int batchIndex = outputInnerRunBatchIndexes[runIndex];
            VectorAccess.BooleanValues accessor = accessors[batchIndex];
            int positionStart = outputInnerRunStarts[runIndex];
            int positionEnd = positionStart + outputInnerRunLengths[runIndex];
            for (int position = positionStart; position < positionEnd; position++) {
                output[position] = accessor.value(sourcePositions[position]);
            }
        }
        return result;
    }

    private static int fixedWidthVectorKind(Vector values)
    {
        Vector base = values;
        while (true) {
            if (base instanceof DictionaryVector dictionary) {
                base = dictionary.values();
                continue;
            }
            if (base instanceof org.weakref.nitro.data.RleVector rle) {
                base = rle.values();
                continue;
            }
            break;
        }
        return switch (base) {
            case I64Vector _ -> 1;
            case I32Vector _ -> 2;
            case F64Vector _ -> 3;
            case BooleanVector _ -> 4;
            default -> 0;
        };
    }

    private Vector tryWrapMultiRunNonRetainedBinaryValues(int innerOutputIndex)
    {
        if (!outputPolicy.unifyMultiRunNonRetainedBinaryValues() || preparedInnerRunCount <= 1 || currentOutputCount == 0) {
            return null;
        }

        UnifiedBuildBinaryColumn unified = unifiedBuildBinaryColumns.get(innerOutputIndex);
        if (unified == unsupportedUnifiedBuildBinaryColumn) {
            return null;
        }
        if (unified == null) {
            long previousRows = multiRunBinaryOutputRows[innerOutputIndex];
            long outputRows = previousRows > Long.MAX_VALUE - currentOutputCount ? Long.MAX_VALUE : previousRows + currentOutputCount;
            multiRunBinaryOutputRows[innerOutputIndex] = outputRows;
            long buildRows = bufferedInner.rowCount();
            long admissionRows = buildRows > Long.MAX_VALUE / outputPolicy.unifyMultiRunBinaryMinOutputRowsPerBuildRow()
                    ? Long.MAX_VALUE
                    : buildRows * outputPolicy.unifyMultiRunBinaryMinOutputRowsPerBuildRow();
            if (outputRows < admissionRows) {
                return null;
            }
            unified = buildUnifiedBinaryColumn(innerOutputIndex);
            unifiedBuildBinaryColumns.put(innerOutputIndex, unified);
            if (unified == unsupportedUnifiedBuildBinaryColumn) {
                return null;
            }
        }

        int[] ids = new int[currentOutputCount];
        int[] batchOffsets = unified.batchOffsets();
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int batchIndex = outputInnerRunBatchIndexes[runIndex];
            int outputStart = outputInnerRunStarts[runIndex];
            int outputEnd = outputStart + outputInnerRunLengths[runIndex];
            int batchOffset = batchOffsets[batchIndex];
            for (int position = outputStart; position < outputEnd; position++) {
                ids[position] = batchOffset + outputInnerLogicalPositions[position];
            }
        }
        return allocator.adopt(allocationContext, DictionaryVector.wrap(ids, unified.values()));
    }

    private UnifiedBuildBinaryColumn buildUnifiedBinaryColumn(int innerOutputIndex)
    {
        List<BufferedJoinInput.InnerBatch> batches = bufferedInner.batches();
        int[] batchOffsets = new int[batches.size() + 1];
        long rowCount = 0;
        long byteCount = 0;
        Set<BinaryVector.Trait> commonTraits = null;
        for (int batchIndex = 0; batchIndex < batches.size(); batchIndex++) {
            BufferedJoinInput.InnerBatch batch = batches.get(batchIndex);
            if (batch.retained() || batch.deferred()) {
                return unsupportedUnifiedBuildBinaryColumn;
            }
            Streams column = batch.columns()[innerOutputIndex];
            if (column == null || !column.hasValues() || !(column.values() instanceof BinaryVector values)) {
                return unsupportedUnifiedBuildBinaryColumn;
            }
            rowCount += batch.length();
            byteCount += (long) values.offsets()[batch.length()] - values.offsets()[0];
            if (rowCount > Integer.MAX_VALUE || byteCount > Integer.MAX_VALUE ||
                    byteCount > outputPolicy.unifyMultiRunBinaryMaxRetainedBytes() - unifiedBuildBinaryRetainedBytes) {
                return unsupportedUnifiedBuildBinaryColumn;
            }
            batchOffsets[batchIndex + 1] = (int) rowCount;
            commonTraits = commonTraits == null ? values.traits() : (commonTraits.equals(values.traits()) ? commonTraits : Set.of());
        }

        BinaryVector result = BinaryVector.allocate(allocator, buildAllocationContext, (int) rowCount, (int) byteCount);
        int outputPosition = 0;
        int outputOffset = 0;
        result.offsets()[0] = 0;
        for (BufferedJoinInput.InnerBatch batch : batches) {
            BinaryVector source = (BinaryVector) batch.columns()[innerOutputIndex].values();
            for (int position = 0; position < batch.length(); position++) {
                int sourceStart = source.startOffset(position);
                int length = source.length(position);
                System.arraycopy(source.data(), sourceStart, result.data(), outputOffset, length);
                outputOffset += length;
                result.offsets()[++outputPosition] = outputOffset;
            }
        }
        if (commonTraits != null) {
            result.addTraits(commonTraits);
        }
        result.freezeContent();
        unifiedBuildBinaryRetainedBytes += byteCount;
        return new UnifiedBuildBinaryColumn(result, batchOffsets);
    }

    private Vector tryWrapMultiRunInnerBooleanStream(int innerOutputIndex, Stream stream)
    {
        if (preparedInnerRunCount <= 1) {
            return null;
        }

        // A non-null Trino build commonly exposes an explicit all-false NULLS stream on every retained page.
        // Detect that physical shape once per referenced build batch before constructing thousands of repeated
        // segments and dictionary ids. The result is one all-false vector for the whole join output.
        boolean[] inspectedBatches = new boolean[bufferedInner.batches().size()];
        boolean allFalse = true;
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int innerBatchIndex = outputInnerRunBatchIndexes[runIndex];
            if (inspectedBatches[innerBatchIndex]) {
                continue;
            }
            inspectedBatches[innerBatchIndex] = true;
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            Vector source;
            if (innerBatch.retained() && !innerBatch.deferred()) {
                Output output = innerBatch.retainedBatch().output(innerOutputIndex);
                if (!output.has(stream)) {
                    return null;
                }
                source = output.borrow(stream);
            }
            else if (!innerBatch.retained()) {
                Streams output = innerBatch.columns()[innerOutputIndex];
                if (output == null || !output.has(stream)) {
                    return null;
                }
                source = output.get(stream);
            }
            else {
                allFalse = false;
                break;
            }
            if (!isKnownAllFalseSource(source)) {
                allFalse = false;
                break;
            }
        }
        if (allFalse) {
            return allFalseBooleanStream(currentOutputCount);
        }

        int[] activeBatchIndexes = joinScratch.activeInnerBatchIndexes;
        int activeBatchCount = 0;
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int innerBatchIndex = outputInnerRunBatchIndexes[runIndex];
            if (preparedInnerBatchPositionCounts[innerBatchIndex] == 0) {
                activeBatchIndexes[activeBatchCount++] = innerBatchIndex;
            }
            preparedInnerBatchPositionCounts[innerBatchIndex] += outputInnerRunLengths[runIndex];
        }

        int groupedOffset = 0;
        for (int activeIndex = 0; activeIndex < activeBatchCount; activeIndex++) {
            int innerBatchIndex = activeBatchIndexes[activeIndex];
            preparedInnerBatchCursors[innerBatchIndex] = groupedOffset;
            groupedOffset += preparedInnerBatchPositionCounts[innerBatchIndex];
        }
        int[] groupedSourcePositions = retainedInnerMaskPositionsScratch();
        int[] sourcePositions = innerSourcePositions();
        for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
            int innerBatchIndex = outputInnerRunBatchIndexes[runIndex];
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
            int positionStart = outputInnerRunStarts[runIndex];
            int positionEnd = positionStart + outputInnerRunLengths[runIndex];
            int cursor = preparedInnerBatchCursors[innerBatchIndex];
            for (int position = positionStart; position < positionEnd; position++) {
                groupedSourcePositions[cursor++] = innerBatch.retained()
                        ? sourcePositions[position]
                        : outputInnerLogicalPositions[position];
            }
            preparedInnerBatchCursors[innerBatchIndex] = cursor;
        }

        try {
            Vector[] segments = new Vector[activeBatchCount];
            int segmentOffset = 0;
            for (int activeIndex = 0; activeIndex < activeBatchCount; activeIndex++) {
                int innerBatchIndex = activeBatchIndexes[activeIndex];
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
                int positionCount = preparedInnerBatchPositionCounts[innerBatchIndex];
                int positionStart = preparedInnerBatchCursors[innerBatchIndex] - positionCount;

                Vector source;
                if (!innerBatch.retained()) {
                    Streams output = innerBatch.columns()[innerOutputIndex];
                    if (output == null || !output.has(stream)) {
                        return null;
                    }
                    source = output.get(stream);
                }
                else {
                    Arrays.sort(groupedSourcePositions, positionStart, positionStart + positionCount);
                    int uniqueCount = 0;
                    int previous = -1;
                    int[] uniquePositions = innerUniqueSourcePositions();
                    for (int position = positionStart; position < positionStart + positionCount; position++) {
                        int sourcePosition = groupedSourcePositions[position];
                        if (uniqueCount == 0 || sourcePosition != previous) {
                            uniquePositions[uniqueCount++] = sourcePosition;
                            previous = sourcePosition;
                        }
                    }
                    constrainRetainedInnerBatchSourcePositions(innerBatchIndex, innerBatch, uniquePositions, uniqueCount);
                    Output output = innerBatch.retainedBatch().output(innerOutputIndex);
                    if (!output.has(stream)) {
                        return null;
                    }
                    source = output.borrow(stream);
                }

                segments[activeIndex] = source;
                preparedInnerBatchCursors[innerBatchIndex] = segmentOffset;
                segmentOffset += source.length();
            }

            int[] dictionaryIds = new int[currentOutputCount];
            for (int runIndex = 0; runIndex < preparedInnerRunCount; runIndex++) {
                int innerBatchIndex = outputInnerRunBatchIndexes[runIndex];
                BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
                int positionStart = outputInnerRunStarts[runIndex];
                int positionEnd = positionStart + outputInnerRunLengths[runIndex];
                int batchOffset = preparedInnerBatchCursors[innerBatchIndex];
                for (int position = positionStart; position < positionEnd; position++) {
                    dictionaryIds[position] = batchOffset + (innerBatch.retained()
                            ? sourcePositions[position]
                            : outputInnerLogicalPositions[position]);
                }
            }

            Vector values = new ConcatenatedBooleanVector(segments);
            return DictionaryVector.wrap(dictionaryIds, values);
        }
        finally {
            for (int activeIndex = 0; activeIndex < activeBatchCount; activeIndex++) {
                preparedInnerBatchPositionCounts[activeBatchIndexes[activeIndex]] = 0;
            }
        }
    }

    private Streams tryWrapSingleBatchInnerOutput(int innerOutputIndex)
    {
        if (currentOutputCount == 0) {
            return null;
        }

        if (preparedInnerRunCount != 1) {
            return null;
        }

        int innerBatchIndex = outputInnerRunBatchIndexes[0];
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
        if (innerBatch.deferred() || !innerBatch.retained()) {
            // Deferred batches must materialize lazily by constraining the source to the matched
            // positions and re-borrowing; non-retained (compacted) batches are flat-copied so the
            // inner VALUES are a dense vector indexable directly by output position. Both fall
            // through to the per-position copy path (copyInnerPositions): for deferred batches it
            // constrains the retained batch before borrowing (keeping projected payloads deferred);
            // for non-retained batches it flat-copies without any constrain. Genuinely retained
            // batches keep the dictionary-wrap shortcut below.
            return null;
        }

        return tryWrapRetainedInnerOutput(innerOutputIndex, innerBatch);
    }

    /**
     * Emits a single-run, non-retained build column's VALUES as a {@link DictionaryVector} over the
     * build values rather than flattening once per matched output row. Binary values may use a unified
     * per-build-column dictionary when cardinality is low; fixed-width flat values wrap the raw build
     * vector directly. Restricted to the common, safe shape: the whole output batch draws from one
     * build batch ({@code preparedInnerRunCount == 1}) and there are no NO-MATCH rows. Any other shape
     * returns {@code null} so the caller falls back to the existing flatten path. Grouping downstream
     * still settles equality by value, so this only changes representation.
     */
    private Vector tryWrapNonRetainedValues(int innerOutputIndex)
    {
        if (currentOutputCount == 0 || preparedInnerRunCount != 1) {
            return null;
        }
        int innerBatchIndex = outputInnerRunBatchIndexes[0];
        BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(innerBatchIndex);
        if (innerBatch.retained()) {
            return null;
        }
        Streams column = innerBatch.columns()[innerOutputIndex];
        if (column == null || !column.hasValues()) {
            return null;
        }
        Vector values = column.values();
        if (!(values instanceof BinaryVector binarySource)) {
            if (outputPolicy.wrapNonRetainedFixedWidthBuildValues() && isRawWrappableFixedWidthValue(values)) {
                return wrapRawNonRetainedValues(values);
            }
            return null;
        }

        if ((long) innerBatch.length() > (long) currentOutputCount * outputPolicy.buildDictionarySparseRatio()) {
            return wrapRawNonRetainedValues(binarySource);
        }

        BuildDictionary dictionary = buildDictionaryFor(innerBatchIndex, innerOutputIndex, binarySource, innerBatch.length());
        if (dictionary == notDictionary) {
            // High-cardinality build column: a dedup dictionary does not pay, but flattening would copy the
            // (variable-width) bytes once per matched output row. Wrap the raw build column directly instead --
            // ids are the matched build positions, values are the build column itself -- so each matched row is
            // referenced by id with no byte copy, at any cardinality (mirrors Trino's DictionaryBlock over a
            // build page). Safe for the same single-batch shape the dedup path requires; downstream grouping
            // still settles equality by value, so this only changes representation.
            return wrapRawNonRetainedValues(binarySource);
        }
        int[] sourceIdByPosition = dictionary.idByPosition();
        int[] valueIds = new int[currentOutputCount];
        for (int index = 0; index < currentOutputCount; index++) {
            valueIds[index] = sourceIdByPosition[outputInnerLogicalPositions[index]];
        }
        return DictionaryVector.wrap(valueIds, dictionary.values());
    }

    private static boolean isRawWrappableFixedWidthValue(Vector values)
    {
        return values instanceof I64Vector
                || values instanceof I32Vector
                || values instanceof F64Vector
                || values instanceof BooleanVector;
    }

    private Vector wrapRawNonRetainedValues(Vector source)
    {
        return DictionaryVector.wrap(innerLogicalDictionaryIds(), currentOutputCount, source);
    }

    private BuildDictionary buildDictionaryFor(int innerBatchIndex, int innerOutputIndex, BinaryVector source, int length)
    {
        int key = innerBatchIndex * Math.max(1, innerOutputCount) + innerOutputIndex;
        BuildDictionary cached = buildDictionaries.get(key);
        if (cached != null) {
            return cached;
        }

        // Scan only the build batch's valid row range: the buffered BinaryVector may be pre-sized with
        // stale trailing offsets beyond the real rows, which are never referenced by a logical position.
        int[] idByPosition = borrowBuildDictionaryIds(length);
        // Deduplicate distinct byte values into a single dictionary. This only pays off when the column
        // is low cardinality: a near-unique build column (e.g. a natural key carried straight to the
        // output and never grouped) gains nothing from dictionary grouping while the dedup scan and the
        // per-output-row id remap are pure overhead. Abandon as soon as the distinct count shows the
        // column is high cardinality and cache a NOT_DICTIONARY marker so the caller wraps the raw
        // build column directly.
        int distinctLimit = Math.max(
                outputPolicy.buildDictionaryMinDistinctValues(),
                (int) ((long) length * outputPolicy.buildDictionaryMaxDistinctPercent() / 100));
        ValueIdInterner interner = new ValueIdInterner(distinctLimit, valueIdPolicy);
        byte[] data = source.data();
        long totalBytes = 0;
        for (int position = 0; position < length; position++) {
            int start = source.startOffset(position);
            int valueLength = source.length(position);
            int distinctBefore = interner.distinctCount();
            int id = interner.intern(data, start, valueLength);
            if (id == ValueIdInterner.TOO_MANY) {
                releaseBuildDictionaryIds(idByPosition);
                buildDictionaries.put(key, notDictionary);
                return notDictionary;
            }
            if (interner.distinctCount() > distinctBefore) {
                totalBytes += valueLength;
            }
            idByPosition[position] = id;
        }

        if (totalBytes > Integer.MAX_VALUE) {
            releaseBuildDictionaryIds(idByPosition);
            buildDictionaries.put(key, notDictionary);
            return notDictionary;
        }
        BinaryVector values = interner.toBinaryVector(allocator, buildAllocationContext);
        values.clearTraits();
        values.addTraits(source.traits());
        BuildDictionary dictionary = new BuildDictionary(idByPosition, values);
        buildDictionaries.put(key, dictionary);
        return dictionary;
    }

    private int[] borrowBuildDictionaryIds(int length)
    {
        long bytes = (long) length * Integer.BYTES;
        return outputPolicy.poolBuildDictionaryIds() && arrayPool.isRetainable(bytes) ? arrayPool.borrowInts(length) : new int[length];
    }

    private void releaseBuildDictionaryIds(int[] ids)
    {
        if (outputPolicy.poolBuildDictionaryIds() && arrayPool.isRetainable((long) ids.length * Integer.BYTES)) {
            arrayPool.release(ids);
        }
    }

    private record BuildDictionary(int[] idByPosition, Vector values) {}

    private record UnifiedBuildBinaryColumn(BinaryVector values, int[] batchOffsets) {}

    private Streams tryWrapRetainedInnerOutput(int innerOutputIndex, BufferedJoinInput.InnerBatch innerBatch)
    {
        if (!innerBatch.retained()) {
            return null;
        }

        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        if (output.isValuesOnly()) {
            return Streams.ofValues(allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.VALUES))));
        }

        Streams.Builder wrapped = Streams.builder();
        if (output.hasValues()) {
            wrapped.put(Stream.VALUES, allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.VALUES))));
        }
        if (output.hasNulls()) {
            wrapped.put(Stream.NULLS, allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.NULLS))));
        }
        if (output.hasErrors()) {
            wrapped.put(Stream.ERRORS, allocator.adopt(allocationContext, wrapInnerSourceDictionary(output.borrow(Stream.ERRORS))));
        }
        return wrapped.build();
    }

    private static Streams sideStreamValues(Streams streams)
    {
        Streams.Builder sideInput = Streams.builder();
        if (streams.hasNulls()) {
            sideInput.put(Stream.NULLS, streams.get(Stream.NULLS));
        }
        if (streams.hasErrors()) {
            sideInput.put(Stream.ERRORS, streams.get(Stream.ERRORS));
        }
        return sideInput.build();
    }

    private DictionaryVector wrapComposedDictionary(int[] dictionaryIds, Vector values)
    {
        return wrapComposedDictionary(dictionaryIds, values, false);
    }

    /**
     * Rewrites {@code dictionaryIds} in place, collapsing nested dictionary/RLE encodings so the result is a single
     * dictionary over the base values. When {@code idsMonotonic} is set (the ids ascend, as the outer/probe positions
     * do -- one output row per match, in probe order), an RLE level resolves run indices with a forward-advancing hint
     * (amortized O(1) per position, O(runs) total) instead of a per-position binary search (O(positions * log runs));
     * for a many-run RLE that is a large win, and for a few-run RLE it is never worse. A dictionary level remaps
     * through arbitrary ids and so breaks the ordering; monotonicity is dropped there. Run indices of ascending
     * positions stay ascending, so a nested RLE keeps the fast path.
     */
    private DictionaryVector wrapComposedDictionary(int[] dictionaryIds, Vector values, boolean idsMonotonic)
    {
        Vector baseValues = values;
        boolean monotonic = idsMonotonic;
        while (true) {
            if (baseValues instanceof DictionaryVector dictionary) {
                int[] baseIds = dictionary.ids();
                for (int index = 0; index < dictionaryIds.length; index++) {
                    dictionaryIds[index] = baseIds[dictionaryIds[index]];
                }
                baseValues = dictionary.values();
                monotonic = false;
                continue;
            }

            if (baseValues instanceof org.weakref.nitro.data.RleVector rle) {
                if (monotonic && outputPolicy.rleRunIndexHint()) {
                    int hint = 0;
                    for (int index = 0; index < dictionaryIds.length; index++) {
                        hint = rle.runIndexFromHint(dictionaryIds[index], hint);
                        dictionaryIds[index] = hint;
                    }
                }
                else {
                    for (int index = 0; index < dictionaryIds.length; index++) {
                        dictionaryIds[index] = rle.runIndex(dictionaryIds[index]);
                    }
                }
                baseValues = rle.values();
                continue;
            }

            break;
        }
        return DictionaryVector.wrap(dictionaryIds, baseValues);
    }

    /**
     * Materializes a non-retained build column's boolean side stream (nulls or errors) for the output.
     * When the source is entirely false it is emitted as a 1-run all-false RLE of the output length --
     * O(1), no per-position id array -- keeping the stream present (its presence is a fixed schema
     * contract, so it cannot simply be dropped) while avoiding the dictionary wrap. Downstream null
     * propagation recognizes the all-false RLE shape (see {@link
     * VectorAccess#isAllFalseNulls}). Otherwise the stream is
     * wrapped as a dictionary over the matched logical positions, like the values alongside it.
     */
    private Vector wrapInnerSideStream(Vector source)
    {
        if (isKnownAllFalseSource(source)) {
            BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            return allocator.allocateSingleRunRle(allocationContext, currentOutputCount, sentinel);
        }
        return allocator.adopt(allocationContext, wrapInnerLogicalDictionary(source));
    }

    /**
     * Whether a build column's boolean side stream is entirely false. Peels dictionary/RLE encodings to
     * the underlying boolean run values and relies on {@link BooleanVector#isAllFalse()}, which caches
     * its result -- and the buffered build column is one reused instance across every output batch, so
     * the scan happens at most once per column.
     */
    private static boolean isKnownAllFalseSource(Vector stream)
    {
        return switch (stream) {
            case BooleanVector booleans -> booleans.isAllFalse();
            case DictionaryVector dictionary -> isKnownAllFalseSource(dictionary.values());
            case org.weakref.nitro.data.RleVector rle -> isKnownAllFalseSource(rle.values());
            default -> false;
        };
    }

    private static Streams sideStreamValues(Output output)
    {
        Streams.Builder sideInput = Streams.builder();
        if (output.hasNulls()) {
            sideInput.put(Stream.NULLS, output.borrow(Stream.NULLS));
        }
        if (output.hasErrors()) {
            sideInput.put(Stream.ERRORS, output.borrow(Stream.ERRORS));
        }
        return sideInput.build();
    }

    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int runIndex, int innerBatchIndex, int innerOutputIndex, int positionStart, int positionCount, int size)
    {
        return copyInnerPositions(existing, innerBatch, runIndex, innerBatchIndex, innerOutputIndex, positionStart, positionCount, size, true, true, true);
    }

    private Streams copyInnerPositions(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int runIndex, int innerBatchIndex, int innerOutputIndex, int positionStart, int positionCount, int size, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        if (!includeValues && !includeNulls && !includeErrors) {
            return existing;
        }
        if (!innerBatch.retained()) {
            Streams input = selectedStreams(innerBatch.columns()[innerOutputIndex], includeValues, includeNulls, includeErrors);
            if (input.streams().isEmpty()) {
                return existing;
            }
            return buffers.copyPositionsFresh(existing, input, outputInnerLogicalPositions, positionStart, positionCount, positionStart, size, innerPositionMappingCache);
        }

        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, outputInnerRunUniqueStarts[runIndex], outputInnerRunUniqueCounts[runIndex]);
        Output output = innerBatch.retainedBatch().output(innerOutputIndex);
        Output selected = selectedStreams(output, includeValues, includeNulls, includeErrors);
        if (selected == null) {
            return existing;
        }
        return buffers.copyPositionsFresh(selected, existing, innerSourcePositions(), positionStart, positionCount, positionStart, size, innerPositionMappingCache);
    }

    private static Streams selectedStreams(Streams input, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        Streams.Builder selected = Streams.builder();
        if (includeValues && input.hasValues()) {
            selected.put(Stream.VALUES, input.values());
        }
        if (includeNulls && input.hasNulls()) {
            selected.put(Stream.NULLS, input.get(Stream.NULLS));
        }
        if (includeErrors && input.hasErrors()) {
            selected.put(Stream.ERRORS, input.get(Stream.ERRORS));
        }
        return selected.build();
    }

    private static Output selectedStreams(Output input, boolean includeValues, boolean includeNulls, boolean includeErrors)
    {
        Set<Stream> selected = EnumSet.noneOf(Stream.class);
        if (includeValues && input.hasValues()) {
            selected.add(Stream.VALUES);
        }
        if (includeNulls && input.hasNulls()) {
            selected.add(Stream.NULLS);
        }
        if (includeErrors && input.hasErrors()) {
            selected.add(Stream.ERRORS);
        }
        if (selected.isEmpty()) {
            return null;
        }
        return input.select(selected);
    }

    private Streams copyInnerSinglePosition(Streams existing, BufferedJoinInput.InnerBatch innerBatch, int innerBatchIndex, int innerOutputIndex, int size, int outputPosition, int logicalPosition, boolean exposeNulls)
    {
        return copyInnerSinglePosition(existing, innerBatch, innerBatchIndex, innerOutputIndex, size, outputPosition, logicalPosition, true, true, true, exposeNulls);
    }

    private Streams copyInnerSinglePosition(
            Streams existing,
            BufferedJoinInput.InnerBatch innerBatch,
            int innerBatchIndex,
            int innerOutputIndex,
            int size,
            int outputPosition,
            int logicalPosition,
            boolean includeValues,
            boolean includeNulls,
            boolean includeErrors,
            boolean exposeNulls)
    {
        if (!innerBatch.retained()) {
            Streams selected = selectedStreams(innerBatch.columns()[innerOutputIndex], includeValues, includeNulls, includeErrors);
            Streams result = selected.streams().isEmpty()
                    ? existing
                    : buffers.copySinglePositionFresh(existing, selected, size, outputPosition, logicalPosition);
            return withSyntheticNulls(existing, result, size, outputPosition, exposeNulls);
        }

        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, logicalPosition);
        Output selected = selectedStreams(innerBatch.retainedBatch().output(innerOutputIndex), includeValues, includeNulls, includeErrors);
        Streams result = selected == null ? existing : buffers.copySinglePositionFresh(selected, existing, size, outputPosition, sourcePosition);
        return withSyntheticNulls(existing, result, size, outputPosition, exposeNulls);
    }

    private Streams copyNullInnerPosition(int innerOutputIndex, Streams existing, Streams schema, int size, int outputPosition)
    {
        Streams.Builder builder = Streams.builder();
        builder.put(Stream.VALUES, existing == null ? nullValuesForInnerOutput(innerOutputIndex, schema.values(), size) : existing.values());
        builder.put(Stream.NULLS, setBooleanPosition(existing == null ? null : existing.getOrNull(Stream.NULLS), size, outputPosition, true));
        if (schema.has(Stream.ERRORS)) {
            builder.put(Stream.ERRORS, setBooleanPosition(existing == null ? null : existing.getOrNull(Stream.ERRORS), size, outputPosition, false));
        }
        return builder.build();
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionCount)
    {
        constrainRetainedInnerBatch(innerBatchIndex, innerBatch, logicalPositions, 0, positionCount);
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int[] logicalPositions, int positionStart, int positionCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        boolean sorted = true;
        int previousSourcePosition = -1;
        int[] scratch = retainedInnerMaskPositionsScratch();
        for (int index = 0; index < positionCount; index++) {
            int sourcePosition = innerBatch.sourcePosition(logicalPositions[positionStart + index]);
            scratch[index] = sourcePosition;
            sorted &= sourcePosition >= previousSourcePosition;
            previousSourcePosition = sourcePosition;
        }
        if (!sorted) {
            Arrays.sort(scratch, 0, positionCount);
        }
        int uniqueCount = 0;
        int previous = -1;
        for (int index = 0; index < positionCount; index++) {
            int position = scratch[index];
            if (position != previous) {
                scratch[uniqueCount++] = position;
                previous = position;
            }
        }
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (uniqueCount == cachedCount &&
                cachedPositions != null &&
                Arrays.equals(scratch, 0, uniqueCount, cachedPositions, 0, uniqueCount)) {
            return;
        }
        replaceRetainedInnerConstraint(innerBatchIndex, innerBatch, allocator.allocateSparseMask(
                allocationContext,
                scratch,
                uniqueCount,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length < uniqueCount) {
            cachedPositions = new int[Math.max(uniqueCount, 4)];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        System.arraycopy(scratch, 0, cachedPositions, 0, uniqueCount);
        retainedConstraintCountsByBatch[innerBatchIndex] = uniqueCount;
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int uniquePositionStart, int uniqueCount)
    {
        if (!innerBatch.retained()) {
            return;
        }
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        int[] uniqueSourcePositions = innerUniqueSourcePositions();
        if (uniqueCount == cachedCount &&
                cachedPositions != null &&
                Arrays.equals(uniqueSourcePositions, uniquePositionStart, uniquePositionStart + uniqueCount, cachedPositions, 0, uniqueCount)) {
            return;
        }
        int[] scratch = retainedInnerMaskPositionsScratch();
        System.arraycopy(uniqueSourcePositions, uniquePositionStart, scratch, 0, uniqueCount);
        replaceRetainedInnerConstraint(innerBatchIndex, innerBatch, allocator.allocateSparseMask(
                allocationContext,
                scratch,
                uniqueCount,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length < uniqueCount) {
            cachedPositions = new int[Math.max(uniqueCount, 4)];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        System.arraycopy(uniqueSourcePositions, uniquePositionStart, cachedPositions, 0, uniqueCount);
        retainedConstraintCountsByBatch[innerBatchIndex] = uniqueCount;
    }

    private void constrainRetainedInnerBatchSourcePositions(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int[] sourcePositions, int positionCount)
    {
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (positionCount == cachedCount &&
                cachedPositions != null &&
                Arrays.equals(sourcePositions, 0, positionCount, cachedPositions, 0, positionCount)) {
            return;
        }
        replaceRetainedInnerConstraint(innerBatchIndex, innerBatch, allocator.allocateSparseMask(
                allocationContext,
                sourcePositions,
                positionCount,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length < positionCount) {
            cachedPositions = new int[Math.max(positionCount, 4)];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        System.arraycopy(sourcePositions, 0, cachedPositions, 0, positionCount);
        retainedConstraintCountsByBatch[innerBatchIndex] = positionCount;
    }

    private int copySortedUniquePositions(int[] sourcePositions, int positionStart, int positionCount, int[] targetPositions, int targetStart)
    {
        int[] scratch = retainedInnerMaskPositionsScratch();
        System.arraycopy(sourcePositions, positionStart, scratch, 0, positionCount);
        Arrays.sort(scratch, 0, positionCount);
        int uniqueCount = 0;
        int previous = -1;
        for (int index = 0; index < positionCount; index++) {
            int position = scratch[index];
            if (uniqueCount == 0 || position != previous) {
                targetPositions[targetStart + uniqueCount] = position;
                previous = position;
                uniqueCount++;
            }
        }
        return uniqueCount;
    }

    private void prepareInnerOutputRuns()
    {
        if (preparedInnerRunCount >= 0) {
            return;
        }

        if (outputInnerLogicalPositionsReady) {
            int batchIndex = outputInnerLogicalPositionsBatchIndex;
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
            outputInnerRunStarts[0] = 0;
            outputInnerRunLengths[0] = currentOutputCount;
            outputInnerRunBatchIndexes[0] = batchIndex;
            outputInnerRunUniqueStarts[0] = 0;
            if (innerBatch.retained()) {
                int[] innerSourcePositions = innerSourcePositions();
                for (int index = 0; index < currentOutputCount; index++) {
                    innerSourcePositions[index] = innerBatch.sourcePosition(outputInnerLogicalPositions[index]);
                }
                outputInnerRunUniqueCounts[0] = copySortedUniquePositions(innerSourcePositions, 0, currentOutputCount, innerUniqueSourcePositions(), 0);
            }
            else {
                outputInnerRunUniqueCounts[0] = 0;
            }
            preparedInnerRunCount = 1;
            return;
        }

        long[] innerRows = outputInnerRows;
        int runCount = 0;
        int runStart = 0;
        int uniquePositionStart = 0;
        while (runStart < currentOutputCount) {
            long rowReference = innerRows[runStart];
            int batchIndex = JoinRowReference.batchIndex(rowReference);
            BufferedJoinInput.InnerBatch innerBatch = bufferedInner.batches().get(batchIndex);
            boolean retained = innerBatch.retained();
            int[] innerSourcePositions = retained ? innerSourcePositions() : null;

            int runEnd = runStart;
            while (runEnd < currentOutputCount && JoinRowReference.batchIndex(innerRows[runEnd]) == batchIndex) {
                int logicalPosition = JoinRowReference.position(innerRows[runEnd]);
                outputInnerLogicalPositions[runEnd] = logicalPosition;
                if (retained) {
                    innerSourcePositions[runEnd] = innerBatch.sourcePosition(logicalPosition);
                }
                runEnd++;
            }

            outputInnerRunStarts[runCount] = runStart;
            outputInnerRunLengths[runCount] = runEnd - runStart;
            outputInnerRunBatchIndexes[runCount] = batchIndex;
            if (retained) {
                int uniqueCount = copySortedUniquePositions(innerSourcePositions, runStart, runEnd - runStart, innerUniqueSourcePositions(), uniquePositionStart);
                outputInnerRunUniqueStarts[runCount] = uniquePositionStart;
                outputInnerRunUniqueCounts[runCount] = uniqueCount;
                uniquePositionStart += uniqueCount;
            }
            else {
                outputInnerRunUniqueStarts[runCount] = uniquePositionStart;
                outputInnerRunUniqueCounts[runCount] = 0;
            }
            runCount++;
            runStart = runEnd;
        }
        preparedInnerRunCount = runCount;
    }

    private void constrainRetainedInnerBatch(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, int logicalPosition)
    {
        if (!innerBatch.retained()) {
            return;
        }
        int sourcePosition = innerBatch.sourcePosition(logicalPosition);
        int cachedCount = retainedConstraintCountsByBatch[innerBatchIndex];
        int[] cachedPositions = retainedConstraintPositionsByBatch[innerBatchIndex];
        if (cachedCount == 1 &&
                cachedPositions != null &&
                cachedPositions[0] == sourcePosition) {
            return;
        }
        int[] scratch = retainedInnerMaskPositionsScratch();
        scratch[0] = sourcePosition;
        replaceRetainedInnerConstraint(innerBatchIndex, innerBatch, allocator.allocateSparseMask(
                allocationContext,
                scratch,
                1,
                innerBatch.retainedBatch().borrowMask().size()));
        if (cachedPositions == null || cachedPositions.length == 0) {
            cachedPositions = new int[4];
            retainedConstraintPositionsByBatch[innerBatchIndex] = cachedPositions;
        }
        cachedPositions[0] = sourcePosition;
        retainedConstraintCountsByBatch[innerBatchIndex] = 1;
    }

    private void replaceRetainedInnerConstraint(int innerBatchIndex, BufferedJoinInput.InnerBatch innerBatch, Mask constraint)
    {
        retainedConstraintMasksByBatch[innerBatchIndex] = replaceRetainedConstraint(
                allocator,
                allocationContext,
                innerBatch.retainedBatch(),
                retainedConstraintMasksByBatch[innerBatchIndex],
                constraint);
    }

    static Mask replaceRetainedConstraint(Allocator allocator, Allocator.Context context, Batch batch, Mask previous, Mask constraint)
    {
        batch.constrain(constraint);
        if (previous != null) {
            allocator.release(context, previous);
        }
        return constraint;
    }

    private void ensureRetainedConstraintCacheCapacity(int batchCount)
    {
        if (retainedConstraintCountsByBatch.length >= batchCount) {
            return;
        }
        int previousLength = retainedConstraintCountsByBatch.length;
        retainedConstraintCountsByBatch = Arrays.copyOf(retainedConstraintCountsByBatch, batchCount);
        Arrays.fill(retainedConstraintCountsByBatch, previousLength, batchCount, -1);
        retainedConstraintPositionsByBatch = Arrays.copyOf(retainedConstraintPositionsByBatch, batchCount);
        retainedConstraintMasksByBatch = Arrays.copyOf(retainedConstraintMasksByBatch, batchCount);
        preparedInnerBatchPositionCounts = Arrays.copyOf(preparedInnerBatchPositionCounts, batchCount);
        preparedInnerBatchCursors = Arrays.copyOf(preparedInnerBatchCursors, batchCount);
    }

    private int expectedInnerRowCount()
    {
        if (expectedIndexedInnerRows >= 0) {
            return Math.max(16, expectedIndexedInnerRows);
        }
        long rowCount = bufferedInner.rowCount();
        if (rowCount <= 0 && buildPolicy.exactStreamingCardinality()) {
            rowCount = inner.exactOutputRows();
        }
        if (rowCount <= 0) {
            return 16;
        }
        return (int) Math.max(16L, Math.min(Integer.MAX_VALUE, rowCount));
    }

    private boolean hasNoMatchRows()
    {
        if (outputInnerLogicalPositionsReady) {
            return false;
        }
        for (int index = 0; index < currentOutputCount; index++) {
            if (outputInnerRows[index] == NO_MATCH_ROW_REFERENCE) {
                return true;
            }
        }
        return false;
    }

    private Set<Stream> innerOutputStreams(int innerOutputIndex)
    {
        Set<Stream> streams = bufferedInner.outputStreams(innerOutputIndex);
        if (!probeOuterJoin || streams == null || streams.contains(Stream.NULLS)) {
            return streams;
        }
        EnumSet<Stream> adjusted = EnumSet.copyOf(streams);
        adjusted.add(Stream.NULLS);
        return Set.copyOf(adjusted);
    }

    private Streams withSyntheticNulls(Streams existing, Streams streams, int size, int outputPosition, boolean exposeNulls)
    {
        if (!exposeNulls || streams.has(Stream.NULLS)) {
            return streams;
        }
        return streams;
    }

    private Set<Stream> resultKnownAllFalseStreams(int outputIndex, Set<Stream> streams)
    {
        if (streams.isEmpty()) {
            return Set.of();
        }

        int knownFlags = 0;
        if (outputIndex < outerOutputCount) {
            Output sourceOutput = currentOuterBatch.output(outputIndex);
            if (streams.contains(Stream.VALUES) && sourceOutput.isKnownAllFalse(Stream.VALUES)) {
                knownFlags |= VALUES_FLAG;
            }
            if (streams.contains(Stream.NULLS) && sourceOutput.isKnownAllFalse(Stream.NULLS)) {
                knownFlags |= NULLS_FLAG;
            }
            if (streams.contains(Stream.ERRORS) && sourceOutput.isKnownAllFalse(Stream.ERRORS)) {
                knownFlags |= ERRORS_FLAG;
            }
            return Output.streamSet(knownFlags);
        }

        int innerOutputIndex = outputIndex - outerOutputCount;
        if (streams.contains(Stream.NULLS) && innerOutputKnownAllFalseNulls(innerOutputIndex)) {
            knownFlags |= NULLS_FLAG;
        }
        if (streams.contains(Stream.ERRORS) && bufferedInner.outputKnownAllFalse(innerOutputIndex, Stream.ERRORS)) {
            knownFlags |= ERRORS_FLAG;
        }
        return Output.streamSet(knownFlags);
    }

    private boolean innerOutputKnownAllFalseNulls(int innerOutputIndex)
    {
        Set<Stream> streams = innerOutputStreams(innerOutputIndex);
        if (streams == null || !streams.contains(Stream.NULLS)) {
            return false;
        }
        if (hasNoMatchRows()) {
            return false;
        }

        Set<Stream> sourceStreams = bufferedInner.outputStreams(innerOutputIndex);
        boolean sourceExposesNulls = sourceStreams != null && sourceStreams.contains(Stream.NULLS);
        if (!sourceExposesNulls) {
            return probeOuterJoin;
        }
        return bufferedInner.outputKnownAllFalse(innerOutputIndex, Stream.NULLS);
    }

    private BooleanVector allFalseBooleanStream(int size)
    {
        return allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
    }

    private BooleanVector setBooleanPosition(Vector existing, int size, int outputPosition, boolean value)
    {
        BooleanVector vector;
        if (existing instanceof BooleanVector booleanVector) {
            vector = allocator.allocateOrGrow(allocationContext, booleanVector, BooleanVector.class, size, BooleanVector::new);
        }
        else {
            vector = allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
            if (existing != null) {
                VectorAccess.BooleanValues existingValues = VectorAccess.booleanValues(existing);
                int existingLength = Math.min(existing.length(), size);
                for (int position = 0; position < existingLength; position++) {
                    vector.values()[position] = existingValues.value(position);
                }
            }
        }
        vector.values()[outputPosition] = value;
        return vector;
    }

    private Streams ensureNullStream(Streams schema)
    {
        if (schema.has(Stream.NULLS)) {
            return schema;
        }
        return schema.with(Stream.NULLS, new BooleanVector(0));
    }

    private Vector nullValuesLike(Vector sample, int size)
    {
        return switch (sample) {
            case org.weakref.nitro.data.I64Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.I64Vector.class, size, org.weakref.nitro.data.I64Vector::new);
            case org.weakref.nitro.data.I32Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.I32Vector.class, size, org.weakref.nitro.data.I32Vector::new);
            case org.weakref.nitro.data.F64Vector _ -> allocator.allocate(allocationContext, org.weakref.nitro.data.F64Vector.class, size, org.weakref.nitro.data.F64Vector::new);
            case BooleanVector _ -> allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new);
            case org.weakref.nitro.data.BinaryVector binary -> {
                org.weakref.nitro.data.BinaryVector values = org.weakref.nitro.data.BinaryVector.allocate(allocator, allocationContext, size, 0);
                values.addTraits(binary.traits());
                yield values;
            }
            case org.weakref.nitro.data.DictionaryVector dictionary -> nullValuesLike(dictionary.values(), size);
            case org.weakref.nitro.data.RleVector rle -> nullValuesLike(rle.values(), size);
            case org.weakref.nitro.data.ArrayVector array -> {
                org.weakref.nitro.data.ArrayVector values = allocator.allocateArray(allocationContext, size);
                values.setElements(buffers.emptyLike(array.elements()));
                yield values;
            }
            case org.weakref.nitro.data.MapVector map -> {
                org.weakref.nitro.data.MapVector values = allocator.allocateMap(allocationContext, size);
                values.setEntries(buffers.emptyLike(map.keys()), buffers.emptyLike(map.values()));
                yield values;
            }
            case org.weakref.nitro.data.StructVector struct -> {
                org.weakref.nitro.data.StructVector values = allocator.allocate(allocationContext, org.weakref.nitro.data.StructVector.class, size, org.weakref.nitro.data.StructVector::new);
                for (Map.Entry<String, Streams> field : struct.fields().entrySet()) {
                    values.setField(field.getKey(), buffers.emptyLike(field.getValue()));
                }
                yield values;
            }
            default -> throw new IllegalArgumentException("Unsupported null materialization type: " + sample.getClass().getSimpleName());
        };
    }

    private Vector nullValuesForInnerOutput(int innerOutputIndex, Vector sample, int size)
    {
        TypeBinding type = fullOutputSchema.field(outerOutputCount + innerOutputIndex).type();
        if (type.isSpecified() && type.vectorFactory().isPresent()) {
            Vector values = type.vectorFactory().orElseThrow().nullValues(typeVectorAllocator, size);
            if (values.length() != size) {
                throw new IllegalArgumentException("Type vector factory returned length %s for requested length %s"
                        .formatted(values.length(), size));
            }
            if (!type.supportsVector(values)) {
                throw new IllegalArgumentException("Type %s does not support factory result %s"
                        .formatted(type.identity(), values.getClass().getName()));
            }
            return values;
        }
        return nullValuesLike(sample, size);
    }

    private static boolean isSingleLongJoinCandidate(Vector values)
    {
        FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
        return handler != null && handler.kind() == FlatTypeHandler.Kind.LONG;
    }

    private static final class JoinScratch
    {
        private final int[] outputOuterPositions;
        private final long[] outputInnerRows;
        private final int[] outputInnerLogicalPositions;
        private final int[] outputInnerRunStarts;
        private final int[] outputInnerRunLengths;
        private final int[] outputInnerRunBatchIndexes;
        private final int[] outputInnerRunUniqueStarts;
        private final int[] outputInnerRunUniqueCounts;
        private final int[] preparedOuterPositions;
        private final long[] preparedSingleRefs;
        private final int[] preparedSingleRefs32;
        private final int[] preparedRangeStarts;
        private final int[] preparedRangeCounts;
        private final int[] activeInnerBatchIndexes;
        private int[] outputInnerSourcePositions;
        private int[] outputInnerUniqueSourcePositions;
        private int[] retainedInnerMaskPositionsScratch;

        private JoinScratch(int capacity)
        {
            outputOuterPositions = new int[capacity];
            outputInnerRows = new long[capacity];
            outputInnerLogicalPositions = new int[capacity];
            outputInnerRunStarts = new int[capacity];
            outputInnerRunLengths = new int[capacity];
            outputInnerRunBatchIndexes = new int[capacity];
            outputInnerRunUniqueStarts = new int[capacity];
            outputInnerRunUniqueCounts = new int[capacity];
            preparedOuterPositions = new int[capacity];
            preparedSingleRefs = new long[capacity];
            preparedSingleRefs32 = new int[capacity];
            preparedRangeStarts = new int[capacity];
            preparedRangeCounts = new int[capacity];
            activeInnerBatchIndexes = new int[capacity];
        }

        private long retainedBytes()
        {
            long bytes = (long) outputOuterPositions.length * (12 * Integer.BYTES + 2 * Long.BYTES);
            if (outputInnerSourcePositions != null) {
                bytes += (long) outputInnerSourcePositions.length * Integer.BYTES;
            }
            if (outputInnerUniqueSourcePositions != null) {
                bytes += (long) outputInnerUniqueSourcePositions.length * Integer.BYTES;
            }
            if (retainedInnerMaskPositionsScratch != null) {
                bytes += (long) retainedInnerMaskPositionsScratch.length * Integer.BYTES;
            }
            return bytes;
        }
    }

    private static final class LongJoinIndex
            extends JoinIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        private static final int EMPTY = -1;
        // A large key-only build does not need payload columns, and an exact non-negative range map can be built
        // without first paying for an open-addressed hash table. Keys outside the bounded domain fall back to the
        // ordinary hash representation; duplicates retain their exact multiplicity through the existing direct
        // builder. Keep small builds on the sequential detector/hash path, where a speculative range map is not
        // amortized.
        private final HashJoinIndexPolicy policy;
        private final HashJoinOutputPolicy outputPolicy;
        private final HashJoinExecutionPolicy executionPolicy;
        private final PrimitiveArrayPool arrayPool;

        private final LongJoinHashTable hashTable;
        // Build rows are indexed by a dense ordinal. The row store owns both their adaptive reference
        // representation and the flat insertion-ordered duplicate chain.
        private final JoinRowStore rows;
        private final LongJoinBuildCardinality buildCardinality;
        private final boolean buildRowReferencesUnused;
        private final boolean batchBuild;
        // Array-mode (Velox kArray-style direct addressing): when the build keys are unique and form a
        // dense integer range, a probe is a bounds check plus one array index — no hash, no probe loop.
        // Built lazily on the first probe; the hash table is the fallback for sparse or duplicate keys.
        private long minKey = Long.MAX_VALUE;
        private long maxKey = Long.MIN_VALUE;
        private boolean finalized;
        private final DirectLongJoinLookup directLookup;
        // Exact membership filter for a sparse but bounded integer domain.  One bit per possible key lets a
        // predominantly-negative probe avoid the larger tag/key hash table entirely.  Unlike a Bloom filter this
        // has no false positives; the hash table is consulted only for keys whose bit is present.
        private final SparseLongRangeMembership sparseMembership;
        private final DenseJoinSequence denseSequence;
        // Range mode (multi-row keys): at finalize each key's chain is compacted into a contiguous FIFO slice, so a
        // probe reads a sequential range instead of pointer-chasing the duplicate chain. Opt-out for A/B.
        private final boolean compactChains;
        private final boolean compressDuplicateReferences;
        private final int expectedBuildRows;
        private final DirectLongBuildIndex directBuild;
        private final CompactedJoinRows compactedRows;
        // A completed duplicate build may have invariant bits inside an otherwise sparse physical key domain.
        // Removing those bits with Long.compress produces an exact dense ordinal without teaching the join about a
        // query, column, or logical type. Each nonzero entry packs an ordered-row start + 1 in 24 bits and the match
        // count in 8 bits, preserving the same insertion-ordered slices as the ordinary compacted hash representation.
        private final CompressedLongRangeIndex compressedRanges;
        private final JoinMatchScratch matchScratch = new JoinMatchScratch();
        private final boolean ownsStorage;

        private LongJoinIndex(
                HashJoinIndexPolicy policy,
                HashJoinOutputPolicy outputPolicy,
                HashJoinExecutionPolicy executionPolicy,
                PrimitiveArrayPool arrayPool,
                int expectedSize,
                boolean keyOnlyBuild,
                boolean capInitialHash,
                boolean groupedHashTable,
                boolean lazyDuplicateSlotState,
                boolean implicitSequentialRowReferences,
                boolean directRangeBuild,
                boolean buildRowReferencesUnused,
                boolean batchBuild)
        {
            this.policy = requireNonNull(policy, "policy is null");
            this.outputPolicy = requireNonNull(outputPolicy, "outputPolicy is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.arrayPool = arrayPool;
            this.buildCardinality = new LongJoinBuildCardinality();
            this.sparseMembership = new SparseLongRangeMembership(policy, arrayPool);
            this.compressedRanges = new CompressedLongRangeIndex(
                    arrayPool,
                    policy.compressedDirectRange(),
                    policy.compressedDirectRangeMinKeys(),
                    policy.compressedDirectRangeMaxEntries(),
                    policy.compressedDirectRangeMaxRatio(),
                    policy.debugCompressedDirectRange());
            this.directBuild = new DirectLongBuildIndex(
                    arrayPool,
                    policy.sparseDirectDuplicateState(),
                    policy.sparseDirectDuplicateMinExpectedRows(),
                    policy.sparseDirectDuplicateMinExpectedDomainRatio(),
                    policy.directDuplicateGroupInitialCapacity(),
                    EMPTY);
            this.denseSequence = new DenseJoinSequence(
                    arrayPool,
                    policy.denseBuildFastPath(),
                    policy.computeDenseSingleBatchRowReferences());
            this.directLookup = new DirectLongJoinLookup(arrayPool, NO_MATCH_ROW_REFERENCE);
            this.compactedRows = new CompactedJoinRows(arrayPool, EMPTY);
            this.compactChains = policy.compactChains() && !keyOnlyBuild;
            // Collapsing duplicate references is safe only when the physical build row is unobservable. Merely
            // having no non-key build columns is not enough: a projected build key still needs a valid reference
            // into its originating batch, particularly when a multi-batch build feeds another join.
            this.compressDuplicateReferences = buildRowReferencesUnused && policy.compressKeyOnlyDuplicates();
            this.expectedBuildRows = expectedSize;
            int initialExpectedSize = capInitialHash ? Math.min(expectedSize, policy.initialHashExpectedCap()) : expectedSize;
            int capacity = 16;
            while (capacity < initialExpectedSize / LOAD_FACTOR) {
                capacity <<= 1;
            }
            hashTable = new LongJoinHashTable(
                    arrayPool,
                    capacity,
                    policy.groupedLongHashTable() && groupedHashTable,
                    lazyDuplicateSlotState,
                    EMPTY);
            // Capping the hash table protects sparse/duplicate builds from speculative over-allocation, but a
            // payload-bearing build stores one row reference and one chain link for every buffered row regardless
            // of key cardinality. Its exact row count is already known, so sizing these append-only arrays from the
            // hash cap only forces geometric growth and temporarily retains every obsolete generation in the
            // engine-owned primitive pool.
            int initialRows = Math.max(16,
                    compressDuplicateReferences && policy.sizeCompressedRowsByDistinctKeys()
                            ? initialExpectedSize
                            : capInitialHash && policy.preSizeCappedRowStorage() ? expectedSize : initialExpectedSize);
            rows = new JoinRowStore(
                    arrayPool,
                    initialRows,
                    capInitialHash && policy.compactChainRowReferences(),
                    implicitSequentialRowReferences,
                    policy.compactDirectRowReferences(),
                    !lazyDuplicateSlotState || !policy.lazyUniqueChainState(),
                    EMPTY);
            this.buildRowReferencesUnused = buildRowReferencesUnused;
            this.batchBuild = batchBuild;
            this.ownsStorage = true;
            if (policy.debugJoinIndex() && directRangeBuild) {
                System.err.printf("[direct-range-build] expected=%d%n", expectedSize);
            }
            if (capInitialHash || directRangeBuild) {
                int directCapacity = policy.directRangeBuildInitialCapacity();
                if (directRangeBuild) {
                    long required = Math.min((long) policy.maxDirectBuildKey(), (long) expectedSize + 1);
                    while (directCapacity < required) {
                        directCapacity <<= 1;
                    }
                }
                directBuild.initialize(directCapacity);
                denseSequence.disableKeyCandidate();
            }
        }

        private LongJoinIndex(LongJoinIndex prepared)
        {
            this.policy = prepared.policy;
            this.outputPolicy = prepared.outputPolicy;
            this.executionPolicy = prepared.executionPolicy;
            this.arrayPool = prepared.arrayPool;
            this.hashTable = prepared.hashTable;
            this.rows = prepared.rows;
            this.buildCardinality = prepared.buildCardinality;
            this.buildRowReferencesUnused = prepared.buildRowReferencesUnused;
            this.batchBuild = prepared.batchBuild;
            this.minKey = prepared.minKey;
            this.maxKey = prepared.maxKey;
            this.finalized = prepared.finalized;
            this.directLookup = prepared.directLookup;
            this.sparseMembership = prepared.sparseMembership;
            this.denseSequence = new DenseJoinSequence(prepared.denseSequence);
            this.compactChains = prepared.compactChains;
            this.compressDuplicateReferences = prepared.compressDuplicateReferences;
            this.expectedBuildRows = prepared.expectedBuildRows;
            this.directBuild = prepared.directBuild;
            this.compactedRows = prepared.compactedRows;
            this.compressedRanges = prepared.compressedRanges;
            this.ownsStorage = false;
        }

        @Override
        long retainedBytes()
        {
            if (!ownsStorage) {
                return denseSequence.retainedBytes();
            }
            long bytes = Math.addExact(hashTable.retainedBytes(), rows.retainedBytes());
            bytes = Math.addExact(bytes, directLookup.retainedBytes());
            bytes = Math.addExact(bytes, sparseMembership.retainedBytes());
            bytes = Math.addExact(bytes, denseSequence.retainedBytes());
            bytes = Math.addExact(bytes, directBuild.retainedBytes());
            bytes = Math.addExact(bytes, compactedRows.retainedBytes());
            return Math.addExact(bytes, compressedRanges.retainedBytes());
        }

        @Override
        public boolean isEmpty()
        {
            return buildCardinality.isEmpty();
        }

        @Override
        public void add(Vector[] values, Vector[] nulls, int position, long rowReference)
        {
            if (JoinIndex.hasNull(nulls, position)) {
                return;
            }
            addNoNulls(values, position, rowReference);
        }

        @Override
        public void addNoNulls(Vector[] values, int position, long rowReference)
        {
            addRow(OperatorVectorSupport.longValue(values[0], position), rowReference);
        }

        @Override
        boolean addBuildRows(
                Vector[] valuesArray,
                Vector[] nullsArray,
                boolean hasNulls,
                BufferedJoinInput.InnerBatch batch,
                int startPosition,
                int length,
                int batchIndex)
        {
            if (!batchBuild) {
                return false;
            }
            Vector values = valuesArray[0];
            Vector nulls = nullsArray[0];
            VectorAccess.LongValues longValues = VectorAccess.longValues(values);
            VectorAccess.BooleanValues nullValues = hasNulls ? VectorAccess.booleanValues(nulls) : null;
            int endPosition = startPosition + length;
            int[] sourcePositions = batch.positions();
            if (useCompressedDirectBuildBatchLoop()) {
                rows.observeReferenceRange(batchIndex, endPosition - 1);
                addCompressedDirectRangeRows(
                        longValues,
                        nullValues,
                        sourcePositions,
                        startPosition,
                        endPosition,
                        (long) batchIndex << Integer.SIZE);
                return true;
            }
            if (sourcePositions == null) {
                for (int position = startPosition; position < endPosition; position++) {
                    if (nullValues == null || !nullValues.value(position)) {
                        addRow(longValues.value(position), JoinRowReference.pack(batchIndex, position));
                    }
                }
                return true;
            }
            for (int position = startPosition; position < endPosition; position++) {
                int sourcePosition = sourcePositions[position];
                if (nullValues == null || !nullValues.value(sourcePosition)) {
                    addRow(longValues.value(sourcePosition), JoinRowReference.pack(batchIndex, position));
                }
            }
            return true;
        }

        @Override
        boolean addBuildRows(
                Vector[] valuesArray,
                Vector[] nullsArray,
                boolean hasNulls,
                Mask mask,
                int batchIndex)
        {
            if (!batchBuild) {
                return false;
            }
            Vector values = valuesArray[0];
            Vector nulls = nullsArray[0];
            VectorAccess.LongValues longValues = VectorAccess.longValues(values);
            VectorAccess.BooleanValues nullValues = hasNulls ? VectorAccess.booleanValues(nulls) : null;
            int count = mask.count();
            if (useCompressedDirectBuildBatchLoop()) {
                rows.observeReferenceRange(batchIndex, count - 1);
                addCompressedDirectRangeRows(longValues, nullValues, mask, count, (long) batchIndex << Integer.SIZE);
                return true;
            }
            for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
                int sourcePosition = mask.all() ? logicalPosition : mask.position(logicalPosition);
                if (nullValues == null || !nullValues.value(sourcePosition)) {
                    addRow(longValues.value(sourcePosition), JoinRowReference.pack(batchIndex, logicalPosition));
                }
            }
            return true;
        }

        private boolean useCompressedDirectBuildBatchLoop()
        {
            // Once the dense duplicate arrays exist, sparse state can no longer be admitted. Select the compact
            // key-only loop once per source batch instead of carrying generic and sparse representation branches
            // through every remaining build row.
            return policy.compressedDirectBuildBatchLoop() && directBuild.isActive() && compressDuplicateReferences && directBuild.denseDuplicatesAllocated();
        }

        private void addCompressedDirectRangeRows(
                VectorAccess.LongValues values,
                VectorAccess.BooleanValues nulls,
                int[] sourcePositions,
                int startPosition,
                int endPosition,
                long rowReferenceBase)
        {
            if (sourcePositions == null) {
                addCompressedDirectRangeDenseRows(values, nulls, startPosition, endPosition, rowReferenceBase);
                return;
            }
            for (int position = startPosition; position < endPosition; position++) {
                int sourcePosition = sourcePositions[position];
                if (nulls == null || !nulls.value(sourcePosition)) {
                    addCompressedDirectRangeRow(values.value(sourcePosition), rowReferenceBase + position);
                }
            }
        }

        private void addCompressedDirectRangeRows(
                VectorAccess.LongValues values,
                VectorAccess.BooleanValues nulls,
                Mask mask,
                int count,
                long rowReferenceBase)
        {
            if (mask.all()) {
                addCompressedDirectRangeDenseRows(values, nulls, 0, count, rowReferenceBase);
                return;
            }
            for (int logicalPosition = 0; logicalPosition < count; logicalPosition++) {
                int sourcePosition = mask.position(logicalPosition);
                if (nulls == null || !nulls.value(sourcePosition)) {
                    addCompressedDirectRangeRow(values.value(sourcePosition), rowReferenceBase + logicalPosition);
                }
            }
        }

        private void addCompressedDirectRangeDenseRows(
                VectorAccess.LongValues values,
                VectorAccess.BooleanValues nulls,
                int startPosition,
                int endPosition,
                long rowReferenceBase)
        {
            for (int position = startPosition; position < endPosition; position++) {
                if (nulls != null && nulls.value(position)) {
                    continue;
                }
                long key = values.value(position);
                long rowReference = rowReferenceBase + position;
                if (key < 0 || key >= policy.maxDirectBuildKey()) {
                    addRow(key, rowReference);
                    continue;
                }
                minKey = Math.min(minKey, key);
                maxKey = Math.max(maxKey, key);
                directBuild.recordRow();
                int intKey = (int) key;
                ensureDirectBuildCapacity(intKey + 1);
                int head = directBuild.entry(intKey);
                if (head != EMPTY) {
                    directBuild.incrementDenseDuplicate(intKey);
                    continue;
                }
                int ordinal = buildCardinality.storedRowCount();
                rows.append(ordinal, rowReference);
                buildCardinality.recordStoredRow();
                directBuild.initializeKey(intKey, ordinal);
                buildCardinality.recordDistinctKey();
            }
        }

        private void addCompressedDirectRangeRow(long key, long rowReference)
        {
            if (key < 0 || key >= policy.maxDirectBuildKey()) {
                addRow(key, rowReference);
                return;
            }
            minKey = Math.min(minKey, key);
            maxKey = Math.max(maxKey, key);
            directBuild.recordRow();
            int intKey = (int) key;
            ensureDirectBuildCapacity(intKey + 1);
            int head = directBuild.entry(intKey);
            if (head != EMPTY) {
                directBuild.incrementDenseDuplicate(intKey);
                return;
            }
            int ordinal = buildCardinality.storedRowCount();
            rows.append(ordinal, rowReference);
            buildCardinality.recordStoredRow();
            directBuild.initializeKey(intKey, ordinal);
            buildCardinality.recordDistinctKey();
        }

        @Override
        public LongList matches(Vector[] values, Vector[] nulls, int position)
        {
            if (JoinIndex.hasNull(nulls, position)) {
                return LongLists.emptyList();
            }
            return matchesNoNulls(values, position);
        }

        @Override
        public LongList matchesNoNulls(Vector[] values, int position)
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return rowsForKey(OperatorVectorSupport.longValue(values[0], position), matchScratch.scalarSingle(), matchScratch.scalarChain());
        }

        @Override
        public void matchRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            matchScratch.prepareBatch(executionPolicy.maxBatchRows());
            Vector values = valuesArray[0];
            Vector nulls = nullsArray == null ? null : nullsArray[0];
            if (!hasNulls || nulls == null) {
                // Probe key is null-free: skip the per-row null read entirely.
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchLongRowsNullFree(longValues.values(), positions, positionCount, matches, singleMatches);
                    case org.weakref.nitro.data.I32Vector intValues -> matchIntRowsNullFree(intValues.values(), positions, positionCount, matches, singleMatches);
                    case DictionaryVector dictionary -> matchDictionaryRowsNullFree(dictionary, positions, positionCount, matches, singleMatches);
                    case org.weakref.nitro.data.RleVector rle -> matchRleRows(rle, VectorAccess.booleanValues(null), positions, positionCount, matches, singleMatches);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            matches[index] = rowsForKey(rowValues.value(position), singleMatches[index], matchScratch.batchChain(index));
                        }
                    }
                }
                return;
            }
            VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> matchLongRows(longValues.values(), nullValues, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.I32Vector intValues -> matchIntRows(intValues.values(), nullValues, positions, positionCount, matches, singleMatches);
                case DictionaryVector dictionary -> matchDictionaryRows(dictionary, nullValues, positions, positionCount, matches, singleMatches);
                case org.weakref.nitro.data.RleVector rle -> matchRleRows(rle, nullValues, positions, positionCount, matches, singleMatches);
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(rowValues.value(position), singleMatches[index], matchScratch.batchChain(index));
                        }
                    }
                }
            }
        }

        @Override
        public boolean supportsRowRanges()
        {
            if (!outputPolicy.directCompactedRangeOutput()) {
                return false;
            }
            if (!finalized) {
                finalizeForProbe(executionPolicy.maxBatchRows());
            }
            return compactedRows.isBuilt();
        }

        @Override
        public boolean matchRowRanges(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, int[] starts, int[] counts)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!compactedRows.isBuilt()) {
                return false;
            }
            VectorAccess.LongValues values = VectorAccess.longValues(valuesArray[0]);
            Vector nulls = nullsArray == null ? null : nullsArray[0];
            VectorAccess.BooleanValues nullValues = hasNulls && nulls != null ? VectorAccess.booleanValues(nulls) : null;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues != null && nullValues.value(position)) {
                    starts[index] = 0;
                    counts[index] = 0;
                    continue;
                }
                setRowRange(values.value(position), index, starts, counts);
            }
            return true;
        }

        private void setRowRange(long key, int index, int[] starts, int[] counts)
        {
            if (compressedRanges.isBuilt()) {
                int entry = compressedRanges.entry(key);
                starts[index] = CompressedLongRangeIndex.start(entry);
                counts[index] = CompressedLongRangeIndex.count(entry);
                return;
            }
            if (!sparseRangeContains(key)) {
                starts[index] = 0;
                counts[index] = 0;
                return;
            }
            int slot = hashTable.findSlot(key);
            int head = hashTable.head(slot);
            if (head == EMPTY) {
                starts[index] = 0;
                counts[index] = 0;
                return;
            }
            starts[index] = compactedRows.start(slot);
            counts[index] = hashTable.count(slot);
        }

        @Override
        public void copyRowRange(int start, long[] output, int outputOffset, int length)
        {
            compactedRows.copy(start, output, outputOffset, length);
        }

        private void matchLongRowsNullFree(long[] values, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                matches[index] = rowsForKey(values[position], singleMatches[index], matchScratch.batchChain(index));
            }
        }

        private void matchIntRowsNullFree(int[] values, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                matches[index] = rowsForKey(values[position], singleMatches[index], matchScratch.batchChain(index));
            }
        }

        private void matchDictionaryRowsNullFree(DictionaryVector values, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], matchScratch.batchChain(index));
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], matchScratch.batchChain(index));
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        matches[index] = rowsForKey(dictionaryValues.value(ids[position]), singleMatches[index], matchScratch.batchChain(index));
                    }
                }
            }
        }

        @Override
        public boolean supportsSingleMatchRefs()
        {
            return !hasDuplicates();
        }

        @Override
        public void matchSingleRows(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            Vector values = valuesArray[0];
            Vector nulls = nullsArray == null ? null : nullsArray[0];
            if (policy.denseSingleBatchProbeSpecialization() && denseSequence.referencesActive()) {
                matchDenseSingleBatchRows(values, nulls, hasNulls, positions, positionCount, refs);
                return;
            }
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchSingleLongRows(longValues.values(), nullValues, positions, positionCount, refs);
                    case org.weakref.nitro.data.I32Vector intValues -> matchSingleIntRows(intValues.values(), nullValues, positions, positionCount, refs);
                    case DictionaryVector dictionary -> matchSingleDictionaryRows(dictionary, nullValues, positions, positionCount, refs);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(rowValues.value(position));
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        refs[index] = singleRef(vv[positions[index]]);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        refs[index] = singleRef(vv[positions[index]]);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                refs[index] = singleRef(dv[ids[positions[index]]]);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                refs[index] = singleRef(dv[ids[positions[index]]]);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                refs[index] = singleRef(dv.value(ids[positions[index]]));
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    for (int index = 0; index < positionCount; index++) {
                        refs[index] = singleRef(rowValues.value(positions[index]));
                    }
                }
            }
        }

        @Override
        public boolean supportsCompactSingleMatchRefs()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return policy.compactDenseSingleMatchReferences() && denseSequence.referencesActive() && rows.referencesFit32();
        }

        @Override
        public boolean supportsSingleMatchPositions()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return policy.denseSingleBatchMatchPositions() && denseSequence.referencesActive();
        }

        @Override
        public boolean supportsSingleMatchPositionRange()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return policy.denseSingleBatchRangeProbe() && denseSequence.referencesActive();
        }

        @Override
        public int singleMatchPositionBatchIndex()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            if (!denseSequence.referencesActive()) {
                throw new IllegalStateException("Position single-match refs require dense single-batch row references");
            }
            return denseSequence.referenceBatchIndex();
        }

        @Override
        public void matchSingleRowsPositions(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, int[] logicalPositions)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!denseSequence.referencesActive()) {
                throw new IllegalStateException("Position single-match refs require dense single-batch row references");
            }
            matchDenseSingleBatchRowsPositions(valuesArray[0], nullsArray == null ? null : nullsArray[0], hasNulls, positions, positionCount, logicalPositions);
        }

        @Override
        public void matchSingleRowsPositionsRange(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int startPosition, int positionCount, int[] logicalPositions)
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            if (!denseSequence.referencesActive()) {
                throw new IllegalStateException("Range position single-match refs require dense single-batch row references");
            }
            matchDenseSingleBatchRowsPositionsRange(valuesArray[0], nullsArray == null ? null : nullsArray[0], hasNulls, startPosition, positionCount, logicalPositions);
        }

        @Override
        public boolean supportsDirectSingleMatchPositionRangeOutput()
        {
            if (!finalized) {
                finalizeForProbe(1);
            }
            return denseSequence.referencesActive();
        }

        @Override
        public int emitSingleRowsPositionsRange(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!denseSequence.referencesActive()) {
                throw new IllegalStateException("Direct range output requires dense single-batch row references");
            }
            return emitDenseSingleBatchRowsPositionsRange(
                    valuesArray[0],
                    nullsArray == null ? null : nullsArray[0],
                    hasNulls,
                    startPosition,
                    positionCount,
                    outputOuterPositions,
                    outputInnerLogicalPositions,
                    outputStart);
        }

        @Override
        public void matchSingleRowsCompact(Vector[] valuesArray, Vector[] nullsArray, boolean hasNulls, int[] positions, int positionCount, int[] refs)
        {
            if (!finalized) {
                finalizeForProbe(positionCount);
            }
            if (!denseSequence.referencesActive() || !rows.referencesFit32()) {
                throw new IllegalStateException("Compact single-match refs require dense single-batch row references that fit 32 bits");
            }
            matchDenseSingleBatchRowsCompact(valuesArray[0], nullsArray == null ? null : nullsArray[0], hasNulls, positions, positionCount, refs);
        }

        @Override
        public long unpackCompactSingleMatchRef(int ref)
        {
            return ref == NO_MATCH_COMPACT_ROW_REFERENCE ? NO_MATCH_ROW_REFERENCE : JoinRowReference.unpackCompact(ref);
        }

        private void matchDenseSingleBatchRowsPositions(Vector values, Vector nulls, boolean hasNulls, int[] positions, int positionCount, int[] logicalPositions)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRowsPositions(longValues.values(), nullValues, positions, positionCount, logicalPositions);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRowsPositions(intValues.values(), nullValues, positions, positionCount, logicalPositions);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRowsPositions(dictionary, nullValues, positions, positionCount, logicalPositions);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int firstPosition = denseSequence.firstReferencePosition();
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            if (nullValues.value(position)) {
                                logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[positions[index]]);
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(positions[index]);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchRowsPositionsRange(Vector values, Vector nulls, boolean hasNulls, int startPosition, int positionCount, int[] logicalPositions)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRowsPositionsRange(longValues.values(), nullValues, startPosition, positionCount, logicalPositions);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRowsPositionsRange(intValues.values(), nullValues, startPosition, positionCount, logicalPositions);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRowsPositionsRange(dictionary, nullValues, startPosition, positionCount, logicalPositions);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int firstPosition = denseSequence.firstReferencePosition();
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[startPosition + index];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[startPosition + index];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[startPosition + index]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[startPosition + index]];
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[startPosition + index]);
                                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(startPosition + index);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private int emitDenseSingleBatchRowsPositionsRange(Vector values, Vector nulls, boolean hasNulls, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                return switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> emitDenseSingleBatchLongRowsPositionsRange(longValues.values(), nullValues, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                    case org.weakref.nitro.data.I32Vector intValues -> emitDenseSingleBatchIntRowsPositionsRange(intValues.values(), nullValues, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                    case DictionaryVector dictionary -> emitDenseSingleBatchDictionaryRowsPositionsRange(dictionary, nullValues, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int firstPosition = denseSequence.firstReferencePosition();
                        int output = outputStart;
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = denseSingleBatchRowPositionForKey(rowValues.value(position), min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                        yield output - outputStart;
                    }
                };
            }
            return switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> emitDenseSingleBatchLongRowsPositionsRange(longValues.values(), null, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                case org.weakref.nitro.data.I32Vector intValues -> emitDenseSingleBatchIntRowsPositionsRange(intValues.values(), null, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                case DictionaryVector dictionary -> emitDenseSingleBatchDictionaryRowsPositionsRange(dictionary, null, startPosition, positionCount, outputOuterPositions, outputInnerLogicalPositions, outputStart);
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int firstPosition = denseSequence.firstReferencePosition();
                    int output = outputStart;
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        int logicalPosition = denseSingleBatchRowPositionForKey(rowValues.value(position), min, max, firstPosition);
                        if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                            outputOuterPositions[output] = position;
                            outputInnerLogicalPositions[output] = logicalPosition;
                            output++;
                        }
                    }
                    yield output - outputStart;
                }
            };
        }

        private void matchDenseSingleBatchRows(Vector values, Vector nulls, boolean hasNulls, int[] positions, int positionCount, long[] refs)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRows(longValues.values(), nullValues, positions, positionCount, refs);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRows(intValues.values(), nullValues, positions, positionCount, refs);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRows(dictionary, nullValues, positions, positionCount, refs);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        long base = denseSequence.referenceBase();
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            if (nullValues.value(position)) {
                                refs[index] = NO_MATCH_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    long base = denseSequence.referenceBase();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    long base = denseSequence.referenceBase();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    long base = denseSequence.referenceBase();
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[positions[index]]);
                                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    long base = denseSequence.referenceBase();
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(positions[index]);
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
            }
        }

        private void matchDenseSingleBatchRowsCompact(Vector values, Vector nulls, boolean hasNulls, int[] positions, int positionCount, int[] refs)
        {
            if (hasNulls && nulls != null) {
                VectorAccess.BooleanValues nullValues = VectorAccess.booleanValues(nulls);
                switch (values) {
                    case org.weakref.nitro.data.I64Vector longValues -> matchDenseSingleBatchLongRowsCompact(longValues.values(), nullValues, positions, positionCount, refs);
                    case org.weakref.nitro.data.I32Vector intValues -> matchDenseSingleBatchIntRowsCompact(intValues.values(), nullValues, positions, positionCount, refs);
                    case DictionaryVector dictionary -> matchDenseSingleBatchDictionaryRowsCompact(dictionary, nullValues, positions, positionCount, refs);
                    default -> {
                        VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                        long min = minKey;
                        long max = maxKey;
                        int batch = denseSequence.referenceBatchIndex();
                        int firstPosition = denseSequence.firstReferencePosition();
                        for (int index = 0; index < positionCount; index++) {
                            int position = positions[index];
                            if (nullValues.value(position)) {
                                refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                                continue;
                            }
                            long key = rowValues.value(position);
                            refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                        }
                    }
                }
                return;
            }
            switch (values) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] vv = longValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseSequence.referenceBatchIndex();
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] vv = intValues.values();
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseSequence.referenceBatchIndex();
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = vv[positions[index]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                case DictionaryVector dictionary -> {
                    int[] ids = dictionary.ids();
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseSequence.referenceBatchIndex();
                    int firstPosition = denseSequence.firstReferencePosition();
                    switch (dictionary.values()) {
                        case org.weakref.nitro.data.I64Vector lv -> {
                            long[] dv = lv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                            }
                        }
                        case org.weakref.nitro.data.I32Vector iv -> {
                            int[] dv = iv.values();
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv[ids[positions[index]]];
                                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                            }
                        }
                        default -> {
                            VectorAccess.LongValues dv = VectorAccess.longValues(dictionary.values());
                            for (int index = 0; index < positionCount; index++) {
                                long key = dv.value(ids[positions[index]]);
                                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
                    long min = minKey;
                    long max = maxKey;
                    int batch = denseSequence.referenceBatchIndex();
                    int firstPosition = denseSequence.firstReferencePosition();
                    for (int index = 0; index < positionCount; index++) {
                        long key = rowValues.value(positions[index]);
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchLongRows(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            long min = minKey;
            long max = maxKey;
            long base = denseSequence.referenceBase();
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
            }
        }

        private void matchDenseSingleBatchIntRows(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            long min = minKey;
            long max = maxKey;
            long base = denseSequence.referenceBase();
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
            }
        }

        private void matchDenseSingleBatchDictionaryRows(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            long base = denseSequence.referenceBase();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        refs[index] = denseSingleBatchRowReferenceForKey(key, min, max, base);
                    }
                }
            }
        }

        private int emitDenseSingleBatchLongRowsPositionsRange(long[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            int output = outputStart;
            if (nullValues == null) {
                for (int index = 0; index < positionCount; index++) {
                    int position = startPosition + index;
                    int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                    if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                        outputOuterPositions[output] = position;
                        outputInnerLogicalPositions[output] = logicalPosition;
                        output++;
                    }
                }
                return output - outputStart;
            }
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    continue;
                }
                int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                    outputOuterPositions[output] = position;
                    outputInnerLogicalPositions[output] = logicalPosition;
                    output++;
                }
            }
            return output - outputStart;
        }

        private int emitDenseSingleBatchIntRowsPositionsRange(int[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            int output = outputStart;
            if (nullValues == null) {
                for (int index = 0; index < positionCount; index++) {
                    int position = startPosition + index;
                    int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                    if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                        outputOuterPositions[output] = position;
                        outputInnerLogicalPositions[output] = logicalPosition;
                        output++;
                    }
                }
                return output - outputStart;
            }
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    continue;
                }
                int logicalPosition = denseSingleBatchRowPositionForKey(values[position], min, max, firstPosition);
                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                    outputOuterPositions[output] = position;
                    outputInnerLogicalPositions[output] = logicalPosition;
                    output++;
                }
            }
            return output - outputStart;
        }

        private int emitDenseSingleBatchDictionaryRowsPositionsRange(DictionaryVector values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] outputOuterPositions, int[] outputInnerLogicalPositions, int outputStart)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            int output = outputStart;
            if (nullValues == null) {
                switch (values.values()) {
                    case org.weakref.nitro.data.I64Vector longValues -> {
                        long[] dictionaryValues = longValues.values();
                        if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                            int[] dictionaryPositions = denseSequence.dictionaryPositions(dictionaryValues, min, max);
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = dictionaryPositions[ids[position]];
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                        else {
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                    }
                    case org.weakref.nitro.data.I32Vector intValues -> {
                        int[] dictionaryValues = intValues.values();
                        if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                            int[] dictionaryPositions = denseSequence.dictionaryPositions(dictionaryValues, min, max);
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = dictionaryPositions[ids[position]];
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                        else {
                            for (int index = 0; index < positionCount; index++) {
                                int position = startPosition + index;
                                int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                                if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                    outputOuterPositions[output] = position;
                                    outputInnerLogicalPositions[output] = logicalPosition;
                                    output++;
                                }
                            }
                        }
                    }
                    default -> {
                        VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues.value(ids[position]), min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                }
                return output - outputStart;
            }
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                        int[] dictionaryPositions = denseSequence.dictionaryPositions(dictionaryValues, min, max);
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = dictionaryPositions[ids[position]];
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                    else {
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    if (useDenseDictionaryProbeCache(dictionaryValues.length, positionCount)) {
                        int[] dictionaryPositions = denseSequence.dictionaryPositions(dictionaryValues, min, max);
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = dictionaryPositions[ids[position]];
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                    else {
                        for (int index = 0; index < positionCount; index++) {
                            int position = startPosition + index;
                            if (nullValues.value(position)) {
                                continue;
                            }
                            int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues[ids[position]], min, max, firstPosition);
                            if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                                outputOuterPositions[output] = position;
                                outputInnerLogicalPositions[output] = logicalPosition;
                                output++;
                            }
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            continue;
                        }
                        int logicalPosition = denseSingleBatchRowPositionForKey(dictionaryValues.value(ids[position]), min, max, firstPosition);
                        if (logicalPosition != NO_MATCH_COMPACT_ROW_REFERENCE) {
                            outputOuterPositions[output] = position;
                            outputInnerLogicalPositions[output] = logicalPosition;
                            output++;
                        }
                    }
                }
            }
            return output - outputStart;
        }

        private boolean useDenseDictionaryProbeCache(int dictionarySize, int positionCount)
        {
            return policy.denseDictionaryProbeCache() && dictionarySize * 2 <= positionCount;
        }

        private void matchDenseSingleBatchLongRowsPositions(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchIntRowsPositions(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchDictionaryRowsPositions(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] logicalPositions)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchLongRowsPositionsRange(long[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchIntRowsPositionsRange(int[] values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] logicalPositions)
        {
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            for (int index = 0; index < positionCount; index++) {
                int position = startPosition + index;
                if (nullValues.value(position)) {
                    logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
            }
        }

        private void matchDenseSingleBatchDictionaryRowsPositionsRange(DictionaryVector values, VectorAccess.BooleanValues nullValues, int startPosition, int positionCount, int[] logicalPositions)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int firstPosition = denseSequence.firstReferencePosition();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = startPosition + index;
                        if (nullValues.value(position)) {
                            logicalPositions[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        logicalPositions[index] = denseSingleBatchRowPositionForKey(key, min, max, firstPosition);
                    }
                }
            }
        }

        private void matchDenseSingleBatchLongRowsCompact(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] refs)
        {
            long min = minKey;
            long max = maxKey;
            int batch = denseSequence.referenceBatchIndex();
            int firstPosition = denseSequence.firstReferencePosition();
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
            }
        }

        private void matchDenseSingleBatchIntRowsCompact(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] refs)
        {
            long min = minKey;
            long max = maxKey;
            int batch = denseSequence.referenceBatchIndex();
            int firstPosition = denseSequence.firstReferencePosition();
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                    continue;
                }
                long key = values[position];
                refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
            }
        }

        private void matchDenseSingleBatchDictionaryRowsCompact(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, int[] refs)
        {
            int[] ids = values.ids();
            long min = minKey;
            long max = maxKey;
            int batch = denseSequence.referenceBatchIndex();
            int firstPosition = denseSequence.firstReferencePosition();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues[ids[position]];
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            refs[index] = NO_MATCH_COMPACT_ROW_REFERENCE;
                            continue;
                        }
                        long key = dictionaryValues.value(ids[position]);
                        refs[index] = denseSingleBatchRowReference32ForKey(key, min, max, batch, firstPosition);
                    }
                }
            }
        }

        private void matchSingleLongRows(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(values[position]);
            }
        }

        private void matchSingleIntRows(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(values[position]);
            }
        }

        private void matchSingleDictionaryRows(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, long[] refs)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(dictionaryValues[ids[position]]);
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(dictionaryValues[ids[position]]);
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        refs[index] = nullValues.value(position) ? NO_MATCH_ROW_REFERENCE : singleRef(dictionaryValues.value(ids[position]));
                    }
                }
            }
        }

        private long singleRef(long key)
        {
            if (directBuild.isActive()) {
                if (key < 0 || key >= directBuild.capacity()) {
                    return NO_MATCH_ROW_REFERENCE;
                }
                int entry = directBuild.entry((int) key);
                return entry == EMPTY ? NO_MATCH_ROW_REFERENCE : rows.referenceAt(directEntryHead(entry));
            }
            if (directLookup.isActive()) {
                return directLookup.reference(key, denseSequence);
            }
            if (!sparseRangeContains(key)) {
                return NO_MATCH_ROW_REFERENCE;
            }
            int slot = hashTable.findSlot(key);
            int head = hashTable.head(slot);
            return head == EMPTY ? NO_MATCH_ROW_REFERENCE : rows.referenceAt(head);
        }

        private static long denseSingleBatchRowReferenceForKey(long key, long minKey, long maxKey, long base)
        {
            if (key < minKey || key > maxKey) {
                return NO_MATCH_ROW_REFERENCE;
            }
            return base + (key - minKey);
        }

        private static int denseSingleBatchRowPositionForKey(long key, long minKey, long maxKey, int firstPosition)
        {
            if (key < minKey || key > maxKey) {
                return NO_MATCH_COMPACT_ROW_REFERENCE;
            }
            return (int) (firstPosition + (key - minKey));
        }

        private static int denseSingleBatchRowReference32ForKey(long key, long minKey, long maxKey, int batchIndex, int firstPosition)
        {
            if (key < minKey || key > maxKey) {
                return NO_MATCH_COMPACT_ROW_REFERENCE;
            }
            int rowPosition = (int) (firstPosition + (key - minKey));
            return (batchIndex << Short.SIZE) | (rowPosition & JoinRowReference.MAX_COMPACT_POSITION);
        }

        private void addRow(long key, long rowReference)
        {
            rows.observeReference(rowReference);
            if (key < minKey) {
                minKey = key;
            }
            if (key > maxKey) {
                maxKey = key;
            }
            if (directBuild.isActive()) {
                if (key >= 0 && key < policy.maxDirectBuildKey()) {
                    addDirectRangeRow((int) key, rowReference);
                    return;
                }
                materializeDirectRangeBuildAsHash();
            }
            if (denseSequence.keyCandidate()) {
                if (denseSequence.acceptsKey(key, buildCardinality.storedRowCount())) {
                    appendDenseRow(key, rowReference);
                    return;
                }
                materializeDenseBuildAsHash();
                denseSequence.reject();
            }
            int slot = hashTable.findSlot(key);
            boolean newKey = !hashTable.isOccupied(slot);
            if (!newKey) {
                hashTable.ensureDuplicateState();
                if (compressDuplicateReferences) {
                    hashTable.incrementCount(slot);
                    return;
                }
                rows.ensureChainState(buildCardinality.storedRowCount());
            }
            int ordinal = buildCardinality.storedRowCount();
            rows.append(ordinal, rowReference);
            buildCardinality.recordStoredRow();
            if (newKey) {
                hashTable.initialize(slot, key, ordinal);
                buildCardinality.recordDistinctKey();
                // Rehash after the slot is populated so it carries a non-empty head into the new table.
                hashTable.growIfNeeded(buildCardinality.distinctKeyCount());
                return;
            }
            // Append at the tail to preserve insertion (FIFO) order within a key.
            rows.link(hashTable.append(slot, ordinal), ordinal);
        }

        private void addDirectRangeRow(int key, long rowReference)
        {
            directBuild.recordRow();
            ensureDirectBuildCapacity(key + 1);
            int entry = directBuild.entry(key);
            if (entry != EMPTY) {
                addDirectRangeDuplicate(key, entry, rowReference);
                return;
            }
            int ordinal = buildCardinality.storedRowCount();
            rows.append(ordinal, rowReference);
            buildCardinality.recordStoredRow();
            directBuild.initializeKey(key, ordinal);
            buildCardinality.recordDistinctKey();
        }

        private void addDirectRangeDuplicate(int key, int entry, long rowReference)
        {
            if (useSparseDirectDuplicateState()) {
                addSparseDirectRangeDuplicate(key, entry, rowReference);
                return;
            }
            if (compressDuplicateReferences) {
                directBuild.incrementDenseDuplicate(key);
                return;
            }
            rows.ensureChainState(buildCardinality.storedRowCount());
            int ordinal = buildCardinality.storedRowCount();
            rows.append(ordinal, rowReference);
            buildCardinality.recordStoredRow();
            rows.link(directBuild.appendDenseDuplicate(key, entry, ordinal), ordinal);
        }

        private boolean useSparseDirectDuplicateState()
        {
            return directBuild.admitSparseDuplicates(expectedBuildRows);
        }

        private void addSparseDirectRangeDuplicate(int key, int entry, long rowReference)
        {
            int groupEntry = directBuild.promoteSparseDuplicate(key, entry);
            if (compressDuplicateReferences) {
                directBuild.incrementSparseDuplicate(groupEntry);
                return;
            }
            rows.ensureChainState(buildCardinality.storedRowCount());
            int ordinal = buildCardinality.storedRowCount();
            rows.append(ordinal, rowReference);
            buildCardinality.recordStoredRow();
            rows.link(directBuild.appendSparseDuplicate(groupEntry, ordinal), ordinal);
        }

        private int directEntryHead(int entry)
        {
            return directBuild.entryHead(entry);
        }

        private int directEntryTail(int key, int entry)
        {
            return directBuild.entryTail(key, entry);
        }

        private int directEntryCount(int key, int entry)
        {
            return directBuild.entryCount(key, entry);
        }

        private void ensureDirectBuildCapacity(int required)
        {
            directBuild.ensureCapacity(required);
        }

        private void materializeDirectRangeBuildAsHash()
        {
            if (hasDuplicates()) {
                hashTable.ensureDuplicateState();
            }
            buildCardinality.resetDistinctKeyCount();
            for (int key = 0; key < directBuild.capacity(); key++) {
                int entry = directBuild.entry(key);
                if (entry == EMPTY) {
                    continue;
                }
                int head = directEntryHead(entry);
                int slot = hashTable.findSlot(key);
                int count = directEntryCount(key, entry);
                hashTable.initialize(
                        slot,
                        key,
                        head,
                        compressDuplicateReferences ? head : directEntryTail(key, entry),
                        count);
                buildCardinality.recordDistinctKey();
                hashTable.growIfNeeded(buildCardinality.distinctKeyCount());
            }
            releaseDirectBuildArrays();
        }

        private void appendDenseRow(long key, long rowReference)
        {
            int ordinal = buildCardinality.storedRowCount();
            denseSequence.observeDenseRow(key, rowReference, ordinal);
            rows.append(ordinal, rowReference);
            buildCardinality.recordStoredRow();
            buildCardinality.recordDistinctKey();
        }

        private void materializeDenseBuildAsHash()
        {
            int previousRows = buildCardinality.storedRowCount();
            long key = denseSequence.firstKey();
            buildCardinality.resetDistinctKeyCount();
            for (int ordinal = 0; ordinal < previousRows; ordinal++) {
                int slot = hashTable.findSlot(key++);
                hashTable.initialize(slot, key - 1, ordinal);
                buildCardinality.recordDistinctKey();
                hashTable.growIfNeeded(buildCardinality.distinctKeyCount());
            }
        }

        private LongList rowsForSlot(int slot, SingleLongList single, ChainLongList chain)
        {
            int head = hashTable.head(slot);
            if (head == EMPTY) {
                return LongLists.emptyList();
            }
            int count = hashTable.count(slot);
            if (compactedRows.isBuilt()) {
                return compactedRows.rows(compactedRows.start(slot), count, single, chain);
            }
            if (count == 1) {
                return single.withValue(rows.referenceAt(head));
            }
            if (compressDuplicateReferences) {
                return chain.resetRepeated(rows.referenceAt(head), count);
            }
            return rows.resetChain(chain, head, count);
        }

        /**
         * Compact each key's insertion-ordered chain once at finalize, so one-to-many probes read sequential ranges.
         */
        private void compactChains()
        {
            compactedRows.build(
                    hashTable,
                    rows,
                    compressedRanges,
                    buildCardinality.distinctKeyCount(),
                    buildCardinality.storedRowCount());
            releaseRowArrays();
            if (compressedRanges.isBuilt()) {
                releaseHashTable();
            }
        }

        @Override
        int[] buildOrderedIntPayload(VectorAccess.LongValues values, long[] directValues, int[] sourcePositions)
        {
            return compactedRows.buildIntPayload(values, directValues, sourcePositions, buildCardinality.storedRowCount());
        }

        // Chooses array mode when the build is unique and its keys form a dense integer range, so the
        // probe can index a direct array by (key - minKey) instead of hashing and probing.
        private void finalizeForProbe(int initialProbeRows)
        {
            finalized = true;
            if (policy.debugJoinIndex()) {
                System.err.printf(
                        "[long-join-build] expected=%d rows=%d keys=%d min=%d max=%d dense=%s direct=%s duplicates=%s implicitReferences=%s compressedDuplicates=%s%n",
                        expectedBuildRows,
                        buildCardinality.storedRowCount(),
                        buildCardinality.distinctKeyCount(),
                        minKey,
                        maxKey,
                        denseSequence.keyCandidate(),
                        directBuild.isActive(),
                        hasDuplicates(),
                        rows.implicitSequentialReferences(),
                        compressDuplicateReferences);
            }
            if (buildCardinality.isEmpty()) {
                return;
            }
            if (directBuild.isActive()) {
                if (policy.compactCompletedDirectRangeBuild()) {
                    compactCompletedDirectRangeBuild();
                }
                if (policy.debugDirectDuplicateState() && hasDuplicates()) {
                    System.err.printf(
                            "[direct-duplicate-state] representation=%s rows=%d keys=%d range=%d sparseGroups=%d expectedRows=%d%n",
                            directBuild.sparseDuplicateGroupCount() > 0 ? "sparse" : "dense",
                            directBuild.rowCount(),
                            buildCardinality.distinctKeyCount(),
                            directBuild.capacity(),
                            directBuild.sparseDuplicateGroupCount(),
                            expectedBuildRows);
                }
                return;
            }
            buildSparseRangeMembership();
            if (hasDuplicates()) {
                // No direct array mode with duplicate keys; compact the multi-row chains so the probe reads a
                // contiguous range instead of chasing chainNext (the one-to-many output loop's dominant cost).
                if (compactChains && initialProbeRows >= policy.compactChainsMinProbeRows()) {
                    compactChains();
                }
                return;
            }
            long range = maxKey - minKey + 1;
            if (range <= 0 ||
                    range > policy.maxArrayRange() ||
                    range > (long) policy.directRangeMaxCardinalityRatio() * buildCardinality.distinctKeyCount()) {
                return;
            }
            if (denseSequence.keyCandidate() && range == buildCardinality.distinctKeyCount() && denseSequence.referenceCandidate()) {
                denseSequence.activateObservedReferences();
                directLookup.activateArithmetic(minKey, maxKey);
                releaseHashTable();
                releaseRowArrays();
                return;
            }
            if (rows.referencesFit32()) {
                if (denseSequence.keyCandidate() && range == buildCardinality.distinctKeyCount()) {
                    directLookup.activateCompact(minKey, maxKey, rows.packReferences32(buildCardinality.storedRowCount()));
                }
                else {
                    int[] direct = directLookup.activateCompact(minKey, maxKey, (int) range);
                    for (int slot = 0; slot < hashTable.capacity(); slot++) {
                        int head = hashTable.head(slot);
                        if (head != EMPTY) {
                            direct[(int) (hashTable.key(slot) - minKey)] = JoinRowReference.packCompact(rows.referenceAt(head));
                        }
                    }
                }
                releaseHashTable();
                releaseRowArrays();
                return;
            }
            if (denseSequence.keyCandidate() && range == buildCardinality.distinctKeyCount()) {
                directLookup.activateFull(minKey, maxKey, rows.takeFullReferences());
                releaseHashTable();
                releaseRowArrays();
                return;
            }
            long[] direct = directLookup.activateFull(minKey, maxKey, (int) range);
            for (int slot = 0; slot < hashTable.capacity(); slot++) {
                int head = hashTable.head(slot);
                if (head != EMPTY) {
                    direct[(int) (hashTable.key(slot) - minKey)] = rows.referenceAt(head);
                }
            }
            // The hash table and chain are no longer consulted in array mode.
            releaseHashTable();
            releaseRowArrays();
        }

        /**
         * The bounded streaming builder initially uses absolute non-negative keys so it can grow without trusting
         * an estimated cardinality. Once the build is complete, its observed range may be both small and dense even
         * when the absolute keys are large. When row references are also sequential in one batch, convert that
         * proven shape to the buffered builder's arithmetic representation, which needs no lookup array.
         */
        private void compactCompletedDirectRangeBuild()
        {
            if (hasDuplicates() || buildCardinality.distinctKeyCount() < policy.compactCompletedDirectRangeMinSize()) {
                return;
            }
            long range = maxKey - minKey + 1;
            if (range <= 0 ||
                    range > policy.maxArrayRange() ||
                    range > (long) policy.directRangeMaxCardinalityRatio() * buildCardinality.distinctKeyCount()) {
                return;
            }

            // A payload-free join needs build row references only to preserve duplicate multiplicity. Once the
            // completed build proves a unique, gap-free key range, membership is exactly a bounds check and the
            // physical build position is unobservable. Reuse the existing dense-position probe machinery with
            // synthetic positions; no query, table, column, or logical-type identity participates in admission.
            if (policy.denseUnusedBuildMembership() &&
                    buildRowReferencesUnused &&
                    buildCardinality.distinctKeyCount() >= policy.denseUnusedBuildMembershipMinKeys() &&
                    range == buildCardinality.distinctKeyCount()) {
                directLookup.activateArithmetic(minKey, maxKey);
                denseSequence.activateReferences(0, 0, 0);
                releaseDirectBuildArrays();
                releaseHashTable();
                releaseRowArrays();
                if (policy.debugJoinIndex()) {
                    System.err.printf("[dense-unused-build-membership] keys=%d min=%d max=%d%n", buildCardinality.distinctKeyCount(), minKey, maxKey);
                }
                return;
            }

            boolean sequentialReferences = range == buildCardinality.distinctKeyCount() && denseSequence.referenceCandidate();
            long firstReference = NO_MATCH_ROW_REFERENCE;
            int firstBatchIndex = 0;
            int firstPosition = 0;
            for (int offset = 0; offset < range; offset++) {
                int head = directBuild.entry((int) minKey + offset);
                if (head == EMPTY) {
                    sequentialReferences = false;
                    continue;
                }
                if (!sequentialReferences) {
                    continue;
                }
                long reference = rows.referenceAt(head);
                if (firstReference == NO_MATCH_ROW_REFERENCE) {
                    firstReference = reference;
                    firstBatchIndex = JoinRowReference.batchIndex(reference);
                    firstPosition = JoinRowReference.position(reference);
                }
                else if (JoinRowReference.batchIndex(reference) != firstBatchIndex || JoinRowReference.position(reference) != firstPosition + offset) {
                    sequentialReferences = false;
                }
            }
            // A shifted lookup array is not an unconditional improvement for multi-batch/non-sequential builds:
            // it adds another random table and can amplify TLB traffic. Keep the bounded construction layout unless
            // the completed shape proves that probe results can be derived arithmetically with no lookup at all.
            if (!sequentialReferences) {
                return;
            }
            directLookup.activateArithmetic(minKey, maxKey);
            denseSequence.activateReferences(firstBatchIndex, firstPosition, firstReference);
            releaseDirectBuildArrays();
            releaseHashTable();
            releaseRowArrays();
        }

        private LongList rowsForKey(long key, SingleLongList single, ChainLongList chain)
        {
            if (compressedRanges.isBuilt()) {
                int entry = compressedRanges.entry(key);
                if (entry == 0) {
                    return LongLists.emptyList();
                }
                int start = CompressedLongRangeIndex.start(entry);
                int count = CompressedLongRangeIndex.count(entry);
                return compactedRows.rows(start, count, single, chain);
            }
            if (directBuild.isActive()) {
                if (key < 0 || key >= directBuild.capacity()) {
                    return LongLists.emptyList();
                }
                int entry = directBuild.entry((int) key);
                if (entry == EMPTY) {
                    return LongLists.emptyList();
                }
                int head = directEntryHead(entry);
                int count = directEntryCount((int) key, entry);
                if (count == 1) {
                    return single.withValue(rows.referenceAt(head));
                }
                if (compressDuplicateReferences) {
                    return chain.resetRepeated(rows.referenceAt(head), count);
                }
                return rows.resetChain(chain, head, count);
            }
            if (directLookup.isActive()) {
                long rowReference = directLookup.reference(key, denseSequence);
                return rowReference == NO_MATCH_ROW_REFERENCE ? LongLists.emptyList() : single.withValue(rowReference);
            }
            if (!sparseRangeContains(key)) {
                return LongLists.emptyList();
            }
            return rowsForSlot(hashTable.findSlot(key), single, chain);
        }

        private void buildSparseRangeMembership()
        {
            sparseMembership.build(hashTable, minKey, maxKey, buildCardinality.distinctKeyCount());
        }

        @Override
        DynamicFilter buildDynamicFilter(int probeColumn)
        {
            buildSparseRangeMembership();
            return sparseMembership.dynamicFilter(probeColumn);
        }

        private boolean sparseRangeContains(long key)
        {
            return sparseMembership.contains(key);
        }

        private boolean hasDuplicates()
        {
            return directBuild.isActive() ? directBuild.hasDuplicates() : hashTable.hasDuplicates();
        }

        @Override
        public void releaseBuffers()
        {
            if (!ownsStorage) {
                return;
            }
            releaseHashTable();
            releaseRowArrays();
            releaseDirectBuildArrays();
            directLookup.release();
            sparseMembership.release();
            compactedRows.release();
            compressedRanges.release();
            denseSequence.release();
        }

        @Override
        void releaseProbeBuffers()
        {
            if (!ownsStorage) {
                denseSequence.release();
            }
        }

        private void releaseHashTable()
        {
            hashTable.release();
        }

        private void releaseRowArrays()
        {
            rows.release();
        }

        private void releaseDirectBuildArrays()
        {
            directBuild.release();
        }

        private void matchLongRows(long[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForKey(values[position], singleMatches[index], matchScratch.batchChain(index));
                }
            }
        }

        private void matchIntRows(int[] values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForKey(values[position], singleMatches[index], matchScratch.batchChain(index));
                }
            }
        }

        private void matchDictionaryRows(DictionaryVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            int[] ids = values.ids();
            switch (values.values()) {
                case org.weakref.nitro.data.I64Vector longValues -> {
                    long[] dictionaryValues = longValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], matchScratch.batchChain(index));
                        }
                    }
                }
                case org.weakref.nitro.data.I32Vector intValues -> {
                    int[] dictionaryValues = intValues.values();
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(dictionaryValues[ids[position]], singleMatches[index], matchScratch.batchChain(index));
                        }
                    }
                }
                default -> {
                    VectorAccess.LongValues dictionaryValues = VectorAccess.longValues(values.values());
                    for (int index = 0; index < positionCount; index++) {
                        int position = positions[index];
                        if (nullValues.value(position)) {
                            matches[index] = LongLists.emptyList();
                        }
                        else {
                            matches[index] = rowsForKey(dictionaryValues.value(ids[position]), singleMatches[index], matchScratch.batchChain(index));
                        }
                    }
                }
            }
        }

        private void matchRleRows(org.weakref.nitro.data.RleVector values, VectorAccess.BooleanValues nullValues, int[] positions, int positionCount, LongList[] matches, SingleLongList[] singleMatches)
        {
            VectorAccess.LongValues rowValues = VectorAccess.longValues(values);
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (nullValues.value(position)) {
                    matches[index] = LongLists.emptyList();
                }
                else {
                    matches[index] = rowsForKey(rowValues.value(position), singleMatches[index], matchScratch.batchChain(index));
                }
            }
        }
    }

    private static SingleLongList[] createSingleLongLists(int size)
    {
        SingleLongList[] matches = new SingleLongList[size];
        for (int index = 0; index < size; index++) {
            matches[index] = new SingleLongList();
        }
        return matches;
    }
}
