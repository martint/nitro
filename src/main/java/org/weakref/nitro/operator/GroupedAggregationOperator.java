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

import org.weakref.nitro.core.function.aggregation.LongStateUpdate;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.GeneratedGroupedAccumulator;
import org.weakref.nitro.operator.aggregation.GeneratedGroupedAccumulatorUpdate;
import org.weakref.nitro.operator.aggregation.StreamAccessors;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.toIntExact;

public class GroupedAggregationOperator
        implements Operator
{
    // Above this group count the group table + accumulator state spill out of cache, where the staged
    // two-pass wins on memory-level parallelism; below it the fused single pass wins. Cardinality-gated.
    private static final int FUSE_GROUP_LIMIT =
            Integer.getInteger("nitro.groupedAggregation.fuseGroupLimit", 1 << 15);
    // Dictionary-mapped or repeatedly adjacent keys have already proved that fusion avoids a second random
    // state walk. Let those physically reusable shapes finish instead of switching representation part-way
    // through and duplicating state growth; random high-cardinality input is still rejected at FUSE_GROUP_LIMIT.
    // Keep a hard bound so a malformed or unexpectedly large domain cannot grow fused state without limit.
    private static final int FUSE_LOCAL_GROUP_LIMIT =
            Integer.getInteger("nitro.groupedAggregation.fuseLocalGroupLimit", 1 << 25);
    private static final int FUSE_MAPPED_ONLY_GROUP_LIMIT =
            Integer.getInteger("nitro.groupedAggregation.fuseMappedOnlyGroupLimit", 1 << 16);

    // Forward a downstream join's dynamic filter on a grouped-key column to the source (so a fact scan below
    // the grouping can drop non-joining rows before they are grouped). On by default; opt out for A/B.
    private static final boolean DYNAMIC_FILTER_THROUGH_AGGREGATION =
            Boolean.parseBoolean(System.getProperty("nitro.dynamicFilter.throughAggregation", "true"));
    private static final boolean SPARSE_CONSTRAINED_RESULTS =
            Boolean.parseBoolean(System.getProperty("nitro.groupedAggregation.sparseConstrainedResults", "true"));
    private static final boolean GROUP_PARTITIONED_LONG_DISTINCT =
            Boolean.parseBoolean(System.getProperty("nitro.distinct.groupPartitionedLong", "true"));
    private static final boolean PARTIAL_GENERATED_GROUPING =
            Boolean.parseBoolean(System.getProperty("nitro.groupedAggregation.partialGeneratedGrouping", "true"));
    private static final boolean FUSED_DICTIONARY_INPUT =
            Boolean.parseBoolean(System.getProperty("nitro.groupedAggregation.fusedDictionaryInput", "true"));
    private static final boolean FUSED_LONG_RUN_CACHE =
            Boolean.parseBoolean(System.getProperty("nitro.groupedAggregation.fusedLongRunCache", "true"));
    private static final boolean FUSED_CONSTANT_RUNS =
            Boolean.parseBoolean(System.getProperty("nitro.groupedAggregation.fusedConstantRuns", "true"));
    private static final int FUSED_CONSTANT_RUN_GROUP_MIN =
            Integer.getInteger("nitro.groupedAggregation.fusedConstantRunGroupMin", 1 << 15);
    private static final boolean FUSED_LONG_DIRECT_GROUPING =
            Boolean.parseBoolean(System.getProperty("nitro.groupedAggregation.fusedLongDirectGrouping", "true"));
    private static final boolean FUSED_MAPPED_CONTINUATION_POWER_OF_TWO_STATE_CAPACITY =
            Boolean.parseBoolean(System.getProperty("nitro.groupedAggregation.fusedMappedContinuationPowerOfTwoStateCapacity", "true"));
    private static final boolean DEBUG_FUSED_GROUPING = Boolean.getBoolean("nitro.debug.fusedGrouping");

    private final Allocator.Context allocationContext = new Allocator.Context("GroupedAggregationOperator");
    private final Allocator allocator;

    private final int groupColumn;
    private final int[] groupedColumns;
    private final int[] groupByColumns;
    private final int[] groupedKeyIndexes;
    private final Accumulator[] aggregations;
    private final int[] plainAggregationIndexes;
    private final int[] filteredAggregationIndexes;
    private final DistinctAggregationGroup[] distinctAggregationGroups;
    private final Operator source;
    private final Streams[] groupedResults;
    private final Streams[] result;
    private final GroupingState inlineGroupingState;
    private final Vector[] inlineGroupValues;
    private final Vector[] inlineGroupNulls;
    private Streams[] states;
    private int stateCapacity;
    private int maxGroup = -1;
    private boolean done;
    private GroupedKeySource groupedKeySource;
    private I64Vector reusableGroups;
    // Fused single-long-key path: assign the group and accumulate every aggregation in one inlined pass, no
    // group-id vector and no per-row accumulator dispatch. The per-shape kernel is generated as bytecode.
    // Eligibility is decided once the grouping mode is known; falls back to the staged path per batch when a
    // batch isn't the flat shape the fused loop handles.
    private boolean fusedEligible;
    private boolean fusedChecked;
    private boolean fusedPhysicalPathCommitted;
    private FusedGroupingKernel fusedKernel;
    private GeneratedGroupedAccumulatorUpdate[] fusedSpecs;
    private int[] fusedAggregationIndexes;
    private Object[] fusedInputs;
    private int[] fusedKeyIds;
    private int[][] fusedInputIds;
    private boolean[][] fusedInputNulls;
    private int[][] fusedInputNullIds;
    private LongStateUpdate[] fusedStateVectors;
    private boolean[] fusedIntInputs;
    private boolean[] fusedMappedInputs;
    private boolean[] fusedMappedInputNulls;
    private boolean[] fusedInputUsesKeyIds;
    private boolean[] fusedInputNullUsesKeyIds;
    private boolean fusedKeyMapped;
    private int fusedPhysicalShape = -1;
    private boolean debugFusedLimitPrinted;
    private boolean debugFusedReuseContinuationPrinted;
    private boolean debugFusedConstantRunsPrinted;

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, groupColumn, List.of(), aggregations, source, null, null, null);
    }

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Integer> groupedColumns, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, groupColumn, groupedColumns, aggregations, source, null, null, null);
    }

    public GroupedAggregationOperator(Allocator allocator, List<Integer> groupByColumns, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, groupByColumns, groupByColumns, aggregations, source);
    }

    public GroupedAggregationOperator(Allocator allocator, List<Integer> groupByColumns, List<Integer> groupedColumns, List<Accumulator> aggregations, Operator source)
    {
        this(
                allocator,
                -1,
                groupedColumns,
                aggregations,
                source,
                toArray(groupByColumns),
                mapGroupedKeyIndexes(groupByColumns, groupedColumns),
                new GroupingState(
                        allocator.primitiveArrays(),
                        allocator.engineResources().operatorCodeGeneration(),
                        allocator.engineResources().groupingState()));
    }

    private GroupedAggregationOperator(
            Allocator allocator,
            int groupColumn,
            List<Integer> groupedColumns,
            List<Accumulator> aggregations,
            Operator source,
            int[] groupByColumns,
            int[] groupedKeyIndexes,
            GroupingState inlineGroupingState)
    {
        if (!groupedColumns.isEmpty() && groupByColumns == null && !(source instanceof GroupedKeySource)) {
            throw new IllegalArgumentException("Source must implement GroupedKeySource when grouped outputs are requested");
        }
        this.allocator = allocator;
        this.groupColumn = groupColumn;
        this.groupedColumns = groupedColumns.stream()
                .mapToInt(Integer::intValue)
                .toArray();
        this.groupByColumns = groupByColumns;
        this.groupedKeyIndexes = groupedKeyIndexes;
        this.aggregations = aggregations.toArray(Accumulator[]::new);
        DistinctAggregationPlan distinctAggregationPlan = planDistinctAggregations(this.aggregations);
        this.plainAggregationIndexes = distinctAggregationPlan.plainAggregationIndexes();
        this.filteredAggregationIndexes = distinctAggregationPlan.filteredAggregationIndexes();
        this.distinctAggregationGroups = distinctAggregationPlan.distinctAggregationGroups();
        this.source = source;
        this.inlineGroupingState = inlineGroupingState;
        this.inlineGroupValues = groupByColumns == null ? null : new Vector[groupByColumns.length];
        this.inlineGroupNulls = groupByColumns == null ? null : new Vector[groupByColumns.length];

        groupedResults = new Streams[this.groupedColumns.length];
        result = new Streams[this.aggregations.length];
    }

    @Override
    public int outputCount()
    {
        return groupedColumns.length + aggregations.length;
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        // A downstream join keyed on a group-by column (e.g. TPC-DS q24 joins store/item after grouping by
        // ss_store_sk/ss_item_sk) pushes a filter on that grouped-key OUTPUT column. Grouping preserves the key
        // value, so the same membership applies to the corresponding group-by INPUT column: retarget the filter
        // to that input column and forward it to the source. Because the join builds its (small) dimension and
        // pushes this filter before it first probes this operator, the fact scan below the grouping receives it
        // before the grouping drains its input -- dropping non-joining rows before they are ever grouped. A
        // filter on an aggregate output has no key column to push and is ignored.
        if (!DYNAMIC_FILTER_THROUGH_AGGREGATION || groupByColumns == null) {
            return;
        }
        int column = filter.column();
        if (column < groupedColumns.length) {
            source.pushDynamicFilter(filter.withColumn(groupByColumns[groupedKeyIndexes[column]]));
        }
    }

    private Mask computeResults()
    {
        if (groupByColumns != null) {
            return computeInlineGroupedResults();
        }

        states = new Streams[aggregations.length];
        stateCapacity = 0;
        long maxObservedGroup = -1;
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }
                I64Vector group = (I64Vector) batch.output(groupColumn).borrow(Stream.VALUES);

                long previousMaxGroup = maxObservedGroup;
                if (mask.all()) {
                    for (int position = 0; position <= mask.maxPosition(); position++) {
                        maxObservedGroup = Math.max(maxObservedGroup, group.values()[position]);
                    }
                }
                else {
                    for (int position : mask) {
                        maxObservedGroup = Math.max(maxObservedGroup, group.values()[position]);
                    }
                }

                int newCapacity = Allocator.computeCapacity(toIntExact(maxObservedGroup + 1));
                var streamAccessor = StreamAccessors.forBatch(batch);
                prepareAggregationStates(previousMaxGroup, maxObservedGroup, newCapacity);
                accumulateGroupedRows(batch, group, mask, streamAccessor, toIntExact(maxObservedGroup + 1));
            }
        }

        this.maxGroup = toIntExact(maxObservedGroup);
        for (int i = 0; i < result.length; i++) {
            if (states[i] == null) {
                states[i] = aggregations[i].allocate(allocator, allocationContext, 0);
            }
            result[i] = null;
        }
        if (groupedColumns.length > 0) {
            groupedKeySource = (GroupedKeySource) source;
            for (int i = 0; i < groupedColumns.length; i++) {
                groupedResults[i] = null;
            }
        }

        done = true;

        return allocator.allocateAllMask(allocationContext, this.maxGroup + 1);
    }

    private Mask computeInlineGroupedResults()
    {
        states = new Streams[aggregations.length];
        stateCapacity = 0;
        long maxObservedGroup = -1;
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    initializeInlineGroupingSchema(batch);
                    continue;
                }

                if (!inlineGroupingState.isInitialized()) {
                    initializeInlineGroupingSchema(batch);
                }
                if (inlineGroupingState.isInitialized() && !fusedChecked) {
                    prepareFusedKernel();
                    fusedChecked = true;
                }

                // Fuse only while the group table + state stay cache-resident. Beyond that the staged two-pass
                // wins on memory-level parallelism (each pass streams one random-access array the OOO window
                // overlaps), whereas fusion serializes probe-miss -> state-miss per row.
                if (fusedEligible && inlineGroupingState.groupCount() < FUSE_LOCAL_GROUP_LIMIT) {
                    if (tryFusedSingleLongAggregation(batch, mask)) {
                        maxObservedGroup = inlineGroupingState.groupCount() - 1;
                        if (filteredAggregationIndexes.length != 0 || distinctAggregationGroups.length != 0) {
                            accumulateFilteredGroupedRows(batch, reusableGroups, mask, StreamAccessors.forBatch(batch));
                            accumulateDistinctGroupedRows(batch, reusableGroups, mask, StreamAccessors.forBatch(batch), toIntExact(inlineGroupingState.groupCount()));
                        }
                        continue;
                    }
                }

                long previousMaxGroup = maxObservedGroup;
                reusableGroups = allocator.reallocateIfNecessary(allocationContext, reusableGroups, I64Vector.class, mask.maxPosition() + 1, I64Vector::new);
                assignInlineGroups(batch, mask, reusableGroups);
                // The grouping state knows the max assigned group id (group ids are dense 0..count-1),
                // so use it directly instead of a separate O(rows) scan of the just-assigned group vector.
                maxObservedGroup = inlineGroupingState.groupCount() - 1;

                int newCapacity = Allocator.computeCapacity(toIntExact(maxObservedGroup + 1));
                var streamAccessor = StreamAccessors.forBatch(batch);
                prepareAggregationStates(previousMaxGroup, maxObservedGroup, newCapacity);
                accumulateGroupedRows(batch, reusableGroups, mask, streamAccessor, toIntExact(inlineGroupingState.groupCount()));
            }
        }

        finishResults(maxObservedGroup);
        return allocator.allocateAllMask(allocationContext, this.maxGroup + 1);
    }

    /**
     * Decides once whether the inline grouped aggregation can use the fused single-long-key path. Plain
     * fusible accumulators run in the generated grouping pass. Filtered or DISTINCT accumulators may remain
     * as explicit second stages; in that case the generated kernel writes the group-id vector they consume.
     */
    private void prepareFusedKernel()
    {
        fusedEligible = groupByColumns != null
                && groupByColumns.length == 1
                // A filtered-only aggregation can still use a generated grouping-only pass that writes
                // group IDs for the explicit masked stage.
                && (plainAggregationIndexes.length > 0 || filteredAggregationIndexes.length > 0)
                && (filteredAggregationIndexes.length == 0 || PARTIAL_GENERATED_GROUPING)
                && (distinctAggregationGroups.length == 0 || PARTIAL_GENERATED_GROUPING)
                && inlineGroupingState.usesSingleLongGrouping()
                && allPlainAggregationsFusible();
        if (!fusedEligible) {
            return;
        }
        fusedAggregationIndexes = plainAggregationIndexes.clone();
        fusedSpecs = new GeneratedGroupedAccumulatorUpdate[fusedAggregationIndexes.length];
        for (int index = 0; index < fusedAggregationIndexes.length; index++) {
            fusedSpecs[index] = ((GeneratedGroupedAccumulator) aggregations[fusedAggregationIndexes[index]]).generatedGroupedUpdate();
        }
        fusedInputs = new Object[fusedSpecs.length];
        fusedInputIds = new int[fusedSpecs.length][];
        fusedInputNulls = new boolean[fusedSpecs.length][];
        fusedInputNullIds = new int[fusedSpecs.length][];
        fusedStateVectors = new LongStateUpdate[fusedSpecs.length];
        fusedIntInputs = new boolean[fusedSpecs.length];
        fusedMappedInputs = new boolean[fusedSpecs.length];
        fusedMappedInputNulls = new boolean[fusedSpecs.length];
        fusedInputUsesKeyIds = new boolean[fusedSpecs.length];
        fusedInputNullUsesKeyIds = new boolean[fusedSpecs.length];
    }

    private boolean allPlainAggregationsFusible()
    {
        for (int aggregationIndex : plainAggregationIndexes) {
            if (!(aggregations[aggregationIndex] instanceof GeneratedGroupedAccumulator)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fused single-long-key aggregation: one generated pass that probes the group table and accumulates
     * every aggregation with no group-id vector and no per-row dispatch. Handles the common flat, null-free
     * batch shape, including independently nullable accumulator inputs; returns false (caller falls back to
     * the staged path) for encoded value/null vectors or when the group key can be null.
     */
    private boolean tryFusedSingleLongAggregation(Batch batch, Mask mask)
    {
        if (inlineGroupingState.usesLongDirectGrouping() && !fusedPhysicalPathCommitted) {
            // Staged fallback won the representation race. Do not reinterpret its broader direct table as a fused
            // admission on a later batch; keep one physical policy for the rest of this aggregation.
            return false;
        }
        // A grouping-only generated pass must still write every group ID for the later filtered stage.
        // On sparse batches the ordinary staged grouper already walks only survivors and has better locality;
        // the generated output-vector round trip does not earn itself. Plain fused accumulators may still use
        // sparse generation because they eliminate an additional accumulator pass.
        if (fusedAggregationIndexes.length == 0 && !mask.all()) {
            return false;
        }
        Output keyOutput = batch.output(groupByColumns[0]);
        Vector keyVector = keyOutput.borrow(Stream.VALUES);
        Object keyValues;
        boolean intKey;
        fusedKeyIds = null;
        fusedKeyMapped = false;
        if (keyVector instanceof DictionaryVector dictionary && dictionary.dictionaryDepth() == 1) {
            if (!FUSED_DICTIONARY_INPUT) {
                return false;
            }
            fusedKeyIds = dictionary.ids();
            fusedKeyMapped = true;
            keyVector = dictionary.values();
        }
        if (keyVector instanceof I64Vector values) {
            keyValues = values.values();
            intKey = false;
        }
        else if (keyVector instanceof I32Vector values) {
            keyValues = values.values();
            intKey = true;
        }
        else {
            return false;
        }
        if (!VectorAccess.isAllFalseNulls(keyOutput.borrowOrNull(Stream.NULLS))) {
            return false;
        }
        boolean directGrouping = FUSED_LONG_DIRECT_GROUPING
                && fusedKeyMapped
                && inlineGroupingState.prepareSingleLongDirectGrouping(mask, keyValues, intKey, fusedKeyIds);
        long runSample = FUSED_LONG_RUN_CACHE ? sampleFusedKeyRuns(mask, keyValues, intKey, fusedKeyIds) : 0;
        int runComparisons = (int) (runSample >>> 32);
        int runHits = (int) runSample;
        boolean runCache = runComparisons >= 4 && runHits * 2 >= runComparisons;
        boolean inputIndependentAccumulators = canBatchInputIndependentFusedAccumulator();
        boolean constantRuns = FUSED_CONSTANT_RUNS
                && runCache
                && inlineGroupingState.groupCount() >= FUSED_CONSTANT_RUN_GROUP_MIN
                && inputIndependentAccumulators;
        boolean idIndexedGrouping = inlineGroupingState.prepareSingleLongIdIndexedGrouping(
                runCache && inputIndependentAccumulators,
                inlineGroupingState.groupCount() + mask.count());
        if (DEBUG_FUSED_GROUPING && constantRuns && !debugFusedConstantRunsPrinted) {
            debugFusedConstantRunsPrinted = true;
            System.err.printf("[fused-grouping-constant-runs] groups=%d rows=%d keyMapped=%s runHits=%d/%d%n",
                    inlineGroupingState.groupCount(), mask.count(), fusedKeyMapped, runHits, runComparisons);
        }
        if (DEBUG_FUSED_GROUPING && !debugFusedLimitPrinted && inlineGroupingState.groupCount() >= FUSE_GROUP_LIMIT) {
            debugFusedLimitPrinted = true;
            System.err.printf("[fused-grouping] groups=%d rows=%d max=%d all=%s accumulators=%d keyMapped=%s runCache=%s direct=%s%n",
                    inlineGroupingState.groupCount(), mask.count(), mask.maxPosition(), mask.all(), fusedSpecs.length, fusedKeyMapped, runCache, directGrouping);
        }
        // The ordinary fused limit protects random high-cardinality flat state probes. Continue to the larger bound
        // only when a mapped key keeps the physical input compact or adjacent keys prove that they reuse a group.
        if (inlineGroupingState.groupCount() >= FUSE_GROUP_LIMIT && !fusedKeyMapped && !runCache) {
            return false;
        }
        // Beyond the established local boundary require adjacent reuse plus a second physical reason to keep the
        // generated pass: compact mapped-key access or an input-independent state update that can be coalesced by
        // run. Flat value-reading state retains staged locality; mapped-only q64 likewise stays staged.
        if (inlineGroupingState.groupCount() >= FUSE_MAPPED_ONLY_GROUP_LIMIT
                && (!runCache || (!fusedKeyMapped && !inputIndependentAccumulators))) {
            return false;
        }
        if (DEBUG_FUSED_GROUPING && !debugFusedReuseContinuationPrinted && inlineGroupingState.groupCount() >= (1 << 16)) {
            debugFusedReuseContinuationPrinted = true;
            System.err.printf("[fused-grouping-reuse-continuation] groups=%d rows=%d keyMapped=%s runCache=%s%n",
                    inlineGroupingState.groupCount(), mask.count(), fusedKeyMapped, runCache);
        }
        for (int index = 0; index < fusedSpecs.length; index++) {
            GeneratedGroupedAccumulatorUpdate spec = fusedSpecs[index];
            if (!spec.readsInput()) {
                fusedInputs[index] = null;
                fusedInputIds[index] = null;
                fusedInputNulls[index] = null;
                fusedInputNullIds[index] = null;
                fusedMappedInputs[index] = false;
                fusedMappedInputNulls[index] = false;
                continue;
            }
            Output valueOutput = batch.output(spec.inputColumn());
            if (spec.readsValue()) {
                Vector valueVector = valueOutput.borrow(Stream.VALUES);
                fusedInputIds[index] = null;
                fusedMappedInputs[index] = false;
                if (valueVector instanceof DictionaryVector dictionary && dictionary.dictionaryDepth() == 1) {
                    if (!FUSED_DICTIONARY_INPUT) {
                        return false;
                    }
                    fusedInputIds[index] = dictionary.ids();
                    fusedMappedInputs[index] = true;
                    valueVector = dictionary.values();
                }
                if (valueVector instanceof I64Vector values) {
                    fusedInputs[index] = values.values();
                    fusedIntInputs[index] = false;
                }
                else if (valueVector instanceof I32Vector values) {
                    fusedInputs[index] = values.values();
                    fusedIntInputs[index] = true;
                }
                else {
                    return false;
                }
            }
            else {
                fusedInputs[index] = null;
                fusedInputIds[index] = null;
                fusedIntInputs[index] = false;
                fusedMappedInputs[index] = false;
            }
            Vector valueNulls = valueOutput.borrowOrNull(Stream.NULLS);
            if (VectorAccess.isAllFalseNulls(valueNulls)) {
                fusedInputNulls[index] = null;
                fusedInputNullIds[index] = null;
                fusedMappedInputNulls[index] = false;
            }
            else {
                fusedInputNullIds[index] = null;
                fusedMappedInputNulls[index] = false;
                if (valueNulls instanceof DictionaryVector dictionary && dictionary.dictionaryDepth() == 1) {
                    if (!FUSED_DICTIONARY_INPUT) {
                        return false;
                    }
                    fusedInputNullIds[index] = dictionary.ids();
                    fusedMappedInputNulls[index] = true;
                    valueNulls = dictionary.values();
                }
                if (valueNulls instanceof BooleanVector flatNulls) {
                    fusedInputNulls[index] = flatNulls.values();
                }
                else {
                    return false;
                }
            }
        }

        int physicalShape = intKey ? 1 : 0;
        physicalShape = physicalShape * 31 + (fusedKeyMapped ? 1 : 0);
        for (int index = 0; index < fusedSpecs.length; index++) {
            fusedInputUsesKeyIds[index] = fusedKeyMapped && fusedMappedInputs[index] && fusedInputIds[index] == fusedKeyIds;
            fusedInputNullUsesKeyIds[index] = fusedKeyMapped && fusedMappedInputNulls[index] && fusedInputNullIds[index] == fusedKeyIds;
            if (fusedSpecs[index].readsValue()) {
                physicalShape = physicalShape * 31 + (fusedIntInputs[index] ? 1 : 0);
                physicalShape = physicalShape * 31 + (fusedMappedInputs[index] ? 1 : 0);
                physicalShape = physicalShape * 31 + (fusedInputUsesKeyIds[index] ? 1 : 0);
            }
            if (fusedSpecs[index].readsInput()) {
                physicalShape = physicalShape * 31 + (fusedMappedInputNulls[index] ? 1 : 0);
                physicalShape = physicalShape * 31 + (fusedInputNullUsesKeyIds[index] ? 1 : 0);
            }
        }
        physicalShape = physicalShape * 31 + (runCache ? 1 : 0);
        physicalShape = physicalShape * 31 + (constantRuns ? 1 : 0);
        physicalShape = physicalShape * 31 + (directGrouping ? 1 : 0);
        physicalShape = physicalShape * 31 + (idIndexedGrouping ? 1 : 0);
        if (fusedPhysicalShape != physicalShape) {
            fusedKernel = allocator.engineResources().operatorCodeGeneration().fusedGrouping().create(
                    List.of(fusedSpecs),
                    filteredAggregationIndexes.length != 0 || distinctAggregationGroups.length != 0,
                    intKey,
                    fusedKeyMapped,
                    runCache,
                    constantRuns,
                    directGrouping,
                    idIndexedGrouping,
                    fusedIntInputs,
                    fusedMappedInputs,
                    fusedMappedInputNulls,
                    fusedInputUsesKeyIds,
                    fusedInputNullUsesKeyIds);
            fusedPhysicalShape = physicalShape;
        }

        if (!fusedPhysicalPathCommitted) {
            fusedPhysicalPathCommitted = true;
            // Only an actually executable fused batch locks the tighter direct-range policy. Merely being
            // logically eligible is insufficient when encoded/null physical inputs force every batch to stage.
            inlineGroupingState.disableStagedLongDirectGrouping();
        }

        int count = mask.count();
        // Pre-reserve so the inlined probe needs no rehash branch and no per-row state growth.
        // A dictionary's value count is a safe upper bound on new groups in this batch. Reserving by logical row
        // count instead can substantially over-allocate state for a low-cardinality encoded key.
        int additionalGroups = fusedKeyMapped ? Math.min(count, keyVector.length()) : count;
        inlineGroupingState.reserveSingleLongTable(additionalGroups);
        ensureFusedStateCapacity(toIntExact(inlineGroupingState.groupCount() + additionalGroups));
        if (filteredAggregationIndexes.length != 0 || distinctAggregationGroups.length != 0) {
            reusableGroups = allocator.reallocateIfNecessary(allocationContext, reusableGroups, I64Vector.class, mask.maxPosition() + 1, I64Vector::new);
        }
        for (int index = 0; index < fusedAggregationIndexes.length; index++) {
            fusedStateVectors[index] = (LongStateUpdate) states[fusedAggregationIndexes[index]].values();
        }

        long nextId = fusedKernel.accumulate(
                mask.selectedPositions(),
                count,
                keyValues,
                fusedKeyIds,
                inlineGroupingState.longGroupKeys,
                inlineGroupingState.longGroupIds,
                inlineGroupingState.longGroupMask,
                inlineGroupingState.longKeysByGroup,
                inlineGroupingState.nextGroupId,
                filteredAggregationIndexes.length == 0 && distinctAggregationGroups.length == 0 ? null : reusableGroups.values(),
                fusedInputs,
                fusedInputIds,
                fusedInputNulls,
                fusedInputNullIds,
                fusedStateVectors);

        inlineGroupingState.nextGroupId = nextId;
        inlineGroupingState.longGroupCount = (int) nextId;
        return true;
    }

    private boolean canBatchInputIndependentFusedAccumulator()
    {
        boolean found = false;
        for (GeneratedGroupedAccumulatorUpdate spec : fusedSpecs) {
            if (!spec.readsInput()) {
                found = true;
            }
            else {
                return false;
            }
        }
        return found;
    }

    private static long sampleFusedKeyRuns(Mask mask, Object keyValues, boolean intKey, int[] keyIds)
    {
        int comparisons = 0;
        int hits = 0;
        boolean havePrevious = false;
        long previous = 0;
        for (int position : mask) {
            int keyPosition = keyIds == null ? position : keyIds[position];
            long key = intKey ? ((int[]) keyValues)[keyPosition] : ((long[]) keyValues)[keyPosition];
            if (havePrevious) {
                comparisons++;
                if (key == previous) {
                    hits++;
                }
                if (comparisons == 64) {
                    break;
                }
            }
            havePrevious = true;
            previous = key;
        }
        return ((long) comparisons << 32) | (hits & 0xFFFF_FFFFL);
    }

    private void ensureFusedStateCapacity(int needed)
    {
        if (states[0] == null) {
            int capacity = computeFusedStateCapacity(needed);
            for (int index = 0; index < aggregations.length; index++) {
                states[index] = aggregations[index].allocate(allocator, allocationContext, capacity);
                aggregations[index].initialize(states[index], 0, capacity);
            }
            stateCapacity = capacity;
            return;
        }
        if (stateCapacity >= needed) {
            return;
        }
        int capacity = computeFusedStateCapacity(needed);
        for (int index = 0; index < aggregations.length; index++) {
            states[index] = aggregations[index].grow(allocator, allocationContext, states[index], capacity);
            aggregations[index].initialize(states[index], stateCapacity, capacity - stateCapacity);
        }
        stateCapacity = capacity;
    }

    private int computeFusedStateCapacity(int needed)
    {
        if (!FUSED_MAPPED_CONTINUATION_POWER_OF_TWO_STATE_CAPACITY
                || !fusedKeyMapped
                || needed <= FUSE_GROUP_LIMIT) {
            return Allocator.computeCapacity(Math.max(16, needed));
        }
        // A mapped continuation reserves from the dictionary's physical cardinality, which is deliberately a
        // conservative upper bound. Match the grouping table's power-of-two high-water mark instead of applying
        // the general vector-growth formula to that bound; the latter can reserve more than twice the live state.
        // Keep the established growth policy below the ordinary fusion boundary: smaller groups can otherwise
        // encounter extra grow/copy steps before reaching their steady high-water mark.
        int capacity = 16;
        while (capacity < needed) {
            capacity = Math.multiplyExact(capacity, 2);
        }
        return capacity;
    }

    private void initializeInlineGroupingSchema(Batch batch)
    {
        if (groupByColumns == null || groupByColumns.length == 0) {
            return;
        }
        try {
            for (int index = 0; index < groupByColumns.length; index++) {
                Output output = batch.output(groupByColumns[index]);
                try {
                    inlineGroupValues[index] = output.borrowOrNull(Stream.VALUES);
                    inlineGroupNulls[index] = output.borrowOrNull(Stream.NULLS);
                }
                catch (IllegalArgumentException ignored) {
                    return;
                }
                if (inlineGroupValues[index] == null) {
                    return;
                }
            }
            inlineGroupingState.initializeSchema(inlineGroupValues, inlineGroupNulls);
        }
        finally {
            Arrays.fill(inlineGroupValues, null);
            Arrays.fill(inlineGroupNulls, null);
        }
    }

    private void assignInlineGroups(Batch batch, Mask mask, I64Vector groups)
    {
        if (groupByColumns.length == 1) {
            Output output = batch.output(groupByColumns[0]);
            inlineGroupingState.assignGroups(
                    output.borrow(Stream.VALUES),
                    output.borrowOrNull(Stream.NULLS),
                    mask,
                    groups);
            return;
        }

        try {
            for (int index = 0; index < groupByColumns.length; index++) {
                Output output = batch.output(groupByColumns[index]);
                inlineGroupValues[index] = output.borrow(Stream.VALUES);
                inlineGroupNulls[index] = output.borrowOrNull(Stream.NULLS);
            }
            inlineGroupingState.assignGroupsForBlockingAggregation(inlineGroupValues, inlineGroupNulls, mask, groups);
        }
        finally {
            Arrays.fill(inlineGroupValues, null);
            Arrays.fill(inlineGroupNulls, null);
        }
    }

    private static long maxGroup(I64Vector groups, Mask mask)
    {
        long maxObservedGroup = -1;
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                maxObservedGroup = Math.max(maxObservedGroup, groups.values()[position]);
            }
            return maxObservedGroup;
        }

        for (int position : mask) {
            maxObservedGroup = Math.max(maxObservedGroup, groups.values()[position]);
        }
        return maxObservedGroup;
    }

    private void prepareAggregationStates(long previousMaxGroup, long maxObservedGroup, int newCapacity)
    {
        // Only (re)allocate when the highest group seen so far no longer fits the current state capacity.
        // newCapacity carries growth headroom, so reallocating to it makes the state large enough for many
        // subsequent batches; recomputing a slightly larger target every batch (as a naive grow would) turns
        // the amortized-doubling headroom into a linear, O(n^2)-copy reallocation on every batch.
        int needed = toIntExact(maxObservedGroup + 1);
        boolean grow = stateCapacity < needed;
        for (int index = 0; index < aggregations.length; index++) {
            Accumulator accumulator = aggregations[index];
            if (states[index] == null) {
                states[index] = accumulator.allocate(allocator, allocationContext, newCapacity);
            }
            else if (grow) {
                states[index] = accumulator.grow(allocator, allocationContext, states[index], newCapacity);
            }
            accumulator.initialize(states[index], toIntExact(previousMaxGroup + 1), toIntExact(maxObservedGroup - previousMaxGroup));
        }
        if (grow) {
            stateCapacity = newCapacity;
        }
    }

    private void accumulateGroupedRows(Batch batch, I64Vector groups, Mask mask, org.weakref.nitro.operator.aggregation.StreamAccessor streamAccessor, int groupCount)
    {
        for (int aggregationIndex : plainAggregationIndexes) {
            aggregations[aggregationIndex].accumulate(states[aggregationIndex], groups, mask, streamAccessor);
        }

        accumulateFilteredGroupedRows(batch, groups, mask, streamAccessor);
        accumulateDistinctGroupedRows(batch, groups, mask, streamAccessor, groupCount);
    }

    private void accumulateFilteredGroupedRows(Batch batch, I64Vector groups, Mask mask, org.weakref.nitro.operator.aggregation.StreamAccessor streamAccessor)
    {
        for (int aggregationIndex : filteredAggregationIndexes) {
            Mask filteredMask = filterMask(batch, aggregations[aggregationIndex].filterInputColumn(), mask);
            try {
                aggregations[aggregationIndex].accumulate(states[aggregationIndex], groups, filteredMask, streamAccessor);
            }
            finally {
                if (filteredMask != mask) {
                    allocator.release(allocationContext, filteredMask);
                }
            }
        }
    }

    private void accumulateDistinctGroupedRows(Batch batch, I64Vector groups, Mask mask, org.weakref.nitro.operator.aggregation.StreamAccessor streamAccessor, int groupCount)
    {
        for (DistinctAggregationGroup distinctAggregationGroup : distinctAggregationGroups) {
            Mask distinctMask = distinctAggregationGroup.select(groups, mask, streamAccessor, groupCount, allocator, allocationContext);
            try {
                for (int aggregationIndex : distinctAggregationGroup.aggregationIndexes()) {
                    int filterColumn = aggregations[aggregationIndex].filterInputColumn();
                    Mask aggregationMask = filterColumn < 0 ? distinctMask : filterMask(batch, filterColumn, distinctMask);
                    try {
                        aggregations[aggregationIndex].accumulateDistinctSelected(states[aggregationIndex], groups, aggregationMask, streamAccessor);
                    }
                    finally {
                        if (aggregationMask != distinctMask) {
                            allocator.release(allocationContext, aggregationMask);
                        }
                    }
                }
            }
            finally {
                allocator.release(allocationContext, distinctMask);
            }
        }
    }

    private Mask filterMask(Batch batch, int filterColumn, Mask mask)
    {
        Output output = batch.output(filterColumn);
        Mask direct = output.tryBorrowMask(Stream.VALUES, mask, true, allocator, allocationContext);
        if (direct != null) {
            return direct;
        }
        Mask selected = allocator.copyMask(allocationContext, mask);
        var values = VectorAccess.booleanValues(output.borrow(Stream.VALUES));
        selected.retainIf(position -> values.value(position));
        return selected;
    }

    private void finishResults(long maxObservedGroup)
    {
        this.maxGroup = toIntExact(maxObservedGroup);
        for (int index = 0; index < result.length; index++) {
            if (states[index] == null) {
                states[index] = aggregations[index].allocate(allocator, allocationContext, 0);
            }
            result[index] = null;
        }
        if (groupedColumns.length > 0 && groupByColumns == null) {
            groupedKeySource = (GroupedKeySource) source;
        }
        for (int index = 0; index < groupedColumns.length; index++) {
            groupedResults[index] = null;
        }
        done = true;
    }

    @Override
    public Batch next()
    {
        Mask batchMask = computeResults();
        BatchState batchState = new BatchState(batchMask);
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = resultOutput(output, batchState).withConstraintSensitiveResolution();
        }
        return new Batch(batchMask, batchState::constrain, takenMask -> allocator.transfer(allocationContext, takenMask), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // Accumulator and grouping state remain live for this output batch. BatchState.constrain
        // retargets lazy materialization to only the retained group positions.
        return true;
    }

    private Output resultOutput(int output, BatchState batchState)
    {
        if (output < groupedResults.length) {
            int groupedOutput = output;
            Set<Stream> outputStreams = groupedKeyStreams(batchState);
            return new Output(
                    outputStreams,
                    stream -> groupedKeyOutput(groupedOutput, batchState).get(stream),
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    // Grouped results are operator-owned, reusable materializations. A downstream filter resolves
                    // one output before constraining the batch; invalidating that cached borrow must not return its
                    // backing vector to this context's pool while groupedResults still owns it.
                    (stream, vector) -> {},
                    (existing, sourcePosition, outputPosition, size) -> groupedKeyCopyPosition(groupedOutput, existing, sourcePosition, outputPosition, size));
        }

        Streams streams = result[output - groupedResults.length];
        return new Output(
                EnumSet.of(Stream.VALUES, Stream.NULLS),
                stream -> aggregationOutput(output - groupedResults.length, batchState).get(stream),
                (stream, vector) -> allocator.transfer(allocationContext, vector),
                // result() may return the accumulator state itself (COUNT, SUM, MIN, ...), and computed result
                // bundles are also cached for reuse after a narrower constraint. Keep both operator-owned until
                // take() transfers them or close() releases the context.
                (stream, vector) -> {},
                (existing, sourcePosition, outputPosition, size) -> aggregationCopyPosition(output - groupedResults.length, existing, sourcePosition, outputPosition, size));
    }

    private Streams aggregationOutput(int output, BatchState batchState)
    {
        Streams streams = result[output];
        if (streams != null && batchState.aggregationMaterializedMask[output] != null && batchState.mask.equals(batchState.aggregationMaterializedMask[output])) {
            return streams;
        }
        Streams sparse = null;
        if (SPARSE_CONSTRAINED_RESULTS && batchState.mask.count() < maxGroup + 1) {
            int size = batchState.mask.none() ? 0 : batchState.mask.maxPosition() + 1;
            boolean supported = !batchState.mask.none();
            for (int group : batchState.mask) {
                sparse = aggregations[output].copyResultPosition(
                        group,
                        maxGroup,
                        states[output],
                        sparse,
                        group,
                        size,
                        allocator,
                        allocationContext);
                if (sparse == null) {
                    supported = false;
                    break;
                }
            }
            if (supported) {
                streams = sparse;
            }
            else {
                streams = aggregations[output].result(maxGroup, states[output], batchState.mask, streams, allocator, allocationContext);
            }
        }
        else {
            streams = aggregations[output].result(maxGroup, states[output], batchState.mask, streams, allocator, allocationContext);
        }
        result[output] = streams;
        batchState.aggregationMaterializedMask[output] = batchState.mask;
        return streams;
    }

    private Streams aggregationCopyPosition(int output, Streams existing, int sourcePosition, int outputPosition, int size)
    {
        return aggregations[output].copyResultPosition(sourcePosition, maxGroup, states[output], existing, outputPosition, size, allocator, allocationContext);
    }

    private Streams groupedKeyOutput(int output, BatchState batchState)
    {
        Streams streams = groupedResults[output];
        if (streams != null && batchState.mask.equals(batchState.materializedMask[output])) {
            return streams;
        }
        if (batchState.mask.none() && groupByColumns != null && !inlineGroupingState.isInitialized()) {
            streams = emptyGroupedKeyOutput(streams);
            groupedResults[output] = streams;
            batchState.materializedMask[output] = batchState.mask;
            return streams;
        }
        if (groupByColumns == null) {
            streams = groupedKeySource.groupedKeyOutput(groupedColumns[output], batchState.mask, streams, allocator, allocationContext);
        }
        else {
            streams = inlineGroupingState.groupedValues(groupedKeyIndexes[output], batchState.mask, streams, allocator, allocationContext);
        }
        groupedResults[output] = streams;
        batchState.materializedMask[output] = batchState.mask;
        return streams;
    }

    private Streams groupedKeyCopyPosition(int output, Streams existing, int sourcePosition, int outputPosition, int size)
    {
        if (groupByColumns == null) {
            return groupedKeySource.copyGroupedKeyPosition(groupedColumns[output], existing, sourcePosition, outputPosition, size, allocator, allocationContext);
        }
        return inlineGroupingState.copyGroupedValuePosition(groupedKeyIndexes[output], existing, sourcePosition, outputPosition, size, allocator, allocationContext);
    }

    private Set<Stream> groupedKeyStreams(BatchState batchState)
    {
        return EnumSet.of(Stream.VALUES, Stream.NULLS);
    }

    private Streams emptyGroupedKeyOutput(Streams output)
    {
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.getOrNull(Stream.VALUES),
                I64Vector.class,
                0,
                I64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                0);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private final class BatchState
    {
        private Mask mask;
        private final Mask[] materializedMask = new Mask[groupedColumns.length];
        private final Mask[] aggregationMaterializedMask = new Mask[aggregations.length];

        private BatchState(Mask mask)
        {
            this.mask = mask;
        }

        private void constrain(Mask mask)
        {
            this.mask = mask;
        }
    }

    @Override
    public void close()
    {
        source.close();
        if (inlineGroupingState != null) {
            inlineGroupingState.releaseBuffers();
        }
        for (DistinctAggregationGroup distinctAggregationGroup : distinctAggregationGroups) {
            distinctAggregationGroup.releaseBuffers();
        }
        if (inlineGroupValues != null) {
            Arrays.fill(inlineGroupValues, null);
            Arrays.fill(inlineGroupNulls, null);
        }
        allocator.release(allocationContext);
    }

    private static int[] toArray(List<Integer> values)
    {
        return values.stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private static int[] mapGroupedKeyIndexes(List<Integer> groupByColumns, List<Integer> groupedColumns)
    {
        int[] indexes = new int[groupedColumns.size()];
        for (int outputIndex = 0; outputIndex < groupedColumns.size(); outputIndex++) {
            int groupedColumn = groupedColumns.get(outputIndex);
            int groupedKeyIndex = groupByColumns.indexOf(groupedColumn);
            if (groupedKeyIndex < 0) {
                throw new IllegalArgumentException("Grouped output " + groupedColumn + " is not present in group by columns " + groupByColumns);
            }
            indexes[outputIndex] = groupedKeyIndex;
        }
        return indexes;
    }

    private static DistinctAggregationPlan planDistinctAggregations(Accumulator[] aggregations)
    {
        List<Integer> plainAggregationIndexes = new ArrayList<>();
        List<Integer> filteredAggregationIndexes = new ArrayList<>();
        Map<DistinctSignature, List<Integer>> aggregationIndexesBySignature = new LinkedHashMap<>();
        for (int aggregationIndex = 0; aggregationIndex < aggregations.length; aggregationIndex++) {
            int[] distinctInputColumns = aggregations[aggregationIndex].distinctInputColumns();
            if (distinctInputColumns == null || distinctInputColumns.length == 0) {
                if (aggregations[aggregationIndex].filterInputColumn() < 0) {
                    plainAggregationIndexes.add(aggregationIndex);
                }
                else {
                    filteredAggregationIndexes.add(aggregationIndex);
                }
                continue;
            }
            aggregationIndexesBySignature.computeIfAbsent(new DistinctSignature(distinctInputColumns), _ -> new ArrayList<>())
                    .add(aggregationIndex);
        }

        DistinctAggregationGroup[] distinctAggregationGroups = aggregationIndexesBySignature.entrySet().stream()
                .map(entry -> new DistinctAggregationGroup(entry.getKey().inputColumns(), entry.getValue().stream().mapToInt(Integer::intValue).toArray()))
                .toArray(DistinctAggregationGroup[]::new);

        return new DistinctAggregationPlan(
                plainAggregationIndexes.stream().mapToInt(Integer::intValue).toArray(),
                filteredAggregationIndexes.stream().mapToInt(Integer::intValue).toArray(),
                distinctAggregationGroups);
    }

    private record DistinctAggregationPlan(int[] plainAggregationIndexes, int[] filteredAggregationIndexes, DistinctAggregationGroup[] distinctAggregationGroups) {}

    private record DistinctSignature(int[] inputColumns)
    {
        private DistinctSignature
        {
            inputColumns = inputColumns.clone();
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof DistinctSignature signature && java.util.Arrays.equals(inputColumns, signature.inputColumns);
        }

        @Override
        public int hashCode()
        {
            return java.util.Arrays.hashCode(inputColumns);
        }
    }

    private static final class DistinctAggregationGroup
    {
        private static final int[] EMPTY_POSITIONS = new int[0];
        private final int[] inputColumns;
        private final int[] aggregationIndexes;
        private final Vector[] values;
        private final Vector[] nulls;
        private DistinctKeySet distinctKeySet;
        private PrimitiveArrayPool arrayPool;
        private int[] distinctPositions = EMPTY_POSITIONS;

        private DistinctAggregationGroup(int[] inputColumns, int[] aggregationIndexes)
        {
            this.inputColumns = inputColumns.clone();
            this.aggregationIndexes = aggregationIndexes;
            this.values = new Vector[inputColumns.length + 1];
            this.nulls = new Vector[inputColumns.length + 1];
        }

        public int[] aggregationIndexes()
        {
            return aggregationIndexes;
        }

        public Mask select(I64Vector groups, Mask mask, org.weakref.nitro.operator.aggregation.StreamAccessor streamAccessor, int groupCount, Allocator allocator, Allocator.Context allocationContext)
        {
            arrayPool = allocator.primitiveArrays();
            if (mask.none()) {
                return allocator.allocateSparseMask(allocationContext, EMPTY_POSITIONS, mask.size());
            }

            values[0] = groups;
            if (distinctPositions.length < mask.selectedCount()) {
                int[] previous = distinctPositions;
                distinctPositions = arrayPool.borrowInts(mask.selectedCount());
                arrayPool.release(previous);
            }

            try {
                for (int index = 0; index < inputColumns.length; index++) {
                    values[index + 1] = streamAccessor.values(inputColumns[index]);
                    nulls[index + 1] = streamAccessor.nulls(inputColumns[index]);
                }
                if (distinctKeySet == null) {
                    distinctKeySet = GROUP_PARTITIONED_LONG_DISTINCT && inputColumns.length == 1
                            ? DistinctKeySet.createGroupedLong(values, arrayPool, allocator.engineResources().operatorCodeGeneration())
                            : DistinctKeySet.create(values, arrayPool, allocator.engineResources().operatorCodeGeneration());
                }
                int selectedCount = distinctKeySet.addGroupedBatch(values, nulls, mask, groupCount, distinctPositions);
                return allocator.allocateSparseMask(allocationContext, distinctPositions, selectedCount, mask.size());
            }
            finally {
                Arrays.fill(values, null);
                Arrays.fill(nulls, null);
            }
        }

        private void releaseBuffers()
        {
            if (distinctKeySet != null) {
                distinctKeySet.releaseBuffers();
                distinctKeySet = null;
            }
            if (arrayPool != null) {
                arrayPool.release(distinctPositions);
            }
            distinctPositions = EMPTY_POSITIONS;
            Arrays.fill(values, null);
            Arrays.fill(nulls, null);
        }
    }
}
