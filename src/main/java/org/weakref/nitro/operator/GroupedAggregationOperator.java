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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.FusedAccumulatorSpec;
import org.weakref.nitro.operator.aggregation.FusedAggregator;
import org.weakref.nitro.operator.aggregation.StreamAccessors;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
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
    private static final int FUSE_GROUP_LIMIT = 1 << 15;

    private final Allocator.Context allocationContext = new Allocator.Context("GroupedAggregationOperator");
    private final Allocator allocator;

    private final int groupColumn;
    private final int[] groupedColumns;
    private final int[] groupByColumns;
    private final int[] groupedKeyIndexes;
    private final Accumulator[] aggregations;
    private final int[] plainAggregationIndexes;
    private final DistinctAggregationGroup[] distinctAggregationGroups;
    private final Operator source;
    private final Streams[] groupedResults;
    private final Streams[] result;
    private final GroupingState inlineGroupingState;
    private Streams[] states;
    private int stateCapacity;
    private int maxGroup = -1;
    private boolean done;
    private GroupedKeySource groupedKeySource;
    private I64Vector reusableGroups;
    // Fused single-long-key path: assign the group and accumulate every aggregation in one inlined pass, no
    // group-id vector and no per-row accumulator dispatch. The per-shape kernel is generated as bytecode.
    // Eligibility is decided once the grouping mode is known; falls back to the staged path per batch when a
    // batch isn't the flat, null-free shape the fused loop handles.
    private boolean fusedEligible;
    private boolean fusedChecked;
    private FusedGroupingKernel fusedKernel;
    private FusedAccumulatorSpec[] fusedSpecs;
    private long[][] fusedInputs;
    private Object[] fusedStateVectors;

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
        this(allocator, -1, groupedColumns, aggregations, source, toArray(groupByColumns), mapGroupedKeyIndexes(groupByColumns, groupedColumns), new GroupingState());
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
        this.distinctAggregationGroups = distinctAggregationPlan.distinctAggregationGroups();
        this.source = source;
        this.inlineGroupingState = inlineGroupingState;

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

    private Mask computeResults()
    {
        if (groupByColumns != null) {
            return computeInlineGroupedResults();
        }

        states = new Streams[aggregations.length];
        stateCapacity = 0;
        long maxObservedGroup = -1;
        while (source.hasNext()) {
            Batch batch = source.next();
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
            accumulateGroupedRows(group, mask, streamAccessor);
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
                if (fusedEligible
                        && inlineGroupingState.groupCount() < FUSE_GROUP_LIMIT
                        && tryFusedSingleLongAggregation(batch, mask)) {
                    maxObservedGroup = inlineGroupingState.groupCount() - 1;
                    continue;
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
                accumulateGroupedRows(reusableGroups, mask, streamAccessor);
            }
        }

        finishResults(maxObservedGroup);
        return allocator.allocateAllMask(allocationContext, this.maxGroup + 1);
    }

    /**
     * Decides once whether the inline grouped aggregation can use the fused single-long-key path: one long
     * group key, no DISTINCT, single-long grouping mode, and every aggregation a {@link FusedAggregator}.
     * When eligible, builds (and caches) the bytecode kernel specialized to this accumulator-set shape.
     */
    private void prepareFusedKernel()
    {
        fusedEligible = groupByColumns != null
                && groupByColumns.length == 1
                && distinctAggregationGroups.length == 0
                && aggregations.length > 0
                && inlineGroupingState.usesSingleLongGrouping()
                && allFusible();
        if (!fusedEligible) {
            return;
        }
        fusedSpecs = new FusedAccumulatorSpec[aggregations.length];
        for (int index = 0; index < aggregations.length; index++) {
            fusedSpecs[index] = ((FusedAggregator) aggregations[index]).fusedSpec();
        }
        fusedKernel = FusedGroupingAggregationKernelGenerator.create(List.of(fusedSpecs));
        fusedInputs = new long[aggregations.length][];
        fusedStateVectors = new Object[aggregations.length];
    }

    private boolean allFusible()
    {
        for (Accumulator aggregation : aggregations) {
            if (!(aggregation instanceof FusedAggregator)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fused single-long-key aggregation: one generated pass that probes the group table and accumulates
     * every aggregation with no group-id vector and no per-row dispatch. Handles the common flat, null-free
     * batch shape; returns false (caller falls back to the staged path) for any other encoding or when nulls
     * are present in the key or any read value column.
     */
    private boolean tryFusedSingleLongAggregation(Batch batch, Mask mask)
    {
        Output keyOutput = batch.output(groupByColumns[0]);
        Vector keyVector = keyOutput.borrow(Stream.VALUES);
        if (!(keyVector instanceof I64Vector keyValues) || !VectorAccess.isAllFalseNulls(keyOutput.borrowOrNull(Stream.NULLS))) {
            return false;
        }
        for (int index = 0; index < fusedSpecs.length; index++) {
            FusedAccumulatorSpec spec = fusedSpecs[index];
            if (!spec.readsValue()) {
                fusedInputs[index] = null;
                continue;
            }
            Output valueOutput = batch.output(spec.valueColumn());
            Vector valueVector = valueOutput.borrow(Stream.VALUES);
            if (!(valueVector instanceof I64Vector valueValues) || !VectorAccess.isAllFalseNulls(valueOutput.borrowOrNull(Stream.NULLS))) {
                return false;
            }
            fusedInputs[index] = valueValues.values();
        }

        int count = mask.count();
        // Pre-reserve so the inlined probe needs no rehash branch and no per-row state growth.
        inlineGroupingState.reserveSingleLongTable(count);
        ensureFusedStateCapacity(toIntExact(inlineGroupingState.groupCount() + count));
        for (int index = 0; index < aggregations.length; index++) {
            fusedStateVectors[index] = states[index].values();
        }

        long nextId = fusedKernel.accumulate(
                mask.selectedPositions(),
                count,
                keyValues.values(),
                inlineGroupingState.longGroupKeys,
                inlineGroupingState.longGroupIds,
                inlineGroupingState.longGroupMask,
                inlineGroupingState.longKeysByGroup,
                inlineGroupingState.nextGroupId,
                fusedInputs,
                fusedStateVectors);

        inlineGroupingState.nextGroupId = nextId;
        inlineGroupingState.longGroupCount = (int) nextId;
        return true;
    }

    private void ensureFusedStateCapacity(int needed)
    {
        if (states[0] == null) {
            int capacity = Allocator.computeCapacity(Math.max(16, needed));
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
        int capacity = Allocator.computeCapacity(needed);
        for (int index = 0; index < aggregations.length; index++) {
            states[index] = aggregations[index].grow(allocator, allocationContext, states[index], capacity);
            aggregations[index].initialize(states[index], stateCapacity, capacity - stateCapacity);
        }
        stateCapacity = capacity;
    }

    private void initializeInlineGroupingSchema(Batch batch)
    {
        if (groupByColumns == null || groupByColumns.length == 0) {
            return;
        }
        Vector[] values = new Vector[groupByColumns.length];
        Vector[] nulls = new Vector[groupByColumns.length];
        for (int index = 0; index < groupByColumns.length; index++) {
            Output output = batch.output(groupByColumns[index]);
            try {
                values[index] = output.borrowOrNull(Stream.VALUES);
                nulls[index] = output.borrowOrNull(Stream.NULLS);
            }
            catch (IllegalArgumentException ignored) {
                return;
            }
            if (values[index] == null) {
                return;
            }
        }
        inlineGroupingState.initializeSchema(values, nulls);
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

        Vector[] values = new Vector[groupByColumns.length];
        Vector[] nulls = new Vector[groupByColumns.length];
        for (int index = 0; index < groupByColumns.length; index++) {
            Output output = batch.output(groupByColumns[index]);
            values[index] = output.borrow(Stream.VALUES);
            nulls[index] = output.borrowOrNull(Stream.NULLS);
        }
        inlineGroupingState.assignGroups(values, nulls, mask, groups);
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

    private void accumulateGroupedRows(I64Vector groups, Mask mask, org.weakref.nitro.operator.aggregation.StreamAccessor streamAccessor)
    {
        for (int aggregationIndex : plainAggregationIndexes) {
            aggregations[aggregationIndex].accumulate(states[aggregationIndex], groups, mask, streamAccessor);
        }

        for (DistinctAggregationGroup distinctAggregationGroup : distinctAggregationGroups) {
            Mask distinctMask = distinctAggregationGroup.select(groups, mask, streamAccessor, allocator, allocationContext);
            try {
                for (int aggregationIndex : distinctAggregationGroup.aggregationIndexes()) {
                    aggregations[aggregationIndex].accumulateDistinctSelected(states[aggregationIndex], groups, distinctMask, streamAccessor);
                }
            }
            finally {
                allocator.release(allocationContext, distinctMask);
            }
        }
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
            outputs[outputIndex] = resultOutput(output, batchState);
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
        // Output is fully computed and constrain() is a no-op, so a downstream constrain + re-borrow
        // would re-read the full, differently-indexed output. Cannot satisfy a constrained re-borrow.
        return false;
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
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }

        Streams streams = result[output - groupedResults.length];
        return new Output(
                EnumSet.of(Stream.VALUES, Stream.NULLS),
                stream -> aggregationOutput(output - groupedResults.length, batchState).get(stream),
                (stream, vector) -> allocator.transfer(allocationContext, vector),
                (stream, vector) -> allocator.release(allocationContext, vector),
                (existing, sourcePosition, outputPosition, size) -> aggregationCopyPosition(output - groupedResults.length, existing, sourcePosition, outputPosition, size));
    }

    private Streams aggregationOutput(int output, BatchState batchState)
    {
        Streams streams = result[output];
        if (streams != null && batchState.aggregationMaterializedMask[output] != null && batchState.mask.equals(batchState.aggregationMaterializedMask[output])) {
            return streams;
        }
        streams = aggregations[output].result(maxGroup, states[output], batchState.mask, streams, allocator, allocationContext);
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
        Map<DistinctSignature, List<Integer>> aggregationIndexesBySignature = new LinkedHashMap<>();
        for (int aggregationIndex = 0; aggregationIndex < aggregations.length; aggregationIndex++) {
            int[] distinctInputColumns = aggregations[aggregationIndex].distinctInputColumns();
            if (distinctInputColumns == null || distinctInputColumns.length == 0) {
                plainAggregationIndexes.add(aggregationIndex);
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
                distinctAggregationGroups);
    }

    private record DistinctAggregationPlan(int[] plainAggregationIndexes, DistinctAggregationGroup[] distinctAggregationGroups) {}

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
        private final int[] inputColumns;
        private final int[] aggregationIndexes;
        private DistinctKeySet distinctKeySet;
        private int[] distinctPositions = new int[0];

        private DistinctAggregationGroup(int[] inputColumns, int[] aggregationIndexes)
        {
            this.inputColumns = inputColumns.clone();
            this.aggregationIndexes = aggregationIndexes;
        }

        public int[] aggregationIndexes()
        {
            return aggregationIndexes;
        }

        public Mask select(I64Vector groups, Mask mask, org.weakref.nitro.operator.aggregation.StreamAccessor streamAccessor, Allocator allocator, Allocator.Context allocationContext)
        {
            if (mask.none()) {
                return allocator.allocateSparseMask(allocationContext, new int[0], mask.size());
            }

            Vector[] values = new Vector[inputColumns.length + 1];
            Vector[] nulls = new Vector[inputColumns.length + 1];
            values[0] = groups;
            for (int index = 0; index < inputColumns.length; index++) {
                values[index + 1] = streamAccessor.values(inputColumns[index]);
                nulls[index + 1] = streamAccessor.nulls(inputColumns[index]);
            }

            if (distinctKeySet == null) {
                distinctKeySet = DistinctKeySet.create(values);
            }
            if (distinctPositions.length < mask.selectedCount()) {
                distinctPositions = new int[mask.selectedCount()];
            }

            int selectedCount = distinctKeySet.addBatch(values, nulls, mask, distinctPositions);
            return allocator.allocateSparseMask(allocationContext, distinctPositions, selectedCount, mask.size());
        }
    }
}
