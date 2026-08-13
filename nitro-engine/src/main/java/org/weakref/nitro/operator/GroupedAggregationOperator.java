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

import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.LongStateUpdate;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.GeneratedLongGroupingBindings;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.GeneratedGroupedAggregationUnit;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.StreamAccessors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class GroupedAggregationOperator
        implements Operator
{
    private final Allocator.Context allocationContext;
    private final Allocator allocator;
    private final OperatorResources operatorResources;
    private final AggregationExecutionContext aggregationExecutionContext;
    // Above this group count the group table + accumulator state spill out of cache, where the staged
    // two-pass wins on memory-level parallelism; below it the fused single pass wins. Cardinality-gated.
    private final int fuseGroupLimit;
    // Dictionary-mapped or repeatedly adjacent keys have already proved that fusion avoids a second random
    // state walk. Let those physically reusable shapes finish instead of switching representation part-way
    // through and duplicating state growth; random high-cardinality input is still rejected at fuseGroupLimit.
    // Keep a hard bound so a malformed or unexpectedly large domain cannot grow fused state without limit.
    private final int fuseLocalGroupLimit;
    private final int fuseMappedOnlyGroupLimit;
    private final boolean dynamicFilterThroughAggregation;
    private final boolean sparseConstrainedResults;
    private final boolean groupPartitionedLongDistinct;
    private final boolean partialGeneratedGrouping;
    private final boolean fusedDictionaryInput;
    private final boolean fusedLongRunCache;
    private final boolean fusedConstantRuns;
    private final int fusedConstantRunGroupMin;
    private final boolean fusedLongDirectGrouping;
    private final boolean fusedMappedContinuationPowerOfTwoStateCapacity;
    private final boolean debugFusedGrouping;
    private final MutableAggregationPhaseMetrics phaseMetrics;

    private final int groupColumn;
    private final int[] groupedColumns;
    private final int[] groupByColumns;
    private final int[] groupedKeyIndexes;
    private final TypeBinding[] inlineGroupedOutputTypes;
    private final PhysicalAggregationProgram program;
    private final Schema outputSchema;
    private final PhysicalAggregationUnit[] aggregations;
    private final int[] plainAggregationIndexes;
    private final int[] filteredAggregationIndexes;
    private final DistinctAggregationPlan.Group[] distinctAggregationGroups;
    private final Operator source;
    private final Streams[] groupedResults;
    private final Streams[] result;
    private final GroupingState inlineGroupingState;
    private final Vector[] inlineGroupValues;
    private final Vector[] inlineGroupNulls;
    private Object[] states;
    private int stateCapacity;
    private long maxObservedGroup = -1;
    private int maxGroup = -1;
    private boolean done;
    private boolean groupIdsDiscarded;
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
    private GroupedAggregationUpdate[] fusedSpecs;
    private boolean fusedIntermediateMerge;
    private int[] fusedAggregationIndexes;
    private int[] fusedStateOffsets;
    private GeneratedLongGroupingBindings fusedBindings;
    private LongStateUpdate[] fusedStateVectors;
    private boolean fusedStateVectorsBound;
    private int fusedPhysicalShape = -1;
    private boolean debugFusedLimitPrinted;
    private boolean debugFusedReuseContinuationPrinted;
    private boolean debugFusedConstantRunsPrinted;
    private int nextSessionOutputPosition;

    long retainedBytes()
    {
        return allocator.scopeCurrentBytes(allocationContext);
    }

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, groupColumn, PhysicalAggregationProgram.independent(aggregations), source, EngineResources.from(allocator).operatorResources());
    }

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Accumulator> aggregations, Operator source, OperatorResources operatorResources)
    {
        this(allocator, groupColumn, PhysicalAggregationProgram.independent(aggregations), source, operatorResources);
    }

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, PhysicalAggregationProgram program, Operator source)
    {
        this(allocator, groupColumn, program, source, EngineResources.from(allocator).operatorResources());
    }

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, PhysicalAggregationProgram program, Operator source, OperatorResources operatorResources)
    {
        this(allocator, groupColumn, List.of(), program, source, null, null, null, operatorResources);
    }

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Integer> groupedColumns, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, groupColumn, groupedColumns, aggregations, source, EngineResources.from(allocator).operatorResources());
    }

    public GroupedAggregationOperator(Allocator allocator, int groupColumn, List<Integer> groupedColumns, List<Accumulator> aggregations, Operator source, OperatorResources operatorResources)
    {
        this(allocator, groupColumn, groupedColumns, PhysicalAggregationProgram.independent(aggregations), source, null, null, null, operatorResources);
    }

    public GroupedAggregationOperator(Allocator allocator, List<Integer> groupByColumns, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, groupByColumns, groupByColumns, aggregations, source);
    }

    public GroupedAggregationOperator(Allocator allocator, List<Integer> groupByColumns, List<Accumulator> aggregations, Operator source, OperatorResources operatorResources)
    {
        this(allocator, groupByColumns, groupByColumns, aggregations, source, operatorResources);
    }

    public GroupedAggregationOperator(Allocator allocator, List<Integer> groupByColumns, PhysicalAggregationProgram program, Operator source)
    {
        this(allocator, groupByColumns, groupByColumns, program, source, EngineResources.from(allocator).operatorResources());
    }

    public GroupedAggregationOperator(Allocator allocator, List<Integer> groupByColumns, PhysicalAggregationProgram program, Operator source, OperatorResources operatorResources)
    {
        this(allocator, groupByColumns, groupByColumns, program, source, operatorResources);
    }

    public GroupedAggregationOperator(Allocator allocator, List<Integer> groupByColumns, List<Integer> groupedColumns, List<Accumulator> aggregations, Operator source)
    {
        this(allocator, groupByColumns, groupedColumns, aggregations, source, EngineResources.from(allocator).operatorResources());
    }

    public GroupedAggregationOperator(
            Allocator allocator,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            List<Accumulator> aggregations,
            Operator source,
            OperatorResources operatorResources)
    {
        this(allocator, groupByColumns, groupedColumns, PhysicalAggregationProgram.independent(aggregations), source, operatorResources);
    }

    public GroupedAggregationOperator(
            Allocator allocator,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            Operator source)
    {
        this(allocator, groupByColumns, groupedColumns, program, source, EngineResources.from(allocator).operatorResources());
    }

    public GroupedAggregationOperator(
            Allocator allocator,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            Operator source,
            OperatorResources operatorResources)
    {
        this(allocator, groupByColumns, groupedColumns, program, source, operatorResources, operatorResources.grouping());
    }

    public GroupedAggregationOperator(
            Allocator allocator,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            Operator source,
            OperatorResources operatorResources,
            GroupingStateResources groupingResources)
    {
        this(
                allocator,
                groupByColumns,
                groupedColumns,
                program,
                source,
                operatorResources,
                groupingResources,
                new Allocator.Context("GroupedAggregationOperator"));
    }

    GroupedAggregationOperator(
            Allocator allocator,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            Operator source,
            OperatorResources operatorResources,
            GroupingStateResources groupingResources,
            Allocator.Context allocationContext)
    {
        this(
                allocator,
                groupByColumns,
                groupedColumns,
                program,
                source,
                operatorResources,
                groupingResources,
                allocationContext,
                new MutableAggregationPhaseMetrics());
    }

    GroupedAggregationOperator(
            Allocator allocator,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            Operator source,
            OperatorResources operatorResources,
            GroupingStateResources groupingResources,
            Allocator.Context allocationContext,
            MutableAggregationPhaseMetrics phaseMetrics)
    {
        this(
                allocator,
                -1,
                groupedColumns,
                program,
                source,
                toArray(groupByColumns),
                mapGroupedKeyIndexes(groupByColumns, groupedColumns),
                groupingTypes(source.outputSchema(), groupByColumns),
                requireNonNull(operatorResources, "operatorResources is null"),
                requireNonNull(groupingResources, "groupingResources is null"),
                requireNonNull(allocationContext, "allocationContext is null"),
                requireNonNull(phaseMetrics, "phaseMetrics is null"));
    }

    private static List<TypeBinding> groupingTypes(Schema sourceSchema, List<Integer> groupByColumns)
    {
        if (groupByColumns.stream().anyMatch(column -> column < 0 || column >= sourceSchema.size())) {
            return List.of();
        }
        return groupByColumns.stream()
                .map(column -> sourceSchema.field(column).type())
                .toList();
    }

    private GroupedAggregationOperator(
            Allocator allocator,
            int groupColumn,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            Operator source,
            int[] groupByColumns,
            int[] groupedKeyIndexes,
            List<TypeBinding> inlineGroupingTypes,
            OperatorResources operatorResources)
    {
        this(
                allocator,
                groupColumn,
                groupedColumns,
                program,
                source,
                groupByColumns,
                groupedKeyIndexes,
                inlineGroupingTypes,
                operatorResources,
                operatorResources.grouping(),
                new Allocator.Context("GroupedAggregationOperator"),
                new MutableAggregationPhaseMetrics());
    }

    private GroupedAggregationOperator(
            Allocator allocator,
            int groupColumn,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            Operator source,
            int[] groupByColumns,
            int[] groupedKeyIndexes,
            List<TypeBinding> inlineGroupingTypes,
            OperatorResources operatorResources,
            GroupingStateResources groupingResources,
            Allocator.Context allocationContext,
            MutableAggregationPhaseMetrics phaseMetrics)
    {
        if (!groupedColumns.isEmpty() && groupByColumns == null && !(source instanceof GroupedKeySource)) {
            throw new IllegalArgumentException("Source must implement GroupedKeySource when grouped outputs are requested");
        }
        this.allocator = allocator;
        this.allocationContext = requireNonNull(allocationContext, "allocationContext is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        AggregationOperatorPolicy policy = operatorResources.aggregation().policy();
        this.fuseGroupLimit = policy.fuseGroupLimit();
        this.fuseLocalGroupLimit = policy.fuseLocalGroupLimit();
        this.fuseMappedOnlyGroupLimit = policy.fuseMappedOnlyGroupLimit();
        this.dynamicFilterThroughAggregation = policy.dynamicFilterThroughAggregation();
        this.sparseConstrainedResults = policy.sparseConstrainedResults();
        this.groupPartitionedLongDistinct = policy.groupPartitionedLongDistinct();
        this.partialGeneratedGrouping = policy.partialGeneratedGrouping();
        this.fusedDictionaryInput = policy.fusedDictionaryInput();
        this.fusedLongRunCache = policy.fusedLongRunCache();
        this.fusedConstantRuns = policy.fusedConstantRuns();
        this.fusedConstantRunGroupMin = policy.fusedConstantRunGroupMin();
        this.fusedLongDirectGrouping = policy.fusedLongDirectGrouping();
        this.fusedMappedContinuationPowerOfTwoStateCapacity = policy.fusedMappedContinuationPowerOfTwoStateCapacity();
        this.debugFusedGrouping = policy.debugFusedGrouping();
        this.phaseMetrics = requireNonNull(phaseMetrics, "phaseMetrics is null");
        this.aggregationExecutionContext = new AggregationExecutionContext(
                allocator,
                allocationContext,
                operatorResources.codeGeneration(),
                operatorResources.distinctKeySetPolicy(),
                operatorResources.adaptiveLongGroupingPolicy(),
                operatorResources.flatKeyTablePolicy(),
                source.outputSchema());
        this.groupColumn = groupColumn;
        this.groupedColumns = groupedColumns.stream()
                .mapToInt(Integer::intValue)
                .toArray();
        this.groupByColumns = groupByColumns;
        this.groupedKeyIndexes = groupedKeyIndexes;
        this.inlineGroupedOutputTypes = groupByColumns == null
                ? null
                : Arrays.stream(groupedKeyIndexes)
                        .mapToObj(index -> index < inlineGroupingTypes.size() ? inlineGroupingTypes.get(index) : null)
                        .toArray(TypeBinding[]::new);
        this.program = requireNonNull(program, "program is null");
        this.outputSchema = outputSchema(source.outputSchema(), this.groupedColumns, program.outputSchema());
        this.aggregations = program.units().toArray(PhysicalAggregationUnit[]::new);
        DistinctAggregationPlan distinctAggregationPlan = DistinctAggregationPlan.plan(
                this.aggregations,
                groupPartitionedLongDistinct,
                source.outputSchema());
        this.plainAggregationIndexes = distinctAggregationPlan.plainAggregationIndexes();
        this.filteredAggregationIndexes = distinctAggregationPlan.filteredAggregationIndexes();
        this.distinctAggregationGroups = distinctAggregationPlan.distinctAggregationGroups();
        this.source = source;
        GroupingStateResources effectiveGroupingResources = this.aggregations.length == 0
                ? groupingResources.forGroupingOnlyAggregation()
                : groupingResources;
        this.inlineGroupingState = groupByColumns == null
                ? null
                : new GroupingState(
                        allocator.primitiveArrays(),
                        operatorResources.codeGeneration(),
                        effectiveGroupingResources,
                        operatorResources.adaptiveLongGroupingPolicy(),
                        operatorResources.flatKeyTablePolicy(),
                        inlineGroupingTypes,
                        allocator,
                        allocationContext);
        this.inlineGroupValues = groupByColumns == null ? null : new Vector[groupByColumns.length];
        this.inlineGroupNulls = groupByColumns == null ? null : new Vector[groupByColumns.length];

        groupedResults = new Streams[this.groupedColumns.length];
        result = new Streams[program.outputs().size()];
    }

    boolean usesPackedFlatIdentitySlots()
    {
        return inlineGroupingState != null && inlineGroupingState.usesPackedFlatIdentitySlots();
    }

    boolean discardsInlineGroupIds()
    {
        return groupIdsDiscarded;
    }

    @Override
    public int outputCount()
    {
        return groupedColumns.length + program.outputs().size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    private static Schema outputSchema(Schema sourceSchema, int[] groupedColumns, Schema aggregationSchema)
    {
        List<Field> fields = new ArrayList<>(groupedColumns.length + aggregationSchema.size());
        Field unspecified = Schema.unspecified(1).field(0);
        for (int groupedColumn : groupedColumns) {
            fields.add(groupedColumn >= 0 && groupedColumn < sourceSchema.size() ? sourceSchema.field(groupedColumn) : unspecified);
        }
        fields.addAll(aggregationSchema.fields());
        return new Schema(fields);
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
        if (!dynamicFilterThroughAggregation || groupByColumns == null) {
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

        states = new Object[aggregations.length];
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
        for (int i = 0; i < aggregations.length; i++) {
            if (states[i] == null) {
                states[i] = aggregations[i].allocate(aggregationExecutionContext, 0);
            }
        }
        for (int i = 0; i < result.length; i++) {
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
        startInlineGrouping();
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                addInlineInput(batch);
            }
        }

        return finishInlineGrouping();
    }

    void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        if (groupByColumns == null) {
            throw new IllegalStateException("incremental input requires inline grouping columns");
        }
        if (done) {
            throw new IllegalStateException("grouped aggregation is finished");
        }
        startInlineGrouping();
        addInlineInput(batch);
    }

    Batch finishInput()
    {
        return finishInput(Integer.MAX_VALUE);
    }

    Batch finishInput(int maxOutputRows)
    {
        if (groupByColumns == null) {
            throw new IllegalStateException("incremental input requires inline grouping columns");
        }
        if (done) {
            throw new IllegalStateException("grouped aggregation is finished");
        }
        if (maxOutputRows <= 0) {
            throw new IllegalArgumentException("maxOutputRows must be positive");
        }
        startInlineGrouping();
        finishResults(maxObservedGroup);
        return nextSessionOutputBatch(maxOutputRows);
    }

    boolean hasSessionOutput()
    {
        return done && nextSessionOutputPosition <= maxGroup;
    }

    int sessionOutputRowCount()
    {
        if (!done) {
            throw new IllegalStateException("grouped aggregation is not finished");
        }
        return maxGroup + 1;
    }

    Batch getSessionOutput(int maxOutputRows)
    {
        if (!hasSessionOutput()) {
            throw new IllegalStateException("grouped aggregation has no session output");
        }
        return nextSessionOutputBatch(maxOutputRows);
    }

    private Batch nextSessionOutputBatch(int maxOutputRows)
    {
        if (maxGroup < 0) {
            return outputBatch(allocator.allocateAllMask(allocationContext, 0));
        }
        if (maxOutputRows == Integer.MAX_VALUE) {
            nextSessionOutputPosition = maxGroup + 1;
            return outputBatch(allocator.allocateAllMask(allocationContext, maxGroup + 1));
        }
        int outputRows = Math.min(maxOutputRows, maxGroup + 1 - nextSessionOutputPosition);
        int sourceStart = nextSessionOutputPosition;
        nextSessionOutputPosition += outputRows;
        return denseOutputBatch(sourceStart, outputRows);
    }

    private Batch denseOutputBatch(int sourceStart, int outputRows)
    {
        Mask mask = allocator.allocateAllMask(allocationContext, outputRows);
        DenseBatchState batchState = new DenseBatchState(sourceStart, outputRows, mask);
        Output[] outputs = new Output[outputCount()];
        for (int output = 0; output < outputs.length; output++) {
            int outputIndex = output;
            outputs[output] = new Output(
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    stream -> batchState.output(outputIndex).get(stream),
                    (_, vector) -> allocator.transfer(allocationContext, vector),
                    (_, vector) -> allocator.release(allocationContext, vector),
                    (_, existing, sourcePositions, sourcePositionStart, sourcePositionCount, outputStart, size, _) -> copyDenseOutputPositions(
                            outputIndex,
                            existing,
                            batchState.sourceStart,
                            sourcePositions,
                            sourcePositionStart,
                            sourcePositionCount,
                            outputStart,
                            size),
                    (existing, sourcePosition, outputPosition, size) -> copyDenseOutputPosition(
                            outputIndex,
                            existing,
                            batchState.sourceStart + sourcePosition,
                            outputPosition,
                            size))
                    .withConstraintSensitiveResolution();
        }
        return new Batch(
                mask,
                batchState::constrain,
                takenMask -> allocator.transfer(allocationContext, takenMask),
                releasedMask -> allocator.release(allocationContext, releasedMask),
                () -> {},
                outputs);
    }

    private Streams copyDenseOutputPosition(int output, Streams existing, int sourcePosition, int outputPosition, int size)
    {
        Streams streams = output < groupedResults.length
                ? groupedKeyCopyPosition(output, existing, sourcePosition, outputPosition, size)
                : aggregationCopyPosition(output - groupedResults.length, existing, sourcePosition, outputPosition, size);
        if (streams != null) {
            return streams;
        }
        if (output < groupedResults.length) {
            throw new IllegalStateException("Grouped key output %s does not support dense position copying".formatted(output));
        }
        return null;
    }

    private Streams copyDenseOutputPositions(
            int output,
            Streams existing,
            int sourceBase,
            int[] sourcePositions,
            int sourceStart,
            int sourceCount,
            int outputStart,
            int size)
    {
        if (output >= groupedResults.length || groupByColumns == null) {
            return null;
        }
        int[] groupedPositions = new int[sourceCount];
        for (int index = 0; index < sourceCount; index++) {
            groupedPositions[index] = sourceBase + sourcePositions[sourceStart + index];
        }
        return inlineGroupingState.copyGroupedValuePositions(
                groupedKeyIndexes[output],
                existing,
                groupedPositions,
                0,
                sourceCount,
                outputStart,
                size,
                allocator,
                allocationContext);
    }

    private void startInlineGrouping()
    {
        if (states != null) {
            return;
        }
        states = new Object[aggregations.length];
        stateCapacity = 0;
        maxObservedGroup = -1;
    }

    private void addInlineInput(Batch batch)
    {
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            initializeInlineGroupingSchema(batch, mask);
            return;
        }

        if (!inlineGroupingState.isInitialized()) {
            initializeInlineGroupingSchema(batch, mask);
        }
        if (inlineGroupingState.isInitialized() && !fusedChecked) {
            prepareFusedKernel();
            fusedChecked = true;
        }

        // Fuse only while the group table + state stay cache-resident. Beyond that the staged two-pass
        // wins on memory-level parallelism (each pass streams one random-access array the OOO window
        // overlaps), whereas fusion serializes probe-miss -> state-miss per row.
        if (fusedEligible && inlineGroupingState.groupCount() < fuseLocalGroupLimit) {
            long start = System.nanoTime();
            boolean fused;
            try {
                fused = tryFusedSingleLongAggregation(batch, mask);
            }
            finally {
                phaseMetrics.recordFused(System.nanoTime() - start);
            }
            if (fused) {
                maxObservedGroup = inlineGroupingState.groupCount() - 1;
                if (filteredAggregationIndexes.length != 0 || distinctAggregationGroups.length != 0) {
                    start = System.nanoTime();
                    try {
                        accumulateFilteredGroupedRows(batch, reusableGroups, mask, StreamAccessors.forBatch(batch));
                        accumulateDistinctGroupedRows(batch, reusableGroups, mask, StreamAccessors.forBatch(batch), toIntExact(inlineGroupingState.groupCount()));
                    }
                    finally {
                        phaseMetrics.recordAccumulation(System.nanoTime() - start);
                    }
                }
                return;
            }
        }

        long previousMaxGroup = maxObservedGroup;
        if (aggregations.length == 0) {
            long start = System.nanoTime();
            boolean discarded;
            try {
                discarded = assignInlineGroupsDiscardingResults(batch, mask);
            }
            finally {
                phaseMetrics.recordGrouping(System.nanoTime() - start);
            }
            if (discarded) {
                groupIdsDiscarded = true;
                maxObservedGroup = inlineGroupingState.groupCount() - 1;
                return;
            }
        }
        reusableGroups = allocator.reallocateIfNecessary(allocationContext, reusableGroups, I64Vector.class, mask.maxPosition() + 1, I64Vector::new);
        long start = System.nanoTime();
        try {
            assignInlineGroups(batch, mask, reusableGroups);
        }
        finally {
            phaseMetrics.recordGrouping(System.nanoTime() - start);
        }
        // The grouping state knows the max assigned group id (group ids are dense 0..count-1),
        // so use it directly instead of a separate O(rows) scan of the just-assigned group vector.
        maxObservedGroup = inlineGroupingState.groupCount() - 1;

        int requiredCapacity = toIntExact(maxObservedGroup + 1);
        int defaultCapacity = Allocator.computeCapacity(requiredCapacity);
        int newCapacity = requiredCapacity;
        for (PhysicalAggregationUnit aggregation : aggregations) {
            int preferredCapacity = aggregation.stateCapacity(requiredCapacity, defaultCapacity);
            if (preferredCapacity < requiredCapacity) {
                throw new IllegalArgumentException("aggregation state capacity is less than required group count");
            }
            newCapacity = Math.max(newCapacity, preferredCapacity);
        }
        var streamAccessor = StreamAccessors.forBatch(batch);
        start = System.nanoTime();
        try {
            prepareAggregationStates(previousMaxGroup, maxObservedGroup, newCapacity);
        }
        finally {
            phaseMetrics.recordStatePreparation(System.nanoTime() - start);
        }
        start = System.nanoTime();
        try {
            accumulateGroupedRows(batch, reusableGroups, mask, streamAccessor, toIntExact(inlineGroupingState.groupCount()));
        }
        finally {
            phaseMetrics.recordAccumulation(System.nanoTime() - start);
        }
    }

    private Mask finishInlineGrouping()
    {
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
                && (filteredAggregationIndexes.length == 0 || partialGeneratedGrouping)
                && (distinctAggregationGroups.length == 0 || partialGeneratedGrouping)
                && inlineGroupingState.usesSingleLongGrouping()
                && allPlainAggregationsFusible();
        if (!fusedEligible) {
            return;
        }
        fusedAggregationIndexes = plainAggregationIndexes.clone();
        fusedStateOffsets = new int[fusedAggregationIndexes.length + 1];
        List<GroupedAggregationUpdate> updates = new ArrayList<>();
        for (int index = 0; index < fusedAggregationIndexes.length; index++) {
            List<GroupedAggregationUpdate> unitUpdates = List.copyOf(
                    ((GeneratedGroupedAggregationUnit) aggregations[fusedAggregationIndexes[index]]).generatedGroupedUpdates());
            if (unitUpdates.isEmpty()) {
                throw new IllegalArgumentException("generated grouped aggregation unit has no updates");
            }
            fusedStateOffsets[index] = updates.size();
            updates.addAll(unitUpdates);
        }
        fusedStateOffsets[fusedAggregationIndexes.length] = updates.size();
        fusedSpecs = updates.toArray(GroupedAggregationUpdate[]::new);
        fusedIntermediateMerge = fusedAggregationIndexes.length > 0;
        for (int aggregationIndex : fusedAggregationIndexes) {
            fusedIntermediateMerge &= ((GeneratedGroupedAggregationUnit) aggregations[aggregationIndex])
                    .mergesIntermediateInput();
        }
        fusedBindings = new GeneratedLongGroupingBindings(fusedSpecs.length, fusedDictionaryInput);
        fusedStateVectors = new LongStateUpdate[fusedSpecs.length];
    }

    private boolean allPlainAggregationsFusible()
    {
        for (int aggregationIndex : plainAggregationIndexes) {
            if (!(aggregations[aggregationIndex] instanceof GeneratedGroupedAggregationUnit)) {
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
        if (!fusedBindings.bindKey(keyVector, keyOutput.borrowOrNull(Stream.NULLS))) {
            return false;
        }
        Object keyValues = fusedBindings.keyValues();
        int[] keyIds = fusedBindings.keyIds();
        boolean intKey = fusedBindings.intKey();
        boolean keyMapped = fusedBindings.keyMapped();
        boolean directGrouping = fusedLongDirectGrouping
                && (keyMapped || fusedIntermediateMerge)
                && inlineGroupingState.prepareSingleLongDirectGrouping(
                mask,
                keyValues,
                intKey,
                keyIds,
                fusedIntermediateMerge);
        long runSample = fusedLongRunCache ? fusedBindings.sampleKeyRuns(mask) : 0;
        int runComparisons = (int) (runSample >>> 32);
        int runHits = (int) runSample;
        boolean runCache = runComparisons >= 4 && runHits * 2 >= runComparisons;
        boolean inputIndependentAccumulators = canBatchInputIndependentFusedAccumulator();
        boolean constantRuns = fusedConstantRuns
                && runCache
                && inlineGroupingState.groupCount() >= fusedConstantRunGroupMin
                && inputIndependentAccumulators;
        boolean idIndexedGrouping = inlineGroupingState.prepareSingleLongIdIndexedGrouping(
                runCache && inputIndependentAccumulators,
                inlineGroupingState.groupCount() + mask.count());
        if (debugFusedGrouping && constantRuns && !debugFusedConstantRunsPrinted) {
            debugFusedConstantRunsPrinted = true;
            System.err.printf("[fused-grouping-constant-runs] groups=%d rows=%d keyMapped=%s runHits=%d/%d%n",
                    inlineGroupingState.groupCount(), mask.count(), keyMapped, runHits, runComparisons);
        }
        if (debugFusedGrouping && !debugFusedLimitPrinted && inlineGroupingState.groupCount() >= fuseGroupLimit) {
            debugFusedLimitPrinted = true;
            System.err.printf("[fused-grouping] groups=%d rows=%d max=%d all=%s accumulators=%d keyMapped=%s runCache=%s direct=%s%n",
                    inlineGroupingState.groupCount(), mask.count(), mask.maxPosition(), mask.all(), fusedSpecs.length, keyMapped, runCache, directGrouping);
        }
        // The ordinary fused limit protects random high-cardinality flat state probes. Continue to the larger bound
        // when a mapped key keeps the physical input compact, adjacent keys prove that they reuse a group, or an
        // admitted direct table lets an intermediate merge avoid materializing and rereading group IDs.
        if (inlineGroupingState.groupCount() >= fuseGroupLimit && !keyMapped && !runCache &&
                !(fusedIntermediateMerge && directGrouping)) {
            return false;
        }
        // Beyond the established local boundary require adjacent reuse plus a second physical reason to keep the
        // generated pass: compact mapped-key access or an input-independent state update that can be coalesced by
        // run. Flat value-reading state retains staged locality; mapped-only q64 likewise stays staged.
        if (inlineGroupingState.groupCount() >= fuseMappedOnlyGroupLimit &&
                !(fusedIntermediateMerge && directGrouping)
                && (!runCache || (!keyMapped && !inputIndependentAccumulators))) {
            return false;
        }
        if (debugFusedGrouping && !debugFusedReuseContinuationPrinted && inlineGroupingState.groupCount() >= (1 << 16)) {
            debugFusedReuseContinuationPrinted = true;
            System.err.printf("[fused-grouping-reuse-continuation] groups=%d rows=%d keyMapped=%s runCache=%s%n",
                    inlineGroupingState.groupCount(), mask.count(), keyMapped, runCache);
        }
        for (int index = 0; index < fusedSpecs.length; index++) {
            GroupedAggregationUpdate spec = fusedSpecs[index];
            if (!spec.readsInput()) {
                fusedBindings.clearInput(index);
                continue;
            }
            Output valueOutput = batch.output(spec.inputColumn());
            Vector values = spec.readsValue() ? valueOutput.borrow(Stream.VALUES) : null;
            if (!fusedBindings.bindInput(index, values, valueOutput.borrowOrNull(Stream.NULLS), spec.readsValue())) {
                return false;
            }
        }

        fusedBindings.finish();
        int physicalShape = fusedBindings.physicalShape();
        physicalShape = physicalShape * 31 + (runCache ? 1 : 0);
        physicalShape = physicalShape * 31 + (constantRuns ? 1 : 0);
        physicalShape = physicalShape * 31 + (directGrouping ? 1 : 0);
        physicalShape = physicalShape * 31 + (idIndexedGrouping ? 1 : 0);
        if (fusedPhysicalShape != physicalShape) {
            fusedKernel = operatorResources.codeGeneration().fusedGrouping().create(
                    List.of(fusedSpecs),
                    filteredAggregationIndexes.length != 0 || distinctAggregationGroups.length != 0,
                    intKey,
                    keyMapped,
                    runCache,
                    constantRuns,
                    directGrouping,
                    idIndexedGrouping,
                    fusedBindings.intInputs(),
                    fusedBindings.mappedInputs(),
                    fusedBindings.mappedInputNulls(),
                    fusedBindings.inputUsesKeyIds(),
                    fusedBindings.inputNullUsesKeyIds());
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
        int additionalGroups = fusedBindings.additionalGroupUpperBound(count);
        inlineGroupingState.reserveSingleLongTable(additionalGroups);
        ensureFusedStateCapacity(toIntExact(inlineGroupingState.groupCount() + additionalGroups));
        if (filteredAggregationIndexes.length != 0 || distinctAggregationGroups.length != 0) {
            reusableGroups = allocator.reallocateIfNecessary(allocationContext, reusableGroups, I64Vector.class, mask.maxPosition() + 1, I64Vector::new);
        }

        long nextId = fusedKernel.accumulate(
                mask.selectedPositions(),
                count,
                keyValues,
                keyIds,
                inlineGroupingState.longGroupKeys,
                inlineGroupingState.longGroupIds,
                inlineGroupingState.longGroupMask,
                inlineGroupingState.longKeysByGroup,
                inlineGroupingState.nextGroupId,
                filteredAggregationIndexes.length == 0 && distinctAggregationGroups.length == 0 ? null : reusableGroups.values(),
                fusedBindings.inputs(),
                fusedBindings.inputIds(),
                fusedBindings.inputNulls(),
                fusedBindings.inputNullIds(),
                fusedStateVectors);

        inlineGroupingState.nextGroupId = nextId;
        inlineGroupingState.longGroupCount = (int) nextId;
        return true;
    }

    private boolean canBatchInputIndependentFusedAccumulator()
    {
        boolean found = false;
        for (GroupedAggregationUpdate spec : fusedSpecs) {
            if (!spec.readsInput()) {
                found = true;
            }
            else {
                return false;
            }
        }
        return found;
    }

    private void ensureFusedStateCapacity(int needed)
    {
        if (states[0] == null) {
            int capacity = computeFusedStateCapacity(needed);
            for (int index = 0; index < aggregations.length; index++) {
                states[index] = aggregations[index].allocate(aggregationExecutionContext, capacity);
                aggregations[index].initialize(states[index], 0, capacity);
            }
            stateCapacity = capacity;
            refreshFusedStateVectors();
            return;
        }
        if (stateCapacity >= needed) {
            if (!fusedStateVectorsBound) {
                refreshFusedStateVectors();
            }
            return;
        }
        int capacity = computeFusedStateCapacity(needed);
        for (int index = 0; index < aggregations.length; index++) {
            states[index] = aggregations[index].grow(allocator, allocationContext, states[index], capacity);
            aggregations[index].initialize(states[index], stateCapacity, capacity - stateCapacity);
        }
        stateCapacity = capacity;
        refreshFusedStateVectors();
    }

    private void refreshFusedStateVectors()
    {
        for (int index = 0; index < fusedAggregationIndexes.length; index++) {
            int unitIndex = fusedAggregationIndexes[index];
            ((GeneratedGroupedAggregationUnit) aggregations[unitIndex]).bindGeneratedGroupedState(
                    states[unitIndex],
                    fusedStateVectors,
                    fusedStateOffsets[index]);
            for (int target = fusedStateOffsets[index]; target < fusedStateOffsets[index + 1]; target++) {
                if (fusedStateVectors[target] == null) {
                    throw new IllegalStateException("generated grouped aggregation unit did not bind update " + (target - fusedStateOffsets[index]));
                }
            }
        }
        fusedStateVectorsBound = true;
    }

    private int computeFusedStateCapacity(int needed)
    {
        if (!fusedMappedContinuationPowerOfTwoStateCapacity
                || !fusedBindings.keyMapped()
                || needed <= fuseGroupLimit) {
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

    private void initializeInlineGroupingSchema(Batch batch, Mask mask)
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
            inlineGroupingState.initializeSchema(inlineGroupValues, inlineGroupNulls, mask);
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

    private boolean assignInlineGroupsDiscardingResults(Batch batch, Mask mask)
    {
        try {
            for (int index = 0; index < groupByColumns.length; index++) {
                Output output = batch.output(groupByColumns[index]);
                inlineGroupValues[index] = output.borrow(Stream.VALUES);
                inlineGroupNulls[index] = output.borrowOrNull(Stream.NULLS);
            }
            return inlineGroupingState.assignGroupsDiscardingResults(inlineGroupValues, inlineGroupNulls, mask);
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
        boolean stateVectorsChanged = false;
        for (int index = 0; index < aggregations.length; index++) {
            PhysicalAggregationUnit accumulator = aggregations[index];
            if (states[index] == null) {
                states[index] = accumulator.allocate(aggregationExecutionContext, newCapacity);
                stateVectorsChanged = true;
            }
            else if (grow) {
                states[index] = accumulator.grow(allocator, allocationContext, states[index], newCapacity);
                stateVectorsChanged = true;
            }
            accumulator.initialize(states[index], toIntExact(previousMaxGroup + 1), toIntExact(maxObservedGroup - previousMaxGroup));
        }
        if (stateVectorsChanged) {
            // A staged batch can replace state vectors after a generated batch has cached their physical update
            // interfaces. A later generated batch must bind the replacements, not keep writing through an old,
            // smaller vector.
            fusedStateVectorsBound = false;
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
        for (DistinctAggregationPlan.Group distinctAggregationGroup : distinctAggregationGroups) {
            Mask distinctMask = distinctAggregationGroup.select(
                    groups,
                    mask,
                    streamAccessor,
                    groupCount,
                    allocator,
                    allocationContext,
                    operatorResources.codeGeneration(),
                    operatorResources.distinctKeySetPolicy(),
                    operatorResources.adaptiveLongGroupingPolicy(),
                    operatorResources.flatKeyTablePolicy());
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
        for (int index = 0; index < aggregations.length; index++) {
            if (states[index] == null) {
                states[index] = aggregations[index].allocate(aggregationExecutionContext, 0);
            }
        }
        for (int index = 0; index < result.length; index++) {
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
        return outputBatch(computeResults());
    }

    private Batch outputBatch(Mask batchMask)
    {
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
                    (_, existing, sourcePositions, sourceStart, sourceCount, outputStart, size, _) -> groupByColumns == null
                            ? null
                            : inlineGroupingState.copyGroupedValuePositions(
                                    groupedKeyIndexes[groupedOutput],
                                    existing,
                                    sourcePositions,
                                    sourceStart,
                                    sourceCount,
                                    outputStart,
                                    size,
                                    allocator,
                                    allocationContext),
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
        PhysicalAggregationProgram.Output binding = program.outputs().get(output);
        PhysicalAggregationUnit unit = aggregations[binding.unit()];
        Object state = states[binding.unit()];
        Streams streams = result[output];
        if (streams != null && batchState.aggregationMaterializedMask[output] != null && batchState.mask.equals(batchState.aggregationMaterializedMask[output])) {
            return streams;
        }
        Streams sparse = null;
        if (sparseConstrainedResults && batchState.mask.count() < maxGroup + 1) {
            int size = batchState.mask.none() ? 0 : batchState.mask.maxPosition() + 1;
            boolean supported = !batchState.mask.none();
            for (int group : batchState.mask) {
                sparse = unit.copyResultPosition(
                        binding.result(),
                        group,
                        maxGroup,
                        state,
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
                streams = unit.result(binding.result(), maxGroup, state, batchState.mask, streams, allocator, allocationContext);
            }
        }
        else {
            streams = unit.result(binding.result(), maxGroup, state, batchState.mask, streams, allocator, allocationContext);
        }
        result[output] = streams;
        batchState.aggregationMaterializedMask[output] = batchState.mask;
        return streams;
    }

    private Streams aggregationCopyPosition(int output, Streams existing, int sourcePosition, int outputPosition, int size)
    {
        PhysicalAggregationProgram.Output binding = program.outputs().get(output);
        return aggregations[binding.unit()].copyResultPosition(
                binding.result(),
                sourcePosition,
                maxGroup,
                states[binding.unit()],
                existing,
                outputPosition,
                size,
                allocator,
                allocationContext);
    }

    private Streams aggregationCopyRange(int output, Streams existing, int sourceStart, int sourceCount, int outputStart, int size)
    {
        PhysicalAggregationProgram.Output binding = program.outputs().get(output);
        return aggregations[binding.unit()].copyResultRange(
                binding.result(),
                sourceStart,
                sourceCount,
                maxGroup,
                states[binding.unit()],
                existing,
                outputStart,
                size,
                allocator,
                allocationContext);
    }

    private Streams groupedKeyOutput(int output, BatchState batchState)
    {
        Streams streams = groupedResults[output];
        if (streams != null && batchState.mask.equals(batchState.materializedMask[output])) {
            return streams;
        }
        if (batchState.mask.none() && groupByColumns != null && !inlineGroupingState.isInitialized()) {
            streams = emptyGroupedKeyOutput(output);
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

    private Streams emptyGroupedKeyOutput(int output)
    {
        TypeBinding type = inlineGroupedOutputTypes[output];
        Vector values = type == null || !type.isSpecified()
                ? allocator.allocate(allocationContext, I64Vector.class, 0, I64Vector::new)
                : type.vectorFactory()
                        .orElseThrow(() -> new IllegalStateException("Grouped output type does not provide a vector factory"))
                        .nullValues(allocator.vectorAllocator(allocationContext), 0);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                null,
                0);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private final class BatchState
    {
        private Mask mask;
        private final Mask[] materializedMask = new Mask[groupedColumns.length];
        private final Mask[] aggregationMaterializedMask = new Mask[program.outputs().size()];

        private BatchState(Mask mask)
        {
            this.mask = mask;
        }

        private void constrain(Mask mask)
        {
            this.mask = mask;
        }
    }

    private final class DenseBatchState
    {
        private final int sourceStart;
        private final int size;
        private final Streams[] materialized = new Streams[outputCount()];
        private Mask mask;

        private DenseBatchState(int sourceStart, int size, Mask mask)
        {
            this.sourceStart = sourceStart;
            this.size = size;
            this.mask = mask;
        }

        private Streams output(int output)
        {
            Streams streams = materialized[output];
            if (streams != null) {
                return streams;
            }
            if (output < groupedResults.length && groupByColumns != null) {
                streams = inlineGroupingState.groupedValueRange(
                        groupedKeyIndexes[output], sourceStart, size, mask, allocator, allocationContext);
            }
            if (streams != null) {
                materialized[output] = streams;
                return streams;
            }
            if (mask.all() && output >= groupedResults.length) {
                streams = aggregationCopyRange(
                        output - groupedResults.length,
                        streams,
                        sourceStart,
                        size,
                        0,
                        size);
                if (streams != null) {
                    materialized[output] = streams;
                    return streams;
                }
            }
            for (int outputPosition : mask) {
                Streams copied = copyDenseOutputPosition(
                        output,
                        streams,
                        sourceStart + outputPosition,
                        outputPosition,
                        size);
                if (copied == null) {
                    copied = copyDenseFallbackPosition(
                            output,
                            streams,
                            sourceStart + outputPosition,
                            outputPosition,
                            size);
                }
                streams = copied;
            }
            if (streams == null) {
                streams = emptyDenseOutput(output);
            }
            materialized[output] = streams;
            return streams;
        }

        private void constrain(Mask mask)
        {
            this.mask = mask;
            Arrays.fill(materialized, null);
        }

        private Streams copyDenseFallbackPosition(
                int output,
                Streams existing,
                int sourcePosition,
                int outputPosition,
                int size)
        {
            if (output < groupedResults.length) {
                throw new IllegalStateException("Grouped key output %s does not support dense position copying".formatted(output));
            }
            int aggregationOutput = output - groupedResults.length;
            Streams source = result[aggregationOutput];
            if (source == null) {
                PhysicalAggregationProgram.Output binding = program.outputs().get(aggregationOutput);
                source = aggregations[binding.unit()].result(
                        binding.result(),
                        maxGroup,
                        states[binding.unit()],
                        null,
                        allocator,
                        allocationContext);
                result[aggregationOutput] = source;
            }

            Streams.Builder copied = Streams.builder();
            for (Stream stream : source.streams()) {
                Vector existingVector = existing == null ? null : existing.getOrNull(stream);
                copied.put(
                        stream,
                        source.get(stream).copySinglePositionInto(
                                allocator,
                                allocationContext,
                                existingVector,
                                sourcePosition,
                                outputPosition,
                                size));
            }
            return copied.build();
        }
    }

    private Streams emptyDenseOutput(int output)
    {
        TypeBinding type = outputSchema.field(output).type();
        Vector values = !type.isSpecified()
                ? allocator.allocate(allocationContext, I64Vector.class, 0, I64Vector::new)
                : type.vectorFactory()
                        .orElseThrow(() -> new IllegalStateException("Output type does not provide a vector factory"))
                        .nullValues(allocator.vectorAllocator(allocationContext), 0);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                null,
                0);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public void close()
    {
        source.close();
        if (inlineGroupingState != null) {
            inlineGroupingState.releaseBuffers();
        }
        for (DistinctAggregationPlan.Group distinctAggregationGroup : distinctAggregationGroups) {
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
}
