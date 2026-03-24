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
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.StreamAccessors;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.toIntExact;

public class GroupedAggregationOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GroupedAggregationOperator");
    private final Allocator allocator;

    private final int groupColumn;
    private final int[] groupedColumns;
    private final int[] groupByColumns;
    private final int[] groupedKeyIndexes;
    private final Accumulator[] aggregations;
    private final Operator source;
    private final Streams[] groupedResults;
    private final Streams[] result;
    private final GroupingState inlineGroupingState;
    private Streams[] states;
    private int maxGroup = -1;
    private boolean done;
    private GroupedKeySource groupedKeySource;
    private I64Vector reusableGroups;

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
            for (int i = 0; i < aggregations.length; i++) {
                Accumulator accumulator = aggregations[i];

                states[i] = states[i] == null
                        ? accumulator.allocate(allocator, ALLOCATION_CONTEXT, newCapacity)
                        : accumulator.grow(allocator, ALLOCATION_CONTEXT, states[i], newCapacity);
                accumulator.initialize(states[i], toIntExact(previousMaxGroup + 1), toIntExact(maxObservedGroup - previousMaxGroup));
                accumulator.accumulate(states[i], group, mask, streamAccessor);
            }
        }

        this.maxGroup = toIntExact(maxObservedGroup);
        for (int i = 0; i < result.length; i++) {
            if (states[i] == null) {
                states[i] = aggregations[i].allocate(allocator, ALLOCATION_CONTEXT, 0);
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

        return allocator.allocateAllMask(ALLOCATION_CONTEXT, this.maxGroup + 1);
    }

    private Mask computeInlineGroupedResults()
    {
        states = new Streams[aggregations.length];
        long maxObservedGroup = -1;
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                if (mask.none()) {
                    continue;
                }

                long previousMaxGroup = maxObservedGroup;
                reusableGroups = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, reusableGroups, I64Vector.class, mask.maxPosition() + 1, I64Vector::new);
                assignInlineGroups(batch, mask, reusableGroups);
                maxObservedGroup = Math.max(maxObservedGroup, maxGroup(reusableGroups, mask));

                int newCapacity = Allocator.computeCapacity(toIntExact(maxObservedGroup + 1));
                var streamAccessor = StreamAccessors.forBatch(batch);
                for (int index = 0; index < aggregations.length; index++) {
                    Accumulator accumulator = aggregations[index];
                    states[index] = states[index] == null
                            ? accumulator.allocate(allocator, ALLOCATION_CONTEXT, newCapacity)
                            : accumulator.grow(allocator, ALLOCATION_CONTEXT, states[index], newCapacity);
                    accumulator.initialize(states[index], toIntExact(previousMaxGroup + 1), toIntExact(maxObservedGroup - previousMaxGroup));
                    accumulator.accumulate(states[index], reusableGroups, mask, streamAccessor);
                }
            }
        }

        finishResults(maxObservedGroup);
        return allocator.allocateAllMask(ALLOCATION_CONTEXT, this.maxGroup + 1);
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
        BooleanVector[] nulls = new BooleanVector[groupByColumns.length];
        for (int index = 0; index < groupByColumns.length; index++) {
            Output output = batch.output(groupByColumns[index]);
            values[index] = output.borrow(Stream.VALUES);
            nulls[index] = (BooleanVector) output.borrowOrNull(Stream.NULLS);
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

    private void finishResults(long maxObservedGroup)
    {
        this.maxGroup = toIntExact(maxObservedGroup);
        for (int index = 0; index < result.length; index++) {
            if (states[index] == null) {
                states[index] = aggregations[index].allocate(allocator, ALLOCATION_CONTEXT, 0);
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
        return new Batch(batchMask, batchState::constrain, takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
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

    private Output resultOutput(int output, BatchState batchState)
    {
        if (output < groupedResults.length) {
            int groupedOutput = output;
            return new Output(
                    groupedKeyStreams(),
                    stream -> groupedKeyOutput(groupedOutput, batchState).get(stream),
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector),
                    (stream, vector) -> allocator.release(ALLOCATION_CONTEXT, vector));
        }

        Streams streams = result[output - groupedResults.length];
        return new Output(
                EnumSet.of(Stream.VALUES, Stream.NULLS),
                stream -> aggregationOutput(output - groupedResults.length, batchState).get(stream),
                (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector),
                (stream, vector) -> allocator.release(ALLOCATION_CONTEXT, vector),
                (existing, sourcePosition, outputPosition, size) -> aggregationCopyPosition(output - groupedResults.length, existing, sourcePosition, outputPosition, size));
    }

    private Streams aggregationOutput(int output, BatchState batchState)
    {
        Streams streams = result[output];
        if (streams != null && batchState.aggregationMaterializedMask[output] != null && batchState.mask.equals(batchState.aggregationMaterializedMask[output])) {
            return streams;
        }
        streams = aggregations[output].result(maxGroup, states[output], batchState.mask, streams, allocator, ALLOCATION_CONTEXT);
        result[output] = streams;
        batchState.aggregationMaterializedMask[output] = batchState.mask;
        return streams;
    }

    private Streams aggregationCopyPosition(int output, Streams existing, int sourcePosition, int outputPosition, int size)
    {
        return aggregations[output].copyResultPosition(sourcePosition, maxGroup, states[output], existing, outputPosition, size, allocator, ALLOCATION_CONTEXT);
    }

    private Streams groupedKeyOutput(int output, BatchState batchState)
    {
        Streams streams = groupedResults[output];
        if (streams != null && batchState.mask.equals(batchState.materializedMask[output])) {
            return streams;
        }
        if (groupByColumns == null) {
            streams = groupedKeySource.groupedKeyOutput(groupedColumns[output], batchState.mask, streams, allocator, ALLOCATION_CONTEXT);
        }
        else {
            streams = inlineGroupingState.groupedValues(groupedKeyIndexes[output], batchState.mask, streams, allocator, ALLOCATION_CONTEXT);
        }
        groupedResults[output] = streams;
        batchState.materializedMask[output] = batchState.mask;
        return streams;
    }

    private Set<Stream> groupedKeyStreams()
    {
        return EnumSet.of(Stream.VALUES, Stream.NULLS);
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
        allocator.release(ALLOCATION_CONTEXT);
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
