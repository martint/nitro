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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeOrderKeyBinder;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/** Streaming bounded state for a partitioned Top-N ranking. */
final class PartitionedTopNRankingState
        implements TopNRankingState
{
    private final Allocator.Context allocationContext =
            new Allocator.Context("PartitionedTopNRankingState", PartitionedTopNRankingState.class);
    private final Allocator allocator;
    private final int limit;
    private final int[] partitionColumns;
    private final TopNRankingOperator.RankingType rankingType;
    private final int maxBatchRows;
    private final boolean outputRanking;
    private final Schema outputSchema;
    private final TopNState rows;
    private final GroupingState partitionGrouping;
    private final StructuralComparisonKernel[] partitionComparisons;
    private final List<Partition> partitions = new ArrayList<>();
    private final IntArrayList freeSlots = new IntArrayList();
    private int nextSlot;
    private int retainedRows;
    private int[] outputSlots;
    private long[] outputRanks;
    private int outputPosition;
    private boolean normalizedOrderingDisabled;
    private boolean finished;
    private boolean closed;

    PartitionedTopNRankingState(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            TopNRankingOperator.RankingType rankingType,
            boolean outputRanking,
            Schema inputSchema,
            Schema rankingSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descending,
                nullsFirst,
                rankingType,
                outputRanking,
                inputSchema,
                rankingSchema,
                resources,
                requireNonNull(resources, "resources is null").topNRankingPolicy());
    }

    PartitionedTopNRankingState(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            TopNRankingOperator.RankingType rankingType,
            boolean outputRanking,
            Schema inputSchema,
            Schema rankingSchema,
            OperatorResources resources,
            TopNRankingOperatorPolicy policy)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.limit = limit;
        this.partitionColumns = partitionColumns.clone();
        this.rankingType = requireNonNull(rankingType, "rankingType is null");
        this.outputRanking = outputRanking;
        resources = requireNonNull(resources, "resources is null");
        this.maxBatchRows = requireNonNull(policy, "policy is null").maxBatchRows();
        this.outputSchema = outputRanking ? appendRanking(inputSchema, rankingSchema) : inputSchema;
        rows = new TopNState(
                orderingColumns,
                descending,
                nullsFirst,
                resources.joinBufferPolicy(),
                allocator,
                allocationContext,
                inputSchema,
                resources.codeGeneration().structuralTypes(),
                limit);
        List<TypeBinding> partitionTypes = Arrays.stream(this.partitionColumns)
                .mapToObj(column -> inputSchema.field(column).type())
                .toList();
        partitionGrouping = new GroupingState(
                allocator.primitiveArrays(),
                resources.codeGeneration(),
                resources.grouping().forGroupingOnlyAggregation(),
                resources.adaptiveLongGroupingPolicy(),
                resources.flatKeyTablePolicy(),
                partitionTypes,
                allocator,
                allocationContext);
        partitionComparisons = partitionTypes.stream()
                .map(resources.codeGeneration().structuralTypes()::comparison)
                .toArray(StructuralComparisonKernel[]::new);
    }

    @Override
    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkAcceptingInput();
        rows.beginBatch();
        rows.captureSchema(batch, true);
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            rows.discardFallbackBatch();
            return;
        }

        Vector[] partitionValues = new Vector[partitionColumns.length];
        Vector[] partitionNulls = new Vector[partitionColumns.length];
        for (int index = 0; index < partitionColumns.length; index++) {
            Output output = batch.output(partitionColumns[index]);
            partitionValues[index] = output.borrow(Stream.VALUES);
            partitionNulls[index] = output.borrowOrNull(Stream.NULLS);
        }

        I64Vector partitionIds = allocator.allocate(allocationContext, I64Vector.class, mask.size(), I64Vector::new);
        IntArrayList touchedSlots = new IntArrayList();
        boolean compactCandidate = mask.size() > (1 << 16);
        TypeOrderKeyBinder.Bound normalizedOrdering = normalizedOrderingDisabled ? null : rows.bindSingleOrderingKey(batch);
        if (normalizedOrdering == null) {
            normalizedOrderingDisabled = true;
        }
        try {
            partitionGrouping.assignGroupsForBlockingAggregation(partitionValues, partitionNulls, mask, partitionIds);
            ensurePartitions(toIntExact(partitionGrouping.groupCount()));
            for (int selectedIndex = 0; selectedIndex < mask.count(); selectedIndex++) {
                int position = mask.position(selectedIndex);
                Partition partition = partitions.get(toIntExact(partitionIds.values()[position]));
                long orderingKey = normalizedOrdering == null ? 0 : normalizedOrdering.key(position);
                int groupIndex = insertionPoint(
                        partition, batch, position, orderingKey, compactCandidate, normalizedOrdering != null);
                if (!isAdmitted(partition, groupIndex)) {
                    continue;
                }

                PeerGroup group;
                if (groupIndex < partition.groups.size() &&
                        compare(batch, position, orderingKey, partition.groups.get(groupIndex), compactCandidate, normalizedOrdering != null) == 0) {
                    if (rankingType == TopNRankingOperator.RankingType.ROW_NUMBER &&
                            partition.retainedRows >= limit &&
                            groupIndex == partition.groups.size() - 1) {
                        continue;
                    }
                    group = partition.groups.get(groupIndex);
                }
                else {
                    group = new PeerGroup(orderingKey);
                    partition.groups.add(groupIndex, group);
                }

                int slot = acquireSlot();
                rows.copyRow(batch, position, slot);
                group.slots.add(slot);
                touchedSlots.add(slot);
                partition.retainedRows++;
                retainedRows++;
                trim(partition);
            }

            IntArrayList liveTouched = new IntArrayList(touchedSlots.size());
            for (int index = 0; index < touchedSlots.size(); index++) {
                int slot = touchedSlots.getInt(index);
                if (rows.isPendingFrom(slot, batch)) {
                    liveTouched.add(slot);
                }
            }
            rows.flushPendingBatch(batch, liveTouched.elements(), 0, liveTouched.size());
        }
        finally {
            allocator.release(allocationContext, partitionIds);
            rows.discardFallbackBatch();
        }
    }

    @Override
    public void finishInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is already finished");
        }
        finished = true;
        partitionGrouping.finishInput();
        List<Integer> orderedPartitions = orderedPartitions();
        outputSlots = new int[retainedRows];
        outputRanks = outputRanking ? new long[retainedRows] : null;
        int output = 0;
        for (int partitionId : orderedPartitions) {
            Partition partition = partitions.get(partitionId);
            long precedingRows = 0;
            int partitionPosition = 0;
            for (int groupIndex = 0; groupIndex < partition.groups.size(); groupIndex++) {
                PeerGroup group = partition.groups.get(groupIndex);
                long groupRank = switch (rankingType) {
                    case ROW_NUMBER -> -1;
                    case RANK -> precedingRows + 1;
                    case DENSE_RANK -> groupIndex + 1L;
                };
                for (int index = 0; index < group.slots.size(); index++) {
                    outputSlots[output] = group.slots.getInt(index);
                    if (outputRanking) {
                        outputRanks[output] = rankingType == TopNRankingOperator.RankingType.ROW_NUMBER
                                ? partitionPosition + 1L
                                : groupRank;
                    }
                    output++;
                    partitionPosition++;
                }
                precedingRows += group.slots.size();
            }
        }
    }

    private List<Integer> orderedPartitions()
    {
        if (partitions.isEmpty()) {
            rows.prepareEmptyOutputSchema();
            return List.of();
        }
        int count = partitions.size();
        Mask mask = allocator.allocateRangeMask(allocationContext, 0, count);
        Streams[] keys = new Streams[partitionColumns.length];
        try {
            for (int index = 0; index < keys.length; index++) {
                keys[index] = partitionGrouping.groupedValues(index, mask, null, allocator, allocationContext);
            }
        }
        finally {
            allocator.release(allocationContext, mask);
        }
        List<Integer> ordered = new ArrayList<>(count);
        for (int partition = 0; partition < count; partition++) {
            ordered.add(partition);
        }
        ordered.sort((left, right) -> comparePartitionKeys(keys, left, right));
        return ordered;
    }

    private int comparePartitionKeys(Streams[] keys, int left, int right)
    {
        for (int index = 0; index < keys.length; index++) {
            Streams key = keys[index];
            int comparison = partitionComparisons[index].compare(
                    key.values(), key.getOrNull(Stream.NULLS), left,
                    key.values(), key.getOrNull(Stream.NULLS), right);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private int insertionPoint(
            Partition partition,
            Batch batch,
            int position,
            long orderingKey,
            boolean compactCandidate,
            boolean normalizedOrdering)
    {
        for (int index = 0; index < partition.groups.size(); index++) {
            if (compare(batch, position, orderingKey, partition.groups.get(index), compactCandidate, normalizedOrdering) >= 0) {
                return index;
            }
        }
        return partition.groups.size();
    }

    private int compare(
            Batch batch,
            int position,
            long orderingKey,
            PeerGroup group,
            boolean compactCandidate,
            boolean normalizedOrdering)
    {
        if (!normalizedOrdering) {
            return rows.compareOrderingValue(batch, position, group.representative(), compactCandidate);
        }
        return rows.singleOrderingDescending()
                ? Long.compareUnsigned(orderingKey, group.orderingKey)
                : Long.compareUnsigned(group.orderingKey, orderingKey);
    }

    private boolean isAdmitted(Partition partition, int groupIndex)
    {
        if (groupIndex < partition.groups.size()) {
            return true;
        }
        return switch (rankingType) {
            case ROW_NUMBER, RANK -> partition.retainedRows < limit;
            case DENSE_RANK -> partition.groups.size() < limit;
        };
    }

    private void ensurePartitions(int count)
    {
        while (partitions.size() < count) {
            partitions.add(new Partition());
        }
    }

    private int acquireSlot()
    {
        if (!freeSlots.isEmpty()) {
            return freeSlots.removeInt(freeSlots.size() - 1);
        }
        int slot = nextSlot++;
        rows.ensureCapacity(nextSlot);
        return slot;
    }

    private void trim(Partition partition)
    {
        switch (rankingType) {
            case ROW_NUMBER -> trimRows(partition, limit);
            case DENSE_RANK -> {
                while (partition.groups.size() > limit) {
                    removeGroup(partition, partition.groups.size() - 1);
                }
            }
            case RANK -> {
                int preceding = 0;
                int retainedGroups = 0;
                for (PeerGroup group : partition.groups) {
                    if (preceding >= limit) {
                        break;
                    }
                    preceding += group.slots.size();
                    retainedGroups++;
                }
                while (partition.groups.size() > retainedGroups) {
                    removeGroup(partition, partition.groups.size() - 1);
                }
            }
        }
    }

    private void trimRows(Partition partition, int maximumRows)
    {
        while (partition.retainedRows > maximumRows) {
            PeerGroup tail = partition.groups.get(partition.groups.size() - 1);
            releaseSlot(partition, tail.slots.removeInt(tail.slots.size() - 1));
            if (tail.slots.isEmpty()) {
                partition.groups.remove(partition.groups.size() - 1);
            }
        }
    }

    private void removeGroup(Partition partition, int index)
    {
        PeerGroup group = partition.groups.remove(index);
        for (int slotIndex = 0; slotIndex < group.slots.size(); slotIndex++) {
            releaseSlot(partition, group.slots.getInt(slotIndex));
        }
    }

    private void releaseSlot(Partition partition, int slot)
    {
        rows.discardPendingSlot(slot);
        freeSlots.add(slot);
        partition.retainedRows--;
        retainedRows--;
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        checkFinished();
        return outputPosition < outputSlots.length;
    }

    @Override
    public Batch next()
    {
        checkFinished();
        if (!hasNext()) {
            throw new IllegalStateException("TopN ranking has no output");
        }
        int batchSize = Math.min(maxBatchRows, outputSlots.length - outputPosition);
        int[] slots = new int[batchSize];
        System.arraycopy(outputSlots, outputPosition, slots, 0, batchSize);
        rows.setOrderedSlots(slots, batchSize);

        Output[] outputs = new Output[outputCount()];
        int sourceOutputs = outputCount() - (outputRanking ? 1 : 0);
        for (int outputIndex = 0; outputIndex < sourceOutputs; outputIndex++) {
            int index = outputIndex;
            outputs[index] = new Output(
                    rows.outputStreams(index),
                    stream -> rows.output(index).get(stream),
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }
        if (outputRanking) {
            I64Vector ranks = allocator.allocate(allocationContext, I64Vector.class, batchSize, I64Vector::new);
            System.arraycopy(outputRanks, outputPosition, ranks.values(), 0, batchSize);
            Streams rankStreams = Streams.ofValues(ranks);
            outputs[outputs.length - 1] = new Output(
                    rankStreams.streams(),
                    rankStreams::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }

        outputPosition += batchSize;
        Mask outputMask = allocator.allocateRangeMask(allocationContext, 0, batchSize);
        return new Batch(
                outputMask,
                rows::constrain,
                takenMask -> allocator.transfer(allocationContext, takenMask),
                batchMask -> allocator.release(allocationContext, batchMask),
                () -> {},
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        rows.constrain(mask);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        partitionGrouping.releaseBuffers();
        rows.close();
        allocator.release(allocationContext);
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is finished");
        }
    }

    private void checkFinished()
    {
        checkOpen();
        if (!finished) {
            throw new IllegalStateException("TopN ranking input is not finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("TopN ranking state is closed");
        }
    }

    private static Schema appendRanking(Schema inputSchema, Schema rankingSchema)
    {
        requireNonNull(inputSchema, "inputSchema is null");
        requireNonNull(rankingSchema, "rankingSchema is null");
        if (rankingSchema.size() != 1) {
            throw new IllegalArgumentException("rankingSchema must contain exactly one field");
        }
        List<Field> fields = new ArrayList<>(inputSchema.fields());
        fields.add(rankingSchema.field(0));
        return new Schema(fields);
    }

    private static final class Partition
    {
        private final List<PeerGroup> groups = new ArrayList<>();
        private int retainedRows;
    }

    private static final class PeerGroup
    {
        private final IntArrayList slots = new IntArrayList();
        private final long orderingKey;

        private PeerGroup(long orderingKey)
        {
            this.orderingKey = orderingKey;
        }

        private int representative()
        {
            return slots.getInt(0);
        }
    }
}
