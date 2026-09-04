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
import org.weakref.nitro.core.type.TypeOrderKeyBinder;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Streaming bounded state for an unpartitioned Top-N ranking. */
final class UnpartitionedTopNRankingState
        implements Operator
{
    private final Allocator.Context allocationContext =
            new Allocator.Context("UnpartitionedTopNRankingState", UnpartitionedTopNRankingState.class);
    private final Allocator allocator;
    private final int limit;
    private final TopNRankingOperator.RankingType rankingType;
    private final int maxBatchRows;
    private final boolean outputRanking;
    private final Schema outputSchema;
    private final TopNState state;
    private final List<PeerGroup> groups = new ArrayList<>();
    private final IntArrayList freeSlots = new IntArrayList();
    private int nextSlot;
    private int retainedRows;
    private int[] outputSlots;
    private long[] outputRanks;
    private int outputPosition;
    private boolean normalizedOrderingDisabled;
    private boolean finished;
    private boolean closed;

    UnpartitionedTopNRankingState(
            Allocator allocator,
            int limit,
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
                orderingColumns,
                descending,
                nullsFirst,
                rankingType,
                outputRanking,
                inputSchema,
                rankingSchema,
                requireNonNull(resources, "resources is null").joinBufferPolicy(),
                resources.codeGeneration().structuralTypes(),
                resources.topNRankingPolicy());
    }

    UnpartitionedTopNRankingState(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            TopNRankingOperator.RankingType rankingType,
            boolean outputRanking,
            Schema inputSchema,
            Schema rankingSchema,
            JoinBufferPolicy joinBufferPolicy,
            StructuralTypeKernelFactory structuralTypes,
            TopNRankingOperatorPolicy policy)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.limit = limit;
        this.rankingType = requireNonNull(rankingType, "rankingType is null");
        this.outputRanking = outputRanking;
        this.maxBatchRows = requireNonNull(policy, "policy is null").maxBatchRows();
        this.outputSchema = outputRanking ? appendRanking(inputSchema, rankingSchema) : inputSchema;
        state = new TopNState(
                orderingColumns,
                descending,
                nullsFirst,
                requireNonNull(joinBufferPolicy, "joinBufferPolicy is null"),
                allocator,
                allocationContext,
                inputSchema,
                requireNonNull(structuralTypes, "structuralTypes is null"),
                limit);
    }

    void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkAcceptingInput();
        state.beginBatch();
        state.captureSchema(batch, true);
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            state.discardFallbackBatch();
            return;
        }

        IntArrayList touchedSlots = new IntArrayList();
        boolean compactCandidate = mask.size() > (1 << 16);
        TypeOrderKeyBinder.Bound normalizedOrdering = normalizedOrderingDisabled ? null : state.bindSingleOrderingKey(batch);
        if (normalizedOrdering == null) {
            normalizedOrderingDisabled = true;
        }
        try {
            for (int selectedIndex = 0; selectedIndex < mask.count(); selectedIndex++) {
                int position = mask.position(selectedIndex);
                long orderingKey = normalizedOrdering == null ? 0 : normalizedOrdering.key(position);
                int groupIndex = insertionPoint(batch, position, orderingKey, compactCandidate, normalizedOrdering != null);
                if (!isAdmitted(groupIndex)) {
                    continue;
                }

                PeerGroup group;
                if (groupIndex < groups.size() &&
                        compare(batch, position, orderingKey, groups.get(groupIndex), compactCandidate, normalizedOrdering != null) == 0) {
                    if (rankingType == TopNRankingOperator.RankingType.ROW_NUMBER &&
                            retainedRows >= limit &&
                            groupIndex == groups.size() - 1) {
                        continue;
                    }
                    group = groups.get(groupIndex);
                }
                else {
                    group = new PeerGroup(orderingKey);
                    groups.add(groupIndex, group);
                }

                int slot = acquireSlot();
                state.copyRow(batch, position, slot);
                group.slots().add(slot);
                touchedSlots.add(slot);
                retainedRows++;
                trim();
            }

            IntArrayList liveTouched = new IntArrayList(touchedSlots.size());
            for (int index = 0; index < touchedSlots.size(); index++) {
                int slot = touchedSlots.getInt(index);
                if (state.isPendingFrom(slot, batch)) {
                    liveTouched.add(slot);
                }
            }
            state.flushPendingBatch(batch, liveTouched.elements(), 0, liveTouched.size());
        }
        finally {
            state.discardFallbackBatch();
        }
    }

    void finishInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is already finished");
        }
        finished = true;
        outputSlots = new int[retainedRows];
        outputRanks = outputRanking ? new long[retainedRows] : null;
        int position = 0;
        long precedingRows = 0;
        for (int groupIndex = 0; groupIndex < groups.size(); groupIndex++) {
            PeerGroup group = groups.get(groupIndex);
            long groupRank = switch (rankingType) {
                case ROW_NUMBER -> -1;
                case RANK -> precedingRows + 1;
                case DENSE_RANK -> groupIndex + 1L;
            };
            for (int index = 0; index < group.slots().size(); index++) {
                outputSlots[position] = group.slots().getInt(index);
                if (outputRanking) {
                    outputRanks[position] = rankingType == TopNRankingOperator.RankingType.ROW_NUMBER ? position + 1L : groupRank;
                }
                position++;
            }
            precedingRows += group.slots().size();
        }
    }

    private int insertionPoint(Batch batch, int position, long orderingKey, boolean compactCandidate, boolean normalizedOrdering)
    {
        for (int index = 0; index < groups.size(); index++) {
            int comparison = compare(batch, position, orderingKey, groups.get(index), compactCandidate, normalizedOrdering);
            if (comparison >= 0) {
                return index;
            }
        }
        return groups.size();
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
            return state.compareOrderingValue(batch, position, group.representative(), compactCandidate);
        }
        return state.singleOrderingDescending()
                ? Long.compareUnsigned(orderingKey, group.orderingKey())
                : Long.compareUnsigned(group.orderingKey(), orderingKey);
    }

    private boolean isAdmitted(int groupIndex)
    {
        if (groupIndex < groups.size()) {
            return true;
        }
        return switch (rankingType) {
            case ROW_NUMBER -> retainedRows < limit;
            case RANK -> retainedRows < limit;
            case DENSE_RANK -> groups.size() < limit;
        };
    }

    private int acquireSlot()
    {
        if (!freeSlots.isEmpty()) {
            return freeSlots.removeInt(freeSlots.size() - 1);
        }
        int slot = nextSlot++;
        state.ensureCapacity(nextSlot);
        return slot;
    }

    private void trim()
    {
        switch (rankingType) {
            case ROW_NUMBER -> trimRows(limit);
            case DENSE_RANK -> {
                while (groups.size() > limit) {
                    removeGroup(groups.size() - 1);
                }
            }
            case RANK -> {
                int preceding = 0;
                int retainedGroups = 0;
                for (PeerGroup group : groups) {
                    if (preceding >= limit) {
                        break;
                    }
                    preceding += group.slots().size();
                    retainedGroups++;
                }
                while (groups.size() > retainedGroups) {
                    removeGroup(groups.size() - 1);
                }
            }
        }
    }

    private void trimRows(int maximumRows)
    {
        while (retainedRows > maximumRows) {
            PeerGroup tail = groups.get(groups.size() - 1);
            releaseSlot(tail.slots().removeInt(tail.slots().size() - 1));
            if (tail.slots().isEmpty()) {
                groups.remove(groups.size() - 1);
            }
        }
    }

    private void removeGroup(int index)
    {
        PeerGroup group = groups.remove(index);
        for (int slotIndex = 0; slotIndex < group.slots().size(); slotIndex++) {
            releaseSlot(group.slots().getInt(slotIndex));
        }
    }

    private void releaseSlot(int slot)
    {
        state.discardPendingSlot(slot);
        freeSlots.add(slot);
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
        state.setOrderedSlots(slots, batchSize);

        Output[] outputs = new Output[outputCount()];
        int sourceOutputs = outputCount() - (outputRanking ? 1 : 0);
        for (int outputIndex = 0; outputIndex < sourceOutputs; outputIndex++) {
            int index = outputIndex;
            outputs[index] = new Output(
                    state.outputStreams(index),
                    stream -> state.output(index).get(stream),
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
                state::constrain,
                takenMask -> allocator.transfer(allocationContext, takenMask),
                batchMask -> allocator.release(allocationContext, batchMask),
                () -> {},
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        state.constrain(mask);
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
        state.close();
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

    private record PeerGroup(IntArrayList slots, long orderingKey)
    {
        private PeerGroup(long orderingKey)
        {
            this(new IntArrayList(), orderingKey);
        }

        private int representative()
        {
            return slots.getInt(0);
        }
    }
}
