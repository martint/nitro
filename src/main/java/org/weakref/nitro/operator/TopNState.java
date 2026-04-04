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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class TopNState
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final int[] orderingColumns;
    private final boolean[] descendingByColumn;
    private final boolean[] orderingColumnFlags;
    private Streams[][] slotColumns;
    private final Streams[] comparisonColumns;
    private final Streams[] schema;
    private final Set<Stream>[] exposedStreams;
    private final JoinBufferSupport buffers;
    private Batch[] pendingBatches;
    private int[] pendingPositions;
    private List<Integer> orderedSlots = List.of();
    private Mask outputMask;
    private Streams[] materialized;
    private Batch fallbackBatch;

    @SuppressWarnings("unchecked")
    TopNState(int[] orderingColumns, boolean[] descendingByColumn, Allocator allocator, Allocator.Context allocationContext, int outputCount, int capacity)
    {
        this.allocator = allocator;
        this.allocationContext = allocationContext;
        this.orderingColumns = orderingColumns.clone();
        this.descendingByColumn = descendingByColumn.clone();
        this.orderingColumnFlags = new boolean[outputCount];
        for (int orderingColumn : orderingColumns) {
            orderingColumnFlags[orderingColumn] = true;
        }
        this.slotColumns = new Streams[outputCount][capacity];
        this.comparisonColumns = new Streams[outputCount];
        this.schema = new Streams[outputCount];
        this.exposedStreams = (Set<Stream>[]) new Set<?>[outputCount];
        this.buffers = new JoinBufferSupport(allocator, allocationContext);
        this.pendingBatches = new Batch[capacity];
        this.pendingPositions = new int[capacity];
    }

    public void ensureCapacity(int requiredCapacity)
    {
        if (pendingBatches.length >= requiredCapacity) {
            return;
        }

        int newCapacity = Math.max(requiredCapacity, Math.max(8, pendingBatches.length * 2));
        for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
            slotColumns[outputIndex] = Arrays.copyOf(slotColumns[outputIndex], newCapacity);
        }
        pendingBatches = Arrays.copyOf(pendingBatches, newCapacity);
        pendingPositions = Arrays.copyOf(pendingPositions, newCapacity);
    }

    public void captureSchema(Batch batch)
    {
        if (fallbackBatch == null) {
            fallbackBatch = batch;
        }
        for (int outputIndex = 0; outputIndex < exposedStreams.length; outputIndex++) {
            if (exposedStreams[outputIndex] == null) {
                Set<Stream> outputStreams = batch.output(outputIndex).streams();
                exposedStreams[outputIndex] = outputStreams.isEmpty() ? Set.of() : EnumSet.copyOf(outputStreams);
            }
            if (schema[outputIndex] == null) {
                try {
                    schema[outputIndex] = buffers.borrowStreams(batch.output(outputIndex));
                }
                catch (IllegalArgumentException ignored) {
                    // Some projected outputs can produce only null/error streams for the
                    // schema row. In that case, defer schema capture until a retained row is
                    // materialized, which is sufficient for non-empty TopN results.
                }
            }
        }
    }

    public int compareOrderingValue(Batch batch, int position, int slot)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int orderingColumn = orderingColumns[orderingIndex];
            Output output = batch.output(orderingColumn);
            Streams slotOrdering = slotColumns[orderingColumn][slot];
            int comparison = tryCompareDirectOrderingValue(output, position, slotOrdering);
            if (comparison == Integer.MIN_VALUE) {
                comparisonColumns[orderingColumn] = buffers.copyPosition(output, comparisonColumns[orderingColumn], position);
                Streams currentOrdering = comparisonColumns[orderingColumn];
                comparison = OperatorOrderingSemantics.compare(
                        currentOrdering.values(),
                        (BooleanVector) currentOrdering.getOrNull(Stream.NULLS),
                        0,
                        slotOrdering.values(),
                        (BooleanVector) slotOrdering.getOrNull(Stream.NULLS),
                        0);
            }
            comparison = descendingByColumn[orderingIndex] ? comparison : -comparison;
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private int tryCompareDirectOrderingValue(Output output, int position, Streams slotOrdering)
    {
        Vector currentValues = output.borrow(Stream.VALUES);
        Vector slotValues = slotOrdering.values();
        Vector currentNulls = output.borrowOrNull(Stream.NULLS);
        Vector slotNulls = slotOrdering.getOrNull(Stream.NULLS);
        boolean currentNull = OperatorVectorSupport.isNull(currentNulls, position);
        boolean slotNull = OperatorVectorSupport.isNull(slotNulls, 0);
        if (currentNull || slotNull) {
            if (currentNull == slotNull) {
                return 0;
            }
            return currentNull ? 1 : -1;
        }

        Vector flattenedCurrent = OperatorVectorSupport.flatten(currentValues);
        Vector flattenedSlot = OperatorVectorSupport.flatten(slotValues);
        if ((flattenedCurrent instanceof I64Vector || flattenedCurrent instanceof I32Vector) &&
                (flattenedSlot instanceof I64Vector || flattenedSlot instanceof I32Vector)) {
            return Long.compare(
                    OperatorVectorSupport.longValue(currentValues, position),
                    OperatorVectorSupport.longValue(slotValues, 0));
        }
        if (flattenedCurrent instanceof BinaryVector && flattenedSlot instanceof BinaryVector) {
            return OperatorVectorSupport.binaryCompare(currentValues, position, slotValues, 0);
        }
        return Integer.MIN_VALUE;
    }

    public int compareSlots(int leftSlot, int rightSlot)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int orderingColumn = orderingColumns[orderingIndex];
            Streams leftOrdering = slotColumns[orderingColumn][leftSlot];
            Streams rightOrdering = slotColumns[orderingColumn][rightSlot];
            int comparison = OperatorOrderingSemantics.compare(
                    leftOrdering.values(),
                    leftOrdering.getOrNull(Stream.NULLS),
                    0,
                    rightOrdering.values(),
                    rightOrdering.getOrNull(Stream.NULLS),
                    0);
            comparison = descendingByColumn[orderingIndex] ? comparison : -comparison;
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    public void copyRow(Batch batch, int position, int slot)
    {
        for (int orderingColumn : orderingColumns) {
            slotColumns[orderingColumn][slot] = buffers.copyPosition(batch.output(orderingColumn), slotColumns[orderingColumn][slot], position);
        }
        for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
            if (isOrderingColumn(outputIndex)) {
                continue;
            }
            slotColumns[outputIndex][slot] = null;
        }
        pendingBatches[slot] = batch;
        pendingPositions[slot] = position;
    }

    public void flushPendingBatch(Batch batch, List<Integer> retainedSlots)
    {
        for (int slot : retainedSlots) {
            if (pendingBatches[slot] != batch) {
                continue;
            }
            for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
                if (isOrderingColumn(outputIndex)) {
                    continue;
                }
                slotColumns[outputIndex][slot] = buffers.copyPosition(batch.output(outputIndex), slotColumns[outputIndex][slot], pendingPositions[slot]);
                if (schema[outputIndex] == null) {
                    schema[outputIndex] = slotColumns[outputIndex][slot];
                }
            }
            pendingBatches[slot] = null;
        }
    }

    public void setOrderedSlots(List<Integer> orderedSlots)
    {
        this.orderedSlots = orderedSlots;
        this.outputMask = allocator.allocateAllMask(allocationContext, orderedSlots.size());
        this.materialized = new Streams[schema.length];
    }

    public void constrain(Mask mask)
    {
        outputMask = mask;
        materialized = new Streams[schema.length];
    }

    public Streams output(int index)
    {
        Streams output = materialized[index];
        if (output != null) {
            return output;
        }

        if (orderedSlots.isEmpty()) {
            Streams columnSchema = ensureEmptySchema(index);
            output = buffers.emptyLike(columnSchema);
        }
        else {
            ensurePendingOutputMaterialized(index);
            Streams columnSchema = ensureMaterializedSchema(index);
            output = materializeColumn(columnSchema, index);
        }
        materialized[index] = output;
        return output;
    }

    public Set<Stream> outputStreams(int index)
    {
        Set<Stream> streams = exposedStreams[index];
        if (streams == null) {
            throw new IllegalStateException("TopN did not observe source output streams");
        }
        return streams;
    }

    public void releaseFallbackBatch()
    {
        if (fallbackBatch != null) {
            fallbackBatch.close();
            fallbackBatch = null;
        }
    }

    public boolean shouldKeepBatchForEmptySchema(Batch batch, boolean queueEmpty)
    {
        return queueEmpty && batch == fallbackBatch && hasMissingSchema();
    }

    private boolean hasMissingSchema()
    {
        for (Streams streams : schema) {
            if (streams == null) {
                return true;
            }
        }
        return false;
    }

    private void ensurePendingOutputMaterialized(int outputIndex)
    {
        if (isOrderingColumn(outputIndex)) {
            return;
        }
        constrainPendingBatches();
        for (int slot : orderedSlots) {
            Batch batch = pendingBatches[slot];
            if (batch == null) {
                continue;
            }
            slotColumns[outputIndex][slot] = buffers.copyPosition(batch.output(outputIndex), slotColumns[outputIndex][slot], pendingPositions[slot]);
            if (schema[outputIndex] == null) {
                schema[outputIndex] = slotColumns[outputIndex][slot];
            }
        }
    }

    private void constrainPendingBatches()
    {
        IdentityHashMap<Batch, List<Integer>> retainedPositionsByBatch = new IdentityHashMap<>();
        for (int slot : orderedSlots) {
            Batch batch = pendingBatches[slot];
            if (batch == null) {
                continue;
            }
            retainedPositionsByBatch.computeIfAbsent(batch, _ -> new ArrayList<>())
                    .add(pendingPositions[slot]);
        }
        for (Map.Entry<Batch, List<Integer>> entry : retainedPositionsByBatch.entrySet()) {
            int[] positions = entry.getValue().stream()
                    .mapToInt(Integer::intValue)
                    .sorted()
                    .toArray();
            entry.getKey().constrain(allocator.allocateSparseMask(allocationContext, positions, entry.getKey().borrowMask().size()));
        }
    }

    private Streams ensureEmptySchema(int outputIndex)
    {
        Streams columnSchema = schema[outputIndex];
        if (columnSchema != null) {
            return columnSchema;
        }
        if (fallbackBatch != null) {
            schema[outputIndex] = buffers.borrowStreams(fallbackBatch.output(outputIndex));
            return schema[outputIndex];
        }
        throw new IllegalStateException("TopN did not observe source output schema");
    }

    private Streams ensureMaterializedSchema(int outputIndex)
    {
        Streams columnSchema = schema[outputIndex];
        if (columnSchema != null) {
            return columnSchema;
        }
        int firstOutputPosition = outputMask == null || outputMask.none() ? 0 : outputMask.position(0);
        columnSchema = slotColumns[outputIndex][orderedSlots.get(firstOutputPosition)];
        if (columnSchema == null) {
            throw new IllegalStateException("TopN output column was not materialized: " + outputIndex);
        }
        schema[outputIndex] = columnSchema;
        return columnSchema;
    }

    private Streams materializeColumn(Streams columnSchema, int outputIndex)
    {
        if (outputMask == null || outputMask.all()) {
            return materializeDenseColumn(columnSchema, outputIndex, orderedSlots);
        }

        Streams result = null;
        for (int outputPosition : outputMask) {
            int slot = orderedSlots.get(outputPosition);
            result = buffers.copySinglePosition(
                    result,
                    slotColumns[outputIndex][slot],
                    outputMask.size(),
                    outputPosition,
                    0);
        }
        return result == null ? buffers.emptyLike(columnSchema) : result;
    }

    private Streams materializeDenseColumn(Streams columnSchema, int outputIndex, List<Integer> orderedSlots)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : columnSchema.asMap().entrySet()) {
            Vector[] rows = new Vector[orderedSlots.size()];
            for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
                rows[rowIndex] = slotColumns[outputIndex][orderedSlots.get(rowIndex)].get(entry.getKey());
            }
            result.put(entry.getKey(), buffers.materializeStream(entry.getValue(), rows));
        }
        return result.build();
    }

    private boolean isOrderingColumn(int outputIndex)
    {
        return orderingColumnFlags[outputIndex];
    }
}
