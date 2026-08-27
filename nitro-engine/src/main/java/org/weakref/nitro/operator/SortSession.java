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

import it.unimi.dsi.fastutil.ints.IntArrays;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.execution.EngineResources;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Host-driven full-sort state for independently owned input batches.
 */
public final class SortSession
        implements AutoCloseable
{
    private final Allocator.Context allocationContext = new Allocator.Context("SortSession", SortSession.class);
    private final Allocator allocator;
    private final Schema outputSchema;
    private final SortOperatorPolicy policy;
    private final TopNState state;
    private final PrimitiveArrayPool arrayPool;

    private int[] orderedSlots = new int[0];
    private int[] sortScratch = new int[0];
    private long[] sortKeys = new long[0];
    private int[] radixCounts = new int[0];
    private int slotCount;
    private boolean finished;
    private boolean closed;

    public SortSession(
            Allocator allocator,
            int[] orderingColumns,
            boolean[] descending,
            Schema inputSchema)
    {
        this(
                allocator,
                orderingColumns,
                descending,
                new boolean[orderingColumns.length],
                inputSchema,
                EngineResources.from(allocator).operatorResources());
    }

    public SortSession(
            Allocator allocator,
            int[] orderingColumns,
            boolean[] descending,
            Schema inputSchema,
            OperatorResources resources)
    {
        this(allocator, orderingColumns, descending, new boolean[orderingColumns.length], inputSchema, resources);
    }

    public SortSession(
            Allocator allocator,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            Schema inputSchema,
            OperatorResources resources)
    {
        requireNonNull(orderingColumns, "orderingColumns is null");
        requireNonNull(descending, "descending is null");
        requireNonNull(nullsFirst, "nullsFirst is null");
        if (orderingColumns.length == 0) {
            throw new IllegalArgumentException("Sort requires at least one ordering column");
        }
        if (orderingColumns.length != descending.length || orderingColumns.length != nullsFirst.length) {
            throw new IllegalArgumentException("Sort ordering columns, directions, and null placements must have the same length");
        }
        this.allocator = requireNonNull(allocator, "allocator is null");
        outputSchema = requireNonNull(inputSchema, "inputSchema is null");
        OperatorResources operatorResources = requireNonNull(resources, "resources is null");
        policy = operatorResources.sortPolicy();
        arrayPool = allocator.primitiveArrays();
        state = new TopNState(
                orderingColumns,
                descending,
                nullsFirst,
                operatorResources.joinBufferPolicy(),
                allocator,
                allocationContext,
                inputSchema,
                operatorResources.codeGeneration().structuralTypes(),
                256);
    }

    public Schema outputSchema()
    {
        return outputSchema;
    }

    /**
     * Copies every selected row before returning; the caller may close the batch immediately.
     */
    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkAcceptingInput();
        state.beginBatch();
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            state.discardFallbackBatch();
            return;
        }

        int batchStart = slotCount;
        int required = slotCount + mask.count();
        ensureSortCapacity(required);
        if (policy.columnarBuffer() && outputSchema.size() <= policy.columnarBufferMaxColumns()) {
            state.appendDenseBatch(batch, mask, batchStart, orderedSlots.length);
            for (int position = 0; position < mask.count(); position++) {
                orderedSlots[slotCount] = slotCount;
                slotCount++;
            }
            return;
        }

        state.captureSchema(batch, false);
        state.ensureCapacity(required);
        try {
            for (int position : mask) {
                int slot = slotCount++;
                state.copyRow(batch, position, slot);
                orderedSlots[slot] = slot;
            }
            state.flushPendingBatch(batch, orderedSlots, batchStart, slotCount);
        }
        finally {
            state.discardFallbackBatch();
        }
    }

    public Optional<Batch> finish()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("Sort session is already finished");
        }
        finished = true;
        if (slotCount == 0) {
            return Optional.empty();
        }

        stableSort(orderedSlots, sortScratch, slotCount);
        state.setOrderedSlots(orderedSlots, slotCount);
        Mask outputMask = allocator.allocateRangeMask(allocationContext, 0, slotCount);
        Output[] outputs = new Output[outputSchema.size()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = new Output(
                    state.outputStreams(output),
                    stream -> state.output(output).get(stream),
                    (stream, vector) -> allocator.transfer(allocationContext, vector));
        }
        return Optional.of(new Batch(
                outputMask,
                state::constrain,
                takenMask -> allocator.transfer(allocationContext, takenMask),
                _ -> {},
                () -> {},
                outputs));
    }

    private void ensureSortCapacity(int required)
    {
        if (orderedSlots.length >= required) {
            return;
        }
        int capacity = Math.max(required, Math.max(256, orderedSlots.length * 2));
        int[] oldOrdered = orderedSlots;
        int[] oldScratch = sortScratch;
        long[] oldKeys = sortKeys;
        orderedSlots = arrayPool.borrowInts(capacity);
        sortScratch = arrayPool.borrowInts(capacity);
        sortKeys = arrayPool.borrowLongs(capacity);
        System.arraycopy(oldOrdered, 0, orderedSlots, 0, Math.min(oldOrdered.length, required));
        arrayPool.release(oldOrdered);
        arrayPool.release(oldScratch);
        arrayPool.release(oldKeys);
    }

    private void stableSort(int[] values, int[] scratch, int count)
    {
        if (state.hasSingleFixedWidthNonNullOrdering(count)) {
            radixSort(values, scratch, count);
            return;
        }
        System.arraycopy(values, 0, scratch, 0, count);
        IntArrays.mergeSort(values, 0, count, (left, right) -> state.compareSlots(right, left), scratch);
    }

    private void radixSort(int[] values, int[] scratch, int count)
    {
        if (radixCounts.length == 0) {
            radixCounts = arrayPool.borrowInts(256);
        }
        for (int index = 0; index < count; index++) {
            int slot = values[index];
            sortKeys[slot] = state.singleFixedWidthSortKey(slot);
        }
        int[] source = values;
        int[] target = scratch;
        boolean descending = state.singleOrderingDescending();
        for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
            java.util.Arrays.fill(radixCounts, 0, 256, 0);
            for (int index = 0; index < count; index++) {
                int digit = (int) (sortKeys[source[index]] >>> shift) & 0xFF;
                radixCounts[descending ? 0xFF - digit : digit]++;
            }
            int offset = 0;
            for (int digit = 0; digit < 256; digit++) {
                int size = radixCounts[digit];
                radixCounts[digit] = offset;
                offset += size;
            }
            for (int index = 0; index < count; index++) {
                int slot = source[index];
                int digit = (int) (sortKeys[slot] >>> shift) & 0xFF;
                int orderedDigit = descending ? 0xFF - digit : digit;
                target[radixCounts[orderedDigit]++] = slot;
            }
            int[] swap = source;
            source = target;
            target = swap;
        }
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("Sort session is finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Sort session is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        state.releaseFallbackBatch();
        allocator.release(allocationContext);
        arrayPool.release(orderedSlots);
        arrayPool.release(sortScratch);
        arrayPool.release(sortKeys);
        arrayPool.release(radixCounts);
        orderedSlots = new int[0];
        sortScratch = new int[0];
        sortKeys = new long[0];
        radixCounts = new int[0];
    }
}
