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

import static java.util.Objects.requireNonNull;

public class SortOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("SortOperator", SortOperator.class);

    private final SortOperatorPolicy policy;
    private final Allocator allocator;
    private final Operator source;
    private final TopNState state;
    private final PrimitiveArrayPool arrayPool;
    private int[] orderedSlots = new int[0];
    private int[] sortScratch = new int[0];
    private long[] sortKeys = new long[0];
    private int[] radixCounts = new int[0];

    private int slotCount;
    private boolean done;

    public SortOperator(Allocator allocator, int[] columns, boolean[] descending, Operator source)
    {
        this(
                allocator,
                columns,
                descending,
                new boolean[columns.length],
                source,
                EngineResources.from(allocator).operatorResources());
    }

    public SortOperator(
            Allocator allocator,
            int[] columns,
            boolean[] descending,
            Operator source,
            OperatorResources resources)
    {
        this(allocator, columns, descending, new boolean[columns.length], source, resources);
    }

    public SortOperator(
            Allocator allocator,
            int[] columns,
            boolean[] descending,
            boolean[] nullsFirst,
            Operator source,
            OperatorResources resources)
    {
        this(
                allocator,
                columns,
                descending,
                nullsFirst,
                source,
                requireNonNull(resources, "resources is null").sortPolicy(),
                resources.joinBufferPolicy(),
                resources.codeGeneration().structuralTypes());
    }

    public SortOperator(
            Allocator allocator,
            int[] columns,
            boolean[] descending,
            Operator source,
            SortOperatorPolicy policy,
            JoinBufferPolicy joinBufferPolicy)
    {
        this(
                allocator,
                columns,
                descending,
                new boolean[columns.length],
                source,
                policy,
                joinBufferPolicy,
                new StructuralTypeKernelFactory());
    }

    private SortOperator(
            Allocator allocator,
            int[] columns,
            boolean[] descending,
            boolean[] nullsFirst,
            Operator source,
            SortOperatorPolicy policy,
            JoinBufferPolicy joinBufferPolicy,
            StructuralTypeKernelFactory structuralTypes)
    {
        if (columns.length == 0) {
            throw new IllegalArgumentException("Sort requires at least one ordering column");
        }
        if (columns.length != descending.length || columns.length != nullsFirst.length) {
            throw new IllegalArgumentException("Sort ordering columns, directions, and null placements must have the same length");
        }
        this.policy = requireNonNull(policy, "policy is null");
        this.allocator = allocator;
        this.arrayPool = allocator.primitiveArrays();
        this.source = source;
        this.state = new TopNState(
                columns,
                descending,
                nullsFirst,
                requireNonNull(joinBufferPolicy, "joinBufferPolicy is null"),
                allocator,
                allocationContext,
                source.outputSchema(),
                requireNonNull(structuralTypes, "structuralTypes is null"),
                256);
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return source.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Batch next()
    {
        Mask batchMask = computeSorted();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = new Output(
                    state.outputStreams(output),
                    stream -> state.output(output).get(stream),
                    (stream, vector) -> allocator.transfer(allocationContext, vector));
        }
        return new Batch(
                batchMask,
                state::constrain,
                takenMask -> allocator.transfer(allocationContext, takenMask),
                _ -> {},
                state::releaseFallbackBatch,
                outputs);
    }

    private Mask computeSorted()
    {
        while (source.hasNext()) {
            Batch batch = source.next();
            // A full sort retains every input row, so eagerly copy every output while this batch is open. Each stream
            // is copied before the next output is borrowed and the source is not advanced until the batch is closed;
            // computed projections may therefore recycle a previously borrowed evaluator vector without invalidating
            // the sort buffer. This requires ordinary batch-lifetime borrowing, not cross-borrow/cross-batch retention.
            if (policy.columnarBuffer() && outputCount() <= policy.columnarBufferMaxColumns()) {
                Mask mask = batch.borrowMask();
                // An empty physical batch contributes no rows and therefore must not replace an already buffered
                // column with whatever partial schema streams that batch happens to expose.  This matters for lazy
                // projections, where an empty batch can expose only NULLS/ERRORS until VALUES is demanded.  Retain
                // the first empty batch only when the complete sort is still empty, so it can supply output schema.
                if (mask.none()) {
                    if (slotCount == 0) {
                        state.captureSchema(batch, true);
                        if (!state.shouldKeepBatchForEmptySchema(batch, true)) {
                            batch.close();
                        }
                    }
                    else {
                        batch.close();
                    }
                    continue;
                }
                int batchStart = slotCount;
                int required = slotCount + mask.count();
                ensureSortCapacity(required);
                state.appendDenseBatch(batch, mask, batchStart, orderedSlots.length);
                for (int position = 0; position < mask.count(); position++) {
                    orderedSlots[slotCount] = slotCount;
                    slotCount++;
                }
                batch.close();
                continue;
            }
            state.captureSchema(batch, false);
            Mask mask = batch.borrowMask();
            int batchStart = slotCount;
            int required = slotCount + mask.count();
            state.ensureCapacity(required);
            ensureSortCapacity(required);

            for (int position : mask) {
                int slot = slotCount++;
                state.copyRow(batch, position, slot);
                orderedSlots[slot] = slot;
            }

            if (!source.supportsRetainedBatches()) {
                state.flushPendingBatch(batch, orderedSlots, batchStart, slotCount);
                if (slotCount > 0) {
                    state.releaseFallbackBatch();
                }
                if (!state.shouldKeepBatchForEmptySchema(batch, slotCount == 0)) {
                    batch.close();
                }
            }
        }

        if (slotCount == 0) {
            state.prepareEmptyOutputSchema();
        }
        stableSort(orderedSlots, sortScratch, slotCount);
        state.setOrderedSlots(orderedSlots, slotCount);
        done = true;
        return allocator.allocateRangeMask(allocationContext, 0, slotCount);
    }

    private void ensureSortCapacity(int required)
    {
        if (orderedSlots.length >= required) {
            return;
        }
        int capacity = Math.max(required, Math.max(256, orderedSlots.length * 2));
        int[] oldOrdered = orderedSlots;
        int[] oldScratch = sortScratch;
        orderedSlots = arrayPool.borrowInts(capacity);
        sortScratch = arrayPool.borrowInts(capacity);
        long[] oldKeys = sortKeys;
        sortKeys = arrayPool.borrowLongs(capacity);
        System.arraycopy(oldOrdered, 0, orderedSlots, 0, Math.min(oldOrdered.length, required));
        arrayPool.release(oldOrdered);
        arrayPool.release(oldScratch);
        arrayPool.release(oldKeys);
        accountRetainedArrays();
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
            accountRetainedArrays();
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

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed.
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

    @Override
    public void close()
    {
        source.close();
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

    private void accountRetainedArrays()
    {
        allocator.setRetainedBytes(
                allocationContext,
                this,
                (long) orderedSlots.length * Integer.BYTES +
                        (long) sortScratch.length * Integer.BYTES +
                        (long) sortKeys.length * Long.BYTES +
                        (long) radixCounts.length * Integer.BYTES);
    }
}
