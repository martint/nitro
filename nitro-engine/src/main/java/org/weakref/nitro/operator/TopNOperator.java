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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.execution.EngineResources;

import static java.util.Objects.requireNonNull;

public class TopNOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("TopNOperator", TopNOperator.class);
    private final Allocator allocator;

    private final int n;
    private final Operator source;
    private final TopNState state;
    private final TopNOperatorPolicy policy;
    private final SlotHeap queue;

    private Boolean denseOrdering;
    private boolean firstBatch = true;
    private Batch currentInputBatch;
    private boolean currentInputProcessed;
    private Mask outputMask;
    private boolean done;

    public TopNOperator(Allocator allocator, int n, int column, Operator source)
    {
        this(allocator, n, new int[] {column}, new boolean[] {true}, source);
    }

    public TopNOperator(Allocator allocator, int n, int column, boolean descending, Operator source)
    {
        this(allocator, n, new int[] {column}, new boolean[] {descending}, source);
    }

    public TopNOperator(Allocator allocator, int n, int[] columns, boolean[] descending, Operator source)
    {
        this(
                allocator,
                n,
                columns,
                descending,
                new boolean[columns.length],
                source,
                EngineResources.from(allocator).operatorResources());
    }

    public TopNOperator(
            Allocator allocator,
            int n,
            int[] columns,
            boolean[] descending,
            Operator source,
            OperatorResources resources)
    {
        this(allocator, n, columns, descending, new boolean[columns.length], source, resources);
    }

    public TopNOperator(
            Allocator allocator,
            int n,
            int[] columns,
            boolean[] descending,
            boolean[] nullsFirst,
            Operator source,
            OperatorResources resources)
    {
        this(
                allocator,
                n,
                columns,
                descending,
                nullsFirst,
                source,
                requireNonNull(resources, "resources is null").joinBufferPolicy(),
                resources.codeGeneration().structuralTypes(),
                resources.topNOperatorPolicy());
    }

    public TopNOperator(
            Allocator allocator,
            int n,
            int[] columns,
            boolean[] descending,
            Operator source,
            JoinBufferPolicy joinBufferPolicy)
    {
        this(
                allocator,
                n,
                columns,
                descending,
                new boolean[columns.length],
                source,
                joinBufferPolicy,
                new StructuralTypeKernelFactory(),
                TopNOperatorPolicy.defaults());
    }

    private TopNOperator(
            Allocator allocator,
            int n,
            int[] columns,
            boolean[] descending,
            boolean[] nullsFirst,
            Operator source,
            JoinBufferPolicy joinBufferPolicy,
            StructuralTypeKernelFactory structuralTypes,
            TopNOperatorPolicy policy)
    {
        if (columns.length == 0) {
            throw new IllegalArgumentException("TopN requires at least one ordering column");
        }
        if (columns.length != descending.length || columns.length != nullsFirst.length) {
            throw new IllegalArgumentException("TopN ordering columns, directions, and null placements must have the same length");
        }
        this.allocator = allocator;
        this.n = n;
        this.source = source;
        this.policy = requireNonNull(policy, "policy is null");
        state = new TopNState(
                columns,
                descending,
                nullsFirst,
                requireNonNull(joinBufferPolicy, "joinBufferPolicy is null"),
                allocator,
                allocationContext,
                source.outputSchema(),
                requireNonNull(structuralTypes, "structuralTypes is null"),
                n);
        queue = new SlotHeap(allocator.primitiveArrays(), n, state);
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
        if (!done && outputMask == null) {
            outputMask = computeTopN();
            if (outputMask.none()) {
                done = true;
            }
        }
        return !done;
    }

    private Mask computeTopN()
    {
        // TODO: flat memory priority queue
        boolean deferSchemaBorrow = source.supportsConstrainedReborrow();
        while (currentInputBatch != null || source.hasNext()) {
            if (currentInputBatch == null) {
                currentInputBatch = source.next();
                currentInputProcessed = false;
            }
            Batch batch = currentInputBatch;
            if (!currentInputProcessed) {
                state.beginBatch();
                state.captureSchema(batch, deferSchemaBorrow);
                Mask mask = batch.borrowMask();
                if (firstBatch &&
                        source.supportsConstrainedReborrow() &&
                        retainedSingleBatchAdmission(batch, mask) &&
                        !source.hasNext()) {
                    currentInputBatch = null;
                    return computeRetainedSingleBatchTopN(batch, mask);
                }
                firstBatch = false;
                // Dense ordering vectors win for modest results. For large lazy grouped results, compact
                // candidate copies avoid materializing every group merely to retain N rows (ClickBench Q33).
                boolean compactOrderingCandidates = mask.size() > (1 << 16);

                if (denseOrdering == null && !mask.none()) {
                    denseOrdering = state.supportsDenseOrdering(
                            batch,
                            n >= policy.columnarOrderingMinLimit(),
                            n >= policy.variableWidthColumnarOrderingMinLimit());
                }

                int copied = 0;
                if (Boolean.TRUE.equals(denseOrdering) && queue.size() < n) {
                    copied = Math.min(n - queue.size(), mask.count());
                    Mask initial = copied == mask.count()
                            ? mask
                            : allocator.firstMask(allocationContext, mask, copied);
                    int outputStart = queue.size();
                    try {
                        state.appendDenseOrderingBatch(batch, initial, outputStart, n);
                        for (int index = 0; index < copied; index++) {
                            int slot = outputStart + index;
                            state.deferPayloadRow(batch, initial.position(index), slot);
                            queue.add(slot);
                        }
                    }
                    finally {
                        if (initial != mask) {
                            allocator.release(allocationContext, initial);
                        }
                    }
                }

                for (int index = copied; index < mask.count(); index++) {
                    int position = mask.position(index);
                    if (queue.size() < n) {
                        int slot = queue.size();
                        state.copyRow(batch, position, slot);
                        queue.add(slot);
                    }
                    else {
                        int head = queue.peek();
                        if (state.compareOrderingValue(batch, position, head, compactOrderingCandidates) > 0) {
                            queue.removeHead();
                            if (Boolean.TRUE.equals(denseOrdering)) {
                                state.copyDenseOrderingRow(batch, position, head, n);
                                state.deferPayloadRow(batch, position, head);
                            }
                            else {
                                state.copyRow(batch, position, head);
                            }
                            queue.add(head);
                        }
                    }
                }
                currentInputProcessed = true;
            }

            // Non-retained sources may invalidate the current batch as soon as the
            // caller probes for the next one, so materialize any deferred payload
            // columns before the next hasNext()/next() cycle can advance upstream.
            // The final batch is exempt only when the source can satisfy a constrained re-borrow
            // (it stays valid until close() and its payload columns can remain deferred until
            // output is requested). Sources whose reader advances irreversibly are always flushed.
            // supportsConstrainedReborrow() is checked first because probing hasNext() on an
            // advancing source (e.g. a Parquet scan) would itself invalidate the current batch.
            boolean canDeferFinalBatch = source.supportsConstrainedReborrow() && !source.hasNext();
            if (!source.supportsRetainedBatches() && !canDeferFinalBatch) {
                state.flushPendingBatch(batch, queue.values(), 0, queue.size());
                if (!queue.isEmpty()) {
                    state.releaseFallbackBatch();
                }
                if (!state.shouldKeepBatchForEmptySchema(batch, queue.isEmpty())) {
                    batch.close();
                }
            }
            currentInputBatch = null;
            currentInputProcessed = false;
        }

        int count = queue.size();
        int[] orderedSlots = queue.removeAllBestFirst();
        state.setOrderedSlots(orderedSlots, orderedSlots.length);

        return allocator.allocateRangeMask(allocationContext, 0, count);
    }

    private boolean retainedSingleBatchAdmission(Batch batch, Mask mask)
    {
        return state.hasPositionOrderingAccessor(batch) ||
                mask.count() >= (long) n * policy.retainedSingleBatchMinimumRowsPerLimit();
    }

    private Mask computeRetainedSingleBatchTopN(Batch batch, Mask mask)
    {
        state.prepareRetainedBatchOrdering(batch);
        int capacity = Math.min(n, mask.count());
        int[] heap = new int[capacity];
        int size = 0;
        for (int position : mask) {
            if (size < capacity) {
                heap[size] = position;
                siftRetainedUp(heap, size++);
            }
            else if (state.compareRetainedBatchPositions(position, heap[0]) > 0) {
                heap[0] = position;
                siftRetainedDown(heap, 0, size);
            }
        }

        int[] orderedPositions = new int[size];
        for (int output = size - 1; output >= 0; output--) {
            orderedPositions[output] = heap[0];
            int remaining = output;
            if (remaining > 0) {
                heap[0] = heap[remaining];
                siftRetainedDown(heap, 0, remaining);
            }
        }
        state.setRetainedBatchPositions(batch, orderedPositions);
        return allocator.allocateRangeMask(allocationContext, 0, orderedPositions.length);
    }

    private void siftRetainedUp(int[] heap, int index)
    {
        while (index > 0) {
            int parent = (index - 1) >>> 1;
            if (state.compareRetainedBatchPositions(heap[index], heap[parent]) >= 0) {
                return;
            }
            int value = heap[index];
            heap[index] = heap[parent];
            heap[parent] = value;
            index = parent;
        }
    }

    private void siftRetainedDown(int[] heap, int index, int size)
    {
        while (true) {
            int left = (index << 1) + 1;
            if (left >= size) {
                return;
            }
            int right = left + 1;
            int child = right < size && state.compareRetainedBatchPositions(heap[right], heap[left]) < 0
                    ? right
                    : left;
            if (state.compareRetainedBatchPositions(heap[child], heap[index]) >= 0) {
                return;
            }
            int value = heap[index];
            heap[index] = heap[child];
            heap[child] = value;
            index = child;
        }
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        Mask batchMask = outputMask;
        outputMask = null;
        done = true;
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
    public void close()
    {
        source.close();
        if (outputMask != null) {
            allocator.release(allocationContext, outputMask);
            outputMask = null;
        }
        queue.close();
        state.close();
        allocator.release(allocationContext);
    }

    private static final class SlotHeap
            implements AutoCloseable
    {
        private final PrimitiveArrayPool arrayPool;
        private final TopNState state;
        private int[] values;
        private int size;

        private SlotHeap(PrimitiveArrayPool arrayPool, int capacity, TopNState state)
        {
            if (capacity <= 0) {
                throw new IllegalArgumentException("TopN limit must be positive");
            }
            this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
            this.state = requireNonNull(state, "state is null");
            values = arrayPool.borrowInts(capacity);
        }

        public int size()
        {
            return size;
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public int[] values()
        {
            return values;
        }

        public int peek()
        {
            if (size == 0) {
                throw new IllegalStateException("TopN heap is empty");
            }
            return values[0];
        }

        public void add(int value)
        {
            if (size == values.length) {
                throw new IllegalStateException("TopN heap exceeds its configured limit");
            }
            int index = size++;
            values[index] = value;
            while (index > 0) {
                int parent = (index - 1) >>> 1;
                if (compare(values[index], values[parent]) >= 0) {
                    return;
                }
                swap(index, parent);
                index = parent;
            }
        }

        public int removeHead()
        {
            int result = peek();
            int remaining = --size;
            if (remaining > 0) {
                values[0] = values[remaining];
                siftDown(0);
            }
            return result;
        }

        public int[] removeAllBestFirst()
        {
            int[] ordered = new int[size];
            for (int output = size - 1; output >= 0; output--) {
                ordered[output] = removeHead();
            }
            return ordered;
        }

        private void siftDown(int index)
        {
            while (true) {
                int left = (index << 1) + 1;
                if (left >= size) {
                    return;
                }
                int right = left + 1;
                int child = right < size && compare(values[right], values[left]) < 0
                        ? right
                        : left;
                if (compare(values[child], values[index]) >= 0) {
                    return;
                }
                swap(index, child);
                index = child;
            }
        }

        private void swap(int left, int right)
        {
            int value = values[left];
            values[left] = values[right];
            values[right] = value;
        }

        private int compare(int left, int right)
        {
            int comparison = state.compareSlots(left, right);
            // SQL does not assign an order to equal sort keys, but stable slot order keeps operator output
            // deterministic and preserves the order produced by the former object heap.
            return comparison != 0 ? comparison : Integer.compare(right, left);
        }

        @Override
        public void close()
        {
            if (values != null) {
                arrayPool.release(values);
                values = null;
                size = 0;
            }
        }
    }
}
