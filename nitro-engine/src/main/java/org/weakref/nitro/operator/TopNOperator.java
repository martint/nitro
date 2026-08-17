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
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

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
        this(
                allocator,
                n,
                columns,
                descending,
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
            Operator source,
            JoinBufferPolicy joinBufferPolicy,
            StructuralTypeKernelFactory structuralTypes,
            TopNOperatorPolicy policy)
    {
        if (columns.length == 0) {
            throw new IllegalArgumentException("TopN requires at least one ordering column");
        }
        if (columns.length != descending.length) {
            throw new IllegalArgumentException("TopN ordering columns and directions must have the same length");
        }
        this.allocator = allocator;
        this.n = n;
        this.source = source;
        this.policy = requireNonNull(policy, "policy is null");
        state = new TopNState(
                columns,
                descending,
                requireNonNull(joinBufferPolicy, "joinBufferPolicy is null"),
                allocator,
                allocationContext,
                source.outputSchema(),
                requireNonNull(structuralTypes, "structuralTypes is null"),
                n);
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

    private Mask computeTopN()
    {
        // TODO: flat memory priority queue
        PriorityQueue<Entry> queue = new PriorityQueue<>(n, (left, right) -> state.compareSlots(left.position(), right.position()));

        boolean deferSchemaBorrow = source.supportsConstrainedReborrow();
        Boolean denseOrdering = null;
        boolean firstBatch = true;
        while (source.hasNext()) {
            Batch batch = source.next();
            state.beginBatch();
            state.captureSchema(batch, deferSchemaBorrow);
            Mask mask = batch.borrowMask();
            if (firstBatch &&
                    source.supportsConstrainedReborrow() &&
                    retainedSingleBatchAdmission(mask) &&
                    !source.hasNext()) {
                return computeRetainedSingleBatchTopN(batch, mask);
            }
            firstBatch = false;
            // Dense ordering vectors win for modest results. For large lazy grouped results, compact
            // candidate copies avoid materializing every group merely to retain N rows (ClickBench Q33).
            boolean compactOrderingCandidates = mask.size() > (1 << 16);

            if (denseOrdering == null && !mask.none()) {
                denseOrdering = n >= policy.columnarOrderingMinLimit() && state.supportsDenseOrdering(batch);
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
                        queue.add(new Entry(slot));
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
                    queue.add(new Entry(slot));
                }
                else {
                    Entry head = queue.peek();
                    if (state.compareOrderingValue(batch, position, head.position(), compactOrderingCandidates) > 0) {
                        queue.poll();
                        if (Boolean.TRUE.equals(denseOrdering)) {
                            state.copyDenseOrderingRow(batch, position, head.position(), n);
                            state.deferPayloadRow(batch, position, head.position());
                        }
                        else {
                            state.copyRow(batch, position, head.position());
                        }
                        queue.add(new Entry(head.position()));
                    }
                }
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
                state.flushPendingBatch(batch, queue.stream()
                        .map(Entry::position)
                        .toList());
                if (!queue.isEmpty()) {
                    state.releaseFallbackBatch();
                }
                if (!state.shouldKeepBatchForEmptySchema(batch, queue.isEmpty())) {
                    batch.close();
                }
            }
        }

        int count = queue.size();
        List<Integer> orderedSlots = orderedSlots(queue);
        if (Boolean.TRUE.equals(denseOrdering)) {
            int[] primitiveOrderedSlots = orderedSlots.stream()
                    .mapToInt(Integer::intValue)
                    .toArray();
            state.setOrderedSlots(primitiveOrderedSlots, primitiveOrderedSlots.length);
        }
        else {
            state.setOrderedSlots(orderedSlots);
        }

        done = true;
        return allocator.allocateRangeMask(allocationContext, 0, count);
    }

    private boolean retainedSingleBatchAdmission(Mask mask)
    {
        return mask.count() >= (long) n * policy.retainedSingleBatchMinimumRowsPerLimit();
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
        done = true;
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
        Mask batchMask = computeTopN();
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

    private List<Integer> orderedSlots(PriorityQueue<Entry> queue)
    {
        List<Entry> entries = new ArrayList<>(queue);
        entries.sort((left, right) -> state.compareSlots(right.position(), left.position()));
        return entries.stream()
                .map(Entry::position)
                .toList();
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
        allocator.release(allocationContext);
    }

    record Entry(int position) {}
}
