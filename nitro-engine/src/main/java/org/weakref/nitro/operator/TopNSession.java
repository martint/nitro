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
import java.util.Optional;
import java.util.PriorityQueue;

import static java.util.Objects.requireNonNull;

/**
 * Host-driven TopN state for execution environments that offer independent input batches.
 *
 * <p>The caller retains ownership of each input batch. All retained rows are copied before
 * {@link #addInput(Batch)} returns, so a host may close or reuse that batch immediately. The
 * session owns its candidate state until it is closed.
 */
public final class TopNSession
        implements AutoCloseable
{
    private final Allocator.Context allocationContext = new Allocator.Context("TopNSession", TopNSession.class);
    private final Allocator allocator;
    private final int limit;
    private final Schema outputSchema;
    private final TopNState state;
    private final PriorityQueue<Integer> candidates;

    private boolean finished;
    private boolean closed;

    public TopNSession(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            Schema inputSchema)
    {
        this(
                allocator,
                limit,
                orderingColumns,
                descending,
                inputSchema,
                EngineResources.from(allocator).operatorResources());
    }

    public TopNSession(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            Schema inputSchema,
            OperatorResources resources)
    {
        if (limit <= 0) {
            throw new IllegalArgumentException("TopN limit must be positive");
        }
        requireNonNull(orderingColumns, "orderingColumns is null");
        requireNonNull(descending, "descending is null");
        if (orderingColumns.length == 0) {
            throw new IllegalArgumentException("TopN requires at least one ordering column");
        }
        if (orderingColumns.length != descending.length) {
            throw new IllegalArgumentException("TopN ordering columns and directions must have the same length");
        }
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.limit = limit;
        this.outputSchema = requireNonNull(inputSchema, "inputSchema is null");
        OperatorResources operatorResources = requireNonNull(resources, "resources is null");
        state = new TopNState(
                orderingColumns,
                descending,
                operatorResources.joinBufferPolicy(),
                allocator,
                allocationContext,
                inputSchema,
                operatorResources.codeGeneration().structuralTypes(),
                limit);
        candidates = new PriorityQueue<>(Math.min(limit, 1024), (left, right) -> state.compareSlots(left, right));
    }

    public Schema outputSchema()
    {
        return outputSchema;
    }

    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkAcceptingInput();

        state.beginBatch();
        state.captureSchema(batch, true);
        Mask mask = batch.borrowMask();
        boolean compactOrderingCandidates = mask.size() > (1 << 16);
        try {
            for (int position : mask) {
                if (candidates.size() < limit) {
                    int slot = candidates.size();
                    state.copyRow(batch, position, slot);
                    candidates.add(slot);
                    continue;
                }
                int head = candidates.element();
                if (state.compareOrderingValue(batch, position, head, compactOrderingCandidates) > 0) {
                    candidates.remove();
                    state.copyRow(batch, position, head);
                    candidates.add(head);
                }
            }
            state.flushPendingBatch(batch, candidates.stream().toList());
        }
        finally {
            state.discardFallbackBatch();
        }
    }

    /**
     * Finishes input and returns the single ordered result batch, or no batch when no row was
     * observed.
     */
    public Optional<Batch> finish()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN session is already finished");
        }
        finished = true;
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        List<Integer> orderedSlots = new ArrayList<>(candidates);
        orderedSlots.sort((left, right) -> state.compareSlots(right, left));
        state.setOrderedSlots(orderedSlots);

        Mask outputMask = allocator.allocateRangeMask(allocationContext, 0, orderedSlots.size());
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

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN session is finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("TopN session is closed");
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
    }
}
