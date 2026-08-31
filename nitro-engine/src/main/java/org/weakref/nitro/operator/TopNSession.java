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
    private final TopNOperatorPolicy policy;

    private boolean finished;
    private boolean closed;
    private Boolean denseOrdering;

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
                new boolean[orderingColumns.length],
                inputSchema,
                EngineResources.from(allocator).operatorResources());
    }

    public TopNSession(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            Schema inputSchema,
            TopNOperatorPolicy policy)
    {
        this(allocator, limit, orderingColumns, descending, new boolean[orderingColumns.length], inputSchema, policy);
    }

    public TopNSession(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            Schema inputSchema,
            TopNOperatorPolicy policy)
    {
        this(
                allocator,
                limit,
                orderingColumns,
                descending,
                nullsFirst,
                inputSchema,
                EngineResources.from(allocator).operatorResources(),
                policy);
    }

    public TopNSession(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            Schema inputSchema,
            OperatorResources resources)
    {
        this(allocator, limit, orderingColumns, descending, new boolean[orderingColumns.length], inputSchema, resources);
    }

    public TopNSession(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            Schema inputSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                limit,
                orderingColumns,
                descending,
                nullsFirst,
                inputSchema,
                resources,
                requireNonNull(resources, "resources is null").topNOperatorPolicy());
    }

    private TopNSession(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            Schema inputSchema,
            OperatorResources resources,
            TopNOperatorPolicy policy)
    {
        if (limit <= 0) {
            throw new IllegalArgumentException("TopN limit must be positive");
        }
        requireNonNull(orderingColumns, "orderingColumns is null");
        requireNonNull(descending, "descending is null");
        requireNonNull(nullsFirst, "nullsFirst is null");
        if (orderingColumns.length == 0) {
            throw new IllegalArgumentException("TopN requires at least one ordering column");
        }
        if (orderingColumns.length != descending.length || orderingColumns.length != nullsFirst.length) {
            throw new IllegalArgumentException("TopN ordering columns, directions, and null placements must have the same length");
        }
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.limit = limit;
        this.outputSchema = requireNonNull(inputSchema, "inputSchema is null");
        OperatorResources operatorResources = requireNonNull(resources, "resources is null");
        this.policy = requireNonNull(policy, "policy is null");
        state = new TopNState(
                orderingColumns,
                descending,
                nullsFirst,
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
        if (mask.none()) {
            state.discardFallbackBatch();
            return;
        }
        if (denseOrdering == null) {
            denseOrdering = mask.count() <= limit && state.supportsDenseOrdering(
                    batch,
                    limit >= policy.columnarOrderingMinLimit(),
                    limit >= policy.variableWidthColumnarOrderingMinLimit());
        }
        try {
            if (!denseOrdering) {
                addRowCandidates(batch, mask, compactOrderingCandidates);
                return;
            }
            int copied = Math.min(limit - candidates.size(), mask.count());
            if (copied > 0) {
                Mask initial = copied == mask.count()
                        ? mask
                        : allocator.firstMask(allocationContext, mask, copied);
                int outputStart = candidates.size();
                try {
                    state.appendDenseOrderingBatch(batch, initial, outputStart, limit);
                    for (int index = 0; index < copied; index++) {
                        int slot = outputStart + index;
                        state.copyPayloadRow(batch, initial.position(index), slot);
                        candidates.add(slot);
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
                int head = candidates.element();
                if (state.compareOrderingValue(batch, position, head, compactOrderingCandidates) > 0) {
                    candidates.remove();
                    state.copyDenseOrderingRow(batch, position, head, limit);
                    state.copyPayloadRow(batch, position, head);
                    candidates.add(head);
                }
            }
        }
        finally {
            state.discardFallbackBatch();
        }
    }

    private void addRowCandidates(Batch batch, Mask mask, boolean compactOrderingCandidates)
    {
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
        if (Boolean.TRUE.equals(denseOrdering)) {
            int[] primitiveOrderedSlots = orderedSlots.stream()
                    .mapToInt(Integer::intValue)
                    .toArray();
            state.setOrderedSlots(primitiveOrderedSlots, primitiveOrderedSlots.length);
        }
        else {
            state.setOrderedSlots(orderedSlots);
        }

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
        state.close();
        allocator.release(allocationContext);
    }
}
