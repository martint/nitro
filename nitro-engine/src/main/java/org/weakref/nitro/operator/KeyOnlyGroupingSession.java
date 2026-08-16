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
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A streaming grouping session for a physical aggregation with keys and no aggregate state.
 *
 * <p>The session applies SQL grouping null semantics, retaining one representative for each
 * null-bearing key. Distinct rows are copied into owned output batches, so the caller remains free
 * to close every input immediately after {@link #addInput(Batch)}.
 */
public final class KeyOnlyGroupingSession
        implements BatchAggregationSession
{
    private static final int[] EMPTY_POSITIONS = new int[0];

    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final OperatorResources operatorResources;
    private final int[] groupByColumns;
    private final List<TypeBinding> keyTypes;
    private final Vector[] values;
    private final Vector[] nulls;
    private final InitialAggregationBatchBuilder outputBuilder;

    private DistinctKeySet distinctKeySet;
    private int[] distinctPositions = EMPTY_POSITIONS;
    private Batch pendingOutput;
    private boolean finished;
    private boolean closed;

    public KeyOnlyGroupingSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            OperatorResources operatorResources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(inputSchema, "inputSchema is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns is null").stream()
                .mapToInt(Integer::intValue)
                .toArray();
        if (this.groupByColumns.length == 0) {
            throw new IllegalArgumentException("groupByColumns is empty");
        }
        this.keyTypes = Arrays.stream(this.groupByColumns)
                .mapToObj(column -> inputSchema.field(column).type())
                .toList();
        this.values = new Vector[this.groupByColumns.length];
        this.nulls = new Vector[this.groupByColumns.length];
        this.allocationContext = new Allocator.Context(
                "KeyOnlyGroupingSession",
                operatorResources.grouping().markDistinctMaskPool());
        this.outputBuilder = new InitialAggregationBatchBuilder(
                allocator,
                inputSchema,
                groupedColumns,
                new PhysicalAggregationProgram(List.of(), List.of()),
                operatorResources);
    }

    @Override
    public Schema outputSchema()
    {
        return outputBuilder.outputSchema();
    }

    @Override
    public void addInput(Batch batch)
    {
        addInput(batch, false);
    }

    @Override
    public InputOwnership addInputWithOwnership(Batch batch, long inputBytes)
    {
        return addInput(batch, true);
    }

    private InputOwnership addInput(Batch batch, boolean mayRetainInput)
    {
        checkAcceptingInput();
        requireNonNull(batch, "batch is null");
        Mask inputMask = batch.borrowMask();
        if (inputMask.none()) {
            return InputOwnership.CALLER;
        }
        if (distinctPositions.length < inputMask.selectedCount()) {
            int[] previous = distinctPositions;
            distinctPositions = allocator.primitiveArrays().borrowInts(inputMask.selectedCount());
            allocator.primitiveArrays().release(previous);
        }

        int selectedCount;
        try {
            for (int key = 0; key < groupByColumns.length; key++) {
                Output output = batch.output(groupByColumns[key]);
                values[key] = output.borrow(Stream.VALUES);
                nulls[key] = output.borrowOrNull(Stream.NULLS);
            }
            if (distinctKeySet == null) {
                distinctKeySet = DistinctKeySet.create(
                        values,
                        true,
                        keyTypes,
                        allocator,
                        allocationContext,
                        allocator.primitiveArrays(),
                        operatorResources.codeGeneration(),
                        operatorResources.distinctKeySetPolicy(),
                        operatorResources.adaptiveLongGroupingPolicy(),
                        operatorResources.flatKeyTablePolicy());
            }
            distinctKeySet.reserveAdditional(inputMask.selectedCount());
            selectedCount = distinctKeySet.addBatch(values, nulls, inputMask, distinctPositions);
        }
        finally {
            Arrays.fill(values, null);
            Arrays.fill(nulls, null);
        }
        if (selectedCount == 0) {
            return InputOwnership.CALLER;
        }
        if (selectedCount == inputMask.selectedCount()) {
            pendingOutput = mayRetainInput ? outputBuilder.buildRetaining(batch) : null;
            if (pendingOutput != null) {
                return InputOwnership.SESSION;
            }
            pendingOutput = outputBuilder.build(batch, inputMask);
            return InputOwnership.CALLER;
        }
        Mask distinctMask = allocator.allocateSparseMask(
                allocationContext,
                distinctPositions,
                selectedCount,
                inputMask.size());
        try {
            pendingOutput = outputBuilder.build(batch, distinctMask);
        }
        finally {
            allocator.release(allocationContext, distinctMask);
        }
        return InputOwnership.CALLER;
    }

    @Override
    public boolean hasOutput()
    {
        return pendingOutput != null;
    }

    @Override
    public Batch getOutput()
    {
        checkOpen();
        if (pendingOutput == null) {
            throw new IllegalStateException("key-only grouping session has no output");
        }
        Batch output = pendingOutput;
        pendingOutput = null;
        return output;
    }

    @Override
    public long retainedBytes()
    {
        return distinctKeySet == null ? 0 : distinctKeySet.retainedBytes();
    }

    @Override
    public Batch finish()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("key-only grouping session is already finished");
        }
        if (pendingOutput != null) {
            throw new IllegalStateException("key-only grouping session has pending output");
        }
        finished = true;
        return outputBuilder.empty();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (pendingOutput != null) {
            pendingOutput.close();
            pendingOutput = null;
        }
        if (distinctKeySet != null) {
            distinctKeySet.releaseBuffers();
            distinctKeySet = null;
        }
        allocator.primitiveArrays().release(distinctPositions);
        distinctPositions = EMPTY_POSITIONS;
        Arrays.fill(values, null);
        Arrays.fill(nulls, null);
        allocator.release(allocationContext);
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("key-only grouping session is finished");
        }
        if (pendingOutput != null) {
            throw new IllegalStateException("key-only grouping session has pending output");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("key-only grouping session is closed");
        }
    }
}
