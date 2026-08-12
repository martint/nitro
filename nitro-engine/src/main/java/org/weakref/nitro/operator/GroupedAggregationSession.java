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
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Long-lived grouped aggregation state for batches scheduled by an external host.
 *
 * <p>Inputs are consumed eagerly and remain owned by the caller. Grouping tables, aggregate
 * states, generated kernels, and reusable buffers live until this session is closed.
 */
public final class GroupedAggregationSession
        implements BatchAggregationSession
{
    private final Allocator allocator;
    private final Schema inputSchema;
    private final List<Integer> groupByColumns;
    private final List<Integer> groupedColumns;
    private final PhysicalAggregationProgram program;
    private final OperatorResources operatorResources;
    private final GroupingStateResources groupingResources;
    private final Allocator.Context aggregationAllocationContext = new Allocator.Context("GroupedAggregationSession");
    private final PartialAggregationControl partialAggregationControl;
    private final int maxFinalOutputBatchRows;
    private final InitialAggregationBatchBuilder initialAggregationBatchBuilder;
    private final MutableAggregationPhaseMetrics phaseMetrics = new MutableAggregationPhaseMetrics();
    private GroupedAggregationOperator currentAggregation;
    private GroupedAggregationOperator flushedAggregation;
    private Batch pendingOutput;
    private boolean aggregatedInput;
    private long inputBytes;
    private long inputRows;
    private boolean finished;
    private boolean closed;

    public GroupedAggregationSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources)
    {
        this(
                allocator,
                inputSchema,
                groupByColumns,
                groupedColumns,
                program,
                operatorResources,
                null,
                operatorResources.aggregation().maxOutputBatchRows());
    }

    public GroupedAggregationSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources,
            PartialAggregationControl partialAggregationControl)
    {
        this(
                allocator,
                inputSchema,
                groupByColumns,
                groupedColumns,
                program,
                operatorResources,
                partialAggregationControl,
                operatorResources.aggregation().maxOutputBatchRows());
    }

    public GroupedAggregationSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources,
            PartialAggregationControl partialAggregationControl,
            int maxFinalOutputBatchRows)
    {
        this(
                allocator,
                inputSchema,
                groupByColumns,
                groupedColumns,
                program,
                operatorResources,
                operatorResources.grouping(),
                partialAggregationControl,
                maxFinalOutputBatchRows);
    }

    public GroupedAggregationSession(
            Allocator allocator,
            Schema inputSchema,
            List<Integer> groupByColumns,
            List<Integer> groupedColumns,
            PhysicalAggregationProgram program,
            OperatorResources operatorResources,
            GroupingStateResources groupingResources,
            PartialAggregationControl partialAggregationControl,
            int maxFinalOutputBatchRows)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.groupByColumns = List.copyOf(requireNonNull(groupByColumns, "groupByColumns is null"));
        this.groupedColumns = List.copyOf(requireNonNull(groupedColumns, "groupedColumns is null"));
        this.program = requireNonNull(program, "program is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.groupingResources = requireNonNull(groupingResources, "groupingResources is null");
        this.partialAggregationControl = partialAggregationControl;
        if (maxFinalOutputBatchRows <= 0) {
            throw new IllegalArgumentException("maxFinalOutputBatchRows must be positive");
        }
        this.maxFinalOutputBatchRows = maxFinalOutputBatchRows;
        initialAggregationBatchBuilder = partialAggregationControl == null ? null : new InitialAggregationBatchBuilder(
                allocator,
                inputSchema,
                groupedColumns,
                program,
                operatorResources,
                phaseMetrics);
        currentAggregation = createAggregation();
    }

    public Schema outputSchema()
    {
        return currentAggregation != null ? currentAggregation.outputSchema() : initialAggregationBatchBuilder.outputSchema();
    }

    @Override
    public long retainedBytes()
    {
        if (currentAggregation != null) {
            return currentAggregation.retainedBytes();
        }
        if (flushedAggregation != null) {
            return flushedAggregation.retainedBytes();
        }
        return 0;
    }

    @Override
    public AggregationPhaseMetrics phaseMetrics()
    {
        return phaseMetrics.snapshot();
    }

    public void addInput(Batch batch)
    {
        addInput(batch, 0);
    }

    @Override
    public void addInput(Batch batch, long inputBytes)
    {
        addInput(batch, inputBytes, false);
    }

    @Override
    public InputOwnership addInputWithOwnership(Batch batch, long inputBytes)
    {
        return addInput(batch, inputBytes, true);
    }

    private InputOwnership addInput(Batch batch, long inputBytes, boolean mayRetainInput)
    {
        checkAcceptingInput();
        requireNonNull(batch, "batch is null");
        if (inputBytes < 0) {
            throw new IllegalArgumentException("inputBytes is negative");
        }
        releaseFlushedAggregation();
        if (partialAggregationControl != null) {
            boolean aggregationEnabled = partialAggregationControl.aggregationEnabled();
            int sampleSize = partialAggregationControl.inputCardinalitySampleSize();
            if (sampleSize > 0) {
                PartialAggregationInputStatistics inputStatistics = GroupingCardinalitySampler.sample(
                        batch,
                        groupByColumns,
                        sampleSize,
                        allocator.primitiveArrays());
                if (inputStatistics.sampledRows() > 0) {
                    aggregationEnabled = partialAggregationControl.aggregationEnabled(inputStatistics);
                }
            }
            if (!aggregationEnabled) {
                pendingOutput = mayRetainInput ? initialAggregationBatchBuilder.buildRetaining(batch) : null;
                InputOwnership ownership = pendingOutput == null ? InputOwnership.CALLER : InputOwnership.SESSION;
                if (pendingOutput == null) {
                    pendingOutput = initialAggregationBatchBuilder.build(batch);
                }
                partialAggregationControl.onPassthroughFlush(inputBytes, batch.borrowMask().count());
                return ownership;
            }
        }
        ensureAggregation();
        currentAggregation.addInput(batch);
        aggregatedInput = true;
        this.inputBytes += inputBytes;
        inputRows += batch.borrowMask().count();
        return InputOwnership.CALLER;
    }

    @Override
    public boolean hasOutput()
    {
        return pendingOutput != null ||
                (flushedAggregation != null && flushedAggregation.hasSessionOutput()) ||
                (finished && currentAggregation != null && currentAggregation.hasSessionOutput());
    }

    @Override
    public Batch getOutput()
    {
        checkOpen();
        if (pendingOutput != null) {
            Batch output = pendingOutput;
            pendingOutput = null;
            return output;
        }
        if (flushedAggregation != null && flushedAggregation.hasSessionOutput()) {
            return flushedAggregation.getSessionOutput(maxFinalOutputBatchRows);
        }
        if (finished && currentAggregation != null && currentAggregation.hasSessionOutput()) {
            return currentAggregation.getSessionOutput(maxFinalOutputBatchRows);
        }
        throw new IllegalStateException("grouped aggregation session has no output");
    }

    @Override
    public void flush()
    {
        checkAcceptingInput();
        if (partialAggregationControl == null) {
            throw new UnsupportedOperationException("grouped aggregation session is not adaptive");
        }
        if (!aggregatedInput) {
            return;
        }
        pendingOutput = currentAggregation.finishInput(maxFinalOutputBatchRows);
        flushedAggregation = currentAggregation;
        currentAggregation = null;
        partialAggregationControl.onAggregatedFlush(inputBytes, inputRows, flushedAggregation.sessionOutputRowCount());
        aggregatedInput = false;
        inputBytes = 0;
        inputRows = 0;
    }

    public Batch finish()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("grouped aggregation session is already finished");
        }
        if (pendingOutput != null) {
            throw new IllegalStateException("grouped aggregation session has pending output");
        }
        if (flushedAggregation != null && flushedAggregation.hasSessionOutput()) {
            throw new IllegalStateException("grouped aggregation session has pending output");
        }
        finished = true;
        releaseFlushedAggregation();
        ensureAggregation();
        return currentAggregation.finishInput(maxFinalOutputBatchRows);
    }

    private void checkAcceptingInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("grouped aggregation session is finished");
        }
        if (pendingOutput != null) {
            throw new IllegalStateException("grouped aggregation session has pending output");
        }
        if (flushedAggregation != null && flushedAggregation.hasSessionOutput()) {
            throw new IllegalStateException("grouped aggregation session has pending output");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("grouped aggregation session is closed");
        }
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
        if (currentAggregation != null) {
            currentAggregation.close();
            currentAggregation = null;
        }
        releaseFlushedAggregation();
        allocator.releasePooledMemory(aggregationAllocationContext);
    }

    private GroupedAggregationOperator createAggregation()
    {
        return new GroupedAggregationOperator(
                allocator,
                groupByColumns,
                groupedColumns,
                program,
                new SchemaSource(inputSchema),
                operatorResources,
                groupingResources,
                aggregationAllocationContext,
                phaseMetrics);
    }

    private void ensureAggregation()
    {
        if (currentAggregation == null) {
            currentAggregation = createAggregation();
        }
    }

    private void releaseFlushedAggregation()
    {
        if (flushedAggregation != null) {
            flushedAggregation.close();
            flushedAggregation = null;
            allocator.releasePooledMemory(aggregationAllocationContext);
        }
    }

    private static final class SchemaSource
            implements Operator
    {
        private final Schema schema;

        private SchemaSource(Schema schema)
        {
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public int outputCount()
        {
            return schema.size();
        }

        @Override
        public Schema outputSchema()
        {
            return schema;
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Batch next()
        {
            throw new IllegalStateException("No more rows");
        }

        @Override
        public void constrain(Mask mask)
        {
            throw new IllegalStateException("No pending batch");
        }

        @Override
        public void close() {}
    }
}
