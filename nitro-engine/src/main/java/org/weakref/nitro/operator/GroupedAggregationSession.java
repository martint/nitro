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
    private final PartialAggregationControl partialAggregationControl;
    private final InitialAggregationBatchBuilder initialAggregationBatchBuilder;
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
        this(allocator, inputSchema, groupByColumns, groupedColumns, program, operatorResources, null);
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
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.groupByColumns = List.copyOf(requireNonNull(groupByColumns, "groupByColumns is null"));
        this.groupedColumns = List.copyOf(requireNonNull(groupedColumns, "groupedColumns is null"));
        this.program = requireNonNull(program, "program is null");
        this.operatorResources = requireNonNull(operatorResources, "operatorResources is null");
        this.partialAggregationControl = partialAggregationControl;
        initialAggregationBatchBuilder = partialAggregationControl == null ? null : new InitialAggregationBatchBuilder(
                allocator,
                inputSchema,
                groupedColumns,
                program,
                operatorResources);
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

    public void addInput(Batch batch)
    {
        addInput(batch, 0);
    }

    @Override
    public void addInput(Batch batch, long inputBytes)
    {
        checkAcceptingInput();
        requireNonNull(batch, "batch is null");
        if (inputBytes < 0) {
            throw new IllegalArgumentException("inputBytes is negative");
        }
        releaseFlushedAggregation();
        if (partialAggregationControl != null && !partialAggregationControl.aggregationEnabled()) {
            pendingOutput = initialAggregationBatchBuilder.build(batch);
            partialAggregationControl.onPassthroughFlush(inputBytes, batch.borrowMask().count());
            return;
        }
        ensureAggregation();
        currentAggregation.addInput(batch);
        aggregatedInput = true;
        this.inputBytes += inputBytes;
        inputRows += batch.borrowMask().count();
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
            throw new IllegalStateException("grouped aggregation session has no output");
        }
        Batch output = pendingOutput;
        pendingOutput = null;
        return output;
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
        pendingOutput = currentAggregation.finishInput();
        flushedAggregation = currentAggregation;
        currentAggregation = null;
        partialAggregationControl.onAggregatedFlush(inputBytes, inputRows, pendingOutput.borrowMask().count());
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
        finished = true;
        releaseFlushedAggregation();
        ensureAggregation();
        return currentAggregation.finishInput();
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
    }

    private GroupedAggregationOperator createAggregation()
    {
        return new GroupedAggregationOperator(
                allocator,
                groupByColumns,
                groupedColumns,
                program,
                new SchemaSource(inputSchema),
                operatorResources);
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
