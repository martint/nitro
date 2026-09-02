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

import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.function.table.TableFunctionArgument;
import org.weakref.nitro.core.function.table.TableFunctionInput;
import org.weakref.nitro.core.function.table.TableFunctionOutputDemand;
import org.weakref.nitro.core.function.table.TableFunctionPassThroughColumn;
import org.weakref.nitro.core.function.table.TableFunctionProcessor;
import org.weakref.nitro.core.function.table.TableFunctionProcessorFactory;
import org.weakref.nitro.core.function.table.TableFunctionProgress;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;

import java.util.List;
import java.util.Set;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/// Pull operator for a single table argument whose input is already partitioned and ordered.
///
/// The operator retains the ordered input, discovers contiguous partition bounds through registry-supplied type
/// kernels, and creates isolated processor state for every partition. Physical partition/order preparation remains
/// an upstream optimizer responsibility. Pass-through references stay partition-relative and are gathered lazily.
public final class PartitionedTableFunctionOperator
        implements Operator
{
    private static final Set<Integer> ARGUMENT = Set.of(0);

    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("PartitionedTableFunctionOperator");
    private final ExecutionContext executionContext;
    private final Operator source;
    private final Schema inputSchema;
    private final int[] inputChannels;
    private final int[] partitionChannels;
    private final Schema outputSchema;
    private final int properOutputCount;
    private final List<TableFunctionPassThroughColumn> passThroughColumns;
    private final TableFunctionProcessorFactory processorFactory;
    private final TableFunctionOutputDemand outputDemand;
    private final int maxInputBatchRows;
    private final boolean processEmptyInput;
    private final StructuralComparisonKernel[] partitionEquality;
    private final EncodedRowBuffer rows;

    private TableFunctionProcessor processor;
    private TableFunctionOutputAssembler outputAssembler;
    private SourceBatch input;
    private Batch staged;
    private Batch currentBatch;
    private int nextPartitionStart;
    private int partitionStart;
    private int partitionEnd;
    private int inputPosition;
    private int inputLength;
    private boolean inputFinished;
    private boolean loaded;
    private boolean emptyPartitionStarted;
    private boolean finished;
    private boolean closed;

    public PartitionedTableFunctionOperator(
            Allocator allocator,
            ExecutionContext executionContext,
            Operator source,
            Schema inputSchema,
            int[] inputChannels,
            int[] partitionChannels,
            WindowInputOrder inputOrder,
            int orderingColumnCount,
            Schema outputSchema,
            int properOutputCount,
            List<TableFunctionPassThroughColumn> passThroughColumns,
            TableFunctionProcessorFactory processorFactory,
            boolean processEmptyInput,
            int maxInputBatchRows,
            OperatorResources resources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.executionContext = requireNonNull(executionContext, "executionContext is null");
        this.source = requireNonNull(source, "source is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null").clone();
        this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null").clone();
        if (!requireNonNull(inputOrder, "inputOrder is null").isFullyOrdered(orderingColumnCount)) {
            throw new IllegalArgumentException("table-function input is not fully partitioned and ordered");
        }
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        if (properOutputCount < 0 || properOutputCount > outputSchema.size()) {
            throw new IllegalArgumentException("proper output count is invalid");
        }
        this.properOutputCount = properOutputCount;
        this.passThroughColumns = List.copyOf(passThroughColumns);
        if (properOutputCount + this.passThroughColumns.size() != outputSchema.size()) {
            throw new IllegalArgumentException("output schema does not match pass-through columns");
        }
        if (this.passThroughColumns.stream().anyMatch(column -> column.argument() != 0)) {
            throw new IllegalArgumentException("partitioned table function has only argument zero");
        }
        this.processorFactory = requireNonNull(processorFactory, "processorFactory is null");
        this.outputDemand = new TableFunctionOutputDemand(
                Operator.fullOutputDemand(properOutputCount),
                this.passThroughColumns.isEmpty() ? Set.of() : ARGUMENT);
        this.processEmptyInput = processEmptyInput;
        if (maxInputBatchRows <= 0) {
            throw new IllegalArgumentException("maxInputBatchRows is not positive");
        }
        this.maxInputBatchRows = maxInputBatchRows;

        if (inputSchema.size() != this.inputChannels.length) {
            throw new IllegalArgumentException("input schema does not match input channels");
        }
        for (int input = 0; input < this.inputChannels.length; input++) {
            int sourceChannel = this.inputChannels[input];
            checkSourceChannel(sourceChannel, "input");
            if (!inputSchema.field(input).type().identity()
                    .equals(source.outputSchema().field(sourceChannel).type().identity())) {
                throw new IllegalArgumentException("input channel type does not match input schema");
            }
        }
        for (int channel : this.partitionChannels) {
            checkSourceChannel(channel, "partition");
        }
        for (int index = 0; index < this.passThroughColumns.size(); index++) {
            int inputColumn = this.passThroughColumns.get(index).inputColumn();
            checkSourceChannel(inputColumn, "pass-through input");
            if (!outputSchema.field(properOutputCount + index).type().identity()
                    .equals(source.outputSchema().field(inputColumn).type().identity())) {
                throw new IllegalArgumentException("pass-through output type does not match input column");
            }
        }

        StructuralTypeKernelFactory structuralTypes = requireNonNull(resources, "resources is null").codeGeneration().structuralTypes();
        partitionEquality = new StructuralComparisonKernel[source.outputCount()];
        for (int channel : this.partitionChannels) {
            partitionEquality[channel] = structuralTypes.comparison(source.outputSchema().field(channel).type());
        }
        rows = new EncodedRowBuffer(
                allocator,
                new Allocator.Context("PartitionedTableFunctionOperator.rows"),
                source.outputCount());
    }

    @Override
    public int outputCount()
    {
        return outputSchema.size();
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    @Override
    public boolean hasNext()
    {
        checkOpen();
        prepareOutput();
        return staged != null;
    }

    @Override
    public Batch next()
    {
        checkOpen();
        prepareOutput();
        if (staged == null) {
            throw new IllegalStateException("table function output is exhausted");
        }
        currentBatch = staged;
        staged = null;
        return currentBatch;
    }

    private void prepareOutput()
    {
        if (staged != null || finished) {
            return;
        }
        load();
        while (true) {
            if (processor == null && !startPartition()) {
                finished = true;
                return;
            }
            prepareInput();
            TableFunctionArgument argument = input == null
                    ? TableFunctionArgument.Finished.FINISHED
                    : new TableFunctionArgument.Rows(input);
            TableFunctionProgress progress = requireNonNull(
                    processor.process(
                            new TableFunctionInput(List.of(argument)),
                            outputDemand,
                            allocator,
                            allocationContext,
                            executionContext),
                    "table function processor returned null");
            switch (progress) {
                case TableFunctionProgress.Produced produced -> {
                    validateConsumed(produced.consumedArguments());
                    staged = outputAssembler.assemble(produced.output());
                    consumeInput(produced.consumedArguments());
                    executionContext.checkpoint();
                    return;
                }
                case TableFunctionProgress.Consumed consumed -> {
                    validateConsumed(consumed.arguments());
                    consumeInput(consumed.arguments());
                }
                case TableFunctionProgress.Blocked blocked -> executionContext.await(blocked.continuation());
                case TableFunctionProgress.Finished _ -> finishPartition();
            }
        }
    }

    private void load()
    {
        if (loaded) {
            return;
        }
        rows.load(source);
        loaded = true;
    }

    private boolean startPartition()
    {
        if (nextPartitionStart >= rows.size()) {
            if (!processEmptyInput || rows.size() != 0 || emptyPartitionStarted) {
                return false;
            }
            emptyPartitionStarted = true;
            partitionStart = 0;
            partitionEnd = 0;
        }
        else {
            partitionStart = nextPartitionStart;
            partitionEnd = partitionStart + 1;
            while (partitionEnd < rows.size() && samePartition(partitionEnd - 1, partitionEnd)) {
                partitionEnd++;
            }
        }
        inputPosition = partitionStart;
        inputFinished = false;
        processor = requireNonNull(processorFactory.create(), "processorFactory returned null");
        outputAssembler = new TableFunctionOutputAssembler(
                allocator,
                outputSchema,
                properOutputCount,
                passThroughColumns,
                List.of(new TableFunctionOutputAssembler.Argument(source.outputSchema(), rows)),
                new int[] {partitionStart},
                new int[] {partitionEnd});
        return true;
    }

    private void prepareInput()
    {
        if (input != null || inputFinished) {
            return;
        }
        if (inputPosition == partitionEnd) {
            inputFinished = true;
            return;
        }
        inputLength = min(maxInputBatchRows, partitionEnd - inputPosition);
        Batch batch = rows.copyRange(inputPosition, inputLength, inputChannels);
        input = OperatorBatchSource.batch(inputSchema, batch);
        input.selection();
        executionContext.checkpoint();
    }

    private boolean samePartition(int leftPosition, int rightPosition)
    {
        for (int channel : partitionChannels) {
            Streams left = rows.column(channel, leftPosition);
            Streams right = rows.column(channel, rightPosition);
            if (!partitionEquality[channel].identical(
                    left.values(), left.getOrNull(Stream.NULLS), rows.sourcePosition(leftPosition),
                    right.values(), right.getOrNull(Stream.NULLS), rows.sourcePosition(rightPosition))) {
                return false;
            }
        }
        return true;
    }

    private void validateConsumed(Set<Integer> consumed)
    {
        if (!consumed.isEmpty() && !consumed.equals(ARGUMENT)) {
            throw new IllegalStateException("table function consumed an unexpected argument");
        }
        if (!consumed.isEmpty() && input == null) {
            throw new IllegalStateException("table function consumed a finished argument");
        }
    }

    private void consumeInput(Set<Integer> consumed)
    {
        if (!consumed.isEmpty()) {
            closeInput();
            inputPosition += inputLength;
            inputLength = 0;
        }
    }

    private void finishPartition()
    {
        closeInput();
        processor.close();
        processor = null;
        outputAssembler = null;
        nextPartitionStart = partitionEnd;
        inputLength = 0;
        inputFinished = false;
    }

    @Override
    public void constrain(Mask mask)
    {
        checkOpen();
        requireNonNull(mask, "mask is null");
        if (staged != null) {
            staged.constrain(mask);
            return;
        }
        if (currentBatch == null) {
            throw new IllegalStateException("No current batch");
        }
        currentBatch.constrain(mask);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        failure = closeBatch(staged, failure);
        staged = null;
        failure = closeBatch(currentBatch, failure);
        currentBatch = null;
        try {
            closeInput();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        if (processor != null) {
            try {
                processor.close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            processor = null;
        }
        try {
            rows.close();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        try {
            source.close();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        try {
            allocator.release(allocationContext);
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void closeInput()
    {
        if (input != null) {
            input.close();
            input = null;
        }
    }

    private void checkSourceChannel(int channel, String kind)
    {
        if (channel < 0 || channel >= source.outputCount()) {
            throw new IllegalArgumentException(kind + " channel is outside source schema");
        }
    }

    private static RuntimeException closeBatch(Batch batch, RuntimeException failure)
    {
        if (batch == null) {
            return failure;
        }
        try {
            batch.close();
        }
        catch (RuntimeException closeFailure) {
            return appendFailure(failure, closeFailure);
        }
        return failure;
    }

    private static RuntimeException appendFailure(RuntimeException failure, RuntimeException closeFailure)
    {
        if (failure == null) {
            return closeFailure;
        }
        failure.addSuppressed(closeFailure);
        return failure;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("operator is closed");
        }
    }
}
