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
import java.util.OptionalInt;
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
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("PartitionedTableFunctionOperator");
    private final ExecutionContext executionContext;
    private final Operator source;
    private final List<TableFunctionArgumentLayout> arguments;
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
    private SourceBatch[] inputs;
    private RowPositionIndex[] partitionRows;
    private Batch staged;
    private Batch currentBatch;
    private int nextPartitionStart;
    private int partitionStart;
    private int partitionEnd;
    private int[] inputPositions;
    private int[] inputLengths;
    private boolean[] inputFinished;
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
        this(
                allocator,
                executionContext,
                source,
                List.of(new TableFunctionArgumentLayout(inputSchema, inputChannels, OptionalInt.empty())),
                partitionChannels,
                inputOrder,
                orderingColumnCount,
                outputSchema,
                properOutputCount,
                passThroughColumns,
                processorFactory,
                processEmptyInput,
                maxInputBatchRows,
                resources);
    }

    public PartitionedTableFunctionOperator(
            Allocator allocator,
            ExecutionContext executionContext,
            Operator source,
            List<TableFunctionArgumentLayout> arguments,
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
        this.arguments = List.copyOf(arguments);
        if (this.arguments.isEmpty()) {
            throw new IllegalArgumentException("table function has no arguments");
        }
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
        if (this.passThroughColumns.stream().anyMatch(column -> column.argument() >= this.arguments.size())) {
            throw new IllegalArgumentException("pass-through argument is outside argument layouts");
        }
        this.processorFactory = requireNonNull(processorFactory, "processorFactory is null");
        this.outputDemand = new TableFunctionOutputDemand(
                Operator.fullOutputDemand(properOutputCount),
                this.passThroughColumns.stream().map(TableFunctionPassThroughColumn::argument).collect(java.util.stream.Collectors.toUnmodifiableSet()));
        this.processEmptyInput = processEmptyInput;
        if (maxInputBatchRows <= 0) {
            throw new IllegalArgumentException("maxInputBatchRows is not positive");
        }
        this.maxInputBatchRows = maxInputBatchRows;

        for (TableFunctionArgumentLayout argument : this.arguments) {
            int[] inputChannels = argument.inputChannelsInternal();
            for (int input = 0; input < inputChannels.length; input++) {
                int sourceChannel = inputChannels[input];
                checkSourceChannel(sourceChannel, "input");
                if (!argument.schema().field(input).type().identity()
                        .equals(source.outputSchema().field(sourceChannel).type().identity())) {
                    throw new IllegalArgumentException("input channel type does not match input schema");
                }
            }
            argument.markerChannel().ifPresent(channel -> checkSourceChannel(channel, "marker"));
        }
        for (int channel : this.partitionChannels) {
            checkSourceChannel(channel, "partition");
        }
        for (int index = 0; index < this.passThroughColumns.size(); index++) {
            TableFunctionPassThroughColumn passThrough = this.passThroughColumns.get(index);
            int inputColumn = passThrough.inputColumn();
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
            prepareInputs();
            List<TableFunctionArgument> functionArguments = new java.util.ArrayList<>(arguments.size());
            for (SourceBatch input : inputs) {
                functionArguments.add(input == null
                        ? TableFunctionArgument.Finished.FINISHED
                        : new TableFunctionArgument.Rows(input));
            }
            TableFunctionProgress progress = requireNonNull(
                    processor.process(
                            new TableFunctionInput(functionArguments),
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
        partitionRows = new RowPositionIndex[arguments.size()];
        inputs = new SourceBatch[arguments.size()];
        inputPositions = new int[arguments.size()];
        inputLengths = new int[arguments.size()];
        inputFinished = new boolean[arguments.size()];
        List<TableFunctionOutputAssembler.Argument> outputArguments = new java.util.ArrayList<>(arguments.size());
        int[] partitionStarts = new int[arguments.size()];
        int[] partitionEnds = new int[arguments.size()];
        for (int argument = 0; argument < arguments.size(); argument++) {
            TableFunctionArgumentLayout layout = arguments.get(argument);
            int end = argumentEnd(layout.markerChannel());
            partitionRows[argument] = new SliceRowPositionIndex(rows, partitionStart, end);
            outputArguments.add(new TableFunctionOutputAssembler.Argument(source.outputSchema(), partitionRows[argument]));
            partitionEnds[argument] = partitionRows[argument].size();
        }
        processor = requireNonNull(processorFactory.create(), "processorFactory returned null");
        outputAssembler = new TableFunctionOutputAssembler(
                allocator,
                outputSchema,
                properOutputCount,
                passThroughColumns,
                outputArguments,
                partitionStarts,
                partitionEnds);
        return true;
    }

    private int argumentEnd(OptionalInt markerChannel)
    {
        if (markerChannel.isEmpty() || partitionStart == partitionEnd) {
            return partitionEnd;
        }
        int channel = markerChannel.orElseThrow();
        int end = partitionStart;
        while (end < partitionEnd && !rows.isNull(channel, end)) {
            end++;
        }
        for (int position = end; position < partitionEnd; position++) {
            if (!rows.isNull(channel, position)) {
                throw new IllegalStateException("table-function marker data is not a partition prefix");
            }
        }
        return end;
    }

    private void prepareInputs()
    {
        for (int argument = 0; argument < arguments.size(); argument++) {
            if (inputs[argument] != null || inputFinished[argument]) {
                continue;
            }
            RowPositionIndex argumentRows = partitionRows[argument];
            if (inputPositions[argument] == argumentRows.size()) {
                inputFinished[argument] = true;
                continue;
            }
            inputLengths[argument] = min(maxInputBatchRows, argumentRows.size() - inputPositions[argument]);
            TableFunctionArgumentLayout layout = arguments.get(argument);
            Batch batch = argumentRows.copyRange(allocator, inputPositions[argument], inputLengths[argument], layout.inputChannelsInternal());
            inputs[argument] = OperatorBatchSource.batch(layout.schema(), batch);
            inputs[argument].selection();
        }
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
        for (int argument : consumed) {
            if (argument < 0 || argument >= arguments.size()) {
                throw new IllegalStateException("table function consumed an unexpected argument");
            }
            if (inputs[argument] == null) {
                throw new IllegalStateException("table function consumed a finished argument");
            }
        }
    }

    private void consumeInput(Set<Integer> consumed)
    {
        for (int argument : consumed) {
            inputs[argument].close();
            inputs[argument] = null;
            inputPositions[argument] += inputLengths[argument];
            inputLengths[argument] = 0;
        }
    }

    private void finishPartition()
    {
        closeInputs();
        processor.close();
        processor = null;
        outputAssembler = null;
        nextPartitionStart = partitionEnd;
        inputs = null;
        partitionRows = null;
        inputPositions = null;
        inputLengths = null;
        inputFinished = null;
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
            closeInputs();
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

    private void closeInputs()
    {
        if (inputs == null) {
            return;
        }
        RuntimeException failure = null;
        for (int argument = 0; argument < inputs.length; argument++) {
            if (inputs[argument] == null) {
                continue;
            }
            try {
                inputs[argument].close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            inputs[argument] = null;
        }
        if (failure != null) {
            throw failure;
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
