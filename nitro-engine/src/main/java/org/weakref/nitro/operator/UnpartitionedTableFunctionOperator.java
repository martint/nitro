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

import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.function.table.TableFunctionArgument;
import org.weakref.nitro.core.function.table.TableFunctionInput;
import org.weakref.nitro.core.function.table.TableFunctionOutputBatch;
import org.weakref.nitro.core.function.table.TableFunctionOutputDemand;
import org.weakref.nitro.core.function.table.TableFunctionProcessor;
import org.weakref.nitro.core.function.table.TableFunctionProgress;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.source.SourceBatchOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;

import java.util.List;
import java.util.Set;

import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/// Pull operator for a single, unpartitioned table argument without pass-through output.
///
/// Partitioned or ordered input, multiple arguments, marker rows, and pass-through gathering are rejected by this
/// execution shape. Input and output remain format-neutral source batches, while the adjacent operator is exposed
/// through a zero-copy native batch-source facade.
public final class UnpartitionedTableFunctionOperator
        implements Operator
{
    private static final Set<Integer> ARGUMENT = Set.of(0);

    private final Schema outputSchema;
    private final Schema inputSchema;
    private final int[] inputChannels;
    private final TableFunctionProcessor processor;
    private final TableFunctionOutputDemand outputDemand;
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("UnpartitionedTableFunctionOperator");
    private final BatchSource source;
    private final SourceBatchOperatorIngress outputIngress;
    private final ExecutionContext executionContext;

    private SourceBatch input;
    private Batch staged;
    private Batch currentBatch;
    private boolean inputFinished;
    private boolean finished;
    private boolean closed;

    public UnpartitionedTableFunctionOperator(
            Schema outputSchema,
            TableFunctionProcessor processor,
            Operator source,
            Schema inputSchema,
            int[] inputChannels,
            Allocator allocator,
            SourceBatchOperatorIngress outputIngress,
            ExecutionContext executionContext)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.processor = requireNonNull(processor, "processor is null");
        this.outputDemand = new TableFunctionOutputDemand(Operator.fullOutputDemand(outputSchema.size()), Set.of());
        source = requireNonNull(source, "source is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null").clone();
        if (inputSchema.size() != inputChannels.length) {
            throw new IllegalArgumentException("input schema does not match input channels");
        }
        for (int input = 0; input < inputChannels.length; input++) {
            int sourceChannel = inputChannels[input];
            if (sourceChannel < 0 || sourceChannel >= source.outputCount()) {
                throw new IllegalArgumentException("input channel is outside source schema");
            }
            if (!inputSchema.field(input).type().identity().equals(source.outputSchema().field(sourceChannel).type().identity())) {
                throw new IllegalArgumentException("input channel type does not match input schema");
            }
        }
        this.source = new OperatorBatchSource(source);
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.outputIngress = requireNonNull(outputIngress, "outputIngress is null");
        this.executionContext = requireNonNull(executionContext, "executionContext is null");
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

        while (true) {
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
                    validateOutput(produced.output());
                    try {
                        staged = outputIngress.adapt(produced.output());
                    }
                    catch (RuntimeException | Error failure) {
                        produced.output().close();
                        throw failure;
                    }
                    consumeInput(produced.consumedArguments());
                    executionContext.checkpoint();
                    return;
                }
                case TableFunctionProgress.Consumed consumed -> {
                    validateConsumed(consumed.arguments());
                    consumeInput(consumed.arguments());
                }
                case TableFunctionProgress.Blocked blocked -> executionContext.await(blocked.continuation());
                case TableFunctionProgress.Finished _ -> {
                    finished = true;
                    closeInput();
                    return;
                }
            }
        }
    }

    private void prepareInput()
    {
        if (input != null || inputFinished) {
            return;
        }
        while (true) {
            switch (source.poll()) {
                case SourcePoll.Ready ready -> {
                    input = new ProjectedSourceBatch(inputSchema, inputChannels, ready.batch());
                    // An operator-backed SourceBatch defers source.next() until its generation is inspected. A
                    // processor is allowed to consume an argument without borrowing a column, so establish the
                    // generation here; closing a consumed input must always advance and release one source batch.
                    input.selection();
                    executionContext.checkpoint();
                    return;
                }
                case SourcePoll.Blocked blocked -> executionContext.await(blocked.continuation());
                case SourcePoll.Finished _ -> {
                    inputFinished = true;
                    return;
                }
            }
        }
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
        }
    }

    private void validateOutput(TableFunctionOutputBatch output)
    {
        requireNonNull(output, "output is null");
        try {
            if (output.properOutputCount() != outputCount()) {
                throw new IllegalStateException("table function returned an unexpected proper output count");
            }
            if (!output.passThroughReferences().isEmpty()) {
                throw new IllegalStateException("unpartitioned table function returned a pass-through reference");
            }
            if (!outputSchema.isLayoutCompatibleWith(output.schema())) {
                throw new IllegalStateException("table function returned an incompatible schema");
            }
            if (output.selection().count() == 0) {
                throw new IllegalStateException("table function returned an empty batch");
            }
        }
        catch (RuntimeException | Error failure) {
            output.close();
            throw failure;
        }
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
        if (staged != null) {
            try {
                staged.close();
            }
            catch (RuntimeException closeFailure) {
                failure = closeFailure;
            }
            staged = null;
        }
        if (currentBatch != null) {
            try {
                currentBatch.close();
            }
            catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
            currentBatch = null;
        }
        try {
            closeInput();
        }
        catch (RuntimeException closeFailure) {
            failure = appendFailure(failure, closeFailure);
        }
        try {
            processor.close();
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

    private static final class ProjectedSourceBatch
            implements SourceBatch
    {
        private final Schema schema;
        private final int[] channels;
        private final SourceBatch delegate;

        private ProjectedSourceBatch(Schema schema, int[] channels, SourceBatch delegate)
        {
            this.schema = requireNonNull(schema, "schema is null");
            this.channels = requireNonNull(channels, "channels is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Schema schema()
        {
            return schema;
        }

        @Override
        public Selection selection()
        {
            return delegate.selection();
        }

        @Override
        public ColumnView column(int index)
        {
            index = checkIndex(index, channels.length);
            ColumnView column = delegate.column(channels[index]);
            if (!schema.field(index).type().identity().equals(column.type().identity())) {
                throw new IllegalArgumentException("source column type does not match projected schema");
            }
            return column;
        }

        @Override
        public void select(Selection selection)
        {
            delegate.select(selection);
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
