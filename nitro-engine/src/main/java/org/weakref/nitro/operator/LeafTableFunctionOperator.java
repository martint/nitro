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

import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.function.table.TableFunctionInput;
import org.weakref.nitro.core.function.table.TableFunctionOutputBatch;
import org.weakref.nitro.core.function.table.TableFunctionOutputDemand;
import org.weakref.nitro.core.function.table.TableFunctionProcessor;
import org.weakref.nitro.core.function.table.TableFunctionProgress;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.source.SourceBatchOperatorIngress;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Pull operator for a native table function without table arguments.
///
/// Data-input table functions require partition storage and lazy pass-through gathering and are intentionally not
/// represented by this operator. Keeping the leaf form separate makes it impossible to admit such a function while
/// silently omitting those semantics.
public final class LeafTableFunctionOperator
        implements Operator
{
    private static final TableFunctionInput EMPTY_INPUT = new TableFunctionInput(List.of());

    private final Schema outputSchema;
    private final TableFunctionProcessor processor;
    private final TableFunctionOutputDemand outputDemand;
    private final Allocator allocator;
    private final Allocator.Context allocationContext = new Allocator.Context("LeafTableFunctionOperator");
    private final SourceBatchOperatorIngress ingress;
    private final ExecutionContext executionContext;

    private TableFunctionOutputBatch staged;
    private Batch currentBatch;
    private boolean finished;
    private boolean closed;

    public LeafTableFunctionOperator(
            Schema outputSchema,
            TableFunctionProcessor processor,
            Allocator allocator,
            SourceBatchOperatorIngress ingress,
            ExecutionContext executionContext)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.processor = requireNonNull(processor, "processor is null");
        this.outputDemand = new TableFunctionOutputDemand(Operator.fullOutputDemand(outputSchema.size()), Set.of());
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.ingress = requireNonNull(ingress, "ingress is null");
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
        if (staged != null) {
            return true;
        }
        if (finished) {
            return false;
        }

        while (true) {
            TableFunctionProgress progress = requireNonNull(
                    processor.process(EMPTY_INPUT, outputDemand, allocator, allocationContext, executionContext),
                    "table function processor returned null");
            switch (progress) {
                case TableFunctionProgress.Produced produced -> {
                    if (!produced.consumedArguments().isEmpty()) {
                        produced.output().close();
                        throw new IllegalStateException("leaf table function consumed a nonexistent argument");
                    }
                    validateOutput(produced.output());
                    staged = produced.output();
                    executionContext.checkpoint();
                    return true;
                }
                case TableFunctionProgress.Blocked blocked -> executionContext.await(blocked.continuation());
                case TableFunctionProgress.Consumed _ ->
                        throw new IllegalStateException("leaf table function consumed a nonexistent argument");
                case TableFunctionProgress.Finished _ -> {
                    finished = true;
                    return false;
                }
            }
        }
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        TableFunctionOutputBatch output = staged;
        staged = null;
        try {
            currentBatch = ingress.adapt(output);
            return currentBatch;
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
            staged.select(ingress.selection(mask));
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
        if (staged != null) {
            staged.close();
            staged = null;
        }
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
        try {
            processor.close();
        }
        finally {
            allocator.release(allocationContext);
        }
    }

    private void validateOutput(TableFunctionOutputBatch output)
    {
        requireNonNull(output, "output is null");
        try {
            if (output.properOutputCount() != outputCount()) {
                throw new IllegalStateException("leaf table function returned an unexpected proper output count");
            }
            if (!output.passThroughReferences().isEmpty()) {
                throw new IllegalStateException("leaf table function returned a pass-through reference");
            }
            if (!outputSchema.isLayoutCompatibleWith(output.schema())) {
                throw new IllegalStateException("leaf table function returned an incompatible schema");
            }
            if (output.selection().count() == 0) {
                throw new IllegalStateException("leaf table function returned an empty batch");
            }
        }
        catch (RuntimeException | Error failure) {
            output.close();
            throw failure;
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("operator is closed");
        }
    }
}
