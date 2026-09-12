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
import org.weakref.nitro.data.Mask;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class SingleBatchOperator
        implements Operator
{
    private final Schema outputSchema;
    private final Supplier<Output[]> outputsSupplier;
    private Batch delegate;
    private Mask currentMask;
    private boolean emitted;
    private boolean closed;
    private boolean completeInput;

    public SingleBatchOperator(int outputCount, Mask mask, Supplier<Output[]> outputsSupplier)
    {
        this(Schema.unspecified(outputCount), mask, outputsSupplier);
    }

    public SingleBatchOperator(Schema outputSchema, Mask mask, Supplier<Output[]> outputsSupplier)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.currentMask = requireNonNull(mask, "mask is null");
        this.outputsSupplier = requireNonNull(outputsSupplier, "outputsSupplier is null");
        this.delegate = null;
        this.completeInput = true;
    }

    /**
     * Exposes one existing batch as an operator while preserving its lazy constraint protocol.
     *
     * <p>The operator owns {@code batch}. Its emitted forwarding batch does not close the delegate;
     * the delegate remains available for constrained re-borrows until this operator is closed.
     */
    public SingleBatchOperator(Schema outputSchema, Batch batch)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.delegate = null;
        this.outputsSupplier = null;
        this.completeInput = true;
        addInput(batch);
    }

    /**
     * Creates a reusable feed for host-scheduled native batches.
     */
    public SingleBatchOperator(Schema outputSchema)
    {
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.delegate = null;
        this.outputsSupplier = null;
    }

    /**
     * Offers the next native batch. Ownership transfers to this operator.
     */
    public void addInput(Batch batch)
    {
        checkOpen();
        if (outputsSupplier != null) {
            throw new IllegalStateException("operator does not accept input");
        }
        if (delegate != null) {
            throw new IllegalStateException("operator already has input");
        }
        delegate = requireNonNull(batch, "batch is null");
        currentMask = batch.borrowMask();
        emitted = false;
    }

    /**
     * Releases the consumed input and makes this operator ready for another batch.
     */
    public void finishInput()
    {
        checkOpen();
        if (delegate == null || !emitted) {
            throw new IllegalStateException("operator has no consumed input");
        }
        delegate.close();
        delegate = null;
        currentMask = null;
        emitted = false;
        completeInput = false;
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
        return !closed && !emitted && (outputsSupplier != null || delegate != null);
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        emitted = true;
        if (delegate != null) {
            return Batch.forwarding(
                    currentMask,
                    this::constrain,
                    java.util.function.Function.identity(),
                    _ -> {},
                    () -> {},
                    delegate);
        }
        return new Batch(currentMask, this::constrain, java.util.function.Function.identity(), outputsSupplier.get());
    }

    @Override
    public void constrain(Mask mask)
    {
        currentMask = mask;
        if (delegate != null) {
            delegate.constrain(mask);
        }
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        // A constructor-supplied batch is complete. Resetting it as a reusable feed opens a new input cohort.
        return completeInput;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // The single in-memory batch stays valid for the operator's lifetime, and its outputs are
        // re-supplied on demand, so a constrained re-borrow yields the requested positions.
        return true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    public Mask currentMask()
    {
        return currentMask;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("operator is closed");
        }
    }
}
