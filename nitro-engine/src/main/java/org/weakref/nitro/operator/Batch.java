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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/**
 * A batch of row selections plus lazily-resolved output streams.
 * <p>
 * A batch owns the current output mask and one {@link Output} per operator column. Closing the
 * batch releases all borrowed-but-not-taken streams and, unless the mask has been taken, releases
 * the mask as well.
 */
public final class Batch
        implements AutoCloseable
{
    private Mask mask;
    private final Mask ownedMask;
    private final Output[] outputs;
    private final Batch outputDelegate;
    private final Function<Mask, Mask> maskTakeResolver;
    private final Consumer<Mask> maskReleaseResolver;
    private final Consumer<Mask> constrainer;
    private final Runnable closeAction;
    private final Lifecycle lifecycle;
    private final BatchBufferScope bufferScope;
    private final AsyncOwnershipTransfer asyncOwnershipTransfer;
    private boolean maskTaken;
    private boolean closed;

    public Batch(Mask mask, Output... outputs)
    {
        this(mask, _ -> {}, Function.identity(), _ -> {}, () -> {}, outputs);
    }

    public Batch(Mask mask, Function<Mask, Mask> maskTakeResolver, Output... outputs)
    {
        this(mask, _ -> {}, maskTakeResolver, _ -> {}, () -> {}, outputs);
    }

    public Batch(Mask mask, Consumer<Mask> constrainer, Function<Mask, Mask> maskTakeResolver, Output... outputs)
    {
        this(mask, constrainer, maskTakeResolver, _ -> {}, () -> {}, outputs);
    }

    public Batch(Mask mask, Consumer<Mask> constrainer, Function<Mask, Mask> maskTakeResolver, Consumer<Mask> maskReleaseResolver, Runnable closeAction, Output... outputs)
    {
        this(mask, constrainer, maskTakeResolver, maskReleaseResolver, closeAction, requireNonNull(outputs, "outputs is null"), null);
    }

    private Batch(
            Mask mask,
            Consumer<Mask> constrainer,
            Function<Mask, Mask> maskTakeResolver,
            Consumer<Mask> maskReleaseResolver,
            Runnable closeAction,
            Output[] outputs,
            Batch outputDelegate)
    {
        this(mask, constrainer, maskTakeResolver, maskReleaseResolver, closeAction, outputs, outputDelegate, null);
    }

    private Batch(
            Mask mask,
            Consumer<Mask> constrainer,
            Function<Mask, Mask> maskTakeResolver,
            Consumer<Mask> maskReleaseResolver,
            Runnable closeAction,
            Output[] outputs,
            Batch outputDelegate,
            Lifecycle lifecycle)
    {
        this(mask, constrainer, maskTakeResolver, maskReleaseResolver, closeAction, outputs, outputDelegate, lifecycle, null, null);
    }

    private Batch(
            Mask mask,
            Consumer<Mask> constrainer,
            Function<Mask, Mask> maskTakeResolver,
            Consumer<Mask> maskReleaseResolver,
            Runnable closeAction,
            Output[] outputs,
            Batch outputDelegate,
            Lifecycle lifecycle,
            BatchBufferScope bufferScope,
            AsyncOwnershipTransfer asyncOwnershipTransfer)
    {
        this.mask = requireNonNull(mask, "mask is null");
        this.ownedMask = mask;
        this.lifecycle = lifecycle;
        this.bufferScope = bufferScope;
        this.asyncOwnershipTransfer = asyncOwnershipTransfer;
        this.constrainer = lifecycle == null ? requireNonNull(constrainer, "constrainer is null") : null;
        this.maskTakeResolver = lifecycle == null && bufferScope == null ? requireNonNull(maskTakeResolver, "maskTakeResolver is null") : null;
        this.maskReleaseResolver = lifecycle == null && bufferScope == null ? requireNonNull(maskReleaseResolver, "maskReleaseResolver is null") : null;
        this.closeAction = lifecycle == null ? requireNonNull(closeAction, "closeAction is null") : null;
        // Batch takes ownership of the freshly-created output array. Every production caller builds this array solely
        // for the batch; cloning it here doubled the per-layer control-plane allocation for no lifetime benefit.
        this.outputs = outputs;
        this.outputDelegate = outputDelegate;
    }

    static Batch owned(Mask mask, Consumer<Mask> constrainer, Runnable closeAction, BatchBufferScope bufferScope, Output[] outputs)
    {
        return new Batch(
                mask,
                constrainer,
                null,
                null,
                closeAction,
                requireNonNull(outputs, "outputs is null"),
                null,
                null,
                requireNonNull(bufferScope, "bufferScope is null"),
                null);
    }

    static Batch retained(Mask mask, Runnable closeAction, AsyncOwnershipTransfer asyncOwnershipTransfer, Output[] outputs)
    {
        return new Batch(
                mask,
                _ -> {},
                Function.identity(),
                _ -> {},
                closeAction,
                requireNonNull(outputs, "outputs is null"),
                null,
                null,
                null,
                requireNonNull(asyncOwnershipTransfer, "asyncOwnershipTransfer is null"));
    }

    /**
     * Creates a batch with independent mask ownership whose output streams are forwarded directly from an existing
     * batch. The supplied close action remains responsible for closing that source batch. This avoids constructing
     * one forwarding {@link Output} and several capturing callbacks per column for pass-through operators.
     */
    public static Batch forwarding(
            Mask mask,
            Consumer<Mask> constrainer,
            Function<Mask, Mask> maskTakeResolver,
            Consumer<Mask> maskReleaseResolver,
            Runnable closeAction,
            Batch outputDelegate)
    {
        return new Batch(
                mask,
                constrainer,
                maskTakeResolver,
                maskReleaseResolver,
                closeAction,
                null,
                requireNonNull(outputDelegate, "outputDelegate is null"));
    }

    /**
     * Creates a forwarding batch whose batch-specific ownership operations share one lifecycle object. This is useful
     * for hot pass-through operators: it preserves a distinct closed generation for every public Batch while avoiding
     * a graph of capturing callbacks for that generation.
     */
    static Batch forwarding(Mask mask, Lifecycle lifecycle, Batch outputDelegate)
    {
        return new Batch(
                mask,
                null,
                null,
                null,
                null,
                null,
                requireNonNull(outputDelegate, "outputDelegate is null"),
                requireNonNull(lifecycle, "lifecycle is null"));
    }

    private Batch(Batch source)
    {
        mask = source.mask;
        ownedMask = source.ownedMask;
        outputs = source.outputs;
        outputDelegate = source.outputDelegate;
        maskTakeResolver = source.maskTakeResolver;
        maskReleaseResolver = source.maskReleaseResolver;
        constrainer = source.constrainer;
        closeAction = source.closeAction;
        lifecycle = source.lifecycle;
        bufferScope = source.bufferScope;
        asyncOwnershipTransfer = source.asyncOwnershipTransfer;
        maskTaken = source.maskTaken;
    }

    private Batch(Batch source, Output[] outputs)
    {
        mask = source.mask;
        ownedMask = source.ownedMask;
        this.outputs = requireNonNull(outputs, "outputs is null");
        outputDelegate = source.outputDelegate;
        maskTakeResolver = source.maskTakeResolver;
        maskReleaseResolver = source.maskReleaseResolver;
        constrainer = source.constrainer;
        closeAction = source.closeAction;
        lifecycle = source.lifecycle;
        bufferScope = source.bufferScope;
        asyncOwnershipTransfer = source.asyncOwnershipTransfer;
        maskTaken = source.maskTaken;
    }

    /**
     * Moves this batch's complete ownership contract to a new facade without resolving or copying any streams.
     * The old facade is closed immediately, so a producer may safely close it after publishing while the returned
     * facade remains responsible for the mask, outputs, buffer scope, and close action.
     */
    public Batch transferOwnership()
    {
        checkOpen();
        Batch transferred = new Batch(this);
        closed = true;
        return transferred;
    }

    /**
     * Moves this batch's ownership contract to a new facade with one additional output. Existing outputs are neither
     * resolved nor copied; only the small output-reference array is extended. The old facade is closed immediately.
     */
    public Batch appendOutput(Output output)
    {
        checkOpen();
        requireNonNull(output, "output is null");
        int existingLocalOutputs = outputs == null ? 0 : outputs.length;
        Output[] appendedOutputs = new Output[existingLocalOutputs + 1];
        if (existingLocalOutputs > 0) {
            System.arraycopy(outputs, 0, appendedOutputs, 0, existingLocalOutputs);
        }
        appendedOutputs[existingLocalOutputs] = output;
        Batch appended = new Batch(this, appendedOutputs);
        closed = true;
        return appended;
    }

    /**
     * Promotes vector ownership held exclusively by this batch for release across an asynchronous boundary.
     * Ordinary batches and retained batches whose vector trees are shared return empty without changing ownership.
     */
    public Optional<Allocator.AsyncVectorTreeLease> tryDetachRetainedVectorsForAsyncRelease(Allocator allocator)
    {
        checkOpen();
        requireNonNull(allocator, "allocator is null");
        if (asyncOwnershipTransfer == null) {
            return Optional.empty();
        }
        return asyncOwnershipTransfer.tryDetach(allocator);
    }

    public Mask borrowMask()
    {
        checkOpen();
        if (maskTaken) {
            throw new IllegalStateException("Mask already taken");
        }
        return mask;
    }

    public Mask takeMask()
    {
        checkOpen();
        Mask borrowedMask = borrowMask();
        maskTaken = true;
        Mask taken = lifecycle != null
                ? lifecycle.takeMask(borrowedMask)
                : bufferScope != null && borrowedMask == ownedMask
                        ? bufferScope.take(borrowedMask)
                        : bufferScope != null ? borrowedMask : maskTakeResolver.apply(borrowedMask);
        return requireNonNull(taken, "maskTakeResolver returned null");
    }

    public void constrain(Mask mask)
    {
        checkOpen();
        if (maskTaken) {
            throw new IllegalStateException("Mask already taken");
        }
        validateOutputsCanBeInvalidated();
        invalidateOutputsForConstraint();
        this.mask = requireNonNull(mask, "mask is null");
        if (lifecycle == null) {
            constrainer.accept(mask);
        }
        else {
            lifecycle.constrain(mask);
        }
    }

    private void validateOutputsCanBeInvalidated()
    {
        if (outputDelegate != null) {
            outputDelegate.validateOutputsCanBeInvalidated();
        }
        if (outputs == null) {
            return;
        }
        for (Output output : outputs) {
            if (output.hasConstraintSensitiveTakenStreams()) {
                throw new IllegalStateException("Cannot constrain batch after an output stream was taken");
            }
        }
    }

    private void invalidateOutputsForConstraint()
    {
        if (outputDelegate != null) {
            outputDelegate.invalidateOutputsForConstraint();
        }
        if (outputs == null) {
            return;
        }
        for (Output output : outputs) {
            output.invalidateResolvedForConstraint();
        }
    }

    public Output output(int outputIndex)
    {
        checkOpen();
        outputIndex = checkIndex(outputIndex, outputCount());
        if (outputDelegate != null) {
            int delegateOutputCount = outputDelegate.outputCount();
            if (outputIndex < delegateOutputCount) {
                return outputDelegate.output(outputIndex);
            }
            outputIndex -= delegateOutputCount;
        }
        return outputs[outputIndex];
    }

    public int outputCount()
    {
        checkOpen();
        return (outputDelegate == null ? 0 : outputDelegate.outputCount()) + (outputs == null ? 0 : outputs.length);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (outputs != null) {
            for (Output output : outputs) {
                output.close();
            }
        }
        if (bufferScope != null) {
            if (!maskTaken || mask != ownedMask) {
                bufferScope.release(ownedMask);
            }
        }
        else if (!maskTaken) {
            if (lifecycle == null) {
                maskReleaseResolver.accept(mask);
            }
            else {
                lifecycle.releaseMask(mask);
            }
        }
        if (bufferScope != null) {
            try {
                closeAction.run();
            }
            finally {
                bufferScope.endBatch();
            }
        }
        else if (lifecycle == null) {
            closeAction.run();
        }
        else {
            lifecycle.close();
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Batch already closed");
        }
    }

    interface Lifecycle
    {
        void constrain(Mask mask);

        Mask takeMask(Mask mask);

        void releaseMask(Mask mask);

        void close();
    }

    interface AsyncOwnershipTransfer
    {
        Optional<Allocator.AsyncVectorTreeLease> tryDetach(Allocator allocator);
    }
}
