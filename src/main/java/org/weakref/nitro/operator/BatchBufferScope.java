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
import org.weakref.nitro.data.Vector;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Reusable ownership scope for one operator's batch buffers.
 * <p>
 * Allocate generation-local vectors and masks in {@link #context()}, then create the public batch with
 * {@link #batch}. Closing the batch returns every untaken allocation, including lazily-created buffers that were
 * never exposed through an {@link Output}. A taken encoded vector transfers only children owned by this scope;
 * borrowed children retain their upstream ownership.
 * <p>
 * Only one batch may be live. Public batch/output facades remain generation-specific, so stale references stay
 * closed after their backing buffers are reused. Long-lived operator state must use a different context.
 */
public final class BatchBufferScope
        implements AutoCloseable, BatchBufferOwner
{
    private final Allocator allocator;
    private final Allocator.Context context;
    private BatchBufferOwner additionalOwner;
    private boolean active;

    public BatchBufferScope(Allocator allocator, String name)
    {
        this(allocator, new Allocator.Context(requireNonNull(name, "name is null")));
    }

    /** Creates an isolated ownership scope backed by a pool shared with compatible scopes using the same key. */
    public BatchBufferScope(Allocator allocator, String name, Object poolGroup)
    {
        this(allocator, new Allocator.Context(requireNonNull(name, "name is null"), requireNonNull(poolGroup, "poolGroup is null")));
    }

    public BatchBufferScope(Allocator allocator, Allocator.Context context)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.context = requireNonNull(context, "context is null");
    }

    public Allocator.Context context()
    {
        return context;
    }

    public Batch batch(Mask mask, Output... outputs)
    {
        return batch(mask, _ -> {}, () -> {}, outputs);
    }

    public Batch batch(Mask mask, Consumer<Mask> constrainer, Runnable closeAction, Output... outputs)
    {
        if (active) {
            throw new IllegalStateException("Previous batch is still open");
        }
        begin(null);
        return Batch.owned(mask, constrainer, closeAction, this, outputs);
    }

    public void begin(BatchBufferOwner additionalOwner)
    {
        if (active) {
            throw new IllegalStateException("Previous batch is still open");
        }
        this.additionalOwner = additionalOwner;
        active = true;
    }

    @Override
    public Vector take(Vector vector)
    {
        checkActive();
        Vector result = allocator.transferOwned(context, requireNonNull(vector, "vector is null"));
        if (additionalOwner != null) {
            additionalOwner.take(vector);
        }
        return result;
    }

    @Override
    public void release(Vector vector)
    {
        checkActive();
        allocator.release(context, requireNonNull(vector, "vector is null"));
        if (additionalOwner != null) {
            additionalOwner.release(vector);
        }
    }

    Mask take(Mask mask)
    {
        checkActive();
        return allocator.transfer(context, requireNonNull(mask, "mask is null"));
    }

    void release(Mask mask)
    {
        checkActive();
        allocator.release(context, requireNonNull(mask, "mask is null"));
    }

    public void endBatch()
    {
        checkActive();
        try {
            allocator.release(context);
            if (additionalOwner != null) {
                additionalOwner.releaseAll();
            }
        }
        finally {
            additionalOwner = null;
            active = false;
        }
    }

    @Override
    public void releaseAll()
    {
        endBatch();
    }

    private void checkActive()
    {
        if (!active) {
            throw new IllegalStateException("No batch is active");
        }
    }

    @Override
    public void close()
    {
        if (active) {
            endBatch();
        }
        else {
            allocator.releaseIfPresent(context);
        }
    }
}
