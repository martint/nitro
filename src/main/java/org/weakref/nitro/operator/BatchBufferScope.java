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
import org.weakref.nitro.data.BatchBufferOwner;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorBatchScope;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Operator-batch facade over a reusable allocator-owned vector generation.
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
    private final VectorBatchScope buffers;

    public BatchBufferScope(Allocator allocator, String name)
    {
        this.buffers = new VectorBatchScope(allocator, name);
    }

    /// Creates an isolated ownership scope backed by a pool shared with compatible scopes using the same key.
    public BatchBufferScope(Allocator allocator, String name, Object poolGroup)
    {
        this.buffers = new VectorBatchScope(allocator, name, poolGroup);
    }

    public BatchBufferScope(Allocator allocator, Allocator.Context context)
    {
        this.buffers = new VectorBatchScope(allocator, context);
    }

    public Allocator.Context context()
    {
        return buffers.context();
    }

    public Batch batch(Mask mask, Output... outputs)
    {
        return batch(mask, _ -> {}, () -> {}, outputs);
    }

    public Batch batch(Mask mask, Consumer<Mask> constrainer, Runnable closeAction, Output... outputs)
    {
        buffers.begin(null);
        return Batch.owned(
                requireNonNull(mask, "mask is null"),
                requireNonNull(constrainer, "constrainer is null"),
                requireNonNull(closeAction, "closeAction is null"),
                this,
                requireNonNull(outputs, "outputs is null"));
    }

    public void begin(BatchBufferOwner additionalOwner)
    {
        buffers.begin(additionalOwner);
    }

    @Override
    public Vector take(Vector vector)
    {
        return buffers.take(vector);
    }

    @Override
    public void release(Vector vector)
    {
        buffers.release(vector);
    }

    Mask take(Mask mask)
    {
        return buffers.take(mask);
    }

    void release(Mask mask)
    {
        buffers.release(mask);
    }

    public void endBatch()
    {
        buffers.endBatch();
    }

    @Override
    public void releaseAll()
    {
        buffers.releaseAll();
    }

    @Override
    public void close()
    {
        buffers.close();
    }
}
