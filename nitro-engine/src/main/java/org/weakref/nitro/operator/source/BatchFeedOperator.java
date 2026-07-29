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
package org.weakref.nitro.operator.source;

import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Reusable operator ingress for batches scheduled by an external host.
///
/// The host offers one source batch, drains the operator pipeline rooted at this feed, closes every
/// resulting native batch, and then calls [#finishInput()] before offering the next source batch.
/// The feed and the downstream pipeline remain alive across inputs, preserving compiled code and
/// instance-owned allocation state without making host scheduling part of the pull-operator
/// protocol.
public final class BatchFeedOperator
        implements Operator
{
    private final Schema schema;
    private final SourceOperatorIngress ingress;
    private final Set<SourceCapability> capabilities;

    private SourceBatch sourceBatch;
    private Batch currentBatch;
    private boolean emitted;
    private boolean closed;

    public BatchFeedOperator(Schema schema, SourceOperatorIngress ingress)
    {
        this(schema, ingress, Set.of());
    }

    public BatchFeedOperator(Schema schema, SourceOperatorIngress ingress, Set<SourceCapability> capabilities)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.ingress = requireNonNull(ingress, "ingress is null");
        this.capabilities = Set.copyOf(requireNonNull(capabilities, "capabilities is null"));
    }

    public BatchFeedOperator(SourceBatch sourceBatch, SourceOperatorIngress ingress)
    {
        this(sourceBatch, ingress, Set.of());
    }

    public BatchFeedOperator(SourceBatch sourceBatch, SourceOperatorIngress ingress, Set<SourceCapability> capabilities)
    {
        this(requireNonNull(sourceBatch, "sourceBatch is null").schema(), ingress, capabilities);
        addInput(sourceBatch);
    }

    /// Offers the next host-owned source batch.
    ///
    /// Ownership transfers to this feed on success. The schema must match the schema supplied at
    /// construction.
    public void addInput(SourceBatch sourceBatch)
    {
        checkOpen();
        if (this.sourceBatch != null) {
            throw new IllegalStateException("operator already has input");
        }
        sourceBatch = requireNonNull(sourceBatch, "sourceBatch is null");
        if (!compatibleSchema(sourceBatch.schema())) {
            throw new IllegalArgumentException("source batch schema does not match feed schema");
        }
        this.sourceBatch = sourceBatch;
        emitted = false;
    }

    /// Completes the current host input after its adapted batch has been closed.
    public void finishInput()
    {
        checkOpen();
        if (sourceBatch == null || !emitted) {
            throw new IllegalStateException("operator has no consumed input");
        }
        sourceBatch = null;
        currentBatch = null;
        emitted = false;
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
        return !closed && sourceBatch != null && !emitted;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        emitted = true;
        try {
            currentBatch = ingress.adapt(sourceBatch);
            return currentBatch;
        }
        catch (RuntimeException | Error failure) {
            sourceBatch.close();
            sourceBatch = null;
            currentBatch = null;
            throw failure;
        }
    }

    @Override
    public void constrain(Mask mask)
    {
        requireNonNull(mask, "mask is null");
        checkOpen();
        if (currentBatch != null) {
            currentBatch.constrain(mask);
            return;
        }
        if (sourceBatch == null) {
            throw new IllegalStateException("operator has no input");
        }
        sourceBatch.select(ingress.selection(mask));
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        return capabilities.contains(SourceCapability.STABLE_BATCH_BORROW);
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return capabilities.contains(SourceCapability.CONSTRAINED_REBORROW);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (!emitted && sourceBatch != null) {
            sourceBatch.close();
        }
        sourceBatch = null;
        currentBatch = null;
    }

    private boolean compatibleSchema(Schema inputSchema)
    {
        if (inputSchema.size() != schema.size()) {
            return false;
        }
        for (int column = 0; column < schema.size(); column++) {
            if (inputSchema.field(column).nullable() != schema.field(column).nullable() ||
                    !inputSchema.field(column).type().identity().equals(schema.field(column).type().identity())) {
                return false;
            }
        }
        return true;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("operator is closed");
        }
    }
}
