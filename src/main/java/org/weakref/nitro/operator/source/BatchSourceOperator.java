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
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;

import static java.util.Objects.requireNonNull;

/// Pull-operator compatibility adapter for a native [BatchSource].
///
/// This first adapter intentionally accepts only the typed native-batch capability. A connector or
/// host adapter must translate its read-only column views at island ingress rather than leak its
/// objects into the native operator pipeline.
public final class BatchSourceOperator
        implements Operator
{
    private final BatchSource source;
    private SourceBatch staged;
    private NativeBatchAccess stagedNativeBatch;
    private Batch currentBatch;
    private boolean finished;

    public BatchSourceOperator(BatchSource source)
    {
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public int outputCount()
    {
        return outputSchema().size();
    }

    @Override
    public Schema outputSchema()
    {
        return source.schema();
    }

    @Override
    public boolean hasNext()
    {
        if (staged != null) {
            return true;
        }
        if (finished) {
            return false;
        }
        SourcePoll poll = source.poll();
        switch (poll) {
            case SourcePoll.Ready(var batch) -> {
                NativeBatchAccess nativeBatch = batch.capability(NativeBatchCapability.NATIVE_BATCH).orElse(null);
                if (nativeBatch == null) {
                    batch.close();
                    throw new IllegalArgumentException("source does not expose a native batch");
                }
                staged = batch;
                stagedNativeBatch = nativeBatch;
                return true;
            }
            case SourcePoll.Blocked _ -> throw new IllegalStateException("pull adapter cannot consume a blocked source");
            case SourcePoll.Finished _ -> {
                finished = true;
                return false;
            }
        }
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        currentBatch = stagedNativeBatch.transfer();
        staged.close();
        staged = null;
        stagedNativeBatch = null;
        return currentBatch;
    }

    @Override
    public void constrain(Mask mask)
    {
        if (staged != null) {
            staged.select(new MaskSelection(mask));
            return;
        }
        if (currentBatch == null) {
            throw new IllegalStateException("No current batch");
        }
        currentBatch.constrain(mask);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return source.capabilities().contains(SourceCapability.RETAINED_BATCHES);
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        return source.capabilities().contains(SourceCapability.STABLE_BATCH_BORROW);
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return source.capabilities().contains(SourceCapability.CONSTRAINED_REBORROW);
    }

    @Override
    public void close()
    {
        if (staged != null) {
            staged.close();
            staged = null;
            stagedNativeBatch = null;
        }
        source.close();
    }
}
