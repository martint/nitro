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
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.StaticFilterEnforcement;

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
    private final SourceOperatorIngress ingress;
    private final Operator nativeSource;
    private SourceBatch staged;
    private Batch currentBatch;
    private boolean finished;

    public BatchSourceOperator(BatchSource source, SourceOperatorIngress ingress)
    {
        this.source = requireNonNull(source, "source is null");
        this.ingress = requireNonNull(ingress, "ingress is null");
        this.nativeSource = ingress.directOperator(source)
                .orElse(null);
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
        if (nativeSource != null) {
            return nativeSource.hasNext();
        }
        if (staged != null) {
            return true;
        }
        if (finished) {
            return false;
        }
        SourcePoll poll = source.poll();
        switch (poll) {
            case SourcePoll.Ready(var batch) -> {
                staged = batch;
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
        if (nativeSource != null) {
            return nativeSource.next();
        }
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        SourceBatch batch = staged;
        staged = null;
        try {
            currentBatch = ingress.adapt(batch);
            return currentBatch;
        }
        catch (RuntimeException | Error failure) {
            batch.close();
            throw failure;
        }
    }

    @Override
    public void constrain(Mask mask)
    {
        if (nativeSource != null) {
            nativeSource.constrain(mask);
            return;
        }
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
    public boolean supportsRetainedBatches()
    {
        if (nativeSource != null) {
            return nativeSource.supportsRetainedBatches();
        }
        return source.capabilities().contains(SourceCapability.RETAINED_BATCHES);
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        if (nativeSource != null) {
            return nativeSource.supportsStableBatchBorrow();
        }
        return source.capabilities().contains(SourceCapability.STABLE_BATCH_BORROW);
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        if (nativeSource != null) {
            return nativeSource.supportsOpenBatchHasNext();
        }
        // The BatchSource protocol advances through poll(). Consumers must close the adapted batch before asking
        // for availability, independently of whether that batch supports a constrained re-borrow while open.
        return false;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        if (nativeSource != null) {
            return nativeSource.supportsConstrainedReborrow();
        }
        return source.capabilities().contains(SourceCapability.CONSTRAINED_REBORROW);
    }

    @Override
    public long exactOutputRows()
    {
        return source.exactRows().orElse(-1);
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        requireNonNull(filter, "filter is null");
        int column = filter.column();
        if (!supportsDynamicFilterPushdown(column)) {
            return;
        }
        var handle = source.column(column);
        source.addRuntimeFilter(ingress.runtimeFilter(handle, filter));
    }

    @Override
    public StaticFilterEnforcement pushStaticFilter(DynamicFilter filter)
    {
        requireNonNull(filter, "filter is null");
        if (!supportsDynamicFilterPushdown(filter.column())) {
            return StaticFilterEnforcement.residual();
        }
        var handle = source.column(filter.column());
        StaticFilterEnforcement enforcement = StaticFilterEnforcement.pending();
        enforcement.complete(source.addRuntimeFilter(ingress.runtimeFilter(handle, filter).withoutResidual()));
        return enforcement;
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        if (column < 0 || column >= outputCount()) {
            return false;
        }
        var handle = source.column(column);
        return ingress.supportsRuntimeFilter(source, handle) && source.supportsRuntimeFilter(handle);
    }

    @Override
    public void close()
    {
        if (staged != null) {
            staged.close();
            staged = null;
        }
        source.close();
    }
}
