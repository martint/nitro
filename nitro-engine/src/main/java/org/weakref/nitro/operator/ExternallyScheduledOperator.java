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
import org.weakref.nitro.operator.source.ExternallyScheduledSource;

import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;

/** Preserves external scheduling state across an operator pipeline rooted at an external feed. */
final class ExternallyScheduledOperator
        implements ExternallyScheduledSource
{
    private final Operator delegate;
    private final BooleanSupplier finished;

    ExternallyScheduledOperator(Operator delegate, BooleanSupplier finished)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.finished = requireNonNull(finished, "finished is null");
    }

    @Override
    public boolean isFinished()
    {
        return finished.getAsBoolean();
    }

    @Override
    public int outputCount()
    {
        return delegate.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return delegate.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    @Override
    public Batch next()
    {
        return delegate.next();
    }

    @Override
    public void constrain(Mask mask)
    {
        delegate.constrain(mask);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return delegate.supportsRetainedBatches();
    }

    @Override
    public boolean supportsStableBatchBorrow()
    {
        return delegate.supportsStableBatchBorrow();
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return delegate.supportsOpenBatchHasNext();
    }

    @Override
    public long exactOutputRows()
    {
        return delegate.exactOutputRows();
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return delegate.supportsConstrainedReborrow();
    }

    @Override
    public void pushDynamicFilter(DynamicFilter filter)
    {
        delegate.pushDynamicFilter(filter);
    }

    @Override
    public boolean supportsDynamicFilterPushdown(int column)
    {
        return delegate.supportsDynamicFilterPushdown(column);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
