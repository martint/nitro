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

public final class CountingNextOperator
        implements Operator
{
    private final Operator delegate;
    private int nextCount;

    public CountingNextOperator(Operator delegate)
    {
        this.delegate = delegate;
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
        nextCount++;
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
    public boolean supportsOpenBatchHasNext()
    {
        return delegate.supportsOpenBatchHasNext();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    public int nextCount()
    {
        return nextCount;
    }
}
