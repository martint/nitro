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

import org.weakref.nitro.data.Mask;

import static java.util.Objects.requireNonNull;

public final class LegacyBatchOperatorAdapter
        implements BatchOperator
{
    private final Operator delegate;

    public LegacyBatchOperatorAdapter(Operator delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public int outputCount()
    {
        return delegate.columnCount();
    }

    @Override
    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    @Override
    public Batch nextBatch()
    {
        Mask mask = delegate.next();
        Output[] outputs = new Output[delegate.columnCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int column = outputIndex;
            outputs[outputIndex] = Output.lazyValues(() -> delegate.column(column));
        }
        return new Batch(mask, outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        delegate.constrain(mask);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
