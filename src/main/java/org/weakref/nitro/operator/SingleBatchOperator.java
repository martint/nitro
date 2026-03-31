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

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class SingleBatchOperator
        implements Operator
{
    private final int outputCount;
    private final Supplier<Output[]> outputsSupplier;
    private Mask currentMask;
    private boolean emitted;

    public SingleBatchOperator(int outputCount, Mask mask, Supplier<Output[]> outputsSupplier)
    {
        this.outputCount = outputCount;
        this.currentMask = requireNonNull(mask, "mask is null");
        this.outputsSupplier = requireNonNull(outputsSupplier, "outputsSupplier is null");
    }

    @Override
    public int outputCount()
    {
        return outputCount;
    }

    @Override
    public boolean hasNext()
    {
        return !emitted;
    }

    @Override
    public Batch next()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more rows");
        }
        emitted = true;
        return new Batch(currentMask, this::constrain, java.util.function.Function.identity(), outputsSupplier.get());
    }

    @Override
    public void constrain(Mask mask)
    {
        currentMask = mask;
    }

    @Override
    public void close() {}

    public Mask currentMask()
    {
        return currentMask;
    }
}
