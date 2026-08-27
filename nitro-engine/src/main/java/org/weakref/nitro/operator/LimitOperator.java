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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;

import static java.lang.Math.toIntExact;

public class LimitOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("LimitOperator", LimitOperator.class);

    private final Allocator allocator;
    private final long limit;
    private final Operator source;

    private long count;
    private Batch currentBatch;
    private Mask currentMask;

    public LimitOperator(Allocator allocator, long limit, Operator source)
    {
        this.allocator = allocator;
        this.limit = limit;
        this.source = source;
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return source.outputSchema();
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        currentBatch = sourceBatch;
        Mask sourceMask = sourceBatch.borrowMask();

        int remaining = toIntExact(Math.min(limit - count, sourceMask.count()));
        currentMask = allocator.firstMask(allocationContext, sourceMask, remaining);
        source.constrain(currentMask);
        sourceBatch.constrain(currentMask);
        count += remaining;

        return Batch.forwarding(currentMask, new BatchState(sourceBatch, sourceMask), sourceBatch);
    }

    @Override
    public boolean hasNext()
    {
        return count < limit && source.hasNext();
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatch != null) {
            currentMask = mask;
            currentBatch.constrain(mask);
        }
    }

    @Override
    public void close()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
        source.close();
        allocator.release(allocationContext);
    }

    private final class BatchState
            implements Batch.Lifecycle
    {
        private final Batch sourceBatch;
        private final Mask sourceMask;

        private BatchState(Batch sourceBatch, Mask sourceMask)
        {
            this.sourceBatch = sourceBatch;
            this.sourceMask = sourceMask;
        }

        @Override
        public void constrain(Mask mask)
        {
            currentMask = mask;
            source.constrain(mask);
            sourceBatch.constrain(mask);
        }

        @Override
        public Mask takeMask(Mask mask)
        {
            return mask == sourceMask ? sourceBatch.takeMask() : allocator.transfer(allocationContext, mask);
        }

        @Override
        public void releaseMask(Mask mask) {}

        @Override
        public void close()
        {
            if (currentBatch == sourceBatch) {
                currentBatch = null;
            }
            sourceBatch.close();
        }
    }
}
