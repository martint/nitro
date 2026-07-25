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

import static java.lang.Math.toIntExact;

public class OffsetOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("OffsetOperator", OffsetOperator.class);

    private final Allocator allocator;
    private final long offset;
    private final Operator source;

    private long skipped;
    private Batch currentBatch;
    private Mask currentMask;

    public OffsetOperator(Allocator allocator, long offset, Operator source)
    {
        this.allocator = allocator;
        this.offset = offset;
        this.source = source;
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public boolean hasNext()
    {
        if (currentBatch != null) {
            return true;
        }
        return skipped < offset ? skipFullyConsumedBatches() : source.hasNext();
    }

    @Override
    public Batch next()
    {
        while (true) {
            Batch sourceBatch = currentBatch != null ? currentBatch : source.next();
            currentBatch = null;
            Mask sourceMask = sourceBatch.borrowMask();
            long remainingSkip = Math.max(0, offset - skipped);
            if (remainingSkip >= sourceMask.selectedCount()) {
                skipped += sourceMask.selectedCount();
                sourceBatch.close();
                continue;
            }

            int keepCount = sourceMask.selectedCount() - toIntExact(remainingSkip);
            currentBatch = sourceBatch;
            currentMask = allocator.lastMask(allocationContext, sourceMask, keepCount);
            skipped += remainingSkip;
            source.constrain(currentMask);
            sourceBatch.constrain(currentMask);

            return Batch.forwarding(currentMask, new BatchState(sourceBatch, sourceMask), sourceBatch);
        }
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

    private boolean skipFullyConsumedBatches()
    {
        while (source.hasNext()) {
            Batch sourceBatch = source.next();
            Mask sourceMask = sourceBatch.borrowMask();
            long remainingSkip = offset - skipped;
            if (remainingSkip < sourceMask.selectedCount()) {
                currentBatch = sourceBatch;
                currentMask = sourceMask;
                return true;
            }
            skipped += sourceMask.selectedCount();
            sourceBatch.close();
        }
        return false;
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
