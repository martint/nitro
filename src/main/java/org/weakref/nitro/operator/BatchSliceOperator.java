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

import static com.google.common.base.Preconditions.checkArgument;

public class BatchSliceOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("BatchSliceOperator");

    private final Allocator allocator;
    private final int maxRowsPerBatch;
    private final Operator source;

    private Batch currentBatch;
    private int currentOffset;

    public BatchSliceOperator(Allocator allocator, int maxRowsPerBatch, Operator source)
    {
        checkArgument(maxRowsPerBatch > 0, "maxRowsPerBatch must be positive");
        this.allocator = allocator;
        this.maxRowsPerBatch = maxRowsPerBatch;
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
        return currentBatch != null || source.hasNext();
    }

    @Override
    public Batch next()
    {
        if (currentBatch == null) {
            currentBatch = source.next();
            currentOffset = 0;
        }

        Batch sourceBatch = currentBatch;
        Mask sourceMask = sourceBatch.borrowMask();
        int remaining = sourceMask.selectedCount() - currentOffset;
        int sliceLength = Math.min(remaining, maxRowsPerBatch);
        Mask sliceMask = sliceMask(sourceMask, currentOffset, sliceLength);
        currentOffset += sliceLength;
        boolean lastSlice = currentOffset == sourceMask.selectedCount();

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Output sourceOutput = sourceBatch.output(outputIndex);
            outputs[outputIndex] = new Output(
                    sourceOutput.streams(),
                    sourceOutput::borrow,
                    (_, vector) -> vector,
                    (_, _) -> {},
                    sourceOutput::copySinglePosition);
        }

        return new Batch(
                sliceMask,
                _ -> {},
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                mask -> allocator.release(ALLOCATION_CONTEXT, mask),
                () -> {
                    if (!lastSlice) {
                        return;
                    }
                    sourceBatch.close();
                    if (currentBatch == sourceBatch) {
                        currentBatch = null;
                        currentOffset = 0;
                    }
                },
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
        // Slice batches intentionally do not push constraints back into the source batch, because
        // later slices still need access to the full underlying batch.
    }

    @Override
    public void close()
    {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private Mask sliceMask(Mask sourceMask, int offset, int length)
    {
        if (length == 0) {
            return allocator.allocateSparseMask(ALLOCATION_CONTEXT, new int[0], 0, sourceMask.size());
        }

        int[] positions = new int[length];
        for (int index = 0; index < length; index++) {
            positions[index] = sourceMask.position(offset + index);
        }
        return allocator.allocateSparseMask(ALLOCATION_CONTEXT, positions, length, sourceMask.size());
    }
}
