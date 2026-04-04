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

import java.util.ArrayList;
import java.util.List;

public class SortOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("SortOperator");

    private final Allocator allocator;
    private final Operator source;
    private final TopNState state;

    private boolean done;

    public SortOperator(Allocator allocator, int[] columns, boolean[] descending, Operator source)
    {
        if (columns.length == 0) {
            throw new IllegalArgumentException("Sort requires at least one ordering column");
        }
        if (columns.length != descending.length) {
            throw new IllegalArgumentException("Sort ordering columns and directions must have the same length");
        }
        this.allocator = allocator;
        this.source = source;
        this.state = new TopNState(columns, descending, allocator, ALLOCATION_CONTEXT, source.outputCount(), 256);
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Batch next()
    {
        Mask batchMask = computeSorted();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = new Output(
                    state.outputStreams(output),
                    stream -> state.output(output).get(stream),
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }
        return new Batch(
                batchMask,
                state::constrain,
                takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask),
                _ -> {},
                state::releaseFallbackBatch,
                outputs);
    }

    private Mask computeSorted()
    {
        List<Integer> orderedSlots = new ArrayList<>();

        while (source.hasNext()) {
            Batch batch = source.next();
            state.captureSchema(batch);
            Mask mask = batch.borrowMask();
            state.ensureCapacity(orderedSlots.size() + mask.size());

            List<Integer> batchSlots = new ArrayList<>(mask.size());
            for (int position : mask) {
                int slot = orderedSlots.size();
                state.copyRow(batch, position, slot);
                orderedSlots.add(slot);
                batchSlots.add(slot);
            }

            if (!source.supportsRetainedBatches()) {
                state.flushPendingBatch(batch, batchSlots);
                if (!orderedSlots.isEmpty()) {
                    state.releaseFallbackBatch();
                }
                if (!state.shouldKeepBatchForEmptySchema(batch, orderedSlots.isEmpty())) {
                    batch.close();
                }
            }
        }

        orderedSlots.sort((left, right) -> state.compareSlots(right, left));
        state.setOrderedSlots(orderedSlots);
        done = true;
        return allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, orderedSlots.size());
    }

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed.
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }
}
