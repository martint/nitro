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
import java.util.PriorityQueue;

public class TopNOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("TopNOperator");
    private final Allocator allocator;

    private final int n;
    private final Operator source;
    private final TopNState state;

    private boolean done;

    public TopNOperator(Allocator allocator, int n, int column, Operator source)
    {
        this(allocator, n, new int[] {column}, new boolean[] {true}, source);
    }

    public TopNOperator(Allocator allocator, int n, int column, boolean descending, Operator source)
    {
        this(allocator, n, new int[] {column}, new boolean[] {descending}, source);
    }

    public TopNOperator(Allocator allocator, int n, int[] columns, boolean[] descending, Operator source)
    {
        if (columns.length == 0) {
            throw new IllegalArgumentException("TopN requires at least one ordering column");
        }
        if (columns.length != descending.length) {
            throw new IllegalArgumentException("TopN ordering columns and directions must have the same length");
        }
        this.allocator = allocator;
        this.n = n;
        this.source = source;
        state = new TopNState(columns, descending, allocator, ALLOCATION_CONTEXT, source.outputCount(), n);
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

    private Mask computeTopN()
    {
        // TODO: flat memory priority queue
        PriorityQueue<Entry> queue = new PriorityQueue<>(n, (left, right) -> state.compareSlots(left.position(), right.position()));

        while (source.hasNext()) {
            Batch batch = source.next();
            state.captureSchema(batch);
            Mask mask = batch.borrowMask();

            for (int position : mask) {
                if (queue.size() < n) {
                    int slot = queue.size();
                    state.copyRow(batch, position, slot);
                    queue.add(new Entry(slot));
                }
                else {
                    Entry head = queue.peek();
                    if (state.compareOrderingValue(batch, position, head.position()) > 0) {
                        queue.poll();
                        state.copyRow(batch, position, head.position());
                        queue.add(new Entry(head.position()));
                    }
                }
            }

            // Non-retained sources may invalidate the current batch as soon as the
            // caller probes for the next one, so materialize any deferred payload
            // columns before the next hasNext()/next() cycle can advance upstream.
            if (!source.supportsRetainedBatches()) {
                state.flushPendingBatch(batch, queue.stream()
                        .map(Entry::position)
                        .toList());
            }
        }

        int count = queue.size();
        state.setOrderedSlots(orderedSlots(queue));

        done = true;
        return allocator.allocateRangeMask(ALLOCATION_CONTEXT, 0, count);
    }

    @Override
    public Batch next()
    {
        Mask batchMask = computeTopN();
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            int output = outputIndex;
            outputs[outputIndex] = new Output(
                    state.outputStreams(output),
                    stream -> state.output(output).get(stream),
                    (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }
        return new Batch(batchMask, takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
    }

    private List<Integer> orderedSlots(PriorityQueue<Entry> queue)
    {
        List<Entry> entries = new ArrayList<>(queue);
        entries.sort((left, right) -> state.compareSlots(right.position(), left.position()));
        return entries.stream()
                .map(Entry::position)
                .toList();
    }

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    record Entry(int position) {}
}
