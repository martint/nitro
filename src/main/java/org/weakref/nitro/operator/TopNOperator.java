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
import org.weakref.nitro.data.LongVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.Comparator;
import java.util.PriorityQueue;

public class TopNOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("TopNOperator");
    private final Allocator allocator;

    private final int n;
    private final int column;
    private final Operator source;

    private final Vector[] result;
    private boolean done;

    public TopNOperator(Allocator allocator, int n, int column, Operator source)
    {
        this.allocator = allocator;
        this.n = n;
        this.column = column;
        this.source = source;
        result = new LongVector[source.columnCount()];
    }

    @Override
    public int columnCount()
    {
        return source.columnCount();
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Mask next()
    {
        PriorityQueue<Entry> queue = new PriorityQueue<>(n, Comparator.comparingLong(e -> e.value));

        for (int i = 0; i < result.length; i++) {
            result[i] = allocator.allocate(ALLOCATION_CONTEXT, n);
        }

        while (source.hasNext()) {
            Mask mask = source.next();

            for (int position : mask) {
                LongVector sortColumn = (LongVector) source.column(column);
                if (sortColumn.nulls()[position]) {
                    // Skip nulls for now
                    continue;
                }

                long value = sortColumn.values()[position];
                if (queue.size() < n) {
                    int slot = queue.size();
                    queue.add(new Entry(value, slot));
                    copyToBuffer(position, slot);
                }
                else {
                    Entry head = queue.peek();
                    if (value > head.value) {
                        queue.poll();
                        queue.add(new Entry(value, head.position));
                        copyToBuffer(position, head.position);
                    }
                }
            }
        }

        int count = queue.size();
        reorderBuffer(queue);

        done = true;
        return Mask.range(0, count);
    }

    private void reorderBuffer(PriorityQueue<Entry> queue)
    {
        long[] temp = new long[columnCount()];

        int[] remap = new int[queue.size()];
        for (int i = 0; i < remap.length; i++) {
            remap[i] = i;
        }

        int current = queue.size() - 1;
        while (!queue.isEmpty()) {
            Entry entry = queue.poll();
            for (int i = 0; i < result.length; i++) {
                long[] values = ((LongVector) result[i]).values();

                temp[i] = values[current];
                values[current] = values[remap[entry.position]];
                values[remap[entry.position]] = temp[i];

                remap[current] = remap[entry.position];
            }

            current--;
        }
    }

    private void copyToBuffer(int from, int to)
    {
        for (int i = 0; i < result.length; i++) {
            Vector column = source.column(i);
            ((LongVector) result[i]).values()[to] = ((LongVector) column).values()[from];
        }
    }

    @Override
    public void constrain(Mask mask)
    {
        // Nothing to do. All output is already computed
    }

    @Override
    public Vector column(int column)
    {
        return result[column];
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    record Entry(long value, int position) {}
}
