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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;

final class TopNState
{
    private final int orderingColumn;
    private final Streams[][] rowSlots;
    private final Streams[] schema;
    private final JoinBufferSupport buffers;
    private Streams[] materialized;

    TopNState(int orderingColumn, Allocator allocator, Allocator.Context allocationContext, int outputCount, int capacity)
    {
        this.orderingColumn = orderingColumn;
        this.rowSlots = new Streams[capacity][outputCount];
        this.schema = new Streams[outputCount];
        this.buffers = new JoinBufferSupport(allocator, allocationContext);
    }

    public void captureSchema(Batch batch)
    {
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            if (schema[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams.Builder streams = Streams.builder();
            for (Stream stream : output.streams()) {
                streams.put(stream, output.borrow(stream));
            }
            schema[outputIndex] = streams.build();
        }
    }

    public int compareOrderingValue(Output output, int position, int slot)
    {
        Streams slotOrdering = rowSlots[slot][orderingColumn];
        return OperatorOrderingSemantics.compare(
                output.borrow(Stream.VALUES),
                (BooleanVector) output.borrowOrNull(Stream.NULLS),
                position,
                slotOrdering.values(),
                (BooleanVector) slotOrdering.getOrNull(Stream.NULLS),
                0);
    }

    public int compareSlots(int leftSlot, int rightSlot)
    {
        Streams leftOrdering = rowSlots[leftSlot][orderingColumn];
        Streams rightOrdering = rowSlots[rightSlot][orderingColumn];
        return OperatorOrderingSemantics.compare(
                leftOrdering.values(),
                (BooleanVector) leftOrdering.getOrNull(Stream.NULLS),
                0,
                rightOrdering.values(),
                (BooleanVector) rightOrdering.getOrNull(Stream.NULLS),
                0);
    }

    public void copyRow(Batch batch, int position, int slot)
    {
        for (int outputIndex = 0; outputIndex < rowSlots[slot].length; outputIndex++) {
            rowSlots[slot][outputIndex] = buffers.copyPosition(batch.output(outputIndex), rowSlots[slot][outputIndex], position);
        }
    }

    public void materialize(List<Integer> orderedSlots)
    {
        materialized = new Streams[schema.length];
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            materialized[outputIndex] = materializeColumn(outputIndex, orderedSlots);
        }
    }

    public Streams output(int index)
    {
        return materialized[index];
    }

    private Streams materializeColumn(int outputIndex, List<Integer> orderedSlots)
    {
        Streams columnSchema = schema[outputIndex];
        if (columnSchema == null) {
            throw new IllegalStateException("TopN did not observe source output schema");
        }
        return buffers.materializeColumn(columnSchema, rowSlots, orderedSlots, outputIndex);
    }
}
