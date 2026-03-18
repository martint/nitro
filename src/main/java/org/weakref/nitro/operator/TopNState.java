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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Map;

final class TopNState
{
    private final int orderingColumn;
    private final Streams[][] slotColumns;
    private final Streams[] schema;
    private final JoinBufferSupport buffers;
    private Streams[] materialized;

    TopNState(int orderingColumn, Allocator allocator, Allocator.Context allocationContext, int outputCount, int capacity)
    {
        this.orderingColumn = orderingColumn;
        this.slotColumns = new Streams[outputCount][capacity];
        this.schema = new Streams[outputCount];
        this.buffers = new JoinBufferSupport(allocator, allocationContext);
    }

    public void captureSchema(Batch batch)
    {
        BufferedJoinInput.captureSchema(batch, schema);
    }

    public int compareOrderingValue(Output output, int position, int slot)
    {
        Streams slotOrdering = slotColumns[orderingColumn][slot];
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
        Streams leftOrdering = slotColumns[orderingColumn][leftSlot];
        Streams rightOrdering = slotColumns[orderingColumn][rightSlot];
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
        for (int outputIndex = 0; outputIndex < slotColumns.length; outputIndex++) {
            slotColumns[outputIndex][slot] = buffers.copyPosition(batch.output(outputIndex), slotColumns[outputIndex][slot], position);
        }
    }

    public void materialize(List<Integer> orderedSlots)
    {
        materialized = new Streams[schema.length];
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            Streams columnSchema = schema[outputIndex];
            if (columnSchema == null) {
                throw new IllegalStateException("TopN did not observe source output schema");
            }
            materialized[outputIndex] = orderedSlots.isEmpty() ? buffers.emptyLike(columnSchema) : materializeColumn(columnSchema, outputIndex, orderedSlots);
        }
    }

    public Streams output(int index)
    {
        return materialized[index];
    }

    private Streams materializeColumn(Streams columnSchema, int outputIndex, List<Integer> orderedSlots)
    {
        Streams.Builder result = Streams.builder();
        for (Map.Entry<Stream, Vector> entry : columnSchema.asMap().entrySet()) {
            Vector[] rows = new Vector[orderedSlots.size()];
            for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
                rows[rowIndex] = slotColumns[outputIndex][orderedSlots.get(rowIndex)].get(entry.getKey());
            }
            result.put(entry.getKey(), buffers.materializeStream(entry.getValue(), rows));
        }
        return result.build();
    }
}
