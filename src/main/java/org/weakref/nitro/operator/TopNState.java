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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.List;

final class TopNState
{
    private final int orderingColumn;
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final Streams[][] rowSlots;
    private final Streams[] schema;
    private Streams[] materialized;

    TopNState(int orderingColumn, Allocator allocator, Allocator.Context allocationContext, int outputCount, int capacity)
    {
        this.orderingColumn = orderingColumn;
        this.allocator = allocator;
        this.allocationContext = allocationContext;
        this.rowSlots = new Streams[capacity][outputCount];
        this.schema = new Streams[outputCount];
    }

    public void captureSchema(Batch batch)
    {
        for (int outputIndex = 0; outputIndex < schema.length; outputIndex++) {
            if (schema[outputIndex] != null) {
                continue;
            }
            Output output = batch.output(outputIndex);
            Streams streams = Streams.empty();
            for (Stream stream : output.streams()) {
                streams = streams.with(stream, output.borrow(stream));
            }
            schema[outputIndex] = streams;
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
            rowSlots[slot][outputIndex] = copyOutput(batch.output(outputIndex), rowSlots[slot][outputIndex], position);
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

        Streams result = Streams.empty();
        for (Stream stream : columnSchema.asMap().keySet()) {
            Vector sample = columnSchema.get(stream);
            Vector[] rows = new Vector[orderedSlots.size()];
            for (int rowIndex = 0; rowIndex < orderedSlots.size(); rowIndex++) {
                rows[rowIndex] = rowSlots[orderedSlots.get(rowIndex)][outputIndex].get(stream);
            }
            result = result.with(stream, materializeStream(sample, rows));
        }
        return result;
    }

    private Streams copyOutput(Output output, Streams existing, int position)
    {
        Streams copied = Streams.empty();
        for (Stream stream : output.streams()) {
            Vector existingVector = existing != null ? existing.getOrNull(stream) : null;
            copied = copied.with(stream, copyVectorPosition(output.borrow(stream), existingVector, position));
        }
        return copied;
    }

    private Vector copyVectorPosition(Vector source, Vector existing, int position)
    {
        return switch (source) {
            case I64Vector values -> copyLongPosition(values, existing, position);
            case BooleanVector values -> copyBooleanPosition(values, existing, position);
            case F64Vector values -> copyDoublePosition(values, existing, position);
            case BinaryVector values -> copyBinaryPosition(values, existing, position);
            case DictionaryVector values -> copyVectorPosition(values.values(), existing, values.ids()[position]);
            case RleVector values -> copyVectorPosition(values.values(), existing, runIndex(values, position));
            default -> throw new IllegalArgumentException("Unsupported TopN output vector type: " + source.getClass().getSimpleName());
        };
    }

    private Vector materializeStream(Vector sample, Vector[] rows)
    {
        Vector flattenedSample = OperatorVectorSupport.flatten(sample);
        return switch (flattenedSample) {
            case I64Vector _ -> materializeLongs(rows);
            case BooleanVector _ -> materializeBooleans(rows);
            case F64Vector _ -> materializeDoubles(rows);
            case BinaryVector binary -> materializeBinary(binary, rows);
            default -> throw new IllegalArgumentException("Unsupported TopN materialized vector type: " + flattenedSample.getClass().getSimpleName());
        };
    }

    private I64Vector copyLongPosition(I64Vector source, Vector existing, int position)
    {
        I64Vector target = allocator.allocateOrGrow(allocationContext, existing instanceof I64Vector vector ? vector : null, I64Vector.class, 1, I64Vector::new);
        target.values()[0] = source.values()[position];
        return target;
    }

    private BooleanVector copyBooleanPosition(BooleanVector source, Vector existing, int position)
    {
        BooleanVector target = allocator.allocateOrGrow(allocationContext, existing instanceof BooleanVector vector ? vector : null, BooleanVector.class, 1, BooleanVector::new);
        target.values()[0] = source.values()[position];
        return target;
    }

    private F64Vector copyDoublePosition(F64Vector source, Vector existing, int position)
    {
        F64Vector target = allocator.allocateOrGrow(allocationContext, existing instanceof F64Vector vector ? vector : null, F64Vector.class, 1, F64Vector::new);
        target.values()[0] = source.values()[position];
        return target;
    }

    private BinaryVector copyBinaryPosition(BinaryVector source, Vector existing, int position)
    {
        int length = source.length(position);
        BinaryVector target = allocator.allocateOrGrowBinary(allocationContext, existing instanceof BinaryVector vector ? vector : null, 1, length);
        Arrays.fill(target.offsets(), 0);
        target.clearTraits();
        target.addTraits(source.traits());
        if (length == 0) {
            target.setNull(0);
        }
        else {
            target.setBytes(0, source.data(), source.startOffset(position), length);
        }
        return target;
    }

    private I64Vector materializeLongs(Vector[] rows)
    {
        I64Vector result = allocator.allocate(allocationContext, I64Vector.class, rows.length, I64Vector::new);
        for (int row = 0; row < rows.length; row++) {
            result.values()[row] = OperatorVectorSupport.longValue(rows[row], 0);
        }
        return result;
    }

    private BooleanVector materializeBooleans(Vector[] rows)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, rows.length, BooleanVector::new);
        for (int row = 0; row < rows.length; row++) {
            result.values()[row] = OperatorVectorSupport.booleanValue(rows[row], 0);
        }
        return result;
    }

    private F64Vector materializeDoubles(Vector[] rows)
    {
        F64Vector result = allocator.allocate(allocationContext, F64Vector.class, rows.length, F64Vector::new);
        for (int row = 0; row < rows.length; row++) {
            result.values()[row] = OperatorVectorSupport.doubleValue(rows[row], 0);
        }
        return result;
    }

    private BinaryVector materializeBinary(BinaryVector sample, Vector[] rows)
    {
        int totalBytes = 0;
        for (Vector row : rows) {
            totalBytes += OperatorVectorSupport.binaryLength(row, 0);
        }

        BinaryVector result = allocator.allocateBinary(allocationContext, rows.length, totalBytes);
        result.addTraits(sample.traits());
        for (int row = 0; row < rows.length; row++) {
            int length = OperatorVectorSupport.binaryLength(rows[row], 0);
            if (length == 0) {
                result.setNull(row);
                continue;
            }
            byte[] bytes = OperatorVectorSupport.binaryBytes(rows[row], 0);
            result.setBytes(row, bytes);
        }
        return result;
    }

    private static int runIndex(RleVector values, int position)
    {
        int offset = 0;
        for (int index = 0; index < values.counts().length; index++) {
            offset += values.counts()[index];
            if (position < offset) {
                return index;
            }
        }
        throw new IndexOutOfBoundsException("Position " + position + " is out of bounds for RLE vector of length " + values.length());
    }
}
