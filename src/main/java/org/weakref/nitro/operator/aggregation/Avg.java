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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.AvgStateVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class Avg
        implements Accumulator
{
    private final int inputColumn;

    public Avg(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, AvgStateVector.class, size, AvgStateVector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        AvgStateVector values = allocator.allocateOrGrow(allocationContext, (AvgStateVector) state.values(), AvgStateVector.class, size, AvgStateVector::new);
        BooleanVector nulls = allocator.allocateOrGrow(allocationContext, (BooleanVector) state.get(Stream.NULLS), BooleanVector.class, size, BooleanVector::new);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        Arrays.fill(stateVector.sums(), offset, offset + length, 0);
        Arrays.fill(stateVector.counts(), offset, offset + length, 0);
        Arrays.fill(stateNulls.values(), offset, offset + length, true);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        Vector inputValues = streams.values(inputColumn);
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                accumulate(stateVector, stateNulls, group, inputValues, inputNulls, position);
            }
        }
        else {
            for (int position : mask) {
                accumulate(stateVector, stateNulls, group, inputValues, inputNulls, position);
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        I64Vector groupVector = (I64Vector) groups;
        Vector inputValues = streams.values(inputColumn);
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, stateNulls, group, inputValues, inputNulls, position);
            }
        }
        else {
            for (int position : mask) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, stateNulls, group, inputValues, inputNulls, position);
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        F64Vector values = output == null ? null : (F64Vector) output.getOrNull(Stream.VALUES);
        BooleanVector nulls = output == null ? null : (BooleanVector) output.getOrNull(Stream.NULLS);
        values = allocator.allocateOrGrow(allocationContext, values, F64Vector.class, stateVector.length(), F64Vector::new);
        nulls = allocator.allocateOrGrow(allocationContext, nulls, BooleanVector.class, stateVector.length(), BooleanVector::new);

        for (int index = 0; index < stateVector.length(); index++) {
            nulls.values()[index] = stateNulls.values()[index];
            values.values()[index] = stateNulls.values()[index] ? 0 : ((double) stateVector.sums()[index] / stateVector.counts()[index]);
        }

        return Streams.ofValuesAndNulls(values, nulls);
    }

    private static void accumulate(AvgStateVector stateVector, BooleanVector stateNulls, int group, Vector inputValues, boolean[] inputNulls, int position)
    {
        if (isNull(inputNulls, position)) {
            return;
        }
        stateNulls.values()[group] = false;
        stateVector.sums()[group] += value(inputValues, position);
        stateVector.counts()[group] += 1;
    }

    private static long value(Vector vector, int position)
    {
        return switch (vector) {
            case I64Vector values -> values.values()[position];
            case I32Vector values -> values.values()[position];
            case DictionaryVector values -> value(values.values(), values.ids()[position]);
            case RleVector values -> value(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static boolean[] nulls(Vector vector)
    {
        return vector == null ? null : ((BooleanVector) vector).values();
    }

    private static boolean isNull(boolean[] nulls, int position)
    {
        return nulls != null && nulls[position];
    }
}
