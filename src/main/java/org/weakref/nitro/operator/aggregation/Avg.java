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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

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
        return Streams.ofValues(allocator.allocate(allocationContext, AvgStateVector.class, size, AvgStateVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        AvgStateVector values = (AvgStateVector) state.values();
        if (values.length() >= size) {
            return state;
        }
        AvgStateVector grown = allocator.adopt(allocationContext, AvgStateVector.grow(values, size));
        allocator.discard(allocationContext, values);
        return Streams.ofValues(grown);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        stateVector.initialize(offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        Vector inputValues = streams.values(inputColumn);
        Vector inputNulls = streams.stream(inputColumn, Stream.NULLS);

        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                accumulate(stateVector, group, inputValues, inputNulls, position);
            }
        }
        else {
            for (int position : mask) {
                accumulate(stateVector, group, inputValues, inputNulls, position);
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        I64Vector groupVector = (I64Vector) groups;
        Vector inputValues = streams.values(inputColumn);
        Vector inputNulls = streams.stream(inputColumn, Stream.NULLS);

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, group, inputValues, inputNulls, position);
            }
        }
        else {
            for (int position : mask) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, group, inputValues, inputNulls, position);
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        F64Vector values = output == null ? null : (F64Vector) output.getOrNull(Stream.VALUES);
        values = allocator.allocateOrGrow(allocationContext, values, F64Vector.class, stateVector.length(), F64Vector::new);
        org.weakref.nitro.data.BooleanVector nulls = output == null ? null : (org.weakref.nitro.data.BooleanVector) output.getOrNull(Stream.NULLS);
        nulls = allocator.allocateOrGrow(allocationContext, nulls, org.weakref.nitro.data.BooleanVector.class, stateVector.length(), org.weakref.nitro.data.BooleanVector::new);

        for (int index = 0; index < stateVector.length(); index++) {
            boolean isNull = stateVector.count(index) == 0;
            nulls.values()[index] = isNull;
            values.values()[index] = isNull ? 0 : ((double) stateVector.sum(index) / stateVector.count(index));
        }

        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        AvgStateVector stateVector = (AvgStateVector) state.values();
        F64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (F64Vector) output.getOrNull(Stream.VALUES),
                F64Vector.class,
                size,
                F64Vector::new);
        org.weakref.nitro.data.BooleanVector nulls = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (org.weakref.nitro.data.BooleanVector) output.getOrNull(Stream.NULLS),
                org.weakref.nitro.data.BooleanVector.class,
                size,
                org.weakref.nitro.data.BooleanVector::new);

        boolean isNull = stateVector.count(group) == 0;
        nulls.values()[outputPosition] = isNull;
        values.values()[outputPosition] = isNull ? 0 : ((double) stateVector.sum(group) / stateVector.count(group));
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private static void accumulate(AvgStateVector stateVector, int group, Vector inputValues, Vector inputNulls, int position)
    {
        if (isNull(inputNulls, position)) {
            return;
        }
        stateVector.increment(group, value(inputValues, position), 1);
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

    private static boolean isNull(Vector nulls, int position)
    {
        return switch (nulls) {
            case null -> false;
            case org.weakref.nitro.data.BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> isNull(vector.values(), vector.ids()[position]);
            case RleVector vector -> isNull(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Expected boolean-backed null vector but found " + nulls.getClass().getSimpleName());
        };
    }
}
