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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.StddevSampStateVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import static java.lang.Math.sqrt;
import static java.lang.Math.toIntExact;

public class StddevSamp
        implements Accumulator
{
    private final int inputColumn;

    public StddevSamp(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    int inputColumn()
    {
        return inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValues(allocator.allocate(allocationContext, StddevSampStateVector.class, size, StddevSampStateVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        StddevSampStateVector values = (StddevSampStateVector) state.values();
        if (values.length() >= size) {
            return state;
        }
        StddevSampStateVector grown = allocator.adopt(allocationContext, StddevSampStateVector.grow(values, size));
        allocator.discard(allocationContext, values);
        return Streams.ofValues(grown);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        ((StddevSampStateVector) state.values()).initialize(offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        StddevSampStateVector stateVector = (StddevSampStateVector) state.values();
        VectorAccess.LongValues inputValues = VectorAccess.longValues(streams.values(inputColumn));
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
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
        StddevSampStateVector stateVector = (StddevSampStateVector) state.values();
        I64Vector groupVector = (I64Vector) groups;
        VectorAccess.LongValues inputValues = VectorAccess.longValues(streams.values(inputColumn));
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

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
        StddevSampStateVector stateVector = (StddevSampStateVector) state.values();
        F64Vector values = output == null ? null : (F64Vector) output.getOrNull(Stream.VALUES);
        values = allocator.allocateOrGrow(allocationContext, values, F64Vector.class, stateVector.length(), F64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                stateVector.length());

        for (int index = 0; index < stateVector.length(); index++) {
            long count = stateVector.count(index);
            boolean isNull = count < 2;
            nulls.values()[index] = isNull;
            values.values()[index] = isNull ? 0 : sqrt(stateVector.m2(index) / (count - 1));
        }

        return Streams.reuseValuesAndNulls(output, values, nulls);
    }

    @Override
    public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        StddevSampStateVector stateVector = (StddevSampStateVector) state.values();
        F64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (F64Vector) output.getOrNull(Stream.VALUES),
                F64Vector.class,
                size,
                F64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                size);

        long count = stateVector.count(group);
        boolean isNull = count < 2;
        nulls.values()[outputPosition] = isNull;
        values.values()[outputPosition] = isNull ? 0 : sqrt(stateVector.m2(group) / (count - 1));
        return Streams.reuseValuesAndNulls(output, values, nulls);
    }

    private static void accumulate(StddevSampStateVector stateVector, int group, VectorAccess.LongValues inputValues, VectorAccess.BooleanValues inputNulls, int position)
    {
        if (inputNulls.value(position)) {
            return;
        }
        stateVector.addSample(group, inputValues.value(position));
    }
}
