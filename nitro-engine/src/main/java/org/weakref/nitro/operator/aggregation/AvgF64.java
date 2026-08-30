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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import static java.lang.Math.toIntExact;

public class AvgF64
        implements Accumulator
{
    private final int inputColumn;

    public AvgF64(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    int inputColumn()
    {
        return inputColumn;
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        Allocator allocator = context.allocator();
        Allocator.Context allocationContext = context.allocationContext();
        return Streams.ofValues(allocator.allocate(allocationContext, AvgStateVector.class, size, AvgStateVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        AvgStateVector values = (AvgStateVector) state.values();
        if (values.length() >= size) {
            return state;
        }
        AvgStateVector grown = allocator.replaceSharedGrowth(
                allocationContext,
                values,
                AvgStateVector.grow(values, size));
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
        Vector inputNullsVector = streams.stream(inputColumn, Stream.NULLS);
        VectorAccess.DoubleValues inputValues = VectorAccess.doubleValues(streams.values(inputColumn));

        if (VectorAccess.isAllFalseNulls(inputNullsVector)) {
            if (mask.all()) {
                int max = mask.maxPosition();
                for (int position = 0; position <= max; position++) {
                    stateVector.incrementDouble(group, inputValues.value(position));
                }
            }
            else {
                for (int position : mask) {
                    stateVector.incrementDouble(group, inputValues.value(position));
                }
            }
            return;
        }

        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(inputNullsVector);
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
        Vector inputNullsVector = streams.stream(inputColumn, Stream.NULLS);
        VectorAccess.DoubleValues inputValues = VectorAccess.doubleValues(streams.values(inputColumn));

        if (VectorAccess.isAllFalseNulls(inputNullsVector)) {
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    int group = toIntExact(groupVector.values()[position]);
                    stateVector.incrementDouble(group, inputValues.value(position));
                }
            }
            else {
                for (int position : mask) {
                    int group = toIntExact(groupVector.values()[position]);
                    stateVector.incrementDouble(group, inputValues.value(position));
                }
            }
            return;
        }

        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(inputNullsVector);
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
        org.weakref.nitro.data.BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                stateVector.length());

        for (int index = 0; index < stateVector.length(); index++) {
            boolean isNull = stateVector.count(index) == 0;
            nulls.values()[index] = isNull;
            values.values()[index] = isNull ? 0 : (stateVector.doubleSum(index) / stateVector.count(index));
        }

        return allocator.reuseValuesAndNulls(output, values, nulls);
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
        org.weakref.nitro.data.BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                size);

        boolean isNull = stateVector.count(group) == 0;
        nulls.values()[outputPosition] = isNull;
        values.values()[outputPosition] = isNull ? 0 : (stateVector.doubleSum(group) / stateVector.count(group));
        return allocator.reuseValuesAndNulls(output, values, nulls);
    }

    private static void accumulate(AvgStateVector stateVector, int group, VectorAccess.DoubleValues inputValues, VectorAccess.BooleanValues inputNulls, int position)
    {
        if (inputNulls.value(position)) {
            return;
        }
        stateVector.incrementDouble(group, inputValues.value(position));
    }
}
