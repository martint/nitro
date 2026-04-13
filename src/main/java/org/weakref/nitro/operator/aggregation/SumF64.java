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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class SumF64
        implements Accumulator
{
    private final int inputColumn;

    public SumF64(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, F64Vector.class, size, F64Vector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        F64Vector values = allocator.allocateOrGrow(allocationContext, (F64Vector) state.values(), F64Vector.class, size, F64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(allocator, allocationContext, state.get(Stream.NULLS), size);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        Arrays.fill(((BooleanVector) state.get(Stream.NULLS)).values(), offset, offset + length, true);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        F64Vector stateVector = (F64Vector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        VectorAccess.DoubleValues inputValues = VectorAccess.doubleValues(streams.values(inputColumn));
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

        double sum = 0;
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                sum += inputNulls.value(position) ? 0 : inputValues.value(position);
            }
        }
        else {
            for (int position : mask) {
                sum += inputNulls.value(position) ? 0 : inputValues.value(position);
            }
        }

        stateNulls.values()[group] = false;
        stateVector.values()[group] += sum;
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        F64Vector stateVector = (F64Vector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        I64Vector groupVector = (I64Vector) groups;
        VectorAccess.DoubleValues inputValues = VectorAccess.doubleValues(streams.values(inputColumn));
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

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

    private static void accumulate(F64Vector state, BooleanVector stateNulls, int group, VectorAccess.DoubleValues inputValues, VectorAccess.BooleanValues inputNulls, int position)
    {
        stateNulls.values()[group] = false;
        state.values()[group] += inputNulls.value(position) ? 0 : inputValues.value(position);
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return state;
    }
}
