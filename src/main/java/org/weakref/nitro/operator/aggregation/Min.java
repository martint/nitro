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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class Min
        implements Accumulator
{
    private final int inputColumn;

    public Min(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    public int inputColumn()
    {
        return inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        I64Vector values = allocator.allocateOrGrow(allocationContext, (I64Vector) state.values(), I64Vector.class, size, I64Vector::new);
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
        I64Vector stateValues = (I64Vector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        VectorAccess.LongValues inputValues = VectorAccess.longValues(streams.values(inputColumn));
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            if (!inputNulls.value(position)) {
                if (stateNulls.values()[group]) {
                    stateValues.values()[group] = inputValues.value(position);
                    stateNulls.values()[group] = false;
                }
                else {
                    stateValues.values()[group] = Math.min(stateValues.values()[group], inputValues.value(position));
                }
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        I64Vector stateValues = (I64Vector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        I64Vector groupVector = (I64Vector) groups;
        VectorAccess.LongValues inputValues = VectorAccess.longValues(streams.values(inputColumn));
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (!inputNulls.value(position)) {
                if (stateNulls.values()[group]) {
                    stateValues.values()[group] = inputValues.value(position);
                    stateNulls.values()[group] = false;
                }
                else {
                    stateValues.values()[group] = Math.min(stateValues.values()[group], inputValues.value(position));
                }
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return state;
    }
}
