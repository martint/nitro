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
import org.weakref.nitro.data.F64VectorWithNulls;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;

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
        return Streams.ofValues((F64VectorWithNulls) allocator.allocate(allocationContext, size, F64VectorWithNulls::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        return Streams.ofValues((F64VectorWithNulls) allocator.allocateOrGrow(allocationContext, state.values(), size, F64VectorWithNulls::new));
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        Arrays.fill(((F64VectorWithNulls) state.values()).nulls(), offset, offset + length, true);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        F64VectorWithNulls stateVector = (F64VectorWithNulls) state.values();
        F64VectorWithNulls inputVector = (F64VectorWithNulls) streams.values(inputColumn);

        boolean[] nulls = inputVector.nulls();
        double[] values = inputVector.values();

        double sum = 0;
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                sum += nulls[position] ? 0 : values[position];
            }
        }
        else {
            for (int position : mask) {
                sum += nulls[position] ? 0 : values[position];
            }
        }

        stateVector.nulls()[group] = false;
        stateVector.values()[group] += sum;
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        F64VectorWithNulls stateVector = (F64VectorWithNulls) state.values();
        I64Vector groupVector = (I64Vector) groups;
        F64VectorWithNulls inputVector = (F64VectorWithNulls) streams.values(inputColumn);

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, group, inputVector, position);
            }
        }
        else {
            for (int position : mask) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, group, inputVector, position);
            }
        }
    }

    private static void accumulate(F64VectorWithNulls state, int group, F64VectorWithNulls input, int position)
    {
        state.nulls()[group] = false;
        state.values()[group] += input.nulls()[position] ? 0 : input.values()[position];
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return state;
    }
}
