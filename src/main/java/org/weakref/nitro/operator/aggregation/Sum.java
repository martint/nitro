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

import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class Sum
        implements Accumulator
{
    private final int inputColumn;

    public Sum(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Vector allocate(int size)
    {
        return new I64VectorWithNulls(size);
    }

    @Override
    public void initialize(Vector state, int offset, int length)
    {
        Arrays.fill(((I64VectorWithNulls) state).nulls(), offset, offset + length, true);
    }

    @Override
    public void accumulate(Vector state, int group, Mask mask, ColumnAccessor columns)
    {
        I64VectorWithNulls stateVector = (I64VectorWithNulls) state;
        I64VectorWithNulls inputVector = (I64VectorWithNulls) columns.column(inputColumn);

        boolean[] nulls = inputVector.nulls();
        long[] values = inputVector.values();

        long sum = 0;
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
    public void accumulate(Vector state, Vector groups, Mask mask, ColumnAccessor columns)
    {
        I64VectorWithNulls stateVector = (I64VectorWithNulls) state;
        I64VectorWithNulls groupVector = (I64VectorWithNulls) groups;
        I64VectorWithNulls inputVector = (I64VectorWithNulls) columns.column(inputColumn);

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

    private static void accumulate(I64VectorWithNulls state, int group, I64VectorWithNulls input, int position)
    {
        state.nulls()[group] = false;
        state.values()[group] += input.nulls()[position] ? 0 : input.values()[position];
    }

    @Override
    public Vector result(int maxGroup, Vector state, Vector output)
    {
        return state;
    }
}
