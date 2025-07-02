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

public class First
        implements Accumulator
{
    private final int inputColumn;

    public First(int inputColumn)
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

        for (int position : mask) {
            accumulate(stateVector, group, inputVector, position);
        }
    }

    @Override
    public void accumulate(Vector state, Vector groups, Mask mask, ColumnAccessor columns)
    {
        I64VectorWithNulls stateVector = (I64VectorWithNulls) state;
        I64VectorWithNulls groupVector = (I64VectorWithNulls) groups;
        I64VectorWithNulls inputVector = (I64VectorWithNulls) columns.column(inputColumn);

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            accumulate(stateVector, group, inputVector, position);
        }
    }

    private static void accumulate(I64VectorWithNulls state, int group, I64VectorWithNulls input, int position)
    {
        if (state.nulls()[group]) {
            state.values()[group] = input.values()[position];
            state.nulls()[group] = input.nulls()[position];
        }
    }

    @Override
    public Vector result(int maxGroup, Vector state, Vector output)
    {
        return state;
    }
}
