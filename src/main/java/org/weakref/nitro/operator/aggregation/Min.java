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

import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

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

    @Override
    public Vector allocate(int size)
    {
        return new I64Vector(size);
    }

    @Override
    public void initialize(Vector state, int offset, int length)
    {
        Arrays.fill(((I64Vector) state).nulls(), offset, offset + length, true);
    }

    @Override
    public void accumulate(Vector state, int group, Mask mask, ColumnAccessor columns)
    {
        I64Vector stateVector = (I64Vector) state;
        I64Vector inputVector = (I64Vector) columns.column(inputColumn);

        for (int position : mask) {
            accumulate(stateVector, group, inputVector, position);
        }
    }

    private static void accumulate(I64Vector state, int group, I64Vector input, int position)
    {
        if (state.nulls()[group]) {
            state.values()[group] = input.values()[position];
            state.nulls()[group] = input.nulls()[position];
        }
        else if (!input.nulls()[position]) {
            state.values()[group] = Math.min(state.values()[group], input.values()[position]);
            state.nulls()[group] = false;
        }
    }

    @Override
    public void accumulate(Vector state, Vector groups, Mask mask, ColumnAccessor columns)
    {
        I64Vector stateVector = (I64Vector) state;
        I64Vector groupVector = (I64Vector) groups;
        I64Vector inputVector = (I64Vector) columns.column(inputColumn);

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            accumulate(stateVector, group, inputVector, position);
        }
    }

    @Override
    public Vector result(int maxGroup, Vector state, Vector output)
    {
        return state;
    }
}
