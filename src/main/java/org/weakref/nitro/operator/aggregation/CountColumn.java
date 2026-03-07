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
import org.weakref.nitro.data.I64VectorWithNulls;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class CountColumn
        implements Accumulator
{
    private final int inputColumn;

    public CountColumn(int inputColumn)
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
        Arrays.fill(((I64VectorWithNulls) state).nulls(), offset, offset + length, false);
        Arrays.fill(((I64VectorWithNulls) state).values(), offset, offset + length, 0);
    }

    @Override
    public void accumulate(Vector state, int group, Mask mask, ColumnAccessor columns)
    {
        I64VectorWithNulls stateVector = (I64VectorWithNulls) state;
        boolean[] inputNulls = nulls(columns.column(inputColumn));

        for (int position : mask) {
            if (!isNull(inputNulls, position)) {
                stateVector.values()[group]++;
            }
        }
    }

    @Override
    public void accumulate(Vector state, Vector groups, Mask mask, ColumnAccessor columns)
    {
        I64VectorWithNulls stateVector = (I64VectorWithNulls) state;
        I64Vector groupVector = (I64Vector) groups;
        boolean[] inputNulls = nulls(columns.column(inputColumn));

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (!isNull(inputNulls, position)) {
                stateVector.values()[group]++;
            }
        }
    }

    @Override
    public Vector result(int maxGroup, Vector state, Vector output)
    {
        return state;
    }

    private static boolean[] nulls(Vector v)
    {
        return v instanceof I64VectorWithNulls iv ? iv.nulls() : null;
    }

    private static boolean isNull(boolean[] nulls, int position)
    {
        return nulls != null && nulls[position];
    }
}
