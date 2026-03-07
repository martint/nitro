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
        Vector input = columns.column(inputColumn);
        long[] inputValues = values(input);
        boolean[] inputNulls = nulls(input);

        for (int position : mask) {
            if (stateVector.nulls()[group]) {
                stateVector.values()[group] = inputValues[position];
                stateVector.nulls()[group] = isNull(inputNulls, position);
            }
        }
    }

    @Override
    public void accumulate(Vector state, Vector groups, Mask mask, ColumnAccessor columns)
    {
        I64VectorWithNulls stateVector = (I64VectorWithNulls) state;
        I64Vector groupVector = (I64Vector) groups;
        Vector input = columns.column(inputColumn);
        long[] inputValues = values(input);
        boolean[] inputNulls = nulls(input);

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (stateVector.nulls()[group]) {
                stateVector.values()[group] = inputValues[position];
                stateVector.nulls()[group] = isNull(inputNulls, position);
            }
        }
    }

    @Override
    public Vector result(int maxGroup, Vector state, Vector output)
    {
        return state;
    }

    private static long[] values(Vector v)
    {
        return switch (v) {
            case I64Vector iv -> iv.values();
            case I64VectorWithNulls iv -> iv.values();
            default -> throw new UnsupportedOperationException(v.getClass().getSimpleName());
        };
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
