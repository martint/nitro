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

public class CountAll
        implements Accumulator
{
    @Override
    public Vector allocate(int size)
    {
        return new I64Vector(size);
    }

    @Override
    public void initialize(Vector state, int offset, int length)
    {
        Arrays.fill(((I64Vector) state).nulls(), offset, offset + length, false);
        Arrays.fill(((I64Vector) state).values(), offset, offset + length, 0L);
    }

    @Override
    public void accumulate(Vector state, int group, Mask mask, ColumnAccessor columns)
    {
        I64Vector stateVector = (I64Vector) state;
        accumulate(stateVector, group, mask.count());
    }

    @Override
    public void accumulate(Vector state, Vector groups, Mask mask, ColumnAccessor columns)
    {
        I64Vector stateVector = (I64Vector) state;
        I64Vector groupVector = (I64Vector) groups;

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                int group = toIntExact(groupVector.values()[position]);
                stateVector.values()[group] += 1;
                stateVector.nulls()[group] = false;
            }
        }
        else {
            for (int position : mask) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, group, 1);
            }
        }
    }

    private static void accumulate(I64Vector stateVector, int group, int count)
    {
        stateVector.values()[group] += count;
        stateVector.nulls()[group] = false;
    }

    @Override
    public Vector result(int maxGroup, Vector state, Vector output)
    {
        return state;
    }
}
