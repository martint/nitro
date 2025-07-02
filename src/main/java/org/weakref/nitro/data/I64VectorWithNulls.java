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
package org.weakref.nitro.data;

import java.util.Arrays;

@Deprecated
public class I64VectorWithNulls
        implements FlatVector
{
    private final boolean[] nulls;
    private final long[] values;

    public I64VectorWithNulls(int size)
    {
        this(new boolean[size], new long[size]);
    }

    public I64VectorWithNulls(boolean[] nulls, long[] values)
    {
        // if nulls contains any true value, throw an exception
        for (boolean isNull : nulls) {
            if (isNull) {
                throw new IllegalArgumentException("Nulls array must not contain any true values");
            }
        }
        this.nulls = nulls;
        this.values = values;
    }

    @Override
    public Vector copy(int size)
    {
        return new I64VectorWithNulls(
                Arrays.copyOf(nulls, size),
                Arrays.copyOf(values, size));
    }

    @Deprecated
    public boolean[] nulls()
    {
        return nulls;
    }

    public long[] values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public Object valueAt(int position)
    {
        return values[position];
    }

    @Override
    public String toString()
    {
        return "I64Vector{" +
                "nulls=" + Arrays.toString(nulls) +
                ", values=" + Arrays.toString(values) +
                '}';
    }
}
