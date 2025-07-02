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

@Deprecated
public class I32VectorWithNulls
        implements FlatVector
{
    private final boolean[] nulls;
    private final int[] values;

    public I32VectorWithNulls(int size)
    {
        this(new boolean[size], new int[size]);
    }

    I32VectorWithNulls(boolean[] nulls, int[] values)
    {
        this.nulls = nulls;
        this.values = values;
    }

    @Override
    public Vector copy(int size)
    {
        return new I32VectorWithNulls(
                java.util.Arrays.copyOf(nulls, size),
                java.util.Arrays.copyOf(values, size));
    }

    @Deprecated
    public boolean[] nulls()
    {
        return nulls;
    }

    public int[] values()
    {
        return values;
    }

    @Override
    public Object valueAt(int position)
    {
        return values[position];
    }

    @Override
    public int length()
    {
        return values.length;
    }
}
