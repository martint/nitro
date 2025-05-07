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

public class BooleanVector
        implements Vector
{
    private final boolean[] nulls;
    private final boolean[] values;

    public BooleanVector(int size)
    {
        this(new boolean[size], new boolean[size]);
    }

    public BooleanVector(boolean[] nulls, boolean[] values)
    {
        this.nulls = nulls;
        this.values = values;
    }

    @Override
    public Vector copy(int size)
    {
        return new BooleanVector(
                java.util.Arrays.copyOf(nulls, size),
                java.util.Arrays.copyOf(values, size));
    }

    public boolean[] nulls()
    {
        return nulls;
    }

    public boolean[] values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BooleanVector{");
        sb.append("nulls=").append(java.util.Arrays.toString(nulls));
        sb.append(", values=").append(java.util.Arrays.toString(values));
        sb.append('}');
        return sb.toString();
    }
}
