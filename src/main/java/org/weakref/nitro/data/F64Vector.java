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

public class F64Vector
        implements Vector
{
    private final boolean[] nulls;
    private final double[] values;

    public F64Vector(int size)
    {
        this(new boolean[size], new double[size]);
    }

    F64Vector(boolean[] nulls, double[] values)
    {
        this.nulls = nulls;
        this.values = values;
    }

    @Override
    public Vector copy(int size)
    {
        return new F64Vector(
                java.util.Arrays.copyOf(nulls, size),
                java.util.Arrays.copyOf(values, size));
    }

    public boolean[] nulls()
    {
        return nulls;
    }

    public double[] values()
    {
        return values;
    }

    @Override
    public int length()
    {
        return values.length;
    }
}
