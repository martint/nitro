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

public class F64Vector
        implements FlatVector
{
    private final double[] values;

    public F64Vector(int size)
    {
        this(new double[size]);
    }

    public F64Vector(double[] values)
    {
        this.values = values;
    }

    @Override
    public Vector copy(int size)
    {
        return new F64Vector(Arrays.copyOf(values, size));
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

    @Override
    public Object valueAt(int position)
    {
        return values[position];
    }

    @Override
    public String toString()
    {
        return "F64Vector" + Arrays.toString(values);
    }
}
