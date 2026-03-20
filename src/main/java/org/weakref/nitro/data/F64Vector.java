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
    public long retainedBytes()
    {
        return (long) values.length * Double.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        F64Vector copy = allocator.allocate(allocationContext, F64Vector.class, values.length, F64Vector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        F64Vector copy = allocator.allocate(allocationContext, F64Vector.class, positions.length, F64Vector::new);
        for (int index = 0; index < positions.length; index++) {
            copy.values()[index] = values[positions[index]];
        }
        return copy;
    }

    @Override
    public void copyInto(Vector target)
    {
        System.arraycopy(values, 0, ((F64Vector) target).values(), 0, values.length);
    }

    @Override
    public void clearForReuse()
    {
        Arrays.fill(values, 0);
    }

    @Override
    public PoolingMode poolingMode()
    {
        return PoolingMode.STANDARD;
    }

    @Override
    public String toString()
    {
        return "F64Vector" + Arrays.toString(values);
    }
}
