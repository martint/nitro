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
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        F64Vector target = allocator.allocateOrGrow(allocationContext, (F64Vector) existing, F64Vector.class, values.length, F64Vector::new);
        for (int position : mask) {
            target.values()[position] = values[position];
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        F64Vector target = allocator.allocateOrGrow(allocationContext, (F64Vector) existing, F64Vector.class, size, F64Vector::new);
        for (int index = 0; index < sourceCount; index++) {
            target.values()[outputStart + index] = values[sourcePositions[index]];
        }
        return target;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        F64Vector target = allocator.allocateOrGrow(allocationContext, (F64Vector) existing, F64Vector.class, size, F64Vector::new);
        target.values()[outputPosition] = values[sourcePosition];
        return target;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocate(allocationContext, F64Vector.class, 0, F64Vector::new);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        F64Vector result = allocator.allocate(allocationContext, F64Vector.class, VectorSupport.totalLength(rows), F64Vector::new);
        int outputStart = 0;
        for (Vector row : rows) {
            int rowLength = row.length();
            if (rowLength == 1) {
                row.copySinglePositionInto(allocator, allocationContext, result, 0, outputStart, result.length());
            }
            else if (rowLength > 1) {
                row.copyPositionsInto(allocator, allocationContext, result, VectorSupport.densePositions(rowLength), rowLength, outputStart, result.length());
            }
            outputStart += rowLength;
        }
        return result;
    }

    @Override
    public void copyInto(Vector target)
    {
        System.arraycopy(values, 0, ((F64Vector) target).values(), 0, values.length);
    }

    @Override
    public void clearForReuse()
    {
        // No buffer clearing: consumers must only read positions the producer wrote (see Allocator contract).
    }

    @Override
    public Object poolFamily()
    {
        return F64Vector.class;
    }

    @Override
    public int poolCapacity()
    {
        return length();
    }

    @Override
    public int poolMaxRetained()
    {
        return 16;
    }

    @Override
    public String toString()
    {
        return "F64Vector" + Arrays.toString(values);
    }
}
