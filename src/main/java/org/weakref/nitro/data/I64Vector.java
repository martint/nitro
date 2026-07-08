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

public class I64Vector
        implements FlatVector
{
    private final long[] values;

    public I64Vector(int size)
    {
        this(new long[size]);
    }

    public I64Vector(long[] values)
    {
        this.values = values;
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
    public long retainedBytes()
    {
        return (long) values.length * Long.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        I64Vector copy = allocator.allocate(allocationContext, I64Vector.class, values.length, I64Vector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        I64Vector copy = allocator.allocate(allocationContext, I64Vector.class, positions.length, I64Vector::new);
        for (int index = 0; index < positions.length; index++) {
            copy.values()[index] = values[positions[index]];
        }
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        I64Vector target = allocator.allocateOrGrow(allocationContext, (I64Vector) existing, I64Vector.class, values.length, I64Vector::new);
        for (int position : mask) {
            target.values()[position] = values[position];
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        I64Vector target = allocator.allocateOrGrow(allocationContext, (I64Vector) existing, I64Vector.class, size, I64Vector::new);
        for (int index = 0; index < sourceCount; index++) {
            target.values()[outputStart + index] = values[sourcePositions[index]];
        }
        return target;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        I64Vector target = allocator.allocateOrGrow(allocationContext, (I64Vector) existing, I64Vector.class, size, I64Vector::new);
        target.values()[outputPosition] = values[sourcePosition];
        return target;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocate(allocationContext, I64Vector.class, 0, I64Vector::new);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        I64Vector result = allocator.allocate(allocationContext, I64Vector.class, VectorSupport.totalLength(rows), I64Vector::new);
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
        System.arraycopy(values, 0, ((I64Vector) target).values(), 0, values.length);
    }

    @Override
    public void clearForReuse()
    {
        // No buffer clearing: consumers must only read positions the producer wrote (see Allocator contract).
    }

    @Override
    public Object poolFamily()
    {
        return I64Vector.class;
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
        return "I64Vector" + Arrays.toString(values);
    }
}
