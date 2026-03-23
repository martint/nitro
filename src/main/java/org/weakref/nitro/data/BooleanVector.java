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

public class BooleanVector
        implements FlatVector
{
    private final boolean[] values;

    public BooleanVector(int size)
    {
        this(new boolean[size]);
    }

    public BooleanVector(boolean[] values)
    {
        this.values = values;
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
    public long retainedBytes()
    {
        return values.length;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        BooleanVector copy = allocator.allocate(allocationContext, BooleanVector.class, values.length, BooleanVector::new);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        BooleanVector copy = allocator.allocate(allocationContext, BooleanVector.class, positions.length, BooleanVector::new);
        for (int index = 0; index < positions.length; index++) {
            copy.values()[index] = values[positions[index]];
        }
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        BooleanVector target = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, values.length, BooleanVector::new);
        for (int position : mask) {
            target.values()[position] = values[position];
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        BooleanVector target = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, size, BooleanVector::new);
        for (int index = 0; index < sourceCount; index++) {
            target.values()[outputStart + index] = values[sourcePositions[index]];
        }
        return target;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        BooleanVector target = allocator.allocateOrGrow(allocationContext, (BooleanVector) existing, BooleanVector.class, size, BooleanVector::new);
        target.values()[outputPosition] = values[sourcePosition];
        return target;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        return allocator.allocate(allocationContext, BooleanVector.class, 0, BooleanVector::new);
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        BooleanVector result = allocator.allocate(allocationContext, BooleanVector.class, VectorSupport.totalLength(rows), BooleanVector::new);
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
        System.arraycopy(values, 0, ((BooleanVector) target).values(), 0, values.length);
    }

    @Override
    public void clearForReuse()
    {
        Arrays.fill(values, false);
    }

    @Override
    public Object poolFamily()
    {
        return BooleanVector.class;
    }

    @Override
    public int poolCapacity()
    {
        return length();
    }

    @Override
    public int poolMaxRetained()
    {
        return 2;
    }

    @Override
    public String toString()
    {
        return "BooleanVector" + Arrays.toString(values);
    }
}
