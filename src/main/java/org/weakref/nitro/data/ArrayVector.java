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

import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

public final class ArrayVector
        implements FlatVector
{
    private final int positionCount;
    private final int[] offsets;
    private Streams elements = Streams.empty();

    public ArrayVector(int positionCount)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;
        this.offsets = new int[positionCount + 1];
    }

    public int[] offsets()
    {
        return offsets;
    }

    public int startOffset(int position)
    {
        return offsets[position];
    }

    public int endOffset(int position)
    {
        return offsets[position + 1];
    }

    public int length(int position)
    {
        return endOffset(position) - startOffset(position);
    }

    public Streams elements()
    {
        return elements;
    }

    public Vector elementValues()
    {
        return elements.values();
    }

    public BooleanVector elementNulls()
    {
        return (BooleanVector) elements.getOrNull(Stream.NULLS);
    }

    public Vector elementStreamOrNull(Stream stream)
    {
        return elements.getOrNull(stream);
    }

    public void setElements(Streams elements)
    {
        this.elements = elements;
    }

    public void clearElements()
    {
        elements = Streams.empty();
    }

    @Override
    public int length()
    {
        return positionCount;
    }

    @Override
    public long retainedBytes()
    {
        return (long) offsets.length * Integer.BYTES;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        ArrayVector copy = allocator.allocateArray(allocationContext, positionCount);
        copyInto(copy);
        copy.setElements(allocator.copyStreams(allocationContext, elements));
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        ArrayVector copy = allocator.allocateArray(allocationContext, positions.length);
        int totalElements = 0;
        for (int index = 0; index < positions.length; index++) {
            copy.offsets()[index] = totalElements;
            totalElements += length(positions[index]);
        }
        copy.offsets()[positions.length] = totalElements;
        copy.setElements(allocator.copyStreams(allocationContext, elements, nestedPositions(positions, totalElements)));
        return copy;
    }

    @Override
    public void copyInto(Vector target)
    {
        ArrayVector arrayTarget = (ArrayVector) target;
        System.arraycopy(offsets, 0, arrayTarget.offsets(), 0, offsets.length);
        arrayTarget.setElements(elements);
    }

    @Override
    public void clearForReuse()
    {
        java.util.Arrays.fill(offsets, 0);
        clearElements();
    }

    @Override
    public PoolingMode poolingMode()
    {
        return PoolingMode.STANDARD;
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        elements.asMap().values().forEach(consumer);
    }

    private int[] nestedPositions(int[] positions, int totalElements)
    {
        int[] result = new int[totalElements];
        int next = 0;
        for (int position : positions) {
            for (int element = offsets[position]; element < offsets[position + 1]; element++) {
                result[next++] = element;
            }
        }
        return result;
    }
}
