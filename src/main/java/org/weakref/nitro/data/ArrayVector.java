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
        copy.setElements(allocator.copyStreams(allocationContext, elements, nestedPositions(positions, positions.length, totalElements)));
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        for (int position : mask) {
            existing = copySinglePositionInto(allocator, allocationContext, existing, position, position, length());
        }
        return existing;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        ArrayVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof ArrayVector vector ? vector : null, ArrayVector.class, size, ArrayVector::new);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalElements = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int valueLength = length(sourcePosition);
            currentOffset += valueLength;
            totalElements += valueLength;
        }
        output.offsets()[outputStart + sourceCount] = currentOffset;
        output.setElements(copyNestedStreams(
                allocator,
                allocationContext,
                existing instanceof ArrayVector vector ? vector.elements() : null,
                elements,
                nestedPositions(sourcePositions, sourceCount, totalElements),
                childOutputStart,
                currentOffset));
        return output;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        ArrayVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof ArrayVector vector ? vector : null, ArrayVector.class, size, ArrayVector::new);
        if (outputPosition == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputPosition];
        int valueLength = length(sourcePosition);
        output.offsets()[outputPosition] = childOutputStart;
        output.offsets()[outputPosition + 1] = childOutputStart + valueLength;
        output.setElements(copyNestedStreams(
                allocator,
                allocationContext,
                existing instanceof ArrayVector vector ? vector.elements() : null,
                elements,
                nestedPositions(new int[] {sourcePosition}, 1, valueLength),
                childOutputStart,
                childOutputStart + valueLength));
        return output;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        ArrayVector empty = allocator.allocateArray(allocationContext, 0);
        empty.setElements(emptyStreamsLike(allocator, allocationContext, elements));
        return empty;
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        ArrayVector result = allocator.allocateArray(allocationContext, VectorSupport.totalLength(rows));
        int outputStart = 0;
        for (Vector row : rows) {
            int rowLength = row.length();
            if (rowLength == 1) {
                result = (ArrayVector) row.copySinglePositionInto(allocator, allocationContext, result, 0, outputStart, result.length());
            }
            else if (rowLength > 1) {
                result = (ArrayVector) row.copyPositionsInto(allocator, allocationContext, result, VectorSupport.densePositions(rowLength), rowLength, outputStart, result.length());
            }
            outputStart += rowLength;
        }
        return result;
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
    public Object poolFamily()
    {
        return ArrayVector.class;
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
    public int childVectorCount()
    {
        return elements.vectorCount();
    }

    @Override
    public Vector childVector(int index)
    {
        return elements.vectorAt(index);
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        elements.asMap().values().forEach(consumer);
    }

    private int[] nestedPositions(int[] positions, int positionCount, int totalElements)
    {
        int[] result = new int[totalElements];
        int next = 0;
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            for (int element = offsets[position]; element < offsets[position + 1]; element++) {
                result[next++] = element;
            }
        }
        return result;
    }

    private Streams copyNestedStreams(Allocator allocator, Allocator.Context allocationContext, Streams existing, Streams source, int[] sourcePositions, int outputStart, int size)
    {
        Streams.Builder result = Streams.builder();
        for (var entry : source.asMap().entrySet()) {
            Vector existingVector = existing != null ? existing.getOrNull(entry.getKey()) : null;
            result.put(entry.getKey(), entry.getValue().copyPositionsInto(allocator, allocationContext, existingVector, sourcePositions, sourcePositions.length, outputStart, size));
        }
        return result.build();
    }

    private Streams emptyStreamsLike(Allocator allocator, Allocator.Context allocationContext, Streams source)
    {
        Streams.Builder result = Streams.builder();
        for (var entry : source.asMap().entrySet()) {
            result.put(entry.getKey(), entry.getValue().emptyLike(allocator, allocationContext));
        }
        return result.build();
    }
}
