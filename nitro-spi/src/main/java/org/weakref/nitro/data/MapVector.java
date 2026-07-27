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

import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

public final class MapVector
        implements FlatVector
{
    private final int positionCount;
    private final int[] offsets;
    private Streams keys = Streams.empty();
    private Streams values = Streams.empty();

    public MapVector(int positionCount)
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

    public Streams keys()
    {
        return keys;
    }

    public Streams values()
    {
        return values;
    }

    public Vector keyValues()
    {
        return keys.values();
    }

    public Vector keyStreamOrNull(Stream stream)
    {
        return keys.getOrNull(stream);
    }

    public Vector valueValues()
    {
        return values.values();
    }

    public Vector valueStreamOrNull(Stream stream)
    {
        return values.getOrNull(stream);
    }

    public void setEntries(Streams keys, Streams values)
    {
        this.keys = keys;
        this.values = values;
    }

    public void clearEntries()
    {
        keys = Streams.empty();
        values = Streams.empty();
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
        MapVector copy = allocator.allocateMap(allocationContext, positionCount);
        copyInto(copy);
        copy.setEntries(
                allocator.copyStreams(allocationContext, keys),
                allocator.copyStreams(allocationContext, values));
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        MapVector copy = allocator.allocateMap(allocationContext, positions.length);
        int totalEntries = 0;
        for (int index = 0; index < positions.length; index++) {
            copy.offsets()[index] = totalEntries;
            totalEntries += length(positions[index]);
        }
        copy.offsets()[positions.length] = totalEntries;
        int[] entryPositions = nestedPositions(positions, positions.length, totalEntries);
        copy.setEntries(
                allocator.copyStreams(allocationContext, keys, entryPositions),
                allocator.copyStreams(allocationContext, values, entryPositions));
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
        MapVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof MapVector vector ? vector : null, MapVector.class, size, MapVector::new);
        if (outputStart == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputStart];
        int currentOffset = childOutputStart;
        int totalEntries = 0;
        for (int index = 0; index < sourceCount; index++) {
            int sourcePosition = sourcePositions[index];
            output.offsets()[outputStart + index] = currentOffset;
            int valueLength = length(sourcePosition);
            currentOffset += valueLength;
            totalEntries += valueLength;
        }
        output.offsets()[outputStart + sourceCount] = currentOffset;

        int[] entryPositions = nestedPositions(sourcePositions, sourceCount, totalEntries);
        output.setEntries(
                copyNestedStreams(allocator, allocationContext, existing instanceof MapVector vector ? vector.keys() : null, keys, entryPositions, childOutputStart, currentOffset),
                copyNestedStreams(allocator, allocationContext, existing instanceof MapVector vector ? vector.values() : null, values, entryPositions, childOutputStart, currentOffset));
        return output;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        MapVector output = allocator.reallocateIfNecessary(allocationContext, existing instanceof MapVector vector ? vector : null, MapVector.class, size, MapVector::new);
        if (outputPosition == 0) {
            output.offsets()[0] = 0;
        }

        int childOutputStart = output.offsets()[outputPosition];
        int valueLength = length(sourcePosition);
        output.offsets()[outputPosition] = childOutputStart;
        output.offsets()[outputPosition + 1] = childOutputStart + valueLength;

        int[] entryPositions = nestedPositions(new int[] {sourcePosition}, 1, valueLength);
        output.setEntries(
                copyNestedStreams(allocator, allocationContext, existing instanceof MapVector vector ? vector.keys() : null, keys, entryPositions, childOutputStart, childOutputStart + valueLength),
                copyNestedStreams(allocator, allocationContext, existing instanceof MapVector vector ? vector.values() : null, values, entryPositions, childOutputStart, childOutputStart + valueLength));
        return output;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        MapVector empty = allocator.allocateMap(allocationContext, 0);
        empty.setEntries(emptyStreamsLike(allocator, allocationContext, keys), emptyStreamsLike(allocator, allocationContext, values));
        return empty;
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        MapVector result = allocator.allocateMap(allocationContext, VectorSupport.totalLength(rows));
        int outputStart = 0;
        for (Vector row : rows) {
            int rowLength = row.length();
            if (rowLength == 1) {
                result = (MapVector) row.copySinglePositionInto(allocator, allocationContext, result, 0, outputStart, result.length());
            }
            else if (rowLength > 1) {
                result = (MapVector) row.copyPositionsInto(allocator, allocationContext, result, VectorSupport.densePositions(rowLength), rowLength, outputStart, result.length());
            }
            outputStart += rowLength;
        }
        return result;
    }

    @Override
    public void copyInto(Vector target)
    {
        MapVector mapTarget = (MapVector) target;
        System.arraycopy(offsets, 0, mapTarget.offsets(), 0, offsets.length);
        mapTarget.setEntries(keys, values);
    }

    @Override
    public void clearForReuse()
    {
        java.util.Arrays.fill(offsets, 0);
        clearEntries();
    }

    @Override
    public Object poolFamily()
    {
        return MapVector.class;
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
        return keys.vectorCount() + values.vectorCount();
    }

    @Override
    public Vector childVector(int index)
    {
        int keyCount = keys.vectorCount();
        return index < keyCount ? keys.vectorAt(index) : values.vectorAt(index - keyCount);
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        keys.asMap().values().forEach(consumer);
        values.asMap().values().forEach(consumer);
    }

    private int[] nestedPositions(int[] positions, int positionCount, int totalEntries)
    {
        int[] result = new int[totalEntries];
        int next = 0;
        for (int index = 0; index < positionCount; index++) {
            int position = positions[index];
            for (int entry = offsets[position]; entry < offsets[position + 1]; entry++) {
                result[next++] = entry;
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
