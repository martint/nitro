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
        int[] entryPositions = nestedPositions(positions, totalEntries);
        copy.setEntries(
                allocator.copyStreams(allocationContext, keys, entryPositions),
                allocator.copyStreams(allocationContext, values, entryPositions));
        return copy;
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
        return 2;
    }

    @Override
    public void forEachChildVector(Consumer<Vector> consumer)
    {
        keys.asMap().values().forEach(consumer);
        values.asMap().values().forEach(consumer);
    }

    private int[] nestedPositions(int[] positions, int totalEntries)
    {
        int[] result = new int[totalEntries];
        int next = 0;
        for (int position : positions) {
            for (int entry = offsets[position]; entry < offsets[position + 1]; entry++) {
                result[next++] = entry;
            }
        }
        return result;
    }
}
