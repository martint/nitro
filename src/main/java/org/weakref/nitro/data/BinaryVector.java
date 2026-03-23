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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class BinaryVector
        implements FlatVector
{
    public enum Trait
    {
        UTF8_STRING,
        ASCII_ONLY,
    }

    private final int positionCount;
    private final int[] offsets;
    private final byte[] data;
    private final EnumSet<Trait> traits = EnumSet.noneOf(Trait.class);

    public BinaryVector(int positionCount, int byteCapacity)
    {
        this(positionCount, new int[positionCount + 1], new byte[byteCapacity]);
    }

    public BinaryVector(int positionCount, int[] offsets, byte[] data)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        checkArgument(offsets.length == positionCount + 1, "offsets length (%s) must equal positionCount + 1 (%s)", offsets.length, positionCount + 1);
        this.positionCount = positionCount;
        this.offsets = offsets;
        this.data = data;
    }

    public int[] offsets()
    {
        return offsets;
    }

    public byte[] data()
    {
        return data;
    }

    public static Object poolFamily(int positionCount)
    {
        return new PoolFamily(positionCount);
    }

    public Set<Trait> traits()
    {
        return Set.copyOf(traits);
    }

    public boolean hasTrait(Trait trait)
    {
        return traits.contains(requireNonNull(trait, "trait is null"));
    }

    public void addTrait(Trait trait)
    {
        traits.add(requireNonNull(trait, "trait is null"));
    }

    public void addTraits(Set<Trait> traits)
    {
        this.traits.addAll(requireNonNull(traits, "traits is null"));
    }

    public void clearTraits()
    {
        traits.clear();
    }

    public int byteCapacity()
    {
        return data.length;
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

    public byte[] copyBytes(int position)
    {
        return Arrays.copyOfRange(data, startOffset(position), endOffset(position));
    }

    public String utf8Value(int position)
    {
        return new String(data, startOffset(position), length(position), StandardCharsets.UTF_8);
    }

    public void setBytes(int position, byte[] source)
    {
        setBytes(position, source, 0, source.length);
    }

    public void setBytes(int position, byte[] source, int sourceOffset, int sourceLength)
    {
        int start = offsets[position];
        checkArgument(start + sourceLength <= data.length, "BinaryVector byte capacity exceeded");
        System.arraycopy(source, sourceOffset, data, start, sourceLength);
        offsets[position + 1] = start + sourceLength;
    }

    public void setNull(int position)
    {
        offsets[position + 1] = offsets[position];
    }

    @Override
    public int length()
    {
        return positionCount;
    }

    @Override
    public long retainedBytes()
    {
        return (long) offsets.length * Integer.BYTES + data.length;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext)
    {
        int byteLength = offsets[positionCount];
        BinaryVector copy = allocator.allocateBinary(allocationContext, positionCount, byteLength);
        copyInto(copy);
        return copy;
    }

    @Override
    public Vector copy(Allocator allocator, Allocator.Context allocationContext, int[] positions)
    {
        int totalBytes = 0;
        for (int position : positions) {
            totalBytes += length(position);
        }
        BinaryVector copy = allocator.allocateBinary(allocationContext, positions.length, totalBytes);
        copy.addTraits(traits);
        for (int index = 0; index < positions.length; index++) {
            int position = positions[index];
            int valueLength = length(position);
            if (valueLength == 0) {
                copy.setNull(index);
            }
            else {
                copy.setBytes(index, data, startOffset(position), valueLength);
            }
        }
        return copy;
    }

    @Override
    public Vector copyMasked(Allocator allocator, Allocator.Context allocationContext, Vector existing, Mask mask)
    {
        int totalBytes = 0;
        for (int position : mask) {
            totalBytes += length(position);
        }

        BinaryVector target = allocator.allocateOrGrowBinary(allocationContext, (BinaryVector) existing, positionCount, totalBytes);
        target.clearTraits();
        target.addTraits(traits);
        Arrays.fill(target.offsets(), 0);

        for (int position : mask) {
            int valueLength = length(position);
            if (valueLength == 0) {
                target.setNull(position);
            }
            else {
                target.setBytes(position, data, startOffset(position), valueLength);
            }
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        int byteCapacity = 0;
        if (existing instanceof BinaryVector output && outputStart > 0) {
            byteCapacity = output.offsets()[outputStart];
        }
        for (int index = 0; index < sourceCount; index++) {
            byteCapacity += length(sourcePositions[index]);
        }

        BinaryVector target = allocator.allocateOrGrowBinary(allocationContext, (BinaryVector) existing, size, byteCapacity);
        if (outputStart == 0) {
            Arrays.fill(target.offsets(), 0);
            target.clearTraits();
        }
        target.addTraits(traits);

        int currentOffset = target.offsets()[outputStart];
        for (int index = 0; index < sourceCount; index++) {
            int targetPosition = outputStart + index;
            target.offsets()[targetPosition] = currentOffset;
            int sourcePosition = sourcePositions[index];
            int valueLength = length(sourcePosition);
            if (valueLength == 0) {
                target.setNull(targetPosition);
            }
            else {
                target.setBytes(targetPosition, data, startOffset(sourcePosition), valueLength);
                currentOffset = target.endOffset(targetPosition);
            }
        }
        return target;
    }

    @Override
    public Vector copySinglePositionInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int sourcePosition, int outputPosition, int size)
    {
        int byteCapacity = length(sourcePosition);
        if (existing instanceof BinaryVector output && outputPosition > 0) {
            byteCapacity += output.offsets()[outputPosition];
        }

        BinaryVector target = allocator.allocateOrGrowBinary(allocationContext, (BinaryVector) existing, size, byteCapacity);
        if (outputPosition == 0) {
            Arrays.fill(target.offsets(), 0);
            target.clearTraits();
        }
        target.addTraits(traits);

        int currentOffset = target.offsets()[outputPosition];
        target.offsets()[outputPosition] = currentOffset;
        int valueLength = length(sourcePosition);
        if (valueLength == 0) {
            target.setNull(outputPosition);
        }
        else {
            target.setBytes(outputPosition, data, startOffset(sourcePosition), valueLength);
        }
        return target;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        BinaryVector empty = allocator.allocateBinary(allocationContext, 0, 0);
        empty.addTraits(traits);
        return empty;
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        int totalPositions = VectorSupport.totalLength(rows);
        BinaryVector result = allocator.allocateBinary(allocationContext, totalPositions, 0);
        result.addTraits(traits);
        int outputStart = 0;
        for (Vector row : rows) {
            int rowLength = row.length();
            if (rowLength == 1) {
                result = (BinaryVector) row.copySinglePositionInto(allocator, allocationContext, result, 0, outputStart, totalPositions);
            }
            else if (rowLength > 1) {
                result = (BinaryVector) row.copyPositionsInto(allocator, allocationContext, result, VectorSupport.densePositions(rowLength), rowLength, outputStart, totalPositions);
            }
            outputStart += rowLength;
        }
        return result;
    }

    @Override
    public void copyInto(Vector target)
    {
        BinaryVector copy = (BinaryVector) target;
        System.arraycopy(offsets, 0, copy.offsets(), 0, offsets.length);
        int byteLength = offsets[positionCount];
        System.arraycopy(data, 0, copy.data(), 0, byteLength);
        copy.addTraits(traits);
    }

    @Override
    public void clearForReuse()
    {
        clearTraits();
        Arrays.fill(offsets, 0);
        Arrays.fill(data, (byte) 0);
    }

    @Override
    public Object poolFamily()
    {
        return poolFamily(length());
    }

    @Override
    public int poolCapacity()
    {
        return byteCapacity();
    }

    @Override
    public int poolMaxRetained()
    {
        return 2;
    }

    @Override
    public String toString()
    {
        return "BinaryVector{positions=" + positionCount + ", byteCapacity=" + data.length + ", traits=" + traits + "}";
    }

    private record PoolFamily(int positionCount)
    {
    }
}
