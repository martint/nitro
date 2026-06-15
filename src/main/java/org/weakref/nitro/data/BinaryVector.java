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
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class BinaryVector
        implements FlatVector
{
    public static final class Trait
    {
        private final String name;
        private final Object value;

        private Trait(String name, Object value)
        {
            this.name = requireNonNull(name, "name is null");
            this.value = value;
        }

        public static Trait flag(String name)
        {
            return new Trait(name, null);
        }

        public static Trait of(String name, Object value)
        {
            return new Trait(name, value);
        }

        public String name()
        {
            return name;
        }

        public Optional<Object> value()
        {
            return Optional.ofNullable(value);
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }
            if (!(object instanceof Trait other)) {
                return false;
            }
            return name.equals(other.name) && Objects.equals(value, other.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, value);
        }

        @Override
        public String toString()
        {
            return value == null ? name : name + "=" + value;
        }
    }

    private final int positionCount;
    private final int[] offsets;
    private final byte[] data;
    private Set<Trait> traits = Set.of();

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

    public static BinaryVector allocate(Allocator allocator, Allocator.Context allocationContext, int positionCount, int byteCapacity)
    {
        return allocator.allocatePooled(
                allocationContext,
                poolFamily(positionCount),
                byteCapacity,
                false,
                BinaryVector.class,
                () -> new BinaryVector(positionCount, byteCapacity));
    }

    public static BinaryVector allocateOrGrow(Allocator allocator, Allocator.Context allocationContext, BinaryVector existing, int positionCount, int byteCapacity)
    {
        return allocateOrGrow(allocator, allocationContext, existing, positionCount, byteCapacity, -1);
    }

    /**
     * As {@link #allocateOrGrow(Allocator, Allocator.Context, BinaryVector, int, int)}, but a non-negative
     * {@code bytesUsed} tells how many bytes of {@code existing}'s data are live so the grow can copy exactly that
     * many instead of scanning the offsets backward from the (often far larger) allocated capacity. Incremental
     * single-position fillers already track this write offset, so passing it avoids an O(capacity) scan per grow.
     */
    public static BinaryVector allocateOrGrow(Allocator allocator, Allocator.Context allocationContext, BinaryVector existing, int positionCount, int byteCapacity, int bytesUsed)
    {
        if (existing == null) {
            return allocate(allocator, allocationContext, positionCount, Allocator.growthCapacity(byteCapacity));
        }
        if (existing.length() < positionCount || existing.byteCapacity() < byteCapacity) {
            BinaryVector grown = allocate(allocator, allocationContext, positionCount, Allocator.growthCapacity(byteCapacity));
            System.arraycopy(existing.offsets(), 0, grown.offsets(), 0, existing.length() + 1);
            int liveBytes = bytesUsed >= 0 ? bytesUsed : bytesUsed(existing);
            System.arraycopy(existing.data(), 0, grown.data(), 0, liveBytes);
            grown.addTraits(existing.traits());
            allocator.discard(allocationContext, existing);
            return grown;
        }
        return existing;
    }

    public Set<Trait> traits()
    {
        return traits;
    }

    public boolean hasTrait(Trait trait)
    {
        return traits.contains(requireNonNull(trait, "trait is null"));
    }

    public boolean hasTrait(String name)
    {
        requireNonNull(name, "name is null");
        return traits.stream()
                .anyMatch(trait -> trait.name().equals(name));
    }

    public Optional<Object> traitValue(String name)
    {
        requireNonNull(name, "name is null");
        return traits.stream()
                .filter(trait -> trait.name().equals(name))
                .findFirst()
                .flatMap(Trait::value);
    }

    public void addTrait(Trait trait)
    {
        Trait newTrait = requireNonNull(trait, "trait is null");
        if (traits.contains(newTrait)) {
            return;
        }
        if (traits.isEmpty()) {
            traits = Set.of(newTrait);
            return;
        }
        LinkedHashSet<Trait> merged = new LinkedHashSet<>(traits);
        merged.add(newTrait);
        traits = Set.copyOf(merged);
    }

    public void addTraits(Set<Trait> traits)
    {
        Set<Trait> newTraits = requireNonNull(traits, "traits is null");
        if (newTraits.isEmpty() || this.traits.equals(newTraits)) {
            return;
        }
        if (this.traits.isEmpty()) {
            this.traits = Set.copyOf(newTraits);
            return;
        }
        LinkedHashSet<Trait> merged = new LinkedHashSet<>(this.traits);
        if (merged.addAll(newTraits)) {
            this.traits = Set.copyOf(merged);
        }
    }

    public void clearTraits()
    {
        traits = Set.of();
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
        BinaryVector copy = allocate(allocator, allocationContext, positionCount, byteLength);
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
        BinaryVector copy = allocate(allocator, allocationContext, positions.length, totalBytes);
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

        BinaryVector target = allocateOrGrow(allocator, allocationContext, (BinaryVector) existing, positionCount, totalBytes);
        target.clearTraits();
        target.addTraits(traits);
        Arrays.fill(target.offsets(), 0);

        int currentOffset = 0;
        for (int position : mask) {
            target.offsets()[position] = currentOffset;
            int valueLength = length(position);
            if (valueLength == 0) {
                target.setNull(position);
            }
            else {
                target.setBytes(position, data, startOffset(position), valueLength);
                currentOffset = target.endOffset(position);
            }
        }
        return target;
    }

    @Override
    public Vector copyPositionsInto(Allocator allocator, Allocator.Context allocationContext, Vector existing, int[] sourcePositions, int sourceCount, int outputStart, int size)
    {
        int byteCapacity = 0;
        if (existing instanceof BinaryVector output && outputStart > 0) {
            byteCapacity = prepareWriteOffset(output, outputStart);
        }
        for (int index = 0; index < sourceCount; index++) {
            byteCapacity += length(sourcePositions[index]);
        }

        BinaryVector target = allocateOrGrow(allocator, allocationContext, (BinaryVector) existing, size, byteCapacity);
        if (outputStart == 0) {
            Arrays.fill(target.offsets(), 0);
            target.clearTraits();
            target.addTraits(traits);
        }
        else if (target.traits.isEmpty() && !traits.isEmpty()) {
            target.addTraits(traits);
        }

        int currentOffset = prepareWriteOffset(target, outputStart);
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
            byteCapacity += prepareWriteOffset(output, outputPosition);
        }

        BinaryVector target = allocateOrGrow(allocator, allocationContext, (BinaryVector) existing, size, byteCapacity);
        if (outputPosition == 0) {
            Arrays.fill(target.offsets(), 0);
            target.clearTraits();
            target.addTraits(traits);
        }
        else if (target.traits.isEmpty() && !traits.isEmpty()) {
            target.addTraits(traits);
        }

        int currentOffset = prepareWriteOffset(target, outputPosition);
        int valueLength = length(sourcePosition);
        if (valueLength == 0) {
            target.setNull(outputPosition);
        }
        else {
            target.setBytes(outputPosition, data, startOffset(sourcePosition), valueLength);
        }
        return target;
    }

    private static int prepareWriteOffset(BinaryVector target, int outputPosition)
    {
        if (outputPosition == 0) {
            return 0;
        }

        int[] offsets = target.offsets();
        int search = outputPosition;
        while (search > 0 && offsets[search] == 0) {
            search--;
        }
        int currentOffset = offsets[search];
        for (int index = search + 1; index <= outputPosition; index++) {
            offsets[index] = currentOffset;
        }
        return currentOffset;
    }

    private static int bytesUsed(BinaryVector vector)
    {
        int[] offsets = vector.offsets();
        for (int index = vector.length(); index > 0; index--) {
            if (offsets[index] != 0) {
                return offsets[index];
            }
        }
        return 0;
    }

    @Override
    public Vector emptyLike(Allocator allocator, Allocator.Context allocationContext)
    {
        BinaryVector empty = allocate(allocator, allocationContext, 0, 0);
        empty.addTraits(traits);
        return empty;
    }

    @Override
    public Vector materializeRows(Allocator allocator, Allocator.Context allocationContext, Vector[] rows)
    {
        int totalPositions = VectorSupport.totalLength(rows);
        BinaryVector result = allocate(allocator, allocationContext, totalPositions, 0);
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
