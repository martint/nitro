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
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class BinaryVector
        implements FlatVector, RecyclableVectorStorage
{
    private static final String FIXED_WIDTH_TRAIT = "fixed_width";

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
    private final boolean recyclableStorage;
    private boolean storageReleased;
    private Set<Trait> traits = Set.of();
    // Object identity is not a content identity for pooled vectors. Consumers that retain derived state across
    // batches (for example dictionary-entry caches) pair this generation with the vector identity; it advances
    // whenever the allocator begins a new logical lifetime for the same backing storage.
    private long contentGeneration;
    private boolean contentImmutable;

    public BinaryVector(int positionCount, int byteCapacity)
    {
        this(positionCount, new int[positionCount + 1], new byte[byteCapacity]);
    }

    public BinaryVector(int positionCount, int[] offsets, byte[] data)
    {
        this(positionCount, offsets, data, false);
    }

    private BinaryVector(int positionCount, int[] offsets, byte[] data, boolean recyclableStorage)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        checkArgument(offsets.length == positionCount + 1, "offsets length (%s) does not match positionCount + 1 (%s)", offsets.length, positionCount + 1);
        this.positionCount = positionCount;
        this.offsets = offsets;
        this.data = data;
        this.recyclableStorage = recyclableStorage;
    }

    public int[] offsets()
    {
        return offsets;
    }

    public byte[] data()
    {
        return data;
    }

    @Override
    public long contentGeneration()
    {
        return contentGeneration;
    }

    /** Marks the current logical contents immutable until this vector begins another allocator lifetime. */
    @Override
    public BinaryVector freezeContent()
    {
        contentImmutable = true;
        return this;
    }

    @Override
    public boolean contentImmutable()
    {
        return contentImmutable;
    }

    @Override
    public long contentFingerprint()
    {
        // This is only a prefilter: equal fingerprints are always followed by hasSameContent(). Sample the domain
        // at fixed logical fractions so large dictionaries do not pay a serial byte-at-a-time hash on every batch;
        // an occasional collision costs one vectorized exact comparison but cannot change results.
        long hash = 0xcbf29ce484222325L;
        int usedBytes = offsets[positionCount];
        hash = (hash ^ positionCount) * 0x100000001b3L;
        hash = (hash ^ usedBytes) * 0x100000001b3L;
        hash = (hash ^ traits.hashCode()) * 0x100000001b3L;
        hash = sampleFingerprint(hash, offsets, offsets.length);
        hash = sampleFingerprint(hash, data, usedBytes);
        return hash == NO_CONTENT_FINGERPRINT ? hash + 1 : hash;
    }

    private static long sampleFingerprint(long hash, int[] values, int length)
    {
        int samples = Math.min(length, 16);
        for (int sample = 0; sample < samples; sample++) {
            int index = samples == 1 ? 0 : (int) ((long) sample * (length - 1) / (samples - 1));
            hash = (hash ^ values[index]) * 0x100000001b3L;
        }
        return hash;
    }

    private static long sampleFingerprint(long hash, byte[] values, int length)
    {
        int samples = Math.min(length, 16);
        for (int sample = 0; sample < samples; sample++) {
            int index = samples == 1 ? 0 : (int) ((long) sample * (length - 1) / (samples - 1));
            hash = (hash ^ (values[index] & 0xff)) * 0x100000001b3L;
        }
        return hash;
    }

    @Override
    public boolean hasSameContent(Vector other)
    {
        if (!(other instanceof BinaryVector binary) || positionCount != binary.positionCount || !traits.equals(binary.traits)) {
            return false;
        }
        if (!java.util.Arrays.equals(offsets, binary.offsets)) {
            return false;
        }
        int usedBytes = offsets[positionCount];
        return usedBytes == binary.offsets[binary.positionCount] &&
                java.util.Arrays.equals(data, 0, usedBytes, binary.data, 0, usedBytes);
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
                () -> allocateStorage(allocator.primitiveArrays(), positionCount, byteCapacity));
    }

    private static BinaryVector allocateStorage(PrimitiveArrayPool storagePool, int positionCount, int byteCapacity)
    {
        int[] offsets = storagePool.borrowInts(positionCount + 1);
        Arrays.fill(offsets, 0);
        byte[] data = storagePool.borrowBytesAtLeast(byteCapacity);
        return new BinaryVector(positionCount, offsets, data, true);
    }

    public static BinaryVector allocate(VectorAllocator allocator, int positionCount, int byteCapacity)
    {
        requireNonNull(allocator, "allocator is null");
        return allocator.allocatePooled(
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
        // Returning the same object starts a new producer fill even though no allocator release/re-borrow occurred.
        // Dictionary-derived caches pair identity with contentGeneration, so they must not mistake the next batch's
        // bytes for the previous logical contents merely because the existing capacity was sufficient.
        existing.contentGeneration++;
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

    /** Declares that every physical position occupies exactly {@code width} contiguous bytes. */
    public void setFixedWidth(int width)
    {
        checkArgument(width >= 0, "width is negative");
        addTrait(Trait.of(FIXED_WIDTH_TRAIT, width));
    }

    public OptionalInt fixedWidth()
    {
        Object width = traitValue(FIXED_WIDTH_TRAIT).orElse(null);
        return width == null ? OptionalInt.empty() : OptionalInt.of((int) width);
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
        checkArgument(!contentImmutable, "BinaryVector content is immutable");
        int start = offsets[position];
        checkArgument(start + sourceLength <= data.length, "BinaryVector byte capacity exceeded");
        System.arraycopy(source, sourceOffset, data, start, sourceLength);
        offsets[position + 1] = start + sourceLength;
        contentGeneration++;
    }

    public void setNull(int position)
    {
        checkArgument(!contentImmutable, "BinaryVector content is immutable");
        offsets[position + 1] = offsets[position];
        contentGeneration++;
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
    public boolean isVariableWidth()
    {
        return true;
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
        BinaryVector previous = (BinaryVector) existing;
        int outputLength = Math.max(positionCount, previous == null ? 0 : previous.length());
        int totalBytes = 0;
        for (int position = 0; position < outputLength; position++) {
            totalBytes = Math.addExact(
                    totalBytes,
                    mask.contains(position)
                            ? length(position)
                            : previous == null ? 0 : previous.length(position));
        }

        // Variable-width offsets form one cumulative chain, so updating an existing vector in place can overwrite
        // the start offset or bytes of an earlier branch when masks are interleaved. Rebuild the chain in logical
        // position order and preserve every inactive position from the previous result.
        BinaryVector target = allocate(allocator, allocationContext, outputLength, totalBytes);
        if (previous == null) {
            target.addTraits(traits);
        }
        else {
            Set<Trait> commonTraits = new LinkedHashSet<>(traits);
            commonTraits.retainAll(previous.traits());
            target.addTraits(commonTraits);
        }

        int currentOffset = 0;
        for (int position = 0; position < outputLength; position++) {
            target.offsets()[position] = currentOffset;
            BinaryVector source = mask.contains(position) ? this : previous;
            int valueLength = source == null ? 0 : source.length(position);
            if (valueLength == 0) {
                target.setNull(position);
            }
            else {
                target.setBytes(position, source.data(), source.startOffset(position), valueLength);
                currentOffset = target.endOffset(position);
            }
        }
        if (previous != null) {
            allocator.discard(allocationContext, previous);
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
        if (existing == null) {
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
        if (existing == null) {
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
        copy.contentGeneration++;
    }

    @Override
    public void clearForReuse()
    {
        contentGeneration++;
        contentImmutable = false;
        clearTraits();
        Arrays.fill(offsets, 0);
    }

    @Override
    public void releaseStorage(PrimitiveArrayPool storagePool)
    {
        requireNonNull(storagePool, "storagePool is null");
        if (!recyclableStorage || storageReleased) {
            return;
        }
        storageReleased = true;
        storagePool.release(offsets);
        storagePool.release(data);
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
        return 16;
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
