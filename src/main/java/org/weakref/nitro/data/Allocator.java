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

import org.weakref.nitro.core.execution.MemoryReservation;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

// TODO: support hierarchical contexts
public class Allocator
        implements AutoCloseable
{
    private final Map<Context, ContextState> states = new HashMap<>();
    private final Map<Object, PoolState> pools = new HashMap<>();
    private final Map<Object, SharedResourceState> sharedResources = new HashMap<>();
    private final Map<Integer, BooleanVector> allFalseBooleanVectors = new HashMap<>();
    private final Set<ContextState> pendingCompatibilityStates = Collections.newSetFromMap(new IdentityHashMap<>());
    private final AllocationResources allocationResources;
    private final EngineResources engineResources;
    private final PrimitiveArrayPool primitiveArrays;
    private final AllocatorPolicy policy;
    private final MemoryReservation memoryReservation;
    private Context lastContext;
    private ContextState lastContextState;
    private CompletableFuture<Void> memoryBlocked = CompletableFuture.completedFuture(null);
    private long residentBytes;
    private boolean closed;

    public Allocator(EngineResources engineResources)
    {
        this(engineResources, null);
    }

    public Allocator(EngineResources engineResources, MemoryReservation memoryReservation)
    {
        this.engineResources = requireNonNull(engineResources, "engineResources is null");
        this.allocationResources = engineResources.allocationResources();
        this.primitiveArrays = allocationResources.primitiveArrays();
        this.policy = allocationResources.allocatorPolicy();
        this.memoryReservation = memoryReservation;
    }

    public Allocator(AllocationResources allocationResources)
    {
        this(allocationResources, null);
    }

    public Allocator(AllocationResources allocationResources, MemoryReservation memoryReservation)
    {
        this.engineResources = null;
        this.allocationResources = requireNonNull(allocationResources, "allocationResources is null");
        this.primitiveArrays = allocationResources.primitiveArrays();
        this.policy = allocationResources.allocatorPolicy();
        this.memoryReservation = memoryReservation;
    }

    public EngineResources engineResources()
    {
        if (engineResources == null) {
            throw new IllegalStateException("Engine resources are not configured for this allocator");
        }
        return engineResources;
    }

    public PrimitiveArrayPool primitiveArrays()
    {
        return primitiveArrays;
    }

    public PrimitiveArrayPool nativeBuffers()
    {
        return allocationResources.nativeBuffers();
    }

    public VectorAllocator vectorAllocator(Context context)
    {
        return new ScopedVectorAllocator(this, requireNonNull(context, "context is null"));
    }

    /**
     * Calculates the capacity of a vector that can hold the desired size, plus some extra space.
     * The extra space is roughly ~2x for small vector sizes and decreases logarithmically as the size of the vector increases
     * to avoid over-allocating too much unused space.
     */
    public static int computeCapacity(int desiredSize)
    {
        if (desiredSize <= 0) {
            return 0;
        }
        // TODO: verify formula
        double growthFactor = 1 + 1.0 / (Math.log(desiredSize + 1) - 6);
        return Math.max(desiredSize, (int) (desiredSize + desiredSize * growthFactor));
    }

    public <T extends Vector> T allocate(Context context, Class<T> vectorType, int size, IntFunction<T> allocator)
    {
        if (policy.directVectorFactory()) {
            ContextState state = state(context);
            T vector = state.borrowVector(vectorType, size, true, vectorType);
            boolean reused = vector != null;
            if (!reused) {
                vector = requireNonNull(allocator.apply(size), "allocator returned null");
            }
            else {
                vector.clearForReuse();
            }
            state.trackVector(vector, reused);
            return vector;
        }
        return allocatePooled(context, vectorType, size, true, vectorType, () -> allocator.apply(size));
    }

    public <T extends Vector> T adopt(Context context, T vector)
    {
        state(context).trackVector(requireNonNull(vector, "vector is null"), false);
        return vector;
    }

    public DictionaryVector allocateDictionary(Context context, int[] ids, Vector values)
    {
        return allocateDictionary(context, ids, ids.length, values);
    }

    public DictionaryVector allocateDictionary(Context context, int[] ids, int length, Vector values)
    {
        DictionaryVector vector = DictionaryVector.ofTrustedIds(ids, length, values);
        state(context).trackVector(vector, false);
        return vector;
    }

    public RleVector allocateRle(Context context, int[] counts, Vector values)
    {
        RleVector vector = new RleVector(Arrays.copyOf(counts, counts.length), values);
        state(context).trackVector(vector, false);
        return vector;
    }

    public RleVector allocateSingleRunRle(Context context, int count, Vector value)
    {
        if (!policy.directSingleRunRle()) {
            return allocateRle(context, new int[] {count}, value);
        }
        RleVector vector = new RleVector(new int[] {count}, value);
        state(context).trackVector(vector, false);
        return vector;
    }

    public Streams reuseOrCreateStreams(Streams existing, Vector values, Vector nulls, Vector errors)
    {
        return Streams.reuseOrCreate(policy.reuseTransportTuples(), existing, values, nulls, errors);
    }

    public Streams reuseValuesAndNulls(Streams existing, Vector values, BooleanVector nulls)
    {
        return reuseOrCreateStreams(existing, values, nulls, null);
    }

    /**
     * Returns a read-only Boolean vector of {@code length} false values.
     *
     * <p>The default representation is an allocator-wide immutable dense constant. It is deliberately not
     * tracked by an allocation context: callers borrow it, may share it across streams and batches, and may pass it to
     * {@link #release(Context, Vector)}, which is a no-op for the unowned constant tree. Caching by logical length makes
     * repeated batches allocation-free while keeping constant metadata independent of operator implementations.
     *
     * <p>The compatibility control returns the former owned dense representation for adjacent benchmark comparisons.
     */
    public Vector borrowAllFalseBoolean(Context context, int length)
    {
        if (!policy.sharedAllFalseBoolean()) {
            BooleanVector vector = allocate(context, BooleanVector.class, length, BooleanVector::new);
            vector.markAllFalse();
            return vector;
        }
        return allFalseBooleanVectors.computeIfAbsent(length, size -> {
            BooleanVector value = new BooleanVector(size);
            value.markAllFalse();
            reserveResident(value.retainedBytes());
            return value;
        });
    }

    public ArrayVector allocateArray(Context context, int positionCount)
    {
        return allocate(context, ArrayVector.class, positionCount, ArrayVector::new);
    }

    public MapVector allocateMap(Context context, int positionCount)
    {
        return allocate(context, MapVector.class, positionCount, MapVector::new);
    }

    public <T extends Vector> T allocateOrGrow(Context context, T vector, Class<T> vectorType, int size, IntFunction<T> vectorAllocator)
    {
        if (vector == null) {
            return allocate(context, vectorType, size, vectorAllocator);
        }
        if (vector.length() < size) {
            T grown = allocate(context, vectorType, size, vectorAllocator);
            vector.copyInto(grown);
            discardVector(context, vector);
            return grown;
        }
        return vector;
    }

    public <T extends Vector> T allocatePooled(Context context, Object poolFamily, int minimumPoolCapacity, boolean exactCapacityMatch, Class<T> vectorType, Supplier<T> allocator)
    {
        ContextState state = state(context);
        T vector = state.borrowVector(poolFamily, minimumPoolCapacity, exactCapacityMatch, vectorType);
        boolean reused = vector != null;
        if (!reused) {
            vector = requireNonNull(allocator.get(), "allocator returned null");
        }
        else {
            vector.clearForReuse();
        }
        state.trackVector(vector, reused);
        return vector;
    }

    /**
     * Borrows a vector with ceiling-capacity semantics without constructing a capturing factory on the hot path.
     *
     * <p>This form is intended for transient scratch whose logical length is carried separately from its backing
     * capacity. A producer can request the exact logical minimum while geometrically sizing only a pool miss; later
     * batches reuse the smallest adequate buffer even when their cardinalities differ slightly.
     */
    public <T extends Vector> T allocatePooled(
            Context context,
            Object poolFamily,
            int minimumPoolCapacity,
            boolean exactCapacityMatch,
            Class<T> vectorType,
            int allocationSize,
            IntFunction<T> allocator)
    {
        ContextState state = state(context);
        T vector = state.borrowVector(poolFamily, minimumPoolCapacity, exactCapacityMatch, vectorType);
        boolean reused = vector != null;
        if (!reused) {
            vector = requireNonNull(allocator.apply(allocationSize), "allocator returned null");
        }
        else {
            vector.clearForReuse();
        }
        state.trackVector(vector, reused);
        return vector;
    }

    public static int growthCapacity(int desiredSize)
    {
        if (desiredSize <= 0) {
            return 0;
        }
        return Math.max(desiredSize, computeCapacity(desiredSize));
    }

    /**
     * Acquires an allocator-scoped resource shared by independently constructed operators in one query.
     *
     * <p>Operators retain ordinary ownership through the returned lease. The resource closes only after the last
     * lease closes, so a build-side pipeline may publish reusable state that remains valid for a later probe-side
     * pipeline without introducing a process-global cache or bespoke operator coordination.
     */
    public synchronized <T extends AutoCloseable> SharedResource<T> acquireSharedResource(Object key, Supplier<T> factory)
    {
        requireNonNull(key, "key is null");
        requireNonNull(factory, "factory is null");
        SharedResourceState state = sharedResources.get(key);
        if (state == null) {
            state = new SharedResourceState(requireNonNull(factory.get(), "factory returned null"));
            sharedResources.put(key, state);
        }
        state.references++;
        @SuppressWarnings("unchecked")
        T value = (T) state.value;
        return new SharedResource<>(this, key, value);
    }

    private synchronized void releaseSharedResource(Object key)
    {
        SharedResourceState state = sharedResources.get(key);
        if (state == null || state.references <= 0) {
            throw new IllegalStateException("Shared resource is not acquired");
        }
        state.references--;
        if (state.references != 0) {
            return;
        }
        sharedResources.remove(key);
        try {
            state.value.close();
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to close shared resource", e);
        }
    }

    public <T extends Vector> T reallocateIfNecessary(Context context, T vector, Class<T> vectorType, int count, IntFunction<T> vectorAllocator)
    {
        if (vector == null) {
            return allocate(context, vectorType, count, vectorAllocator);
        }

        if (vector.length() < count) {
            T grown = allocate(context, vectorType, count, vectorAllocator);
            vector.copyInto(grown);
            discardVector(context, vector);
            return grown;
        }

        return vector;
    }

    public Mask allocateAllMask(Context context, int size)
    {
        ContextState state = state(context);
        Mask mask = state.borrowMask(0);
        boolean reused = mask != null;
        if (!reused) {
            mask = Mask.all(size);
        }
        else {
            mask.selectAll(size);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    public Mask allocateRangeMask(Context context, int start, int length)
    {
        ContextState state = state(context);
        int requiredCapacity = start == 0 ? 0 : length;
        Mask mask = state.borrowMask(requiredCapacity);
        boolean reused = mask != null;
        if (!reused) {
            mask = Mask.range(start, length);
        }
        else if (start == 0) {
            mask.selectAll(length);
        }
        else {
            int[] positions = mask.positionsArrayForOverwrite(length);
            for (int index = 0; index < length; index++) {
                positions[index] = start + index;
            }
            mask.setSelection(start + length, length, false);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    public Mask allocateSparseMask(Context context, int[] activePositions, int totalPositions)
    {
        return allocateSparseMask(context, activePositions, activePositions.length, totalPositions);
    }

    public Mask copyMask(Context context, Mask source)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(source.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = source.copy();
        }
        else {
            copyMask(result, source);
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask constantComparisonMask(Context context, Mask source, long[] values, long literal, Mask.ComparisonOperator operator, boolean[] nulls)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(source.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = source.copy();
        }
        else {
            copyMask(result, source);
        }
        result.retainConstantComparison(values, literal, operator, nulls);
        state.trackMask(result, reused);
        return result;
    }

    public Mask constantComparisonMask(Context context, Mask source, int[] values, long literal, Mask.ComparisonOperator operator, boolean[] nulls)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(source.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = source.copy();
        }
        else {
            copyMask(result, source);
        }
        result.retainConstantComparison(values, literal, operator, nulls);
        state.trackMask(result, reused);
        return result;
    }

    public Mask allocateSparseMask(Context context, int[] activePositions, int selectedCount, int totalPositions)
    {
        if (selectedCount == 0) {
            return allocateEmptyMask(context, totalPositions);
        }
        ContextState state = state(context);
        Mask mask = state.borrowMask(selectedCount);
        boolean reused = mask != null;
        if (!reused) {
            if (policy.singleCopySparseMasks() && selectedCount == totalPositions && isAllPositions(activePositions, totalPositions)) {
                mask = Mask.all(totalPositions);
            }
            else if (policy.singleCopySparseMasks()) {
                mask = Mask.sparseTrusted(Arrays.copyOf(activePositions, selectedCount), selectedCount, totalPositions);
            }
            else if (selectedCount == activePositions.length) {
                mask = Mask.sparse(activePositions, totalPositions);
            }
            else {
                mask = Mask.sparse(Arrays.copyOf(activePositions, selectedCount), totalPositions);
            }
        }
        else if (selectedCount == totalPositions && isAllPositions(activePositions, totalPositions)) {
            mask.selectAll(totalPositions);
        }
        else {
            int[] positions = mask.positionsArrayForOverwrite(selectedCount);
            System.arraycopy(activePositions, 0, positions, 0, selectedCount);
            mask.setSelection(totalPositions, selectedCount, false);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    public Mask allocateEmptyMask(Context context, int totalPositions)
    {
        ContextState state = state(context);
        Mask mask = state.borrowMask(0);
        boolean reused = mask != null;
        if (!reused) {
            mask = totalPositions == 0 ? Mask.all(0) : Mask.none(totalPositions);
        }
        else if (totalPositions == 0) {
            mask.selectAll(0);
        }
        else {
            mask.clear(totalPositions);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    /**
     * Borrows a sparse mask whose position storage the caller will fill directly. The returned mask already has the
     * requested logical size/count; callers must overwrite {@code [0, selectedCount)} before exposing it. This avoids
     * allocating a temporary exact-sized position array only to copy it into a pooled mask.
     */
    public Mask allocateUninitializedSparseMask(Context context, int selectedCount, int totalPositions)
    {
        if (selectedCount < 0 || selectedCount > totalPositions) {
            throw new IllegalArgumentException("Invalid selected count: " + selectedCount + " for size " + totalPositions);
        }
        ContextState state = state(context);
        Mask mask = state.borrowMask(selectedCount);
        boolean reused = mask != null;
        if (selectedCount == totalPositions) {
            if (!reused) {
                mask = Mask.all(totalPositions);
            }
            else {
                mask.selectAll(totalPositions);
            }
            state.trackMask(mask, reused);
            return mask;
        }
        if (!reused) {
            mask = Mask.sparse(new int[selectedCount], totalPositions);
        }
        else {
            mask.positionsArrayForOverwrite(selectedCount);
            mask.setSelection(totalPositions, selectedCount, false);
        }
        state.trackMask(mask, reused);
        return mask;
    }

    public Mask intersectMask(Context context, Mask mask, BooleanVector other)
    {
        if (other.length() == 0 || mask.none()) {
            return allocateEmptyMask(context, mask.size());
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(mask.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = mask.and(other);
        }
        else {
            int[] positions = result.positionsArrayForOverwrite(mask.selectedCount());
            int selectedCount = 0;
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    if (other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            else {
                for (int index = 0; index < mask.selectedCount(); index++) {
                    int position = mask.position(index);
                    if (other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            if (selectedCount == mask.size() && isAllPositions(positions, selectedCount)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), selectedCount, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask intersectMask(Context context, Mask left, Mask right)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(Math.min(left.selectedCount(), right.selectedCount()));
        boolean reused = result != null;
        if (!reused) {
            result = left.difference(left.difference(right));
        }
        else if (left.none() || right.none()) {
            result.clear(left.size());
        }
        else if (left.all()) {
            copyMask(result, right);
        }
        else if (right.all()) {
            copyMask(result, left);
        }
        else {
            int[] positions = result.positionsArrayForOverwrite(Math.min(left.selectedCount(), right.selectedCount()));
            int leftIndex = 0;
            int rightIndex = 0;
            int outputIndex = 0;
            while (leftIndex < left.selectedCount() && rightIndex < right.selectedCount()) {
                int leftPosition = left.position(leftIndex);
                int rightPosition = right.position(rightIndex);
                if (leftPosition < rightPosition) {
                    leftIndex++;
                }
                else if (leftPosition > rightPosition) {
                    rightIndex++;
                }
                else {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                    rightIndex++;
                }
            }
            if (outputIndex == left.size() && isAllPositions(positions, outputIndex)) {
                result.selectAll(left.size());
            }
            else {
                result.setSelection(left.size(), outputIndex, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask differenceMask(Context context, Mask left, Mask right)
    {
        ContextState state = state(context);
        int requiredCapacity = policy.complementDifferenceMasks() && left.all() && !right.all() ? right.selectedCount() : left.selectedCount();
        Mask result = state.borrowMask(requiredCapacity);
        boolean reused = result != null;
        if (!reused) {
            result = policy.complementDifferenceMasks() || !left.all() || right.all()
                    ? left.difference(right)
                    : materializedAllDifference(left, right);
        }
        else if (right.none() || left.none()) {
            copyMask(result, left);
        }
        else if (right.all()) {
            result.clear(left.size());
        }
        else if (left.all()) {
            if (policy.complementDifferenceMasks()) {
                int[] positions = result.positionsArrayForOverwrite(right.selectedCount());
                for (int index = 0; index < right.selectedCount(); index++) {
                    positions[index] = right.position(index);
                }
                result.setExclusion(left.size(), right.selectedCount());
            }
            else {
                fillMaterializedAllDifference(result, left, right);
            }
        }
        else {
            int[] positions = result.positionsArrayForOverwrite(left.selectedCount());
            int leftIndex = 0;
            int rightIndex = 0;
            int outputIndex = 0;
            while (leftIndex < left.selectedCount() && rightIndex < right.selectedCount()) {
                int leftPosition = left.position(leftIndex);
                int rightPosition = right.position(rightIndex);
                if (leftPosition < rightPosition) {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                }
                else if (leftPosition > rightPosition) {
                    rightIndex++;
                }
                else {
                    leftIndex++;
                    rightIndex++;
                }
            }
            while (leftIndex < left.selectedCount()) {
                positions[outputIndex++] = left.position(leftIndex++);
            }
            result.setSelection(left.size(), outputIndex, false);
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask differenceMask(Context context, Mask mask, BooleanVector other)
    {
        if (other.length() == 0 || mask.none()) {
            return allocateSparseMask(context, positions(mask), mask.size());
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(mask.selectedCount());
        boolean reused = result != null;
        if (!reused) {
            result = mask.andNot(other);
        }
        else {
            int[] positions = result.positionsArrayForOverwrite(mask.selectedCount());
            int selectedCount = 0;
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    if (!other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            else {
                for (int index = 0; index < mask.selectedCount(); index++) {
                    int position = mask.position(index);
                    if (!other.values()[position]) {
                        positions[selectedCount++] = position;
                    }
                }
            }
            if (selectedCount == mask.size() && isAllPositions(positions, selectedCount)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), selectedCount, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask unionMask(Context context, Mask left, Mask right)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(Math.min(left.size(), left.selectedCount() + right.selectedCount()));
        boolean reused = result != null;
        if (!reused) {
            result = left.or(right);
        }
        else if (left.all() || right.all()) {
            result.selectAll(left.size());
        }
        else if (left.none()) {
            copyMask(result, right);
        }
        else if (right.none()) {
            copyMask(result, left);
        }
        else {
            int[] positions = result.positionsArrayForOverwrite(Math.min(left.size(), left.selectedCount() + right.selectedCount()));
            int leftIndex = 0;
            int rightIndex = 0;
            int outputIndex = 0;
            while (leftIndex < left.selectedCount() && rightIndex < right.selectedCount()) {
                int leftPosition = left.position(leftIndex);
                int rightPosition = right.position(rightIndex);
                if (leftPosition < rightPosition) {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                }
                else if (leftPosition > rightPosition) {
                    positions[outputIndex++] = rightPosition;
                    rightIndex++;
                }
                else {
                    positions[outputIndex++] = leftPosition;
                    leftIndex++;
                    rightIndex++;
                }
            }
            while (leftIndex < left.selectedCount()) {
                positions[outputIndex++] = left.position(leftIndex++);
            }
            while (rightIndex < right.selectedCount()) {
                positions[outputIndex++] = right.position(rightIndex++);
            }
            if (outputIndex == left.size() && isAllPositions(positions, outputIndex)) {
                result.selectAll(left.size());
            }
            else {
                result.setSelection(left.size(), outputIndex, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask lastMask(Context context, Mask mask, int count)
    {
        if (count >= mask.selectedCount()) {
            return mask;
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(count);
        boolean reused = result != null;
        if (!reused) {
            result = mask.last(count);
        }
        else if (count <= 0) {
            result.clear(mask.size());
        }
        else {
            int[] positions = result.positionsArrayForOverwrite(count);
            for (int index = 0; index < count; index++) {
                positions[index] = mask.position(mask.selectedCount() - count + index);
            }
            if (count == mask.size() && isAllPositions(positions, count)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), count, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    public Mask firstMask(Context context, Mask mask, int count)
    {
        if (count >= mask.selectedCount()) {
            return mask;
        }

        ContextState state = state(context);
        Mask result = state.borrowMask(count);
        boolean reused = result != null;
        if (!reused) {
            result = mask.first(count);
        }
        else if (count <= 0) {
            result.clear(mask.size());
        }
        else {
            int[] positions = result.positionsArrayForOverwrite(count);
            for (int index = 0; index < count; index++) {
                positions[index] = mask.position(index);
            }
            if (count == mask.size() && isAllPositions(positions, count)) {
                result.selectAll(mask.size());
            }
            else {
                result.setSelection(mask.size(), count, false);
            }
        }
        state.trackMask(result, reused);
        return result;
    }

    @Override
    public String toString()
    {
        return states.entrySet().stream()
                .map(e -> "%s: total=%s, peak=%s, current=%s".formatted(
                        e.getKey().name(),
                        e.getValue().stats().total(),
                        e.getValue().stats().peak(),
                        e.getValue().stats().current()))
                .collect(Collectors.joining("\n"));
    }

    public long totalBytes(Context context)
    {
        return state(context).stats().total();
    }

    public long currentBytes(Context context)
    {
        // Each Context carries a unique scopeId, so allocation routing keys on the exact instance.
        // Profiling queries, however, identify a context by its (stable) name: aggregate the live
        // bytes across every scope that shares the requested name so callers can probe usage with a
        // freshly constructed Context("name") without having to hold the original instance.
        long current = 0;
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            if (entry.getKey().name().equals(context.name())) {
                current += entry.getValue().stats().current();
            }
        }
        return current;
    }

    /** Registers a context's pool groups before execution so compatibility admission is independent of pull order. */
    public void register(Context context)
    {
        ContextState state = state(requireNonNull(context, "context is null"));
        if (state.hasCompatibilityCandidate()) {
            pendingCompatibilityStates.add(state);
        }
    }

    /**
     * Freezes compatibility admission for the contexts registered by the current execution.
     *
     * <p>An allocator may outlive an operator plan, so admission must count current registrations rather than local
     * groups retained from earlier executions. Compatible released storage remains reusable across executions, but a
     * later plan cannot inherit an earlier plan's group count.
     */
    public void beginExecution()
    {
        if (pendingCompatibilityStates.isEmpty()) {
            return;
        }

        Map<PoolState, Set<PoolState>> contributors = new IdentityHashMap<>();
        for (ContextState state : pendingCompatibilityStates) {
            contributors.computeIfAbsent(
                            state.compatibilityCandidate(),
                            _ -> Collections.newSetFromMap(new IdentityHashMap<>()))
                    .add(state.localPool());
        }
        for (ContextState state : pendingCompatibilityStates) {
            state.configureCompatibility(
                    contributors.get(state.compatibilityCandidate()).size() >= Math.max(1, policy.minCompatibilityDomainGroups()));
        }
        pendingCompatibilityStates.clear();
    }

    /// Continuation supplied by the host when the current resident reservation is blocked.
    public Optional<CompletionStage<Void>> memoryBlocked()
    {
        if (memoryBlocked.isDone()) {
            return Optional.empty();
        }
        return Optional.of(memoryBlocked);
    }

    public long residentBytes()
    {
        return residentBytes;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (memoryReservation != null && residentBytes != 0) {
            memoryReservation.release(residentBytes);
        }
        residentBytes = 0;
        states.clear();
        pools.clear();
        for (SharedResourceState state : sharedResources.values()) {
            try {
                state.value.close();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to close shared allocator resource", e);
            }
        }
        sharedResources.clear();
        allFalseBooleanVectors.clear();
        pendingCompatibilityStates.clear();
        lastContext = null;
        lastContextState = null;
    }

    public long peakBytes(Context context)
    {
        long peak = 0;
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            if (entry.getKey().name().equals(context.name())) {
                peak += entry.getValue().stats().peak();
            }
        }
        return peak;
    }

    public void release(Context context)
    {
        state(context).release();
    }

    public void release(Context context, Mask mask)
    {
        state(context).releaseMask(mask);
    }

    public void release(Context context, Vector vector)
    {
        releaseVectorTree(context, vector);
    }

    /**
     * Releases the owned nodes in {@code vector}'s tree except identities reachable from a replacement tree.
     * This supports copy/compact operations that may preserve selected encoded children while replacing their
     * wrappers and all unrelated buffers.
     */
    public void releaseUnreferenced(Context context, Vector vector, Set<Vector> retained)
    {
        requireNonNull(retained, "retained is null");
        releaseUnreferencedVectorTree(context, requireNonNull(vector, "vector is null"), retained);
    }

    public void discard(Context context, Vector vector)
    {
        discardVector(context, vector);
    }

    public void releaseIfPresent(Context context)
    {
        ContextState state = states.get(context);
        if (state != null) {
            state.release();
        }
    }

    /**
     * Drops every vector and mask currently tracked as in-use for {@code context} without returning
     * them to the context's reuse pools. Unlike {@link #releaseIfPresent(Context)}, the freed buffers
     * become eligible for garbage collection instead of being re-handed-out by a later borrow.
     * <p>
     * This is the correct teardown for contexts whose tracked buffers double as produced result
     * vectors that may still be referenced by a consumer once the producing evaluation is torn down
     * (for example, scalar primitives whose output buffer is handed back to a {@code PlanEvaluator}).
     * Pooling such buffers would let a subsequent borrow overwrite a value the consumer still holds.
     */
    public void discardAllIfPresent(Context context)
    {
        ContextState state = states.get(context);
        if (state != null) {
            state.discardAll();
        }
    }

    public Mask transfer(Context context, Mask mask)
    {
        transferMask(mask, context);
        return mask;
    }

    public <T extends Vector> T transfer(Context context, T vector)
    {
        transferVector(vector, context);
        return vector;
    }

    /**
     * Transfers only the parts of {@code vector} owned by {@code context}. This does not search other allocation
     * contexts, so borrowed children of an encoded vector retain their upstream ownership.
     */
    public <T extends Vector> T transferOwned(Context context, T vector)
    {
        transferOwnedVector(context, vector);
        return vector;
    }

    public Streams copyStreams(Context context, Streams streams)
    {
        Streams.Builder copied = Streams.builder();
        if (streams.hasValues()) {
            copied.put(Stream.VALUES, copyVector(context, streams.values()));
        }
        if (streams.hasNulls()) {
            copied.put(Stream.NULLS, copyVector(context, streams.get(Stream.NULLS)));
        }
        if (streams.hasErrors()) {
            copied.put(Stream.ERRORS, copyVector(context, streams.get(Stream.ERRORS)));
        }
        return copied.build();
    }

    public Streams copyStreams(Context context, Streams streams, int[] positions)
    {
        Streams.Builder copied = Streams.builder();
        if (streams.hasValues()) {
            copied.put(Stream.VALUES, copyVector(context, streams.values(), positions));
        }
        if (streams.hasNulls()) {
            copied.put(Stream.NULLS, copyVector(context, streams.get(Stream.NULLS), positions));
        }
        if (streams.hasErrors()) {
            copied.put(Stream.ERRORS, copyVector(context, streams.get(Stream.ERRORS), positions));
        }
        return copied.build();
    }

    public Streams copyStreams(Context context, Streams streams, Mask mask)
    {
        Streams.Builder copied = Streams.builder();
        if (streams.hasValues()) {
            copied.put(Stream.VALUES, copyVector(context, streams.values(), mask));
        }
        if (streams.hasNulls()) {
            copied.put(Stream.NULLS, copyVector(context, streams.get(Stream.NULLS), mask));
        }
        if (streams.hasErrors()) {
            copied.put(Stream.ERRORS, copyVector(context, streams.get(Stream.ERRORS), mask));
        }
        return copied.build();
    }

    public Vector copyVector(Context context, Vector vector)
    {
        return vector.copy(this, context);
    }

    public Vector copyVector(Context context, Vector vector, int[] positions)
    {
        return vector.copy(this, context, positions);
    }

    public Vector copyVector(Context context, Vector vector, Mask mask)
    {
        if (mask.none()) {
            return vector.emptyLike(this, context);
        }
        if (mask.all() && mask.selectedCount() == vector.length()) {
            return vector.copy(this, context);
        }

        Vector copy = null;
        int outputPosition = 0;
        int outputSize = mask.selectedCount();
        for (int sourcePosition : mask) {
            copy = vector.copySinglePositionInto(this, context, copy, sourcePosition, outputPosition, outputSize);
            outputPosition++;
        }
        return copy == null ? vector.emptyLike(this, context) : copy;
    }

    private void transferMask(Mask mask, Context preferredContext)
    {
        ContextState preferredState = states.get(preferredContext);
        if (preferredState != null && preferredState.transferMask(mask)) {
            return;
        }
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            if (entry.getKey().equals(preferredContext)) {
                continue;
            }
            if (entry.getValue().transferMask(mask)) {
                return;
            }
        }
    }

    private void transferVector(Vector vector, Context preferredContext)
    {
        if (policy.indexedVectorTreeTraversal()) {
            for (int index = 0; index < vector.childVectorCount(); index++) {
                transferVector(vector.childVector(index), preferredContext);
            }
        }
        else {
            vector.forEachChildVector(child -> transferVector(child, preferredContext));
        }
        ContextState preferredState = states.get(preferredContext);
        if (preferredState != null && preferredState.transferVector(vector)) {
            return;
        }
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            if (entry.getKey().equals(preferredContext)) {
                continue;
            }
            if (entry.getValue().transferVector(vector)) {
                return;
            }
        }
    }

    private void transferOwnedVector(Context context, Vector vector)
    {
        vector.prepareBufferTransfer(this, context);
        if (policy.indexedVectorTreeTraversal()) {
            for (int index = 0; index < vector.childVectorCount(); index++) {
                transferOwnedVector(context, vector.childVector(index));
            }
        }
        else {
            vector.forEachChildVector(child -> transferOwnedVector(context, child));
        }
        ContextState contextState = states.get(context);
        if (contextState != null) {
            contextState.transferVector(vector);
        }
    }

    /**
     * Detaches one producer-owned vector from bulk context teardown and returns a lease that can release it back to
     * the same compatible pool after ownership has crossed one or more output boundaries.
     */
    public BufferLeaseOwner lease(Context context, Vector vector)
    {
        ContextState contextState = states.get(requireNonNull(context, "context is null"));
        return contextState == null ? null : contextState.leaseVector(requireNonNull(vector, "vector is null"));
    }

    private void transferStreams(Streams streams, Context preferredContext)
    {
        if (streams.hasValues()) {
            transferVector(streams.values(), preferredContext);
        }
        if (streams.hasNulls()) {
            transferVector(streams.get(Stream.NULLS), preferredContext);
        }
        if (streams.hasErrors()) {
            transferVector(streams.get(Stream.ERRORS), preferredContext);
        }
    }

    private void releaseVector(Context context, Vector vector)
    {
        state(context).releaseVector(vector);
    }

    private void releaseVectorTree(Context context, Vector vector)
    {
        if (policy.indexedVectorTreeTraversal()) {
            for (int index = 0; index < vector.childVectorCount(); index++) {
                releaseVectorTree(context, vector.childVector(index));
            }
        }
        else {
            vector.forEachChildVector(child -> releaseVectorTree(context, child));
        }
        releaseVector(context, vector);
    }

    private void releaseUnreferencedVectorTree(Context context, Vector vector, Set<Vector> retained)
    {
        if (retained.contains(vector)) {
            return;
        }
        if (policy.indexedVectorTreeTraversal()) {
            for (int index = 0; index < vector.childVectorCount(); index++) {
                releaseUnreferencedVectorTree(context, vector.childVector(index), retained);
            }
        }
        else {
            vector.forEachChildVector(child -> releaseUnreferencedVectorTree(context, child, retained));
        }
        releaseVector(context, vector);
    }

    private void discardVector(Context context, Vector vector)
    {
        state(context).discardVector(vector);
    }

    private ContextState state(Context context)
    {
        if (closed) {
            throw new IllegalStateException("allocator is closed");
        }
        if (context == lastContext) {
            return lastContextState;
        }

        ContextState state = states.computeIfAbsent(context, key -> new ContextState(
                this,
                pools.computeIfAbsent(key.poolGroup(), _ -> new PoolState()),
                pools.computeIfAbsent(key.compatibilityGroup(), _ -> new PoolState())));
        lastContext = context;
        lastContextState = state;
        return state;
    }

    private void reserveResident(long bytes)
    {
        if (bytes <= 0) {
            return;
        }
        if (memoryReservation != null) {
            CompletionStage<Void> continuation = requireNonNull(memoryReservation.reserve(bytes), "memory continuation is null");
            CompletableFuture<Void> future = continuation.toCompletableFuture();
            if (!future.isDone()) {
                memoryBlocked = memoryBlocked.isDone()
                        ? future
                        : CompletableFuture.allOf(memoryBlocked, future);
            }
        }
        residentBytes += bytes;
    }

    private void releaseResident(long bytes)
    {
        if (bytes <= 0) {
            return;
        }
        if (bytes > residentBytes) {
            throw new IllegalStateException("released %s resident bytes with only %s allocated".formatted(bytes, residentBytes));
        }
        residentBytes -= bytes;
        if (memoryReservation != null) {
            memoryReservation.release(bytes);
        }
    }

    private static int[] positions(Mask mask)
    {
        int[] positions = new int[mask.selectedCount()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = mask.position(index);
        }
        return positions;
    }

    private static Mask materializedAllDifference(Mask left, Mask right)
    {
        int[] positions = new int[left.size() - right.selectedCount()];
        int outputIndex = fillMaterializedAllDifference(positions, left, right);
        if (outputIndex == positions.length) {
            return Mask.sparseTrusted(positions, outputIndex, left.size());
        }
        return Mask.sparse(Arrays.copyOf(positions, outputIndex), left.size());
    }

    private static void fillMaterializedAllDifference(Mask result, Mask left, Mask right)
    {
        int[] positions = result.positionsArrayForOverwrite(left.size() - right.selectedCount());
        int outputIndex = fillMaterializedAllDifference(positions, left, right);
        result.setSelection(left.size(), outputIndex, false);
    }

    private static int fillMaterializedAllDifference(int[] positions, Mask left, Mask right)
    {
        int outputIndex = 0;
        int position = 0;
        for (int index = 0; index < right.selectedCount(); index++) {
            int rightPosition = right.position(index);
            while (position < rightPosition) {
                positions[outputIndex++] = position++;
            }
            position++;
        }
        while (position < left.size()) {
            positions[outputIndex++] = position++;
        }
        return outputIndex;
    }

    private static boolean isAllPositions(int[] positions, int selectedCount)
    {
        for (int index = 0; index < selectedCount; index++) {
            if (positions[index] != index) {
                return false;
            }
        }
        return true;
    }

    private static void copyMask(Mask target, Mask source)
    {
        if (source.all()) {
            target.selectAll(source.size());
            return;
        }

        int[] positions = target.positionsArrayForOverwrite(source.selectedCount());
        for (int index = 0; index < source.selectedCount(); index++) {
            positions[index] = source.position(index);
        }
        target.setSelection(source.size(), source.selectedCount(), false);
    }

    private static long maskBytes(Mask mask)
    {
        if (mask.all()) {
            return 0;
        }
        return (long) mask.capacity() * Integer.BYTES;
    }

    private static final class ContextState
            implements BufferLeaseOwner, Mask.CapacityListener
    {
        private final Allocator allocator;
        private final Stats stats = new Stats();
        private final PoolState pool;
        private final PoolState compatibilityCandidate;
        private PoolState compatibilityPool;
        private final Set<Vector> inUseVectors = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Map<Object, Integer> inUseVectorCounts = new HashMap<>();
        private final Map<Object, Integer> vectorHighWater = new HashMap<>();
        private Mask inUseMasksHead;
        private boolean borrowedVectorResident;

        private ContextState(Allocator allocator, PoolState pool, PoolState compatibilityPool)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.pool = requireNonNull(pool, "pool is null");
            this.compatibilityCandidate = requireNonNull(compatibilityPool, "compatibilityPool is null");
            this.compatibilityPool = pool;
        }

        public Stats stats()
        {
            return stats;
        }

        public <T extends Vector> T borrowVector(Object family, int minimumCapacity, boolean exactCapacityMatch, Class<T> vectorType)
        {
            T vector = borrowVector(pool, family, minimumCapacity, exactCapacityMatch, vectorType);
            borrowedVectorResident = vector != null;
            if (vector == null && compatibilityActive()) {
                vector = borrowVector(compatibilityPool, family, minimumCapacity, exactCapacityMatch, vectorType);
                borrowedVectorResident = vector != null;
            }
            if (vector != null) {
                return vector;
            }
            borrowedVectorResident = false;
            return allocator.primitiveArrays.borrow(family, minimumCapacity, vectorType);
        }

        private <T extends Vector> T borrowVector(
                PoolState source,
                Object family,
                int minimumCapacity,
                boolean exactCapacityMatch,
                Class<T> vectorType)
        {
            TreeMap<Integer, ArrayDeque<Vector>> vectors = source.vectorPool.get(family);
            if (vectors == null) {
                return null;
            }
            Map.Entry<Integer, ArrayDeque<Vector>> entry = vectors.ceilingEntry(minimumCapacity);
            if (entry == null || (exactCapacityMatch && entry.getKey() != minimumCapacity)) {
                return null;
            }
            T vector = vectorType.cast(entry.getValue().removeFirst());
            if (entry.getValue().isEmpty()) {
                vectors.remove(entry.getKey());
            }
            ArrayDeque<Vector> order = source.vectorPoolOrder.get(family);
            if (order != null) {
                removeFromOrder(order, vector);
                if (order.isEmpty()) {
                    source.vectorPoolOrder.remove(family);
                }
            }
            if (vectors.isEmpty()) {
                source.vectorPool.remove(family);
            }
            removeFromOrder(source.vectorPoolGlobalOrder, vector);
            source.vectorPoolBytes -= vector.retainedBytes();
            return vector;
        }

        public void trackVector(Vector vector, boolean reused)
        {
            if (!borrowedVectorResident) {
                allocator.reserveResident(vector.retainedBytes());
            }
            borrowedVectorResident = false;
            // Only track vectors that participate in pooling. Non-pooled vectors (e.g. DictionaryVector
            // wrapping borrowed data) can be left to GC without going through the IdentityHashMap on
            // adoption, which avoids per-position overhead in join output materialization.
            Object family = vector.poolFamily();
            if ((family != null || allocator.memoryReservation != null) && inUseVectors.add(vector)) {
                if (family != null) {
                    int inUse = inUseVectorCounts.merge(family, 1, Integer::sum);
                    if (allocator.policy.adaptiveVectorPoolHighWater()) {
                        vectorHighWater.merge(family, inUse, Math::max);
                    }
                }
            }
            stats.acquire(vector.retainedBytes(), reused);
        }

        public void releaseVector(Vector vector)
        {
            if (!untrackVector(vector)) {
                return;
            }

            stats.releaseBytes(vector.retainedBytes());
            Object family = vector.poolFamily();
            if (family == null) {
                allocator.releaseResident(vector.retainedBytes());
                return;
            }
            addVectorToPool(family, vector.poolCapacity(), vector.poolMaxRetained(), vector);
        }

        public void discardVector(Vector vector)
        {
            if (!untrackVector(vector)) {
                return;
            }
            stats.releaseBytes(vector.retainedBytes());
            allocator.releaseResident(vector.retainedBytes());
        }

        public boolean transferVector(Vector vector)
        {
            if (!untrackVector(vector)) {
                return false;
            }
            stats.releaseBytes(vector.retainedBytes());
            return true;
        }

        public BufferLeaseOwner leaseVector(Vector vector)
        {
            if (!untrackVector(vector)) {
                return null;
            }
            stats.releaseBytes(vector.retainedBytes());
            return this;
        }

        @Override
        public void releaseLeased(Vector vector)
        {
            Object family = vector.poolFamily();
            if (family != null) {
                addVectorToPool(family, vector.poolCapacity(), vector.poolMaxRetained(), vector);
            }
        }

        public Mask borrowMask(int requiredCapacity)
        {
            Mask mask = borrowMask(pool, requiredCapacity);
            if (mask == null && compatibilityActive()) {
                mask = borrowMask(compatibilityPool, requiredCapacity);
            }
            return mask;
        }

        private static Mask borrowMask(PoolState source, int requiredCapacity)
        {
            Map.Entry<Integer, ArrayDeque<Mask>> entry = source.maskPool.ceilingEntry(requiredCapacity);
            if (entry == null) {
                return null;
            }

            Mask mask = entry.getValue().removeFirst();
            if (entry.getValue().isEmpty()) {
                source.maskPool.remove(entry.getKey());
            }
            return mask;
        }

        public void trackMask(Mask mask, boolean reused)
        {
            if (!mask.residentTracked()) {
                allocator.reserveResident(maskBytes(mask));
                mask.markResidentTracked(this);
            }
            if (!mask.trackedInUse()) {
                mask.markTrackedInUse(this);
                mask.trackedPrevious(null);
                mask.trackedNext(inUseMasksHead);
                if (inUseMasksHead != null) {
                    inUseMasksHead.trackedPrevious(mask);
                }
                inUseMasksHead = mask;
            }
            stats.acquire(maskBytes(mask), reused);
        }

        public boolean transferMask(Mask mask)
        {
            if (!mask.trackedBy(this)) {
                return false;
            }
            unlinkTrackedMask(mask);
            stats.releaseBytes(maskBytes(mask));
            return true;
        }

        public void releaseMask(Mask mask)
        {
            if (!mask.trackedBy(this)) {
                return;
            }
            unlinkTrackedMask(mask);
            stats.releaseBytes(maskBytes(mask));
            addMaskToPool(mask);
        }

        public void release()
        {
            // The normal BatchBufferScope close path has already released every resolved output and its owned mask.
            // Avoid constructing an IdentityHashMap iterator for that overwhelmingly common empty generation; the
            // full sweep below remains the safety net for lazy or otherwise unexposed allocations.
            if (inUseVectors.isEmpty() && inUseMasksHead == null) {
                stats.release();
                return;
            }
            for (Vector vector : inUseVectors) {
                Object family = vector.poolFamily();
                if (family != null) {
                    addVectorToPool(family, vector.poolCapacity(), vector.poolMaxRetained(), vector);
                }
                else {
                    allocator.releaseResident(vector.retainedBytes());
                }
            }
            Mask mask = inUseMasksHead;
            while (mask != null) {
                Mask next = mask.trackedNext();
                addMaskToPool(mask);
                mask.clearTrackedInUse();
                mask = next;
            }
            inUseMasksHead = null;
            inUseVectors.clear();
            inUseVectorCounts.clear();
            stats.release();
        }

        public void discardAll()
        {
            Mask mask = inUseMasksHead;
            while (mask != null) {
                Mask next = mask.trackedNext();
                discardMaskResident(mask);
                mask.clearTrackedInUse();
                mask = next;
            }
            inUseMasksHead = null;
            for (Vector vector : inUseVectors) {
                allocator.releaseResident(vector.retainedBytes());
            }
            inUseVectors.clear();
            inUseVectorCounts.clear();
            stats.release();
        }

        private boolean untrackVector(Vector vector)
        {
            if (!inUseVectors.remove(vector)) {
                return false;
            }
            Object family = vector.poolFamily();
            if (family == null) {
                return true;
            }
            int remaining = inUseVectorCounts.get(family) - 1;
            if (remaining == 0) {
                inUseVectorCounts.remove(family);
            }
            else {
                inUseVectorCounts.put(family, remaining);
            }
            return true;
        }

        private void unlinkTrackedMask(Mask mask)
        {
            Mask previous = mask.trackedPrevious();
            Mask next = mask.trackedNext();
            if (previous != null) {
                previous.trackedNext(next);
            }
            else {
                inUseMasksHead = next;
            }
            if (next != null) {
                next.trackedPrevious(previous);
            }
            mask.clearTrackedInUse();
        }

        private void addVectorToPool(Object family, int capacity, int maxRetained, Vector vector)
        {
            if (!allocator.policy.localVectorWorkingSet() && maxRetained > 0 && allocator.primitiveArrays.retain(family, capacity, vector.retainedBytes(), vector)) {
                allocator.releaseResident(vector.retainedBytes());
                return;
            }
            if (maxRetained <= 0 || allocator.policy.maxLocalVectorPoolBytes() <= 0) {
                allocator.releaseResident(vector.retainedBytes());
                return;
            }
            int retentionLimit = Math.max(maxRetained, vectorHighWater.getOrDefault(family, 0));
            if (compatibilityActive()) {
                int reserve = allocator.policy.compatiblePoolLocalReserve();
                retentionLimit = Math.min(retentionLimit, Math.max(0, reserve));
            }
            // Keep the active allocator's reuse pool in front of the engine-owned cross-execution pool. In particular,
            // variable-width vectors are borrowed with ceiling-capacity semantics, while the owner pool is
            // deliberately keyed by exact capacity. Sending a large BinaryVector directly to the shared pool
            // therefore made parquet scans allocate again whenever the next batch had a slightly different byte
            // length. Retain the bounded working set locally and use the owner pool only for excess idle vectors.
            pool.vectorPool
                    .computeIfAbsent(family, _ -> new TreeMap<>())
                    .computeIfAbsent(capacity, _ -> new ArrayDeque<>())
                    .addLast(vector);
            ArrayDeque<Vector> order = pool.vectorPoolOrder.computeIfAbsent(family, _ -> new ArrayDeque<>());
            order.addLast(vector);
            pool.vectorPoolGlobalOrder.addLast(vector);
            pool.vectorPoolBytes += vector.retainedBytes();
            while (order.size() > retentionLimit) {
                evictLocalVector(order.getFirst());
            }
            while (pool.vectorPoolBytes > allocator.policy.maxLocalVectorPoolBytes() && !pool.vectorPoolGlobalOrder.isEmpty()) {
                evictLocalVector(pool.vectorPoolGlobalOrder.getFirst());
            }
        }

        private void evictLocalVector(Vector vector)
        {
            Object family = requireNonNull(vector.poolFamily(), "discarded vector has no pool family");
            removeVectorFromPool(family, vector.poolCapacity(), vector);
            ArrayDeque<Vector> familyOrder = pool.vectorPoolOrder.get(family);
            if (familyOrder != null) {
                removeFromOrder(familyOrder, vector);
                if (familyOrder.isEmpty()) {
                    pool.vectorPoolOrder.remove(family);
                }
            }
            if (removeFromOrder(pool.vectorPoolGlobalOrder, vector)) {
                pool.vectorPoolBytes -= vector.retainedBytes();
            }
            if (compatibilityActive()) {
                addVectorToCompatibilityPool(family, vector);
            }
            else {
                allocator.primitiveArrays.retain(family, vector.poolCapacity(), vector.retainedBytes(), vector);
                allocator.releaseResident(vector.retainedBytes());
            }
        }

        private void addVectorToCompatibilityPool(Object family, Vector vector)
        {
            compatibilityPool.vectorPool
                    .computeIfAbsent(family, _ -> new TreeMap<>())
                    .computeIfAbsent(vector.poolCapacity(), _ -> new ArrayDeque<>())
                    .addLast(vector);
            ArrayDeque<Vector> order = compatibilityPool.vectorPoolOrder.computeIfAbsent(family, _ -> new ArrayDeque<>());
            order.addLast(vector);
            compatibilityPool.vectorPoolGlobalOrder.addLast(vector);
            compatibilityPool.vectorPoolBytes += vector.retainedBytes();
            int compatibilityRetentionLimit = (int) Math.min(
                    Integer.MAX_VALUE,
                    (long) vector.poolMaxRetained() * Math.max(1, allocator.policy.compatibleVectorPoolRetentionMultiplier()));
            while (order.size() > compatibilityRetentionLimit) {
                evictCompatibilityVector(order.getFirst());
            }
            while (compatibilityPool.vectorPoolBytes > allocator.policy.maxCompatibleVectorPoolBytes() && !compatibilityPool.vectorPoolGlobalOrder.isEmpty()) {
                evictCompatibilityVector(compatibilityPool.vectorPoolGlobalOrder.getFirst());
            }
        }

        private void evictCompatibilityVector(Vector vector)
        {
            Object family = requireNonNull(vector.poolFamily(), "discarded compatible vector has no pool family");
            removeVectorFromPool(compatibilityPool, family, vector.poolCapacity(), vector);
            ArrayDeque<Vector> familyOrder = compatibilityPool.vectorPoolOrder.get(family);
            if (familyOrder != null) {
                removeFromOrder(familyOrder, vector);
                if (familyOrder.isEmpty()) {
                    compatibilityPool.vectorPoolOrder.remove(family);
                }
            }
            if (removeFromOrder(compatibilityPool.vectorPoolGlobalOrder, vector)) {
                compatibilityPool.vectorPoolBytes -= vector.retainedBytes();
            }
            allocator.primitiveArrays.retain(family, vector.poolCapacity(), vector.retainedBytes(), vector);
            allocator.releaseResident(vector.retainedBytes());
        }

        private void addMaskToPool(Mask mask)
        {
            ArrayDeque<Mask> bucket = pool.maskPool.computeIfAbsent(mask.capacity(), _ -> new ArrayDeque<>());
            bucket.addLast(mask);
            int localLimit = compatibilityActive() ? Math.max(0, allocator.policy.compatibleMaskPoolLocalReserve()) : allocator.policy.maxPooledMasksPerBucket();
            while (bucket.size() > localLimit) {
                Mask excess = bucket.removeFirst();
                if (compatibilityPool != pool) {
                    ArrayDeque<Mask> compatibleBucket = compatibilityPool.maskPool
                            .computeIfAbsent(excess.capacity(), _ -> new ArrayDeque<>());
                    compatibleBucket.addLast(excess);
                    while (compatibleBucket.size() > allocator.policy.maxPooledMasksPerBucket()) {
                        discardMaskResident(compatibleBucket.removeFirst());
                    }
                }
                else {
                    discardMaskResident(excess);
                }
            }
            if (bucket.isEmpty()) {
                pool.maskPool.remove(mask.capacity());
            }
        }

        private void discardMaskResident(Mask mask)
        {
            if (!mask.residentTracked()) {
                return;
            }
            mask.clearResidentTracked();
            allocator.releaseResident(maskBytes(mask));
        }

        @Override
        public void capacityChanged(int oldCapacity, int newCapacity)
        {
            long delta = (long) (newCapacity - oldCapacity) * Integer.BYTES;
            if (delta > 0) {
                allocator.reserveResident(delta);
            }
            else {
                allocator.releaseResident(-delta);
            }
        }

        private void removeVectorFromPool(Object family, int capacity, Vector vector)
        {
            removeVectorFromPool(pool, family, capacity, vector);
        }

        private void removeVectorFromPool(PoolState source, Object family, int capacity, Vector vector)
        {
            TreeMap<Integer, ArrayDeque<Vector>> vectors = source.vectorPool.get(family);
            if (vectors == null) {
                return;
            }

            ArrayDeque<Vector> bucket = vectors.get(capacity);
            if (bucket == null) {
                return;
            }

            removeFromOrder(bucket, vector);
            if (bucket.isEmpty()) {
                vectors.remove(capacity);
            }
            if (vectors.isEmpty()) {
                source.vectorPool.remove(family);
            }
        }

        private boolean removeFromOrder(ArrayDeque<Vector> order, Vector vector)
        {
            if (!allocator.policy.fastVectorPoolOrderRemove()) {
                return order.remove(vector);
            }
            if (order.peekFirst() == vector) {
                order.removeFirst();
                return true;
            }
            if (order.peekLast() == vector) {
                order.removeLast();
                return true;
            }
            return order.remove(vector);
        }

        private boolean compatibilityActive()
        {
            return compatibilityPool != pool;
        }

        private boolean hasCompatibilityCandidate()
        {
            return compatibilityCandidate != pool;
        }

        private PoolState localPool()
        {
            return pool;
        }

        private PoolState compatibilityCandidate()
        {
            return compatibilityCandidate;
        }

        private void configureCompatibility(boolean active)
        {
            compatibilityPool = active ? compatibilityCandidate : pool;
        }
    }

    private static final class PoolState
    {
        private final Map<Object, TreeMap<Integer, ArrayDeque<Vector>>> vectorPool = new HashMap<>();
        private final Map<Object, ArrayDeque<Vector>> vectorPoolOrder = new HashMap<>();
        private final ArrayDeque<Vector> vectorPoolGlobalOrder = new ArrayDeque<>();
        private long vectorPoolBytes;
        private final TreeMap<Integer, ArrayDeque<Mask>> maskPool = new TreeMap<>();
    }

    // TODO: track amount of reallocated memory (i.e., how much effort is wasted due to potentially poor allocation strategies)
    private static class Stats
    {
        private long total;
        private long peak;
        private long current;

        public void acquire(long bytes, boolean reused)
        {
            if (!reused && bytes > 0) {
                total += bytes;
            }
            current += bytes;
            peak = Math.max(peak, current);
        }

        public void releaseBytes(long bytes)
        {
            current -= bytes;
        }

        public void release()
        {
            current = 0;
        }

        public long total()
        {
            return total;
        }

        public long current()
        {
            return current;
        }

        public long peak()
        {
            return peak;
        }
    }

    public interface BufferLeaseOwner
    {
        void releaseLeased(Vector vector);
    }

    public record Context(String name, Object scopeId, Object poolGroup, Object compatibilityGroup)
    {
        public Context(String name)
        {
            this(name, new Object());
        }

        public Context(String name, long scopeId)
        {
            this(name, scopeId, scopeId, scopeId);
        }

        public Context(String name, Object poolGroup)
        {
            this(name, new Object(), requireNonNull(poolGroup, "poolGroup is null"), poolGroup);
        }

        public Context(String name, Object poolGroup, Object compatibilityGroup)
        {
            this(
                    name,
                    new Object(),
                    requireNonNull(poolGroup, "poolGroup is null"),
                    requireNonNull(compatibilityGroup, "compatibilityGroup is null"));
        }

        public Context(String name, long scopeId, Object poolGroup)
        {
            this(name, scopeId, requireNonNull(poolGroup, "poolGroup is null"), poolGroup);
        }
    }

    public static final class SharedResource<T extends AutoCloseable>
            implements AutoCloseable
    {
        private final Allocator allocator;
        private final Object key;
        private final T value;
        private boolean closed;

        private SharedResource(Allocator allocator, Object key, T value)
        {
            this.allocator = allocator;
            this.key = key;
            this.value = value;
        }

        public T value()
        {
            if (closed) {
                throw new IllegalStateException("Shared resource lease is closed");
            }
            return value;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            allocator.releaseSharedResource(key);
        }
    }

    private static final class SharedResourceState
    {
        private final AutoCloseable value;
        private int references;

        private SharedResourceState(AutoCloseable value)
        {
            this.value = value;
        }
    }
}
