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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// TODO: support hierarchical contexts
public class Allocator
        implements AutoCloseable
{
    private final Map<Context, ContextState> states = new HashMap<>();
    private final Map<Object, PoolState> pools = new HashMap<>();
    private final Map<Vector, ContextState> vectorOwners = new IdentityHashMap<>();
    private final Map<Vector, VectorLeaseState> vectorLeases = new IdentityHashMap<>();
    private final Map<Vector, AsyncVectorLeaseState> asyncVectorLeases = new IdentityHashMap<>();
    private final Map<Object, SharedResourceState> sharedResources = new HashMap<>();
    private final Map<Integer, BooleanVector> allFalseBooleanVectors = new HashMap<>();
    private final Object allFalseBooleanOwner = new Object();
    private int sharedSingleRunCount = -1;
    private int[] sharedSingleRunCounts;
    private final Set<ContextState> pendingCompatibilityStates = Collections.newSetFromMap(new IdentityHashMap<>());
    private final AllocationResources allocationResources;
    private final AllocationResourcesOwner resourcesOwner;
    private final PrimitiveArrayPool primitiveArrays;
    private final AllocatorPolicy policy;
    private final MemoryReservation memoryReservation;
    private Context lastContext;
    private ContextState lastContextState;
    private CompletableFuture<Void> memoryBlocked = CompletableFuture.completedFuture(null);
    private long residentBytes;
    private boolean closed;

    public Allocator(AllocationResourcesOwner resourcesOwner)
    {
        this(resourcesOwner, null);
    }

    public Allocator(AllocationResourcesOwner resourcesOwner, MemoryReservation memoryReservation)
    {
        this.resourcesOwner = requireNonNull(resourcesOwner, "resourcesOwner is null");
        this.allocationResources = resourcesOwner.allocationResources();
        this.primitiveArrays = allocationResources.primitiveArrays();
        this.policy = allocationResources.allocatorPolicy();
        this.memoryReservation = memoryReservation;
    }

    public Allocator(AllocationResources allocationResources)
    {
        this((AllocationResourcesOwner) allocationResources, null);
    }

    public Allocator(AllocationResources allocationResources, MemoryReservation memoryReservation)
    {
        this((AllocationResourcesOwner) allocationResources, memoryReservation);
    }

    public AllocationResourcesOwner resourcesOwner()
    {
        return resourcesOwner;
    }

    public PrimitiveArrayPool primitiveArrays()
    {
        return primitiveArrays;
    }

    byte[] borrowVariableWidthStorage(Context context, int minimumLength)
    {
        requireNonNull(context, "context is null");
        return primitiveArrays.borrowBytesBetween(
                minimumLength,
                maximumVariableWidthStorageLength(context, minimumLength));
    }

    int maximumVariableWidthStorageLength(Context context, int minimumLength)
    {
        requireNonNull(context, "context is null");
        int maximumOversizeRatio = context.variableWidthStorageReusePolicy()
                .maximumOversizeRatioOrElse(policy.maxVariableWidthStorageOversizeRatio());
        long maximumLength = (long) minimumLength * maximumOversizeRatio;
        return (int) Math.min(Integer.MAX_VALUE, maximumLength);
    }

    int maximumVariableWidthVectorCapacity(Context context, int minimumCapacity)
    {
        requireNonNull(context, "context is null");
        if (context.variableWidthStorageReusePolicy().maximumOversizeRatio().isEmpty()) {
            return Integer.MAX_VALUE;
        }
        long maximumCapacity = (long) minimumCapacity *
                context.variableWidthStorageReusePolicy().maximumOversizeRatio().getAsInt();
        return (int) Math.min(Integer.MAX_VALUE, maximumCapacity);
    }

    public PrimitiveArrayPool nativeBuffers()
    {
        return allocationResources.nativeBuffers();
    }

    public NativeBufferAdvice nativeBufferAdvice()
    {
        return allocationResources.nativeBufferAdvice();
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

    /**
     * Replaces an owned vector with a larger logical wrapper that shares all of the previous vector's storage.
     *
     * <p>Chunked state vectors use this when growth preserves old chunks and allocates only the retained-size
     * delta. Unlike discard followed by adopt, this records only that physical delta as newly allocated while
     * transferring ownership to the replacement wrapper.
     */
    public <T extends Vector> T replaceSharedGrowth(Context context, T previous, T replacement)
    {
        requireNonNull(context, "context is null");
        requireNonNull(previous, "previous is null");
        requireNonNull(replacement, "replacement is null");
        state(context).replaceSharedGrowth(previous, replacement);
        return replacement;
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

    public DictionaryVector allocateDictionaryWithDomainPresence(
            Context context,
            int[] ids,
            int length,
            Vector values,
            long domainPresenceBits)
    {
        DictionaryVector vector = DictionaryVector.ofTrustedIdsWithDomainPresence(
                ids,
                length,
                values,
                domainPresenceBits);
        state(context).trackVector(vector, false);
        return vector;
    }

    public DictionaryVector allocateDictionaryWithDomainFrequencies(
            Context context,
            int[] ids,
            int length,
            Vector values,
            int[] domainFrequencies)
    {
        // The mapping is copied by DictionaryVector; ownership of the caller-created frequency table transfers to
        // the tracked result. This keeps an escaping dictionary self-contained with one allocation per metadata
        // component rather than defensively copying the small frequency table twice.
        DictionaryVector vector = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                length,
                values,
                domainFrequencies);
        state(context).trackVector(vector, false);
        return vector;
    }

    /**
     * Allocates a dictionary wrapper that shares a caller-owned, immutable id mapping. This is intended for sibling
     * vectors produced by one operation, where copying the same mapping for every value/null/error stream would add
     * allocation and bandwidth without adding isolation.
     */
    public DictionaryVector allocateDictionarySharedIds(Context context, int[] ids, int length, Vector values)
    {
        DictionaryVector vector = DictionaryVector.wrap(ids, length, values);
        state(context).trackVector(vector, false);
        return vector;
    }

    /**
     * Replaces a dictionary's physical value domain without changing its logical-row mapping. Allocator-owned
     * mappings and their frequency metadata are reparented without copying. Borrowed/raw mappings retain the
     * existing shared-mapping contract: the producer must keep their immutable id array alive for the replacement's
     * lifetime.
     */
    public DictionaryVector replaceDictionaryValues(Context context, DictionaryVector source, Vector values)
    {
        requireNonNull(context, "context is null");
        requireNonNull(source, "source is null");
        requireNonNull(values, "values is null");
        if (source.hasOwnedMapping()) {
            return source.ownedMappingWithValues(values);
        }
        return allocateDictionarySharedIds(context, source.ids(), source.length(), values);
    }

    public RleVector allocateRle(Context context, int[] counts, Vector values)
    {
        if (policy.directSingleRunRle() && counts.length == 1) {
            return allocateSingleRunRle(context, counts[0], values);
        }
        RleVector vector = new RleVector(Arrays.copyOf(counts, counts.length), values);
        state(context).trackVector(vector, false);
        return vector;
    }

    public RleVector allocateSingleRunRle(Context context, int count, Vector value)
    {
        if (!policy.directSingleRunRle()) {
            return allocateRle(context, new int[] {count}, value);
        }
        if (sharedSingleRunCounts == null || sharedSingleRunCount != count) {
            sharedSingleRunCount = count;
            sharedSingleRunCounts = new int[] {count};
        }
        RleVector vector = RleVector.wrapCounts(sharedSingleRunCounts, value);
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
     * Copies one position from every physical stream into caller-owned output storage.
     *
     * The immutable {@link Streams} transport tuple is reused after its backing vectors reach capacity, so a
     * row-at-a-time materializer does not allocate stream iterators, builders, or tuples in steady state.
     */
    public Streams copySinglePositionInto(
            Context context,
            Streams source,
            Streams existing,
            int sourcePosition,
            int outputPosition,
            int size)
    {
        requireNonNull(source, "source is null");
        Vector values = copySinglePositionStream(context, source, existing, Stream.VALUES, sourcePosition, outputPosition, size);
        Vector nulls = copySinglePositionStream(context, source, existing, Stream.NULLS, sourcePosition, outputPosition, size);
        Vector errors = copySinglePositionStream(context, source, existing, Stream.ERRORS, sourcePosition, outputPosition, size);
        return reuseOrCreateStreams(existing, values, nulls, errors);
    }

    /** Copies one source position across an output range while preserving transport-tuple reuse. */
    public Streams copySinglePositionRangeInto(
            Context context,
            Streams source,
            Streams existing,
            int sourcePosition,
            int outputStart,
            int outputEnd,
            int size)
    {
        requireNonNull(source, "source is null");
        Vector values = copySinglePositionRangeStream(context, source, existing, Stream.VALUES, sourcePosition, outputStart, outputEnd, size);
        Vector nulls = copySinglePositionRangeStream(context, source, existing, Stream.NULLS, sourcePosition, outputStart, outputEnd, size);
        Vector errors = copySinglePositionRangeStream(context, source, existing, Stream.ERRORS, sourcePosition, outputStart, outputEnd, size);
        return reuseOrCreateStreams(existing, values, nulls, errors);
    }

    private Vector copySinglePositionStream(
            Context context,
            Streams source,
            Streams existing,
            Stream stream,
            int sourcePosition,
            int outputPosition,
            int size)
    {
        Vector sourceVector = source.getOrNull(stream);
        if (sourceVector == null) {
            return null;
        }
        Vector existingVector = existing == null ? null : existing.getOrNull(stream);
        return sourceVector.copySinglePositionInto(this, context, existingVector, sourcePosition, outputPosition, size);
    }

    private Vector copySinglePositionRangeStream(
            Context context,
            Streams source,
            Streams existing,
            Stream stream,
            int sourcePosition,
            int outputStart,
            int outputEnd,
            int size)
    {
        Vector sourceVector = source.getOrNull(stream);
        if (sourceVector == null) {
            return null;
        }
        Vector existingVector = existing == null ? null : existing.getOrNull(stream);
        return sourceVector.copySinglePositionRangeInto(this, context, existingVector, sourcePosition, outputStart, outputEnd, size);
    }

    public void copyBooleanValues(Vector input, Mask mask, boolean[] output)
    {
        VectorAccess.copyBooleanValues(input, mask, output, policy.booleanCopies().directDense());
    }

    boolean monotonicConcatenatedBooleanPositionCopy()
    {
        return policy.booleanCopies().monotonicConcatenatedPositions();
    }

    boolean directConcatenatedBooleanPositionCopy()
    {
        return policy.booleanCopies().directConcatenatedPositions();
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
            value.markSharedAllFalse(allFalseBooleanOwner);
            reserveResident(value.retainedBytes());
            return value;
        });
    }

    /**
     * Returns whether {@code vector} is this allocator's immutable, shared all-false boolean constant.
     */
    public boolean isSharedAllFalseBoolean(Vector vector)
    {
        return policy.sharedAllFalseBoolean() &&
                vector instanceof BooleanVector booleanVector &&
                booleanVector.isSharedAllFalse(allFalseBooleanOwner);
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
        if (isSharedAllFalseBoolean(vector)) {
            T writable = allocate(context, vectorType, size, vectorAllocator);
            vector.copyInto(writable);
            return writable;
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
        return allocatePooled(
                context,
                poolFamily,
                minimumPoolCapacity,
                exactCapacityMatch ? minimumPoolCapacity : Integer.MAX_VALUE,
                exactCapacityMatch,
                vectorType,
                allocator);
    }

    public <T extends Vector> T allocatePooled(
            Context context,
            Object poolFamily,
            int minimumPoolCapacity,
            int maximumPoolCapacity,
            boolean exactCapacityMatch,
            Class<T> vectorType,
            Supplier<T> allocator)
    {
        checkArgument(maximumPoolCapacity >= minimumPoolCapacity, "maximumPoolCapacity is less than minimumPoolCapacity");
        ContextState state = state(context);
        T vector = state.borrowVector(poolFamily, minimumPoolCapacity, maximumPoolCapacity, exactCapacityMatch, vectorType);
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
        // Allocator shutdown owns and closes every remaining shared resource. A host may subsequently unwind a
        // child source or operator that still holds a lease, especially after cancellation. Its close is therefore
        // already satisfied rather than an ownership error.
        if (closed) {
            return;
        }
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
            mask = Mask.all(size, policy.maskFiltering());
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
            mask = Mask.range(start, length, policy.maskFiltering());
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
            result = source.copy(policy.maskFiltering());
        }
        else {
            copyMask(result, source);
        }
        state.trackMask(result, reused);
        return result;
    }

    /** Returns the logical complement while preserving compact encoded-domain mask state. */
    public Mask complementMask(Context context, Mask source)
    {
        ContextState state = state(context);
        Mask result = state.borrowMask(source.capacity());
        boolean reused = result != null;
        if (!reused) {
            result = source.complement(policy.maskFiltering());
        }
        else {
            result.copyComplementFrom(source);
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
            result = source.copy(policy.maskFiltering());
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
            result = source.copy(policy.maskFiltering());
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
                mask = Mask.all(totalPositions, policy.maskFiltering());
            }
            else if (policy.singleCopySparseMasks()) {
                mask = Mask.sparseTrusted(
                        Arrays.copyOf(activePositions, selectedCount),
                        selectedCount,
                        totalPositions,
                        policy.maskFiltering());
            }
            else if (selectedCount == activePositions.length) {
                mask = Mask.sparse(activePositions, totalPositions, policy.maskFiltering());
            }
            else {
                mask = Mask.sparse(Arrays.copyOf(activePositions, selectedCount), totalPositions, policy.maskFiltering());
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
            mask = totalPositions == 0
                    ? Mask.all(0, policy.maskFiltering())
                    : Mask.none(totalPositions, policy.maskFiltering());
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
                mask = Mask.all(totalPositions, policy.maskFiltering());
            }
            else {
                mask.selectAll(totalPositions);
            }
            state.trackMask(mask, reused);
            return mask;
        }
        if (!reused) {
            mask = Mask.sparse(new int[selectedCount], totalPositions, policy.maskFiltering());
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

    /** Returns storage allocated across this allocator's contexts, excluding successful pool reuse. */
    public long allocatedBytes()
    {
        long total = 0;
        for (ContextState state : states.values()) {
            total = Math.addExact(total, state.stats().total());
        }
        return total;
    }

    /** Returns storage allocated by stable context name, excluding successful pool reuse. */
    public Map<String, Long> allocatedBytesByContext()
    {
        Map<String, Long> result = new TreeMap<>();
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            result.merge(entry.getKey().name(), entry.getValue().stats().total(), Math::addExact);
        }
        return Map.copyOf(result);
    }

    /** Returns vector storage allocated by implementation type, excluding successful pool reuse and non-vector storage. */
    public Map<String, Long> allocatedVectorBytesByType()
    {
        Map<String, Long> result = new TreeMap<>();
        for (ContextState state : states.values()) {
            state.allocatedVectorBytesByType.forEach((type, bytes) -> result.merge(type, bytes, Math::addExact));
        }
        return Map.copyOf(result);
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

    /** Returns live bytes owned by this exact allocation scope, without aggregating same-named diagnostic scopes. */
    public long scopeCurrentBytes(Context context)
    {
        return state(requireNonNull(context, "context is null")).stats().current();
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

    /**
     * Releases all idle vectors and masks retained in this allocator's local reuse pools.
     *
     * <p>In-use allocations and shared immutable resources are unaffected. Hosts can use this at a phase boundary
     * where lowering the current memory reservation is more important than preserving reuse across that boundary.
     */
    public void releasePooledMemory()
    {
        long releasedBytes = 0;
        for (PoolState pool : pools.values()) {
            releasedBytes = Math.addExact(releasedBytes, releasePooledMemory(pool));
        }
        releaseResident(releasedBytes);
    }

    /**
     * Releases idle vectors and masks retained by the pool group associated with {@code context}.
     *
     * <p>Contexts that deliberately share a pool group are released together. Other allocator users are unaffected.
     */
    public void releasePooledMemory(Context context)
    {
        requireNonNull(context, "context is null");
        PoolState pool = pools.get(context.poolGroup());
        if (pool != null) {
            releaseResident(releasePooledMemory(pool));
        }
    }

    private static long releasePooledMemory(PoolState pool)
    {
        long releasedBytes = pool.vectorPoolBytes;
        for (ArrayDeque<Mask> masks : pool.maskPool.values()) {
            for (Mask mask : masks) {
                releasedBytes = Math.addExact(releasedBytes, maskBytes(mask));
                mask.clearResidentTracked();
            }
        }
        pool.vectorPool.clear();
        pool.vectorPoolOrder.clear();
        pool.vectorPoolGlobalOrder.clear();
        pool.vectorPoolBytes = 0;
        pool.maskPool.clear();
        return releasedBytes;
    }

    /**
     * Accounts for a retained-size change made in place by an allocator-owned vector.
     *
     * <p>Callers must capture {@link Vector#retainedBytes()} before mutation and report it
     * immediately afterward. This keeps host memory reservations and allocator statistics
     * synchronized for opaque state vectors whose payload grows without replacing the vector.
     */
    public void retainedBytesChanged(Context context, Vector vector, long previousRetainedBytes)
    {
        requireNonNull(context, "context is null");
        requireNonNull(vector, "vector is null");
        if (previousRetainedBytes < 0) {
            throw new IllegalArgumentException("previous retained bytes is negative");
        }
        state(context).retainedBytesChanged(vector, previousRetainedBytes);
    }

    /**
     * Sets the retained bytes for non-vector execution state owned by {@code owner}.
     *
     * <p>Owner identity is scoped to {@code context}. Repeated calls replace the previous amount, so callers can
     * report a table or index after any representation change without calculating reservation deltas themselves.
     * Setting zero removes the owner. Releasing the context also releases every remaining owner.
     */
    public void setRetainedBytes(Context context, Object owner, long retainedBytes)
    {
        requireNonNull(context, "context is null");
        requireNonNull(owner, "owner is null");
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retained bytes is negative");
        }
        state(context).setRetainedBytes(owner, retainedBytes);
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        Set<Vector> vectors = Collections.newSetFromMap(new IdentityHashMap<>());
        for (ContextState state : states.values()) {
            vectors.addAll(state.inUseVectors);
        }
        for (PoolState pool : pools.values()) {
            vectors.addAll(pool.vectorPoolGlobalOrder);
        }
        for (Vector vector : vectors) {
            releaseStorage(vector);
        }
        if (memoryReservation != null && residentBytes != 0) {
            memoryReservation.release(residentBytes);
        }
        residentBytes = 0;
        states.clear();
        pools.clear();
        vectorOwners.clear();
        vectorLeases.clear();
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

    private void releaseStorage(Vector vector)
    {
        if (vector instanceof RecyclableVectorStorage recyclable) {
            try {
                recyclable.releaseStorage(primitiveArrays);
            }
            catch (IllegalStateException ignored) {
                // The embedding may close its resource owner while an allocator is unwinding. The storage is then
                // simply left for GC rather than re-entering a closed owner pool.
            }
        }
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

    /**
     * Returns the sum of allocation-scope high-water marks grouped by stable context name.
     *
     * <p>Scopes with the same name may peak at different times, so these values attribute allocator pressure but do
     * not necessarily sum to the allocator-wide concurrent peak.
     */
    public Map<String, Long> peakBytesByContext()
    {
        Map<String, Long> result = new TreeMap<>();
        for (Map.Entry<Context, ContextState> entry : states.entrySet()) {
            result.merge(entry.getKey().name(), entry.getValue().stats().peak(), Math::addExact);
        }
        return Map.copyOf(result);
    }

    public void release(Context context)
    {
        state(context).release();
    }

    public void release(Context context, Mask mask)
    {
        state(context).releaseMask(mask);
    }

    public synchronized void release(Context context, Vector vector)
    {
        // An asynchronous boundary can retain a source generation beyond the operator allocator's lifetime. Closing
        // the allocator has already released every ordinary owner; detached vector leases remain independently
        // releasable. A later source-generation unwind is therefore satisfied rather than an ownership error.
        if (closed) {
            return;
        }
        releaseVectorTree(context, vector);
    }

    /**
     * Pins every allocator-owned buffer reachable from the supplied vector roots while an asynchronous consumer
     * retains the vector tree. Borrowed encoded children remain owned by their producer context, but that context
     * cannot return them to a reuse pool until the lease closes.
     */
    public VectorTreeLease leaseVectorTree(List<? extends Vector> roots)
    {
        requireNonNull(roots, "roots is null");
        Set<Vector> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Vector> leased = new java.util.ArrayList<>();
        for (Vector root : roots) {
            leaseVectorTree(requireNonNull(root, "root is null"), visited, leased);
        }
        return new VectorTreeLease(this, leased);
    }

    /**
     * Detaches every allocator-owned buffer reachable from the supplied roots for release by an asynchronous
     * consumer.
     *
     * <p>Unlike {@link #leaseVectorTree(List)}, final close never re-enters a producer-local vector pool. Detachment
     * immediately removes the vectors from the producer context and its memory reservation. The last consumer may
     * then close on any thread. Detached vector objects are not returned to another allocator because their position
     * and initialization contracts belong to the producer, but storage with an explicit reset contract is returned
     * to the shared {@link AllocationResources} primitive pool. This is intended for host boundaries such as buffered
     * exchanges whose consumer is not confined to the producer driver's execution thread.
     */
    public synchronized AsyncVectorTreeLease detachVectorTreeForAsyncRelease(List<? extends Vector> roots)
    {
        requireNonNull(roots, "roots is null");
        Set<Vector> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Vector> vectors = new java.util.ArrayList<>();
        for (Vector root : roots) {
            collectVectorTree(requireNonNull(root, "root is null"), visited, vectors);
        }
        for (Vector vector : vectors) {
            if (vectorLeases.containsKey(vector)) {
                throw new IllegalStateException("vector already has a producer-context lease");
            }
        }

        List<Vector> detached = new java.util.ArrayList<>();
        for (Vector vector : vectors) {
            AsyncVectorLeaseState existing = asyncVectorLeases.get(vector);
            if (existing != null) {
                existing.references++;
                detached.add(vector);
                continue;
            }
            ContextState owner = vectorOwners.get(vector);
            if (owner != null) {
                AsyncVectorLeaseState lease = owner.detachForAsyncRelease(vector);
                if (lease != null) {
                    asyncVectorLeases.put(vector, lease);
                    detached.add(vector);
                }
            }
        }
        return new AsyncVectorTreeLease(this, detached);
    }

    private synchronized Optional<AsyncVectorTreeLease> detachVectorTreeLeaseForAsyncRelease(VectorTreeLease treeLease)
    {
        requireNonNull(treeLease, "treeLease is null");
        if (treeLease.allocator != this || treeLease.closed) {
            return Optional.empty();
        }
        for (Vector vector : treeLease.vectors) {
            VectorLeaseState lease = vectorLeases.get(vector);
            if (lease == null || lease.references != 1) {
                return Optional.empty();
            }
        }

        List<Vector> detached = new java.util.ArrayList<>(treeLease.vectors.size());
        for (Vector vector : treeLease.vectors) {
            VectorLeaseState lease = vectorLeases.remove(vector);
            boolean recyclable = !lease.ownerDiscarded &&
                    lease.owner.lastDiscardEpoch <= lease.ownerEpoch &&
                    lease.owner.maxRetained(vector) > 0;
            releaseResident(vector.retainedBytes());
            asyncVectorLeases.put(vector, new AsyncVectorLeaseState(recyclable));
            detached.add(vector);
        }
        treeLease.closed = true;
        return Optional.of(new AsyncVectorTreeLease(this, detached));
    }

    /**
     * Returns whether every non-shared vector reachable from {@code root} is owned or leased by this allocator.
     * Boundaries use this to distinguish a detachable local result from a borrowed upstream vector tree that must
     * first be copied into boundary-owned storage.
     */
    public synchronized boolean ownsVectorTree(Vector root)
    {
        requireNonNull(root, "root is null");
        Set<Vector> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Vector> vectors = new java.util.ArrayList<>();
        collectVectorTree(root, visited, vectors);
        for (Vector vector : vectors) {
            if (!requiresTracking(vector) || isSharedAllFalseBoolean(vector) || vectorLeases.containsKey(vector) || asyncVectorLeases.containsKey(vector)) {
                continue;
            }
            if (!vectorOwners.containsKey(vector)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether a vector tree can be detached for asynchronous ownership without invalidating an existing
     * producer-local lease. A leased tree is safe to retain inside its current execution island, but an asynchronous
     * boundary must copy it before detaching because the local lease returns buffers to the producer's reuse pool.
     */
    public synchronized boolean canDetachVectorTreeForAsyncRelease(Vector root)
    {
        requireNonNull(root, "root is null");
        Set<Vector> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Vector> vectors = new java.util.ArrayList<>();
        collectVectorTree(root, visited, vectors);
        for (Vector vector : vectors) {
            if (!requiresTracking(vector) || isSharedAllFalseBoolean(vector)) {
                continue;
            }
            if (vectorLeases.containsKey(vector)) {
                return false;
            }
            if (asyncVectorLeases.containsKey(vector)) {
                continue;
            }
            if (!vectorOwners.containsKey(vector)) {
                return false;
            }
        }
        return true;
    }

    private static boolean requiresTracking(Vector vector)
    {
        return vector.poolFamily() != null || vector.retainedBytes() != 0 || vector instanceof DynamicRetainedBytesVector;
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

    Streams interleaveStreams(Context context, List<Streams> columns, int positionCount)
    {
        requireNonNull(context, "context is null");
        requireNonNull(columns, "columns is null");
        checkArgument(positionCount >= 0, "positionCount is negative");
        checkArgument(!columns.isEmpty(), "columns is empty");

        int outputSize = Math.multiplyExact(columns.size(), positionCount);
        for (Streams column : columns) {
            requireNonNull(column, "column is null");
            checkArgument(column.hasValues(), "column does not have VALUES");
            for (Vector vector : column.asMap().values()) {
                checkArgument(
                        vector.length() == positionCount,
                        "column stream length %s does not match position count %s",
                        vector.length(),
                        positionCount);
            }
        }

        Streams.Builder result = Streams.builder();
        for (Stream stream : Stream.values()) {
            boolean present = stream == Stream.VALUES || columns.stream().anyMatch(column -> column.has(stream));
            if (!present) {
                continue;
            }

            Vector output = stream == Stream.VALUES
                    ? null
                    : allocate(context, BooleanVector.class, outputSize, BooleanVector::new);
            for (int position = 0; position < positionCount; position++) {
                for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                    Vector source = columns.get(columnIndex).getOrNull(stream);
                    if (source != null) {
                        output = source.copySinglePositionInto(
                                this,
                                context,
                                output,
                                position,
                                position * columns.size() + columnIndex,
                                outputSize);
                    }
                }
            }
            if (output == null) {
                output = columns.getFirst().values().emptyLike(this, context);
            }
            result.put(stream, output);
        }
        return result.build();
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

        // Let each vector implementation size its compact result once. Building variable-width vectors one
        // position at a time repeatedly grows and recopies the accumulated payload when the selection is sparse.
        return vector.copy(this, context, positions(mask));
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
        ContextState owner = vectorOwners.get(vector);
        if (owner != null) {
            owner.transferVector(vector);
        }
    }

    private void leaseVectorTree(Vector vector, Set<Vector> visited, List<Vector> leased)
    {
        if (!visited.add(vector)) {
            return;
        }
        if (policy.indexedVectorTreeTraversal()) {
            for (int index = 0; index < vector.childVectorCount(); index++) {
                leaseVectorTree(vector.childVector(index), visited, leased);
            }
        }
        else {
            vector.forEachChildVector(child -> leaseVectorTree(child, visited, leased));
        }
        if (leaseVector(vector)) {
            leased.add(vector);
        }
    }

    private void collectVectorTree(Vector vector, Set<Vector> visited, List<Vector> vectors)
    {
        if (!visited.add(vector)) {
            return;
        }
        if (policy.indexedVectorTreeTraversal()) {
            for (int index = 0; index < vector.childVectorCount(); index++) {
                collectVectorTree(vector.childVector(index), visited, vectors);
            }
        }
        else {
            vector.forEachChildVector(child -> collectVectorTree(child, visited, vectors));
        }
        vectors.add(vector);
    }

    private boolean leaseVector(Vector vector)
    {
        VectorLeaseState existing = vectorLeases.get(vector);
        if (existing != null) {
            existing.references++;
            return true;
        }
        ContextState owner = vectorOwners.get(vector);
        if (owner != null) {
            if (owner.detachForLease(vector)) {
                vectorLeases.put(vector, new VectorLeaseState(owner, owner.lifecycleEpoch));
                return true;
            }
        }
        return false;
    }

    private void releaseVectorLease(Vector vector)
    {
        VectorLeaseState lease = vectorLeases.get(vector);
        if (lease == null) {
            return;
        }
        lease.references--;
        if (lease.references > 0) {
            return;
        }
        vectorLeases.remove(vector);
        lease.owner.restoreAfterLease(vector, lease.ownerEpoch, lease.ownerReleased, lease.ownerDiscarded);
    }

    private synchronized void releaseAsyncVectorLease(Vector vector)
    {
        AsyncVectorLeaseState lease = asyncVectorLeases.get(vector);
        if (lease == null) {
            return;
        }
        lease.references--;
        if (lease.references > 0) {
            return;
        }
        asyncVectorLeases.remove(vector);
        if (!lease.recyclable) {
            return;
        }
        // Detached vector objects may encode position/mask ownership that is local to their producer, so never put
        // the object back into a vector pool. Primitive storage with its own explicit reset contract is independent
        // of that identity and can safely return to the explicitly owned, thread-safe AllocationResources pool.
        releaseStorage(vector);
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
    BufferLeaseOwner leaseTransferredBuffer(Context context, Vector vector)
    {
        if (!policy.transferableBufferLeases()) {
            return null;
        }
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
        ContextState contextState = state(context);
        VectorLeaseState lease = vectorLeases.get(vector);
        if (lease != null && lease.owner == contextState) {
            lease.ownerReleased = true;
            return;
        }
        contextState.releaseVector(vector);
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
        ContextState contextState = state(context);
        VectorLeaseState lease = vectorLeases.get(vector);
        if (lease != null && lease.owner == contextState) {
            lease.ownerDiscarded = true;
            return;
        }
        contextState.discardVector(vector);
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
                key,
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

    private Mask materializedAllDifference(Mask left, Mask right)
    {
        int[] positions = new int[left.size() - right.selectedCount()];
        int outputIndex = fillMaterializedAllDifference(positions, left, right);
        if (outputIndex == positions.length) {
            return Mask.sparseTrusted(positions, outputIndex, left.size(), policy.maskFiltering());
        }
        return Mask.sparse(Arrays.copyOf(positions, outputIndex), left.size(), policy.maskFiltering());
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
        target.copyFrom(source);
    }

    private static long maskBytes(Mask mask)
    {
        return (long) mask.capacity() * Integer.BYTES;
    }

    private static final class ContextState
            implements BufferLeaseOwner, Mask.CapacityListener
    {
        private final Allocator allocator;
        private final Context context;
        private final Stats stats = new Stats();
        private final PoolState pool;
        private final PoolState compatibilityCandidate;
        private PoolState compatibilityPool;
        private final Set<Vector> inUseVectors = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Map<Object, Integer> inUseVectorCounts = new HashMap<>();
        private final Map<Object, Integer> vectorHighWater = new HashMap<>();
        private final Map<String, Long> allocatedVectorBytesByType = new HashMap<>();
        private final Map<Object, Long> retainedBytesByOwner = new IdentityHashMap<>();
        private Mask inUseMasksHead;
        private boolean borrowedVectorResident;
        private long lifecycleEpoch;
        private long lastDiscardEpoch = -1;

        private ContextState(Allocator allocator, Context context, PoolState pool, PoolState compatibilityPool)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.context = requireNonNull(context, "context is null");
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
            return borrowVector(
                    family,
                    minimumCapacity,
                    exactCapacityMatch ? minimumCapacity : Integer.MAX_VALUE,
                    exactCapacityMatch,
                    vectorType);
        }

        public <T extends Vector> T borrowVector(
                Object family,
                int minimumCapacity,
                int maximumCapacity,
                boolean exactCapacityMatch,
                Class<T> vectorType)
        {
            T vector = borrowVector(pool, family, minimumCapacity, maximumCapacity, exactCapacityMatch, vectorType);
            borrowedVectorResident = vector != null;
            if (vector == null && compatibilityActive()) {
                vector = borrowVector(compatibilityPool, family, minimumCapacity, maximumCapacity, exactCapacityMatch, vectorType);
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
                int maximumCapacity,
                boolean exactCapacityMatch,
                Class<T> vectorType)
        {
            TreeMap<Integer, ArrayDeque<Vector>> vectors = source.vectorPool.get(family);
            if (vectors == null) {
                return null;
            }
            Map.Entry<Integer, ArrayDeque<Vector>> entry = vectors.ceilingEntry(minimumCapacity);
            if (entry == null || entry.getKey() > maximumCapacity || (exactCapacityMatch && entry.getKey() != minimumCapacity)) {
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
            long retainedBytes = vector.retainedBytes();
            if (!borrowedVectorResident) {
                allocator.reserveResident(retainedBytes);
            }
            borrowedVectorResident = false;
            // Storage-free wrappers own neither reusable storage nor resident bytes. Their children remain tracked
            // independently, so recording the wrapper in the identity map adds lifecycle work without protecting a
            // resource. Dynamically sized vectors remain tracked even when their initial retained size is zero.
            Object family = vector.poolFamily();
            if (requiresTracking(vector) && inUseVectors.add(vector)) {
                ContextState previousOwner = allocator.vectorOwners.put(vector, this);
                if (previousOwner != null) {
                    inUseVectors.remove(vector);
                    allocator.vectorOwners.put(vector, previousOwner);
                    throw new IllegalStateException("vector is already owned by an allocation context");
                }
                if (family != null) {
                    int inUse = inUseVectorCounts.merge(family, 1, Integer::sum);
                    if (allocator.policy.adaptiveVectorPoolHighWater()) {
                        vectorHighWater.merge(family, inUse, Math::max);
                    }
                }
            }
            stats.acquire(retainedBytes, reused);
            if (!reused && retainedBytes > 0) {
                allocatedVectorBytesByType.merge(vector.getClass().getSimpleName(), retainedBytes, Math::addExact);
            }
            if (vector instanceof DynamicRetainedBytesVector dynamicRetainedBytesVector) {
                dynamicRetainedBytesVector.bindRetainedBytesAccounting(allocator, context);
            }
        }

        public void replaceSharedGrowth(Vector previous, Vector replacement)
        {
            if (previous == replacement) {
                return;
            }
            if (!inUseVectors.contains(previous)) {
                throw new IllegalArgumentException("previous vector is not owned by allocation context");
            }
            if (!Objects.equals(previous.poolFamily(), replacement.poolFamily())) {
                throw new IllegalArgumentException("shared growth must preserve vector pool family");
            }
            if (allocator.vectorOwners.containsKey(replacement)) {
                throw new IllegalArgumentException("replacement vector is already owned by an allocation context");
            }
            if (allocator.vectorOwners.get(previous) != this) {
                throw new IllegalArgumentException("previous vector is not owned by allocation context");
            }
            long previousBytes = previous.retainedBytes();
            long replacementBytes = replacement.retainedBytes();
            if (replacementBytes < previousBytes) {
                throw new IllegalArgumentException("shared growth replacement is smaller than previous vector");
            }
            long delta = replacementBytes - previousBytes;
            allocator.reserveResident(delta);
            inUseVectors.remove(previous);
            inUseVectors.add(replacement);
            if (allocator.vectorOwners.remove(previous) != this) {
                throw new IllegalStateException("previous vector owner is inconsistent");
            }
            allocator.vectorOwners.put(replacement, this);
            stats.replaceSharedGrowth(previousBytes, replacementBytes);
            if (delta > 0) {
                allocatedVectorBytesByType.merge(replacement.getClass().getSimpleName(), delta, Math::addExact);
            }
            if (replacement instanceof DynamicRetainedBytesVector dynamicRetainedBytesVector) {
                dynamicRetainedBytesVector.bindRetainedBytesAccounting(allocator, context);
            }
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
            addVectorToPool(family, vector.poolCapacity(), maxRetained(vector), vector);
        }

        private void retainedBytesChanged(Vector vector, long previousRetainedBytes)
        {
            long delta = vector.retainedBytes() - previousRetainedBytes;
            if (delta == 0) {
                return;
            }
            if (allocator.memoryReservation != null && !inUseVectors.contains(vector)) {
                throw new IllegalArgumentException("vector is not owned by allocation context");
            }
            if (delta > 0) {
                allocator.reserveResident(delta);
                stats.acquire(delta, false);
                allocatedVectorBytesByType.merge(vector.getClass().getSimpleName(), delta, Math::addExact);
            }
            else if (delta < 0) {
                allocator.releaseResident(-delta);
                stats.releaseBytes(-delta);
            }
        }

        private void setRetainedBytes(Object owner, long retainedBytes)
        {
            long previousRetainedBytes = retainedBytesByOwner.getOrDefault(owner, 0L);
            if (retainedBytes == previousRetainedBytes) {
                return;
            }
            long delta = retainedBytes - previousRetainedBytes;
            if (delta > 0) {
                // Reserve before publishing the new owner value. A host reservation may reject the increase; context
                // teardown must then release only the amount that was successfully admitted.
                allocator.reserveResident(delta);
                stats.acquire(delta, false);
            }
            else {
                allocator.releaseResident(-delta);
                stats.releaseBytes(-delta);
            }
            if (retainedBytes == 0) {
                retainedBytesByOwner.remove(owner);
            }
            else {
                retainedBytesByOwner.put(owner, retainedBytes);
            }
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

        private boolean detachForLease(Vector vector)
        {
            if (vector.poolFamily() == null || !untrackVector(vector)) {
                return false;
            }
            stats.releaseBytes(vector.retainedBytes());
            return true;
        }

        private AsyncVectorLeaseState detachForAsyncRelease(Vector vector)
        {
            if (vector.poolFamily() == null || !untrackVector(vector)) {
                return null;
            }
            stats.releaseBytes(vector.retainedBytes());
            allocator.releaseResident(vector.retainedBytes());
            return new AsyncVectorLeaseState(maxRetained(vector) > 0);
        }

        private void restoreAfterLease(Vector vector, long ownerEpoch, boolean ownerReleased, boolean ownerDiscarded)
        {
            if (ownerDiscarded || lastDiscardEpoch > ownerEpoch) {
                allocator.releaseResident(vector.retainedBytes());
                return;
            }
            if (ownerReleased) {
                releaseLeased(vector);
                return;
            }
            if (lifecycleEpoch == ownerEpoch) {
                if (allocator.vectorOwners.containsKey(vector) || !inUseVectors.add(vector)) {
                    throw new IllegalStateException("leased vector is already owned by an allocation context");
                }
                allocator.vectorOwners.put(vector, this);
                inUseVectorCounts.merge(requireNonNull(vector.poolFamily(), "leased vector has no pool family"), 1, Integer::sum);
                stats.acquire(vector.retainedBytes(), true);
                return;
            }
            releaseLeased(vector);
        }

        @Override
        public void releaseLeased(Vector vector)
        {
            Object family = vector.poolFamily();
            if (family != null) {
                addVectorToPool(family, vector.poolCapacity(), maxRetained(vector), vector);
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
            lifecycleEpoch++;
            // The normal BatchBufferScope close path has already released every resolved output and its owned mask.
            // Avoid constructing an IdentityHashMap iterator for that overwhelmingly common empty generation; the
            // full sweep below remains the safety net for lazy or otherwise unexposed allocations.
            if (inUseVectors.isEmpty() && inUseMasksHead == null && retainedBytesByOwner.isEmpty()) {
                stats.release();
                return;
            }
            for (Vector vector : inUseVectors) {
                if (allocator.vectorOwners.remove(vector) != this) {
                    throw new IllegalStateException("vector owner is inconsistent during context release");
                }
                Object family = vector.poolFamily();
                if (family != null) {
                    addVectorToPoolAtContextRelease(family, vector.poolCapacity(), maxRetained(vector), vector);
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
            releaseRetainedBytes();
            stats.release();
        }

        private void addVectorToPoolAtContextRelease(Object family, int capacity, int maxRetained, Vector vector)
        {
            if (compatibilityActive()) {
                addVectorToPool(family, capacity, maxRetained, vector);
                return;
            }
            if (maxRetained <= 0 || allocator.policy.maxLocalVectorPoolBytes() <= 0) {
                allocator.releaseResident(vector.retainedBytes());
                return;
            }
            int retentionLimit = Math.max(maxRetained, vectorHighWater.getOrDefault(family, 0));
            ArrayDeque<Vector> order = pool.vectorPoolOrder.get(family);
            long retainedBytes = vector.retainedBytes();
            if ((order != null && order.size() >= retentionLimit) ||
                    retainedBytes > allocator.policy.maxLocalVectorPoolBytes() - pool.vectorPoolBytes) {
                allocator.releaseResident(retainedBytes);
                return;
            }
            pool.vectorPool
                    .computeIfAbsent(family, _ -> new TreeMap<>())
                    .computeIfAbsent(capacity, _ -> new ArrayDeque<>())
                    .addLast(vector);
            pool.vectorPoolOrder.computeIfAbsent(family, _ -> new ArrayDeque<>()).addLast(vector);
            pool.vectorPoolGlobalOrder.addLast(vector);
            pool.vectorPoolBytes += retainedBytes;
        }

        public void discardAll()
        {
            lifecycleEpoch++;
            lastDiscardEpoch = lifecycleEpoch;
            Mask mask = inUseMasksHead;
            while (mask != null) {
                Mask next = mask.trackedNext();
                discardMaskResident(mask);
                mask.clearTrackedInUse();
                mask = next;
            }
            inUseMasksHead = null;
            for (Vector vector : inUseVectors) {
                if (allocator.vectorOwners.remove(vector) != this) {
                    throw new IllegalStateException("vector owner is inconsistent during context discard");
                }
                allocator.releaseResident(vector.retainedBytes());
            }
            inUseVectors.clear();
            inUseVectorCounts.clear();
            releaseRetainedBytes();
            stats.release();
        }

        private void releaseRetainedBytes()
        {
            long retainedBytes = 0;
            for (long bytes : retainedBytesByOwner.values()) {
                retainedBytes = Math.addExact(retainedBytes, bytes);
            }
            retainedBytesByOwner.clear();
            allocator.releaseResident(retainedBytes);
        }

        private boolean untrackVector(Vector vector)
        {
            if (!inUseVectors.remove(vector)) {
                return false;
            }
            if (allocator.vectorOwners.remove(vector) != this) {
                throw new IllegalStateException("vector owner is inconsistent during release");
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

        private int maxRetained(Vector vector)
        {
            VectorPoolRetentionClass retentionClass = vector.poolRetentionClass();
            if (retentionClass == VectorPoolRetentionClass.VECTOR_DEFAULT) {
                return vector.poolMaxRetained();
            }
            return allocator.policy.aggregateStateVectorRetention().maxRetained(retentionClass, vector.retainedBytes());
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
                    (long) maxRetained(vector) * Math.max(1, allocator.policy.compatibleVectorPoolRetentionMultiplier()));
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

        public void replaceSharedGrowth(long previousBytes, long replacementBytes)
        {
            long delta = replacementBytes - previousBytes;
            total += delta;
            current += delta;
            peak = Math.max(peak, current);
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

    public static final class VectorTreeLease
            implements AutoCloseable
    {
        private final Allocator allocator;
        private final List<Vector> vectors;
        private boolean closed;

        private VectorTreeLease(Allocator allocator, List<Vector> vectors)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.vectors = List.copyOf(requireNonNull(vectors, "vectors is null"));
        }

        /**
         * Converts this exclusive producer-local lease into ownership that may be released by an asynchronous
         * consumer. Conversion fails without changing ownership when any vector is shared by another local lease.
         */
        public Optional<AsyncVectorTreeLease> tryDetachForAsyncRelease()
        {
            return allocator.detachVectorTreeLeaseForAsyncRelease(this);
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            for (Vector vector : vectors) {
                allocator.releaseVectorLease(vector);
            }
        }
    }

    public static final class AsyncVectorTreeLease
            implements AutoCloseable
    {
        private final Allocator allocator;
        private final List<Vector> vectors;
        private boolean closed;

        private AsyncVectorTreeLease(Allocator allocator, List<Vector> vectors)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.vectors = List.copyOf(requireNonNull(vectors, "vectors is null"));
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            for (Vector vector : vectors) {
                allocator.releaseAsyncVectorLease(vector);
            }
        }
    }

    private static final class VectorLeaseState
    {
        private final ContextState owner;
        private final long ownerEpoch;
        private int references = 1;
        private boolean ownerReleased;
        private boolean ownerDiscarded;

        private VectorLeaseState(ContextState owner, long ownerEpoch)
        {
            this.owner = requireNonNull(owner, "owner is null");
            this.ownerEpoch = ownerEpoch;
        }
    }

    private static final class AsyncVectorLeaseState
    {
        private final boolean recyclable;
        private int references = 1;

        private AsyncVectorLeaseState(boolean recyclable)
        {
            this.recyclable = recyclable;
        }
    }

    public record Context(
            String name,
            Object scopeId,
            Object poolGroup,
            Object compatibilityGroup,
            VariableWidthStorageReusePolicy variableWidthStorageReusePolicy)
    {
        public Context
        {
            requireNonNull(name, "name is null");
            requireNonNull(scopeId, "scopeId is null");
            requireNonNull(poolGroup, "poolGroup is null");
            requireNonNull(compatibilityGroup, "compatibilityGroup is null");
            requireNonNull(variableWidthStorageReusePolicy, "variableWidthStorageReusePolicy is null");
        }

        public Context(String name, Object scopeId, Object poolGroup, Object compatibilityGroup)
        {
            this(name, scopeId, poolGroup, compatibilityGroup, VariableWidthStorageReusePolicy.allocatorDefault());
        }

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

        public Context(String name, Object poolGroup, VariableWidthStorageReusePolicy variableWidthStorageReusePolicy)
        {
            this(
                    name,
                    new Object(),
                    requireNonNull(poolGroup, "poolGroup is null"),
                    poolGroup,
                    requireNonNull(variableWidthStorageReusePolicy, "variableWidthStorageReusePolicy is null"));
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
