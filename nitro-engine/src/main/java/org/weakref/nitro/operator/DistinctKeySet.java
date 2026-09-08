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
package org.weakref.nitro.operator;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.toIntExact;

final class DistinctKeySet
{
    private final DistinctIndex index;
    private final List<TypeBinding> keyTypes;
    private final int unboundKeyPrefix;
    private final Allocator allocator;
    private final Allocator.Context allocationContext;

    private DistinctKeySet(DistinctIndex index, List<TypeBinding> keyTypes, int unboundKeyPrefix)
    {
        this(index, keyTypes, unboundKeyPrefix, null, null);
    }

    private DistinctKeySet(
            DistinctIndex index,
            List<TypeBinding> keyTypes,
            int unboundKeyPrefix,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        this.index = index;
        this.keyTypes = List.copyOf(keyTypes);
        this.unboundKeyPrefix = unboundKeyPrefix;
        this.allocator = allocator;
        this.allocationContext = allocationContext;
    }

    /**
     * Creates a distinct-key set that drops rows with any NULL key column. This matches the SQL semantics of
     * {@code count(distinct ...)} and distinct aggregation, where NULL keys are ignored.
     */
    public static DistinctKeySet create(
            Vector[] samples,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        return createWithUnboundPrefix(
                samples,
                false,
                0,
                List.of(),
                null,
                null,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
    }

    public static DistinctKeySet create(
            Vector[] samples,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        return createWithUnboundPrefix(
                samples,
                false,
                0,
                keyTypes,
                allocator,
                allocationContext,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
    }

    /**
     * Creates an exact distinct set for the common grouped {@code count(distinct long)} shape. The first
     * vector is a dense non-null group id and the second is the nullable value. Partitioning by group avoids
     * repeating the group id in every hash slot and keeps each probe table smaller.
     */
    public static DistinctKeySet createGroupedLong(
            Vector[] samples,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        if (samples.length != 2 || !(samples[0] instanceof I64Vector) || !isIntegerVector(samples[1])) {
            return create(samples, arrayPool, codeGeneration, policy, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        }
        return new DistinctKeySet(new GroupedLongDistinctIndex(arrayPool, policy), List.of(), 0);
    }

    public static DistinctKeySet createGroupedLong(
            Vector[] samples,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        return createGroupedLong(
                samples,
                keyTypes,
                allocator,
                allocationContext,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                samples[0].length());
    }

    public static DistinctKeySet createGroupedLong(
            Vector[] samples,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy,
            int expectedSize)
    {
        if (expectedSize < 0) {
            throw new IllegalArgumentException("expectedSize is negative");
        }
        if (samples.length != 2 || !(samples[0] instanceof I64Vector) || !isIntegerVector(samples[1])) {
            return createWithUnboundPrefix(
                    samples,
                    false,
                    1,
                    keyTypes,
                    allocator,
                    allocationContext,
                    arrayPool,
                    codeGeneration,
                    policy,
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy,
                    expectedSize);
        }
        validateKeyVectors(keyTypes, 1, samples);
        StructuralKeyKernel[] kernels = structuralKeyKernels(samples.length, 1, keyTypes, codeGeneration);
        boolean rawLongKeyIdentity = keyTypes.size() == 1 && keyTypes.getFirst().supportsRawLongKeyIdentity();
        if (!allowsLegacyPhysicalShortcuts(kernels) && !rawLongKeyIdentity) {
            ResolvedFixedWidthKeyLayout fixedWidth = ResolvedFixedWidthKeyLayout.tryCreate(
                    flatKeyTypes(samples.length, keyTypes, 1),
                    kernels,
                    samples);
            if (fixedWidth != null) {
                return new DistinctKeySet(
                        new FixedWidthDistinctIndex(
                                fixedWidth,
                                false,
                                expectedSize,
                                arrayPool,
                                codeGeneration,
                                adaptiveLongGroupingPolicy),
                        keyTypes,
                        1,
                        allocator,
                        allocationContext);
            }
            DistinctIndex projectedFlat = tryCreateProjectedFlatIndex(
                    samples,
                    flatKeyTypes(samples.length, keyTypes, 1),
                    kernels,
                    false,
                    expectedSize,
                    arrayPool,
                    codeGeneration,
                    flatKeyTablePolicy);
            if (projectedFlat != null) {
                return new DistinctKeySet(
                        projectedFlat,
                        keyTypes,
                        1,
                        allocator,
                        allocationContext);
            }
            throw PersistentKeyTableSupport.unsupportedLayout(
                    "grouped distinct",
                    flatKeyTypes(samples.length, keyTypes, 1),
                    samples);
        }
        return new DistinctKeySet(
                new GroupedLongDistinctIndex(arrayPool, policy),
                keyTypes,
                1,
                allocator,
                allocationContext);
    }

    /**
     * Creates a distinct-key set.
     *
     * @param retainNulls when {@code true}, rows whose key contains NULLs are retained and de-duplicated with
     * SQL {@code DISTINCT}/{@code UNION} semantics (two NULLs in the same column are equal; a NULL is distinct
     * from any concrete value), so a single representative null-keyed row survives. When {@code false}, any row
     * with a NULL key column is dropped (the {@code count(distinct ...)} semantics).
     */
    public static DistinctKeySet create(
            Vector[] samples,
            boolean retainNulls,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        return createWithUnboundPrefix(
                samples,
                retainNulls,
                0,
                List.of(),
                null,
                null,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
    }

    public static DistinctKeySet create(
            Vector[] samples,
            boolean retainNulls,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        return createWithUnboundPrefix(
                samples,
                retainNulls,
                0,
                keyTypes,
                allocator,
                allocationContext,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
    }

    /**
     * Creates a distinct-key set sized for the rows that can actually reach it. The physical vectors may retain a
     * much larger addressable domain after an upstream filter, so using their length here can eagerly allocate and
     * clear a table for rows excluded by the mask.
     */
    public static DistinctKeySet create(
            Vector[] samples,
            boolean retainNulls,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy,
            int expectedSize)
    {
        if (expectedSize < 0) {
            throw new IllegalArgumentException("expectedSize is negative");
        }
        return createWithUnboundPrefix(
                samples,
                retainNulls,
                0,
                keyTypes,
                allocator,
                allocationContext,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                expectedSize);
    }

    static DistinctKeySet createWithUnboundPrefix(
            Vector[] samples,
            boolean retainNulls,
            int unboundKeyPrefix,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy)
    {
        return createWithUnboundPrefix(
                samples,
                retainNulls,
                unboundKeyPrefix,
                keyTypes,
                allocator,
                allocationContext,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                samples[0].length());
    }

    static DistinctKeySet createWithUnboundPrefix(
            Vector[] samples,
            boolean retainNulls,
            int unboundKeyPrefix,
            List<TypeBinding> keyTypes,
            Allocator allocator,
            Allocator.Context allocationContext,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy,
            int expectedSize)
    {
        validateKeyVectors(keyTypes, unboundKeyPrefix, samples);
        StructuralKeyKernel[] kernels = structuralKeyKernels(samples.length, unboundKeyPrefix, keyTypes, codeGeneration);
        if (!allowsLegacyPhysicalShortcuts(kernels)) {
            ResolvedFixedWidthKeyLayout fixedWidth = ResolvedFixedWidthKeyLayout.tryCreate(
                    flatKeyTypes(samples.length, keyTypes, unboundKeyPrefix),
                    kernels,
                    samples);
            if (fixedWidth != null) {
                return new DistinctKeySet(
                        new FixedWidthDistinctIndex(
                                fixedWidth,
                                retainNulls,
                                expectedSize,
                                arrayPool,
                                codeGeneration,
                                adaptiveLongGroupingPolicy),
                        keyTypes,
                        unboundKeyPrefix,
                        allocator,
                        allocationContext);
            }
            DistinctIndex projectedFlat = tryCreateProjectedFlatIndex(
                    samples,
                    flatKeyTypes(samples.length, keyTypes, unboundKeyPrefix),
                    kernels,
                    retainNulls,
                    expectedSize,
                    arrayPool,
                    codeGeneration,
                    flatKeyTablePolicy);
            if (projectedFlat != null) {
                return new DistinctKeySet(
                        projectedFlat,
                        keyTypes,
                        unboundKeyPrefix,
                        allocator,
                        allocationContext);
            }
            throw PersistentKeyTableSupport.unsupportedLayout(
                    "distinct",
                    flatKeyTypes(samples.length, keyTypes, unboundKeyPrefix),
                    samples);
        }
        DistinctIndex index = createIndex(
                samples,
                keyTypes,
                unboundKeyPrefix,
                arrayPool,
                codeGeneration,
                policy,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                expectedSize);
        if (retainNulls) {
            index = new RetainNullsDistinctIndex(index, samples.length, arrayPool, policy);
        }
        return new DistinctKeySet(index, keyTypes, unboundKeyPrefix, allocator, allocationContext);
    }

    private static DistinctIndex tryCreateProjectedFlatIndex(
            Vector[] samples,
            List<TypeBinding> types,
            StructuralKeyKernel[] kernels,
            boolean retainNulls,
            int expectedSize,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            FlatKeyTablePolicy policy)
    {
        ResolvedPersistentKeyLayout persistentLayout = ResolvedPersistentKeyLayout.tryCreate(types, kernels, samples);
        if (persistentLayout == null) {
            return null;
        }
        ProjectedFlatKeyLayout layout = codeGeneration.projectedFlatKeyLayouts().create(
                persistentLayout,
                samples,
                types,
                arrayPool,
                codeGeneration,
                policy);
        return new ProjectedFlatDistinctIndex(layout, retainNulls, Math.max(16, expectedSize));
    }

    private static StructuralKeyKernel[] structuralKeyKernels(
            int keyCount,
            int unboundKeyPrefix,
            List<TypeBinding> keyTypes,
            OperatorCodeGenerationResources codeGeneration)
    {
        StructuralKeyKernel[] kernels = new StructuralKeyKernel[keyCount];
        StructuralTypeKernelFactory structuralTypes = codeGeneration.structuralTypes();
        for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
            TypeBinding keyType = keyTypes.isEmpty() || keyIndex < unboundKeyPrefix
                    ? Schema.unspecified(keyCount).field(keyIndex).type()
                    : keyTypes.get(keyIndex - unboundKeyPrefix);
            kernels[keyIndex] = structuralTypes.key(keyType);
        }
        return kernels;
    }

    private static boolean allowsLegacyPhysicalShortcuts(StructuralKeyKernel[] kernels)
    {
        for (StructuralKeyKernel kernel : kernels) {
            if (!kernel.allowsLegacyPhysicalShortcuts()) {
                return false;
            }
        }
        return true;
    }

    private static DistinctIndex createIndex(
            Vector[] samples,
            List<TypeBinding> keyTypes,
            int unboundKeyPrefix,
            PrimitiveArrayPool arrayPool,
            OperatorCodeGenerationResources codeGeneration,
            DistinctKeySetPolicy policy,
            AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
            FlatKeyTablePolicy flatKeyTablePolicy,
            int expectedSize)
    {
        if (samples.length == 1 && isIntegerVector(samples[0])) {
            return new LongDistinctIndex(Math.max(16, expectedSize), arrayPool, policy);
        }
        if (samples.length == 2 && isIntegerVector(samples[0]) && isIntegerVector(samples[1])) {
            return new LongPairDistinctIndex(
                    Math.max(16, expectedSize),
                    policy.adaptiveCompactLongPair() && admitsAdaptiveCompactLongPair(samples, policy),
                    arrayPool,
                    codeGeneration,
                    policy,
                    adaptiveLongGroupingPolicy);
        }
        if (policy.adaptiveCompactMultiLong() &&
                samples.length >= policy.adaptiveCompactMultiLongMinArity() &&
                samples.length <= AbstractFixedWidthKeyTable.MAX_ARITY &&
                allIntegerVectors(samples)) {
            return new AdaptiveMultiLongDistinctIndex(
                    samples.length,
                    Math.max(16, expectedSize),
                    arrayPool,
                    codeGeneration,
                    policy,
                    adaptiveLongGroupingPolicy);
        }
        if (samples.length == 3 && isIntegerVector(samples[0]) && isIntegerVector(samples[1]) && isIntegerVector(samples[2])) {
            return new LongTripleDistinctIndex(Math.max(16, expectedSize));
        }
        if (samples.length == 4 && isIntegerVector(samples[0]) && isIntegerVector(samples[1]) && isIntegerVector(samples[2]) && isIntegerVector(samples[3])) {
            return new LongQuadDistinctIndex(Math.max(16, expectedSize));
        }
        if (samples.length >= 5 && samples.length <= AbstractFixedWidthKeyTable.MAX_ARITY && allIntegerVectors(samples)) {
            return new MultiLongDistinctIndex(
                    samples.length,
                    Math.max(16, expectedSize),
                    arrayPool,
                    codeGeneration,
                    adaptiveLongGroupingPolicy);
        }
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                samples,
                false,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                flatKeyTypes(samples.length, keyTypes, unboundKeyPrefix));
        if (layout != null) {
            return new FlatDistinctIndex(layout, Math.max(16, expectedSize), arrayPool, policy);
        }
        return new ObjectDistinctIndex(samples.length);
    }

    private static List<TypeBinding> flatKeyTypes(int keyCount, List<TypeBinding> keyTypes, int unboundKeyPrefix)
    {
        if (keyTypes.isEmpty()) {
            return List.of();
        }
        TypeBinding[] types = new TypeBinding[keyCount];
        Schema unspecified = Schema.unspecified(keyCount);
        for (int index = 0; index < unboundKeyPrefix; index++) {
            types[index] = unspecified.field(index).type();
        }
        for (int index = unboundKeyPrefix; index < keyCount; index++) {
            types[index] = keyTypes.get(index - unboundKeyPrefix);
        }
        return List.of(types);
    }

    /**
     * The adaptive table earns its compact topology only when the observed physical lanes fit it. A bounded,
     * evenly spaced first-batch sample avoids admitting a full-width long pair merely because its schema has two
     * integer columns; any later out-of-domain value still promotes the retained table exactly.
     */
    private static boolean admitsAdaptiveCompactLongPair(Vector[] samples, DistinctKeySetPolicy policy)
    {
        int length = samples[0].length();
        int sampleSize = Math.min(length, policy.adaptiveCompactLongPairSampleSize());
        if (sampleSize == 0) {
            return false;
        }
        VectorAccess.LongValues first = VectorAccess.longValues(samples[0]);
        VectorAccess.LongValues second = VectorAccess.longValues(samples[1]);
        for (int sample = 0; sample < sampleSize; sample++) {
            int position = (int) ((long) sample * length / sampleSize);
            long firstValue = first.value(position);
            long secondValue = second.value(position);
            if (firstValue != (int) firstValue || secondValue != (int) secondValue) {
                if (policy.debugDistinctShapes()) {
                    System.err.printf("[adaptive-pair-distinct] rows=%d sample=%d compact=false%n", length, sampleSize);
                }
                return false;
            }
        }
        if (policy.debugDistinctShapes()) {
            System.err.printf("[adaptive-pair-distinct] rows=%d sample=%d compact=true%n", length, sampleSize);
        }
        return true;
    }

    public boolean add(Vector[] values, Vector[] nulls, int position)
    {
        try {
            return index.add(values, nulls, position);
        }
        finally {
            accountRetainedBytes();
        }
    }

    public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
    {
        validateKeyVectors(values);
        try {
            return index.addBatch(values, nulls, mask, distinctPositions);
        }
        finally {
            accountRetainedBytes();
        }
    }

    /** Adds a grouped batch whose first key is a dense group id in {@code [0, groupCount)}. */
    public int addGroupedBatch(Vector[] values, Vector[] nulls, Mask mask, int groupCount, int[] distinctPositions)
    {
        validateKeyVectors(values);
        try {
            return index.addGroupedBatch(values, nulls, mask, groupCount, distinctPositions);
        }
        finally {
            accountRetainedBytes();
        }
    }

    void validateKeyVectors(Vector[] values)
    {
        validateKeyVectors(keyTypes, unboundKeyPrefix, values);
    }

    private static void validateKeyVectors(List<TypeBinding> keyTypes, int unboundKeyPrefix, Vector[] values)
    {
        if (keyTypes.isEmpty()) {
            return;
        }
        if (keyTypes.size() + unboundKeyPrefix != values.length) {
            throw new IllegalArgumentException("Distinct key type count does not match vector count");
        }
        for (int typeIndex = 0; typeIndex < keyTypes.size(); typeIndex++) {
            int vectorIndex = typeIndex + unboundKeyPrefix;
            TypeBinding type = keyTypes.get(typeIndex);
            if (type.isSpecified() && !type.supportsVector(values[vectorIndex])) {
                throw new IllegalArgumentException(
                        "Distinct key vector at index " + vectorIndex +
                                " is incompatible with plan-time type " + type.identity());
            }
        }
    }

    public void reserveAdditional(int additionalEntries)
    {
        index.reserveAdditional(additionalEntries);
        accountRetainedBytes();
    }

    public void releaseBuffers()
    {
        index.releaseBuffers();
        accountRetainedBytes();
    }

    long retainedBytes()
    {
        return index.retainedBytes();
    }

    private void accountRetainedBytes()
    {
        if (allocator != null) {
            allocator.setRetainedBytes(allocationContext, index, index.retainedBytes());
        }
    }

    private interface DistinctIndex
    {
        boolean add(Vector[] values, Vector[] nulls, int position);

        default int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (add(values, nulls, position)) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (add(values, nulls, position)) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        default int addGroupedBatch(Vector[] values, Vector[] nulls, Mask mask, int groupCount, int[] distinctPositions)
        {
            return addBatch(values, nulls, mask, distinctPositions);
        }

        /** Processes positions whose key columns have already been proven non-null by a wrapper. */
        default int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            int count = 0;
            for (int index = 0; index < positionCount; index++) {
                int position = positions[index];
                if (add(values, nulls, position)) {
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        default int addNonNullDenseBatch(Vector[] values, Vector[] nulls, int positionCount, int[] positions, int[] distinctPositions)
        {
            return addNonNullBatch(values, nulls, positions, positionCount, distinctPositions);
        }

        /** Returns {@code -1} when this representation cannot retain SQL-null keys without the object fallback. */
        default int addRetainingNullBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            return -1;
        }

        default void reserveAdditional(int additionalEntries) {}

        default void releaseBuffers() {}

        default long retainedBytes()
        {
            return 0;
        }
    }

    private static final class LongDistinctIndex
            implements DistinctIndex
    {
        private static final int PAGE_SHIFT = 16;
        private static final int PAGE_BITS = 1 << PAGE_SHIFT;
        private static final int PAGE_WORDS = PAGE_BITS / Long.SIZE;
        private final PrimitiveArrayPool arrayPool;
        private final DistinctKeySetPolicy policy;

        private PooledLongHashSet pooledKeys;
        private Long2ObjectOpenHashMap<long[]> bitmapPages;
        private long minimumKey = Long.MAX_VALUE;
        private long maximumKey = Long.MIN_VALUE;
        private int size;
        private long addCalls;

        private LongDistinctIndex(int expectedSize, PrimitiveArrayPool arrayPool, DistinctKeySetPolicy policy)
        {
            this.arrayPool = arrayPool;
            this.policy = policy;
            createHash(expectedSize);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            if (hashPresent()) {
                ensureHashCapacity(hashSize() + Math.max(0, additionalEntries));
            }
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls[0], position)) {
                return false;
            }
            addCalls++;
            return addKey(OperatorVectorSupport.longValue(values[0], position));
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values[0]);
            VectorAccess.BooleanValues keyNulls = VectorAccess.booleanValues(nulls[0]);
            if (bitmapPages != null && mask.all()) {
                return addDenseBitmapBatch(keyValues, keyNulls, mask.size(), distinctPositions);
            }
            if (bitmapPages == null && (!policy.adaptivePagedLongBitmap() || size >= policy.pagedLongBitmapMinKeys())) {
                pooledKeys.enableVectorTags(addCalls);
                return addFinalHashBatch(keyValues, keyNulls, mask, distinctPositions);
            }
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (keyNulls.value(position)) {
                        continue;
                    }
                    addCalls++;
                    if (addKey(keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (keyNulls.value(position)) {
                        continue;
                    }
                    addCalls++;
                    if (addKey(keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        /** Dense bitmap loop with representation fallback handled only when a new page is encountered. */
        private int addDenseBitmapBatch(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                int positionCount,
                int[] distinctPositions)
        {
            int count = 0;
            for (int position = 0; position < positionCount; position++) {
                if (keyNulls.value(position)) {
                    continue;
                }
                addCalls++;
                long key = keyValues.value(position);
                long pageId = key >> PAGE_SHIFT;
                long[] page = bitmapPages.get(pageId);
                if (page == null) {
                    page = arrayPool.borrowLongs(PAGE_WORDS);
                    Arrays.fill(page, 0);
                    bitmapPages.put(pageId, page);
                    if ((long) bitmapPages.size() * PAGE_BITS >
                            Math.max(policy.pagedLongBitmapMinKeys(), size) * policy.pagedLongBitmapMaxBitsPerKey()) {
                        convertToHash();
                        if (addHashKey(key)) {
                            size++;
                            distinctPositions[count++] = position;
                        }
                        return addFinalDenseHashRange(
                                keyValues,
                                keyNulls,
                                position + 1,
                                positionCount,
                                distinctPositions,
                                count);
                    }
                }
                int pagePosition = (int) key & (PAGE_BITS - 1);
                int wordIndex = pagePosition >>> 6;
                long bit = 1L << pagePosition;
                if ((page[wordIndex] & bit) == 0) {
                    page[wordIndex] |= bit;
                    size++;
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        /** Plain steady-state hash loop selected once per batch after bitmap admission has closed. */
        private int addFinalHashBatch(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                Mask mask,
                int[] distinctPositions)
        {
            if (pooledKeys.vectorTagsEnabled()) {
                return addFinalTaggedHashBatch(keyValues, keyNulls, mask, distinctPositions);
            }
            return addFinalScalarHashBatch(keyValues, keyNulls, mask, distinctPositions);
        }

        private int addFinalTaggedHashBatch(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                Mask mask,
                int[] distinctPositions)
        {
            int count = 0;
            if (mask.all()) {
                return addFinalDenseTaggedHashRange(keyValues, keyNulls, 0, mask.size(), distinctPositions, 0);
            }
            int currentSize = size;
            for (int position : mask) {
                if (keyNulls.value(position)) {
                    continue;
                }
                addCalls++;
                if (pooledKeys.addTaggedFinal(keyValues.value(position))) {
                    distinctPositions[count++] = position;
                    currentSize++;
                }
            }
            size = currentSize;
            return count;
        }

        private int addFinalScalarHashBatch(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                Mask mask,
                int[] distinctPositions)
        {
            int count = 0;
            if (mask.all()) {
                return addFinalDenseScalarHashRange(keyValues, keyNulls, 0, mask.size(), distinctPositions, 0);
            }
            int currentSize = size;
            for (int position : mask) {
                if (keyNulls.value(position)) {
                    continue;
                }
                addCalls++;
                if (pooledKeys.addScalarFinal(keyValues.value(position))) {
                    distinctPositions[count++] = position;
                    currentSize++;
                }
            }
            size = currentSize;
            return count;
        }

        private int addFinalDenseHashRange(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                int startPosition,
                int endPosition,
                int[] distinctPositions,
                int count)
        {
            if (pooledKeys.vectorTagsEnabled()) {
                return addFinalDenseTaggedHashRange(keyValues, keyNulls, startPosition, endPosition, distinctPositions, count);
            }
            return addFinalDenseScalarHashRange(keyValues, keyNulls, startPosition, endPosition, distinctPositions, count);
        }

        private int addFinalDenseTaggedHashRange(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                int startPosition,
                int endPosition,
                int[] distinctPositions,
                int count)
        {
            int currentSize = size;
            for (int position = startPosition; position < endPosition; position++) {
                if (keyNulls.value(position)) {
                    continue;
                }
                addCalls++;
                if (pooledKeys.addTaggedFinal(keyValues.value(position))) {
                    distinctPositions[count++] = position;
                    currentSize++;
                }
            }
            size = currentSize;
            return count;
        }

        private int addFinalDenseScalarHashRange(
                VectorAccess.LongValues keyValues,
                VectorAccess.BooleanValues keyNulls,
                int startPosition,
                int endPosition,
                int[] distinctPositions,
                int count)
        {
            int currentSize = size;
            for (int position = startPosition; position < endPosition; position++) {
                if (keyNulls.value(position)) {
                    continue;
                }
                addCalls++;
                if (pooledKeys.addScalarFinal(keyValues.value(position))) {
                    distinctPositions[count++] = position;
                    currentSize++;
                }
            }
            size = currentSize;
            return count;
        }

        private boolean addKey(long key)
        {
            if (bitmapPages != null) {
                return addBitmapKey(key);
            }
            if (!addHashKey(key)) {
                return false;
            }
            size++;
            // Representation selection is a bounded admission phase, not a permanent steady-state tax. A stream
            // whose first window is sparse remains a hash set; continuing to maintain its extrema and retest the
            // same predicate made wide 64-bit domains pay several operations for every later distinct key.
            if (policy.adaptivePagedLongBitmap() && size <= policy.pagedLongBitmapMinKeys()) {
                minimumKey = Math.min(minimumKey, key);
                maximumKey = Math.max(maximumKey, key);
                if (size == policy.pagedLongBitmapMinKeys() && denseEnoughForBitmap()) {
                    convertToBitmap();
                }
            }
            return true;
        }

        private boolean addBitmapKey(long key)
        {
            long pageId = key >> PAGE_SHIFT;
            long[] page = bitmapPages.get(pageId);
            if (page == null) {
                page = arrayPool.borrowLongs(PAGE_WORDS);
                Arrays.fill(page, 0);
                bitmapPages.put(pageId, page);
                if ((long) bitmapPages.size() * PAGE_BITS >
                        Math.max(policy.pagedLongBitmapMinKeys(), size) * policy.pagedLongBitmapMaxBitsPerKey()) {
                    convertToHash();
                    return addKey(key);
                }
            }
            int pagePosition = (int) key & (PAGE_BITS - 1);
            int wordIndex = pagePosition >>> 6;
            long bit = 1L << pagePosition;
            if ((page[wordIndex] & bit) != 0) {
                return false;
            }
            page[wordIndex] |= bit;
            size++;
            return true;
        }

        private boolean denseEnoughForBitmap()
        {
            long range = maximumKey - minimumKey;
            return range >= 0 && range / policy.pagedLongBitmapMaxBitsPerKey() < size;
        }

        private void convertToBitmap()
        {
            bitmapPages = new Long2ObjectOpenHashMap<>();
            forEachHashKey(key -> {
                long pageId = key >> PAGE_SHIFT;
                long[] page = bitmapPages.get(pageId);
                if (page == null) {
                    page = arrayPool.borrowLongs(PAGE_WORDS);
                    Arrays.fill(page, 0);
                    bitmapPages.put(pageId, page);
                }
                int pagePosition = (int) key & (PAGE_BITS - 1);
                page[pagePosition >>> 6] |= 1L << pagePosition;
            });
            releaseHash();
        }

        private void convertToHash()
        {
            createHash(Math.max(policy.pagedLongBitmapMinKeys(), size));
            for (Long2ObjectMap.Entry<long[]> entry : bitmapPages.long2ObjectEntrySet()) {
                long pageBase = entry.getLongKey() << PAGE_SHIFT;
                long[] page = entry.getValue();
                for (int wordIndex = 0; wordIndex < page.length; wordIndex++) {
                    long word = page[wordIndex];
                    while (word != 0) {
                        int bit = Long.numberOfTrailingZeros(word);
                        addHashKey(pageBase | ((long) wordIndex << 6) | bit);
                        word &= word - 1;
                    }
                }
                arrayPool.release(page);
            }
            bitmapPages = null;
            pooledKeys.enableVectorTags(addCalls);
        }

        @Override
        public void releaseBuffers()
        {
            releaseHash();
            if (bitmapPages != null) {
                for (long[] page : bitmapPages.values()) {
                    arrayPool.release(page);
                }
                bitmapPages = null;
            }
            pooledKeys = null;
        }

        private void createHash(int expectedSize)
        {
            pooledKeys = new PooledLongHashSet(expectedSize, arrayPool, policy.pooledLongHashSetPolicy(), !policy.adaptivePagedLongBitmap());
        }

        private boolean hashPresent()
        {
            return pooledKeys != null;
        }

        private int hashSize()
        {
            return pooledKeys.size();
        }

        private void ensureHashCapacity(int expectedSize)
        {
            pooledKeys.ensureCapacity(expectedSize);
        }

        private boolean addHashKey(long key)
        {
            return pooledKeys.add(key);
        }

        private void forEachHashKey(java.util.function.LongConsumer consumer)
        {
            pooledKeys.forEach(consumer);
        }

        private void releaseHash()
        {
            if (pooledKeys != null) {
                pooledKeys.releaseBuffers();
                pooledKeys = null;
            }
        }

        @Override
        public long retainedBytes()
        {
            long bytes = pooledKeys == null ? 0 : pooledKeys.retainedBytes();
            if (bitmapPages != null) {
                bytes += (long) bitmapPages.size() * PAGE_WORDS * Long.BYTES;
            }
            return bytes;
        }
    }

    private static final class FlatDistinctIndex
            implements DistinctIndex
    {
        private final PrimitiveArrayPool arrayPool;
        private final FlatKeyLayout layout;
        private final DistinctKeySetPolicy policy;
        private final FlatGroupingTable table;
        private int[] probePositions;
        private long[] generatedHashScratch;
        private int[] dictionaryFirstLogicalPositions = new int[0];
        private int[] dictionaryDomainPositions = new int[0];
        private final Vector[] dictionaryDomainValues = new Vector[1];
        private final Vector[] dictionaryDomainNulls = new Vector[1];
        private boolean emptyBinarySeen;

        private FlatDistinctIndex(FlatKeyLayout layout, int expectedSize, PrimitiveArrayPool arrayPool, DistinctKeySetPolicy policy)
        {
            this.arrayPool = arrayPool;
            this.layout = layout;
            this.policy = policy;
            // A set assigns one monotonically increasing ordinal per retained record and never exposes or
            // reorders that ordinal. Record index is therefore the exact group id: let the general table's
            // identity mode avoid a redundant slot->group array and reverse group->record map.
            this.table = new FlatGroupingTable(layout, expectedSize, true);
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            if (isTrackedSentinel(values, position)) {
                if (emptyBinarySeen) {
                    return false;
                }
                emptyBinarySeen = true;
                return true;
            }
            int recordCount = table.recordCount();
            return table.assignGroup(values, position, recordCount) == recordCount;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            int encodedCount = addSingleDictionaryBatch(values, nulls, mask, distinctPositions);
            if (encodedCount >= 0) {
                return encodedCount;
            }
            boolean nullFree = true;
            for (Vector nullsVector : nulls) {
                if (!VectorAccess.isAllFalseNulls(nullsVector)) {
                    nullFree = false;
                    break;
                }
            }

            table.beginBatch(values, nulls);
            try {
                layout.admitFrequentDictionarySentinel(values, mask);
                if (policy.filterSentinelBeforeHash() && hasTrackedSentinel(values)) {
                    return addFlatBinaryBatch(values, nulls, mask, distinctPositions, nullFree);
                }
                if (nullFree && !hasTrackedSentinel(values)) {
                    int generatedCount = addGeneratedPhysicalBatch(values, nulls, mask, distinctPositions);
                    if (generatedCount >= 0) {
                        return generatedCount;
                    }
                }
                table.prepareBatchHashes(values, nulls, mask);
                int count = 0;
                int[] positions = mask.selectedPositions();
                int positionCount = mask.selectedCount();
                for (int index = 0; index < positionCount; index++) {
                    int position = positions == null ? index : positions[index];
                    if (!nullFree && hasNull(nulls, position)) {
                        continue;
                    }
                    if (isTrackedSentinel(values, position)) {
                        if (!emptyBinarySeen) {
                            emptyBinarySeen = true;
                            distinctPositions[count++] = position;
                        }
                        continue;
                    }
                    int recordCount = table.recordCount();
                    if (table.assignGroup(values, nulls, position, recordCount) == recordCount) {
                        distinctPositions[count++] = position;
                    }
                }
                return count;
            }
            finally {
                table.endBatch();
            }
        }

        private int addSingleDictionaryBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            if (values.length != 1 ||
                    !(values[0] instanceof DictionaryVector dictionary) ||
                    dictionary.dictionaryDepth() != 1) {
                return -1;
            }
            Vector baseNulls;
            if (nulls[0] == null) {
                baseNulls = null;
            }
            else if (nulls[0] instanceof DictionaryVector nullDictionary &&
                    nullDictionary.dictionaryDepth() == 1 &&
                    sameDictionaryMapping(dictionary, nullDictionary)) {
                baseNulls = nullDictionary.values();
            }
            else if (VectorAccess.isAllFalseNulls(nulls[0])) {
                baseNulls = null;
            }
            else {
                return -1;
            }

            int domainSize = dictionary.values().length();
            if (dictionaryFirstLogicalPositions.length < domainSize) {
                int[] previousFirstPositions = dictionaryFirstLogicalPositions;
                int[] previousDomainPositions = dictionaryDomainPositions;
                dictionaryFirstLogicalPositions = arrayPool.borrowInts(domainSize);
                dictionaryDomainPositions = arrayPool.borrowInts(domainSize);
                arrayPool.release(previousFirstPositions);
                arrayPool.release(previousDomainPositions);
            }
            Arrays.fill(dictionaryFirstLogicalPositions, 0, domainSize, -1);
            int domainCount = 0;
            int[] ids = dictionary.ids();
            for (int logicalPosition : mask) {
                int domainPosition = ids[logicalPosition];
                if (dictionaryFirstLogicalPositions[domainPosition] < 0) {
                    dictionaryFirstLogicalPositions[domainPosition] = logicalPosition;
                    dictionaryDomainPositions[domainCount++] = domainPosition;
                }
            }
            if (domainCount == 0) {
                return 0;
            }
            Mask domainMask;
            if (domainCount == domainSize) {
                domainMask = Mask.all(domainSize);
            }
            else {
                int[] selectedDomainPositions = Arrays.copyOf(dictionaryDomainPositions, domainCount);
                Arrays.sort(selectedDomainPositions);
                domainMask = Mask.sparse(selectedDomainPositions, domainSize);
            }
            dictionaryDomainValues[0] = dictionary.values();
            dictionaryDomainNulls[0] = baseNulls;
            int distinctCount;
            try {
                distinctCount = addBatch(dictionaryDomainValues, dictionaryDomainNulls, domainMask, distinctPositions);
            }
            finally {
                dictionaryDomainValues[0] = null;
                dictionaryDomainNulls[0] = null;
            }
            for (int index = 0; index < distinctCount; index++) {
                distinctPositions[index] = dictionaryFirstLogicalPositions[distinctPositions[index]];
            }
            return distinctCount;
        }

        private int addGeneratedPhysicalBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            int positionCount = mask.selectedCount();
            if (generatedHashScratch == null || generatedHashScratch.length < positionCount) {
                long[] previous = generatedHashScratch;
                generatedHashScratch = arrayPool.borrowLongs(positionCount);
                arrayPool.release(previous);
            }
            if (!mask.all()) {
                return table.assignGeneratedDictionaryDistinctSelectedBatch(
                        values,
                        nulls,
                        mask,
                        generatedHashScratch,
                        distinctPositions,
                        table.recordCount());
            }
            return table.assignGeneratedDictionaryDistinctBatch(
                    values,
                    nulls,
                    mask,
                    generatedHashScratch,
                    distinctPositions,
                    table.recordCount());
        }

        private int addFlatBinaryBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions, boolean nullFree)
        {
            if (probePositions == null || probePositions.length < mask.count()) {
                int[] previous = probePositions;
                probePositions = arrayPool.borrowInts(mask.count());
                arrayPool.release(previous);
            }
            int positionCount = 0;
            int firstNewEmptyPosition = -1;
            for (int position : mask) {
                if (!nullFree && hasNull(nulls, position)) {
                    continue;
                }
                if (isTrackedSentinel(values, position)) {
                    if (!emptyBinarySeen && firstNewEmptyPosition < 0) {
                        firstNewEmptyPosition = position;
                    }
                    continue;
                }
                probePositions[positionCount++] = position;
            }

            table.prepareBatchHashes(values, nulls, probePositions, positionCount);
            int count = 0;
            boolean emptyEmitted = false;
            for (int index = 0; index < positionCount; index++) {
                int position = probePositions[index];
                if (!emptyEmitted && firstNewEmptyPosition >= 0 && firstNewEmptyPosition < position) {
                    emptyBinarySeen = true;
                    emptyEmitted = true;
                    distinctPositions[count++] = firstNewEmptyPosition;
                }
                int recordCount = table.recordCount();
                if (table.assignGroup(values, nulls, position, recordCount) == recordCount) {
                    distinctPositions[count++] = position;
                }
            }
            if (!emptyEmitted && firstNewEmptyPosition >= 0) {
                emptyBinarySeen = true;
                distinctPositions[count++] = firstNewEmptyPosition;
            }
            return count;
        }

        private boolean hasTrackedSentinel(Vector[] values)
        {
            return policy.emptyBinaryFastPath() && layout.hasTrackedSentinel(values);
        }

        private boolean isTrackedSentinel(Vector[] values, int position)
        {
            return policy.emptyBinaryFastPath() && layout.isTrackedSentinel(values, position);
        }

        private static boolean hasNull(Vector[] nulls, int position)
        {
            for (Vector nullsVector : nulls) {
                if (OperatorVectorSupport.isNull(nullsVector, position)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public long retainedBytes()
        {
            return table.retainedBytes() +
                    (probePositions == null ? 0 : (long) probePositions.length * Integer.BYTES) +
                    (generatedHashScratch == null ? 0 : (long) generatedHashScratch.length * Long.BYTES) +
                    (long) dictionaryFirstLogicalPositions.length * Integer.BYTES +
                    (long) dictionaryDomainPositions.length * Integer.BYTES;
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
            arrayPool.release(probePositions);
            probePositions = null;
            arrayPool.release(generatedHashScratch);
            generatedHashScratch = null;
            arrayPool.release(dictionaryFirstLogicalPositions);
            arrayPool.release(dictionaryDomainPositions);
            dictionaryFirstLogicalPositions = new int[0];
            dictionaryDomainPositions = new int[0];
        }
    }

    /** Payload-free DISTINCT over one generated composition of direct flat fields and canonical projected lanes. */
    private static final class ProjectedFlatDistinctIndex
            implements DistinctIndex
    {
        private final boolean retainNulls;
        private final FlatGroupingTable table;
        private final int[] singlePosition = new int[1];
        private final int[] singleDistinct = new int[1];

        private ProjectedFlatDistinctIndex(ProjectedFlatKeyLayout layout, boolean retainNulls, int expectedSize)
        {
            this.retainNulls = retainNulls;
            table = new FlatGroupingTable(layout, expectedSize, true);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            table.ensureCapacity(table.recordCount() + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            singlePosition[0] = position;
            return addPositions(values, nulls, singlePosition, 1, singleDistinct, retainNulls) == 1;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            table.beginBatch(values, nulls, mask);
            try {
                table.ensureCapacity(table.recordCount() + mask.selectedCount());
                table.prepareBatchHashes(values, nulls, mask);
                return addBoundPositions(
                        values,
                        nulls,
                        mask.all() ? null : mask.selectedPositions(),
                        mask.selectedCount(),
                        distinctPositions,
                        retainNulls);
            }
            finally {
                table.endBatch();
            }
        }

        @Override
        public int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            return addPositions(values, nulls, positions, positionCount, distinctPositions, true);
        }

        @Override
        public int addNonNullDenseBatch(Vector[] values, Vector[] nulls, int positionCount, int[] positions, int[] distinctPositions)
        {
            return addPositions(values, nulls, null, positionCount, distinctPositions, true);
        }

        @Override
        public int addRetainingNullBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            table.beginBatch(values, nulls, mask);
            try {
                table.ensureCapacity(table.recordCount() + mask.selectedCount());
                table.prepareBatchHashes(values, nulls, mask);
                return addBoundPositions(
                        values,
                        nulls,
                        mask.all() ? null : mask.selectedPositions(),
                        mask.selectedCount(),
                        distinctPositions,
                        true);
            }
            finally {
                table.endBatch();
            }
        }

        private int addPositions(
                Vector[] values,
                Vector[] nulls,
                int[] positions,
                int positionCount,
                int[] distinctPositions,
                boolean retainNullKeys)
        {
            table.beginBatch(values, nulls);
            try {
                table.ensureCapacity(table.recordCount() + positionCount);
                if (positions == null) {
                    table.prepareBatchHashes(values, nulls, Mask.all(positionCount));
                }
                else {
                    table.prepareBatchHashes(values, nulls, positions, positionCount);
                }
                return addBoundPositions(
                        values,
                        nulls,
                        positions,
                        positionCount,
                        distinctPositions,
                        retainNullKeys);
            }
            finally {
                table.endBatch();
            }
        }

        private int addBoundPositions(
                Vector[] values,
                Vector[] nulls,
                int[] positions,
                int positionCount,
                int[] distinctPositions,
                boolean retainNullKeys)
        {
            int count = 0;
            for (int index = 0; index < positionCount; index++) {
                int position = positions == null ? index : positions[index];
                if (!retainNullKeys && hasNull(nulls, position)) {
                    continue;
                }
                int recordCount = table.recordCount();
                if (table.assignGroup(values, nulls, position, recordCount) == recordCount) {
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        @Override
        public long retainedBytes()
        {
            return table.retainedBytes();
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
        }

        private static boolean hasNull(Vector[] nulls, int position)
        {
            for (Vector nullVector : nulls) {
                if (OperatorVectorSupport.isNull(nullVector, position)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class MultiLongDistinctIndex
            implements DistinctIndex
    {
        private static final VectorAccess.BooleanValues ALWAYS_FALSE = _ -> false;

        private final AbstractFixedWidthKeyTable table;
        private final VectorAccess.LongValues[] keyAccessors;
        private final VectorAccess.BooleanValues[] nullAccessors;
        private final int[] singlePosition = new int[1];
        private final int[] singleDistinctPosition = new int[1];

        private MultiLongDistinctIndex(
                int arity,
                int expectedSize,
                PrimitiveArrayPool arrayPool,
                OperatorCodeGenerationResources codeGeneration,
                AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy)
        {
            table = codeGeneration.fixedWidthKeyTables().createDistinct(
                    FixedWidthKeyTableLayout.rawI64(arity),
                    expectedSize,
                    arrayPool,
                    adaptiveLongGroupingPolicy);
            keyAccessors = new VectorAccess.LongValues[arity];
            nullAccessors = new VectorAccess.BooleanValues[arity];
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            table.ensureCapacity((long) table.size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            prepareAccessors(values, nulls, true);
            singlePosition[0] = position;
            return table.assignDistinctBatch(keyAccessors, nullAccessors, singlePosition, 1, singleDistinctPosition, table.size) == 1;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            boolean nullFree = true;
            for (Vector nullsVector : nulls) {
                if (!VectorAccess.isAllFalseNulls(nullsVector)) {
                    nullFree = false;
                    break;
                }
            }
            prepareAccessors(values, nulls, nullFree);

            int positionCount = 0;
            if (nullFree) {
                for (int position : mask) {
                    distinctPositions[positionCount++] = position;
                }
            }
            else {
                for (int position : mask) {
                    boolean hasNull = false;
                    for (VectorAccess.BooleanValues nullAccessor : nullAccessors) {
                        if (nullAccessor.value(position)) {
                            hasNull = true;
                            break;
                        }
                    }
                    if (!hasNull) {
                        distinctPositions[positionCount++] = position;
                    }
                }
            }
            if (nullFree) {
                return table.assignDistinctBatchNullFree(keyAccessors, nullAccessors, distinctPositions, positionCount, distinctPositions, table.size);
            }
            return table.assignDistinctBatch(keyAccessors, nullAccessors, distinctPositions, positionCount, distinctPositions, table.size);
        }

        @Override
        public int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            prepareAccessors(values, nulls, true);
            return table.assignDistinctBatchNullFree(keyAccessors, nullAccessors, positions, positionCount, distinctPositions, table.size);
        }

        private void prepareAccessors(Vector[] values, Vector[] nulls, boolean nullFree)
        {
            for (int index = 0; index < keyAccessors.length; index++) {
                keyAccessors[index] = VectorAccess.longValues(values[index]);
                nullAccessors[index] = nullFree ? ALWAYS_FALSE : VectorAccess.booleanValues(nulls[index]);
            }
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
            Arrays.fill(keyAccessors, null);
            Arrays.fill(nullAccessors, null);
        }

        @Override
        public long retainedBytes()
        {
            return table.retainedBytes();
        }
    }

    /**
     * DISTINCT adapter over the same schema-generated compact table used by grouping. The table stores each key
     * once in packed reverse lanes, so growth rehashes only its compact slot index rather than copying a full
     * arity-wide record for every occupied slot. Values outside the compact domain promote exactly to the ordinary
     * full-width generated table.
     */
    private static final class AdaptiveMultiLongDistinctIndex
            implements DistinctIndex
    {
        private static final VectorAccess.BooleanValues ALWAYS_FALSE = _ -> false;
        private static final int[] EMPTY_POSITIONS = new int[0];
        private static final long[] EMPTY_GROUPS = new long[0];

        private final PrimitiveArrayPool arrayPool;
        private final DistinctKeySetPolicy policy;
        private final LongGroupingTable table;
        private final VectorAccess.LongValues[] keyAccessors;
        private final VectorAccess.BooleanValues[] nullAccessors;
        private final int[] singlePosition = new int[1];
        private final int[] singleDistinctPosition = new int[1];
        private int[] nonNullPositions = EMPTY_POSITIONS;
        private long[] assignedGroups = EMPTY_GROUPS;
        private long[] processedBasePositions = EMPTY_GROUPS;
        private long nextGroupId;
        private SharedDictionaryPositionResolver sharedDictionaryPositionResolver;
        private Vector[] sharedDictionaryBases;
        private Vector[] sharedDictionaryNullBases;
        private Vector[] cachedSharedDictionaryBases;
        private Vector[] cachedSharedDictionaryNullBases;
        private long[] cachedSharedDictionaryGenerations;
        private long[] cachedSharedDictionaryNullGenerations;
        private final long[] sharedDictionaryGenerationScratch;
        private final long[] sharedDictionaryNullGenerationScratch;
        private final IndependentDictionaryTupleDomain independentDictionaryTupleDomain;
        private boolean debugSharedDictionaryResolverPrinted;
        private boolean debugSharedDictionaryNullResolverPrinted;
        private boolean debugSharedDictionaryBaseCachePrinted;

        private AdaptiveMultiLongDistinctIndex(
                int arity,
                int expectedSize,
                PrimitiveArrayPool arrayPool,
                OperatorCodeGenerationResources codeGeneration,
                DistinctKeySetPolicy policy,
                AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy)
        {
            this.arrayPool = arrayPool;
            this.policy = policy;
            table = AdaptiveLongGroupingTable.createDistinct(
                    arity,
                    expectedSize,
                    arrayPool,
                    codeGeneration,
                    adaptiveLongGroupingPolicy);
            keyAccessors = new VectorAccess.LongValues[arity];
            nullAccessors = new VectorAccess.BooleanValues[arity];
            sharedDictionaryGenerationScratch = new long[arity];
            sharedDictionaryNullGenerationScratch = new long[arity];
            independentDictionaryTupleDomain = new IndependentDictionaryTupleDomain(arity, arrayPool, policy);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            table.ensureCapacity(nextGroupId + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }
            prepareAccessors(values);
            singlePosition[0] = position;
            long startGroupId = nextGroupId;
            if (policy.adaptiveDirectBatch()) {
                nextGroupId = ((AdaptiveLongGroupingTable) table).assignDistinctBatch(
                        keyAccessors, singlePosition, 1, values[0].length(), singleDistinctPosition, startGroupId);
            }
            else {
                ensureAssignedCapacity(values[0].length());
                nextGroupId = table.assignBatch(keyAccessors, null, singlePosition, 1, assignedGroups, startGroupId);
            }
            return nextGroupId != startGroupId;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            ensurePositionCapacity(mask.selectedCount());
            int dictionaryPositionCount = independentDictionaryTupleDomain.filter(values, nulls, mask, nonNullPositions);
            if (dictionaryPositionCount >= 0) {
                prepareAccessors(values);
                return assignAndCollect(nonNullPositions, dictionaryPositionCount, values[0].length(), distinctPositions);
            }
            prepareAccessors(values);
            int positionCount = 0;
            if (policy.sharedDictionaryNullResolver() && prepareSharedDictionaryNullAccessors(nulls)) {
                boolean cacheBasePositions = prepareSharedDictionaryBasePositionCache();
                for (int position : mask) {
                    int basePosition = sharedDictionaryPositionResolver.resolve(position);
                    if (cacheBasePositions && !markBasePositionUnprocessed(basePosition)) {
                        continue;
                    }
                    boolean hasNull = false;
                    for (VectorAccess.BooleanValues nullAccessor : nullAccessors) {
                        if (nullAccessor.value(basePosition)) {
                            hasNull = true;
                            break;
                        }
                    }
                    if (!hasNull) {
                        nonNullPositions[positionCount++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (!hasNull(nulls, position)) {
                        nonNullPositions[positionCount++] = position;
                    }
                }
            }
            boolean dense = positionCount == mask.size() && mask.all();
            return assignAndCollect(dense ? null : nonNullPositions, positionCount, values[0].length(), distinctPositions);
        }

        @Override
        public int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            prepareAccessors(values);
            return assignAndCollect(positions, positionCount, values[0].length(), distinctPositions);
        }

        @Override
        public int addNonNullDenseBatch(Vector[] values, Vector[] nulls, int positionCount, int[] positions, int[] distinctPositions)
        {
            prepareAccessors(values);
            return assignAndCollect(null, positionCount, values[0].length(), distinctPositions);
        }

        @Override
        public int addRetainingNullBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            prepareNullableAccessors(values, nulls);
            int positionCount = mask.selectedCount();
            int[] positions = null;
            if (!mask.all()) {
                ensurePositionCapacity(positionCount);
                int index = 0;
                for (int position : mask) {
                    nonNullPositions[index++] = position;
                }
                positions = nonNullPositions;
            }

            ensureAssignedCapacity(values[0].length());
            long startGroupId = nextGroupId;
            nextGroupId = table.assignBatch(
                    keyAccessors,
                    nullableAccessorsPresent() ? nullAccessors : null,
                    positions,
                    positionCount,
                    assignedGroups,
                    startGroupId);

            long expectedNewGroup = startGroupId;
            int distinctCount = 0;
            for (int row = 0; row < positionCount; row++) {
                int position = positions == null ? row : positions[row];
                if (assignedGroups[position] == expectedNewGroup) {
                    distinctPositions[distinctCount++] = position;
                    expectedNewGroup++;
                }
            }
            if (expectedNewGroup != nextGroupId) {
                throw new IllegalStateException("Generated nullable compact distinct table returned non-sequential group ids");
            }
            return distinctCount;
        }

        private int assignAndCollect(int[] positions, int positionCount, int resultLength, int[] distinctPositions)
        {
            long startGroupId = nextGroupId;
            if (policy.adaptiveDirectBatch()) {
                nextGroupId = ((AdaptiveLongGroupingTable) table).assignDistinctBatch(
                        keyAccessors, positions, positionCount, resultLength, distinctPositions, startGroupId);
                return toIntExact(nextGroupId - startGroupId);
            }
            ensureAssignedCapacity(resultLength);
            nextGroupId = table.assignBatch(keyAccessors, null, positions, positionCount, assignedGroups, startGroupId);
            long expectedNewGroup = startGroupId;
            int distinctCount = 0;
            for (int row = 0; row < positionCount; row++) {
                int position = positions == null ? row : positions[row];
                long groupId = assignedGroups[position];
                if (groupId == expectedNewGroup) {
                    distinctPositions[distinctCount++] = position;
                    expectedNewGroup++;
                }
            }
            if (expectedNewGroup != nextGroupId) {
                throw new IllegalStateException("Generated compact distinct table returned non-sequential group ids");
            }
            return distinctCount;
        }

        private void prepareAccessors(Vector[] values)
        {
            sharedDictionaryPositionResolver = null;
            sharedDictionaryBases = null;
            sharedDictionaryNullBases = null;
            if (policy.sharedDictionaryPositionResolver() && prepareSharedDictionaryAccessors(values)) {
                Arrays.fill(nullAccessors, ALWAYS_FALSE);
                return;
            }
            for (int index = 0; index < keyAccessors.length; index++) {
                keyAccessors[index] = VectorAccess.longValues(values[index]);
                nullAccessors[index] = ALWAYS_FALSE;
            }
        }

        private void prepareNullableAccessors(Vector[] values, Vector[] nulls)
        {
            sharedDictionaryPositionResolver = null;
            sharedDictionaryBases = null;
            sharedDictionaryNullBases = null;
            for (int index = 0; index < keyAccessors.length; index++) {
                keyAccessors[index] = VectorAccess.longValues(values[index]);
                nullAccessors[index] = VectorAccess.isAllFalseNulls(nulls[index])
                        ? ALWAYS_FALSE
                        : VectorAccess.booleanValues(nulls[index]);
            }
        }

        private boolean nullableAccessorsPresent()
        {
            for (VectorAccess.BooleanValues nullAccessor : nullAccessors) {
                if (nullAccessor != ALWAYS_FALSE) {
                    return true;
                }
            }
            return false;
        }

        private boolean prepareSharedDictionaryNullAccessors(Vector[] nulls)
        {
            if (sharedDictionaryPositionResolver == null || nulls.length != nullAccessors.length) {
                return false;
            }
            int[][] mappings = sharedDictionaryPositionResolver.mappings;
            Vector[] bases = new Vector[nulls.length];
            for (int index = 0; index < nulls.length; index++) {
                Vector current = nulls[index];
                for (int level = 0; level < mappings.length; level++) {
                    if (!(current instanceof DictionaryVector dictionary) || dictionary.ids() != mappings[level]) {
                        return false;
                    }
                    current = dictionary.values();
                }
                if (current instanceof DictionaryVector) {
                    return false;
                }
                bases[index] = current;
                nullAccessors[index] = VectorAccess.booleanValues(current);
            }
            sharedDictionaryNullBases = bases;
            if (policy.debugDistinctShapes() && !debugSharedDictionaryNullResolverPrinted) {
                debugSharedDictionaryNullResolverPrinted = true;
                System.err.printf("[shared-dictionary-distinct-nulls] keys=%d rows=%d depth=%d%n",
                        nulls.length,
                        nulls[0].length(),
                        mappings.length);
            }
            return true;
        }

        private boolean prepareSharedDictionaryAccessors(Vector[] values)
        {
            if (values.length < 3 || !(values[0] instanceof DictionaryVector first)) {
                return false;
            }

            int depth = first.dictionaryDepth();
            int[][] mappings = new int[depth][];
            Vector current = first;
            for (int level = 0; level < depth; level++) {
                DictionaryVector dictionary = (DictionaryVector) current;
                mappings[level] = dictionary.ids();
                current = dictionary.values();
            }

            Vector[] bases = new Vector[values.length];
            bases[0] = current;
            for (int index = 1; index < values.length; index++) {
                current = values[index];
                for (int level = 0; level < depth; level++) {
                    if (!(current instanceof DictionaryVector dictionary) || dictionary.ids() != mappings[level]) {
                        return false;
                    }
                    current = dictionary.values();
                }
                if (current instanceof DictionaryVector) {
                    return false;
                }
                bases[index] = current;
            }

            SharedDictionaryPositionResolver resolver = new SharedDictionaryPositionResolver(mappings);
            sharedDictionaryPositionResolver = resolver;
            sharedDictionaryBases = bases;
            if (policy.debugDistinctShapes() && !debugSharedDictionaryResolverPrinted) {
                debugSharedDictionaryResolverPrinted = true;
                System.err.printf("[shared-dictionary-distinct] keys=%d rows=%d depth=%d%n",
                        values.length,
                        values[0].length(),
                        depth);
            }
            VectorAccess.LongValues firstBase = VectorAccess.longValues(bases[0]);
            keyAccessors[0] = position -> firstBase.value(resolver.resolve(position));
            for (int index = 1; index < keyAccessors.length; index++) {
                VectorAccess.LongValues base = VectorAccess.longValues(bases[index]);
                keyAccessors[index] = _ -> base.value(resolver.resolvedPosition());
            }
            return true;
        }

        /**
         * A shared dictionary position identifies the complete tuple only while every value/null base retains the
         * same object identity and content generation. Once that is proven, a compact bitmap remembers physical
         * positions already submitted to the exact table. Later logical aliases can be skipped without changing
         * first-position output semantics; separate base positions with equal values still meet in the table.
         */
        private boolean prepareSharedDictionaryBasePositionCache()
        {
            if (!policy.sharedDictionaryBasePositionCache() || sharedDictionaryBases == null || sharedDictionaryNullBases == null) {
                return false;
            }
            if (!readContentGenerations(sharedDictionaryBases, sharedDictionaryGenerationScratch) ||
                    !readContentGenerations(sharedDictionaryNullBases, sharedDictionaryNullGenerationScratch)) {
                return false;
            }

            int baseLength = sharedDictionaryBases[0].length();
            int wordCount = (baseLength + Long.SIZE - 1) / Long.SIZE;
            boolean generationChanged = !sameIdentity(cachedSharedDictionaryBases, sharedDictionaryBases) ||
                    !sameIdentity(cachedSharedDictionaryNullBases, sharedDictionaryNullBases) ||
                    !Arrays.equals(cachedSharedDictionaryGenerations, sharedDictionaryGenerationScratch) ||
                    !Arrays.equals(cachedSharedDictionaryNullGenerations, sharedDictionaryNullGenerationScratch);
            if (processedBasePositions.length < wordCount) {
                arrayPool.release(processedBasePositions);
                processedBasePositions = arrayPool.borrowLongs(wordCount);
                generationChanged = true;
            }
            if (generationChanged) {
                Arrays.fill(processedBasePositions, 0);
                cachedSharedDictionaryBases = sharedDictionaryBases.clone();
                cachedSharedDictionaryNullBases = sharedDictionaryNullBases.clone();
                cachedSharedDictionaryGenerations = sharedDictionaryGenerationScratch.clone();
                cachedSharedDictionaryNullGenerations = sharedDictionaryNullGenerationScratch.clone();
            }
            if (policy.debugDistinctShapes() && !debugSharedDictionaryBaseCachePrinted) {
                debugSharedDictionaryBaseCachePrinted = true;
                System.err.printf("[shared-dictionary-distinct-base-cache] keys=%d base=%d words=%d depth=%d%n",
                        sharedDictionaryBases.length,
                        baseLength,
                        wordCount,
                        sharedDictionaryPositionResolver.mappings.length);
            }
            return true;
        }

        private static boolean readContentGenerations(Vector[] vectors, long[] generations)
        {
            for (int index = 0; index < vectors.length; index++) {
                generations[index] = vectors[index].contentGeneration();
                if (generations[index] < 0) {
                    return false;
                }
            }
            return true;
        }

        private static boolean sameIdentity(Vector[] first, Vector[] second)
        {
            if (first == null || first.length != second.length) {
                return false;
            }
            for (int index = 0; index < first.length; index++) {
                if (first[index] != second[index]) {
                    return false;
                }
            }
            return true;
        }

        private boolean markBasePositionUnprocessed(int basePosition)
        {
            int word = basePosition >>> 6;
            long bit = 1L << (basePosition & 63);
            long previous = processedBasePositions[word];
            processedBasePositions[word] = previous | bit;
            return (previous & bit) == 0;
        }

        private void importPairs(LongPairDistinctIndex source)
        {
            ensurePositionCapacity(source.size);
            int positionCount = 0;
            for (int position = 0; position < source.firstKeys.length; position++) {
                if (source.isOccupied(position)) {
                    nonNullPositions[positionCount++] = position;
                }
            }
            keyAccessors[0] = position -> source.firstKeys[position];
            keyAccessors[1] = position -> source.secondKeys[position];
            nextGroupId = ((AdaptiveLongGroupingTable) table).assignDistinctBatch(
                    keyAccessors,
                    nonNullPositions,
                    positionCount,
                    source.firstKeys.length,
                    nonNullPositions,
                    0);
            if (nextGroupId != source.size) {
                throw new IllegalStateException("Pair distinct migration changed the key count");
            }
        }

        private void ensurePositionCapacity(int size)
        {
            if (nonNullPositions.length >= size) {
                return;
            }
            arrayPool.release(nonNullPositions);
            nonNullPositions = arrayPool.borrowInts(size);
        }

        private void ensureAssignedCapacity(int size)
        {
            if (assignedGroups.length >= size) {
                return;
            }
            arrayPool.release(assignedGroups);
            assignedGroups = arrayPool.borrowLongs(size);
        }

        @Override
        public void releaseBuffers()
        {
            table.releaseBuffers();
            independentDictionaryTupleDomain.releaseBuffers();
            arrayPool.release(nonNullPositions);
            arrayPool.release(assignedGroups);
            arrayPool.release(processedBasePositions);
            nonNullPositions = EMPTY_POSITIONS;
            assignedGroups = EMPTY_GROUPS;
            processedBasePositions = EMPTY_GROUPS;
            sharedDictionaryBases = null;
            sharedDictionaryNullBases = null;
            cachedSharedDictionaryBases = null;
            cachedSharedDictionaryNullBases = null;
            cachedSharedDictionaryGenerations = null;
            cachedSharedDictionaryNullGenerations = null;
            Arrays.fill(keyAccessors, null);
            Arrays.fill(nullAccessors, null);
        }

        @Override
        public long retainedBytes()
        {
            return table.retainedBytes() +
                    (long) nonNullPositions.length * Integer.BYTES +
                    (long) assignedGroups.length * Long.BYTES +
                    (long) processedBasePositions.length * Long.BYTES +
                    independentDictionaryTupleDomain.retainedBytes();
        }
    }

    private static final class SharedDictionaryPositionResolver
    {
        private final int[][] mappings;
        private int basePosition;

        private SharedDictionaryPositionResolver(int[][] mappings)
        {
            this.mappings = mappings;
        }

        private int resolve(int position)
        {
            for (int[] mapping : mappings) {
                position = mapping[position];
            }
            basePosition = position;
            return basePosition;
        }

        private int resolvedPosition()
        {
            return basePosition;
        }
    }

    /**
     * Tracks the Cartesian physical domain of independently dictionary-encoded key vectors. A physical tuple is
     * passed to the authoritative logical-key index once per stable set of dictionary values; repeated logical rows
     * that carry the same tuple of dictionary ids are skipped without decoding or hashing their values again.
     *
     * <p>The helper is deliberately arity-independent. It admits only bounded, null-free domains with enough row
     * reduction to repay mixed-radix tuple-id construction. Dictionary values, rather than row mappings, define the
     * cache generation: mappings may change freely while a tuple id continues to denote the same physical values.
     */
    private static final class IndependentDictionaryTupleDomain
    {
        private final int arity;
        private final PrimitiveArrayPool arrayPool;
        private final DistinctKeySetPolicy policy;
        private final Vector[] dictionaryValues;
        private final long[] dictionaryGenerations;
        private final int[] cardinalities;
        private final DictionaryVector[] dictionaries;
        private byte[] seenTuples;

        private IndependentDictionaryTupleDomain(int arity, PrimitiveArrayPool arrayPool, DistinctKeySetPolicy policy)
        {
            this.arity = arity;
            this.arrayPool = arrayPool;
            this.policy = policy;
            dictionaryValues = new Vector[arity];
            dictionaryGenerations = new long[arity];
            cardinalities = new int[arity];
            dictionaries = new DictionaryVector[arity];
        }

        /** Returns -1 when the ordinary logical-row path should be used. */
        private int filter(Vector[] values, Vector[] nulls, Mask mask, int[] positions)
        {
            if (!policy.independentDictionaryTupleDomain() || values.length != arity || nulls.length != arity) {
                return -1;
            }
            try {
                return filterDictionaryTuples(values, nulls, mask, positions);
            }
            finally {
                // Retain only the small physical value domains used as the cache signature, never a logical-row
                // mapping from an upstream batch.
                Arrays.fill(dictionaries, null);
            }
        }

        private int filterDictionaryTuples(Vector[] values, Vector[] nulls, Mask mask, int[] positions)
        {
            int domainSize = 1;
            boolean generationChanged = seenTuples == null;
            for (int field = 0; field < arity; field++) {
                if (!(values[field] instanceof DictionaryVector dictionary) ||
                        !VectorAccess.isAllFalseNulls(nulls[field])) {
                    return -1;
                }
                int cardinality = dictionary.values().length();
                if (cardinality == 0 || domainSize > policy.independentDictionaryTupleDomainMaxEntries() / cardinality) {
                    return -1;
                }
                domainSize *= cardinality;
                if (domainSize > policy.independentDictionaryTupleDomainMaxEntries()) {
                    return -1;
                }
                dictionaries[field] = dictionary;
                Vector dictionaryValue = dictionary.values();
                long generation = dictionaryValue.contentGeneration();
                generationChanged |= dictionaryValues[field] != dictionaryValue ||
                        dictionaryGenerations[field] != generation ||
                        cardinalities[field] != cardinality;
            }
            int selectedCount = mask.selectedCount();
            if ((long) domainSize * policy.independentDictionaryTupleDomainMinimumReduction() > selectedCount) {
                return -1;
            }

            if (seenTuples == null || seenTuples.length < domainSize) {
                arrayPool.release(seenTuples);
                seenTuples = arrayPool.borrowBytes(domainSize);
                generationChanged = true;
            }
            if (generationChanged) {
                Arrays.fill(seenTuples, (byte) 0);
                for (int field = 0; field < arity; field++) {
                    Vector dictionaryValue = dictionaries[field].values();
                    dictionaryValues[field] = dictionaryValue;
                    dictionaryGenerations[field] = dictionaryValue.contentGeneration();
                    cardinalities[field] = dictionaryValue.length();
                }
            }

            int count = 0;
            for (int position : mask) {
                int tupleId = 0;
                for (int field = 0; field < arity; field++) {
                    tupleId = tupleId * cardinalities[field] + dictionaries[field].ids()[position];
                }
                if (seenTuples[tupleId] == 0) {
                    seenTuples[tupleId] = 1;
                    positions[count++] = position;
                }
            }
            return count;
        }

        private void releaseBuffers()
        {
            arrayPool.release(seenTuples);
            seenTuples = null;
            Arrays.fill(dictionaryValues, null);
        }

        private long retainedBytes()
        {
            return seenTuples == null ? 0 : seenTuples.length;
        }
    }

    private static final class LongPairDistinctIndex
            implements DistinctIndex
    {
        private static final float LOAD_FACTOR = 0.75f;
        private final PrimitiveArrayPool arrayPool;
        private final OperatorCodeGenerationResources codeGeneration;
        private final DistinctKeySetPolicy policy;
        private final AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy;

        private long[] firstKeys;
        private long[] secondKeys;
        private boolean[] occupied;
        private byte[] tags;
        private int mask;
        private int maxFill;
        private int size;
        private final boolean adaptiveCompactCandidate;
        private int batchCount;
        private int pendingAdditional;
        private AdaptiveMultiLongDistinctIndex adaptiveDelegate;
        private final IndependentDictionaryTupleDomain independentDictionaryTupleDomain;

        private LongPairDistinctIndex(
                int expectedSize,
                boolean adaptiveCompactCandidate,
                PrimitiveArrayPool arrayPool,
                OperatorCodeGenerationResources codeGeneration,
                DistinctKeySetPolicy policy,
                AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy)
        {
            this.arrayPool = arrayPool;
            this.codeGeneration = codeGeneration;
            this.policy = policy;
            this.adaptiveLongGroupingPolicy = adaptiveLongGroupingPolicy;
            this.adaptiveCompactCandidate = adaptiveCompactCandidate;
            independentDictionaryTupleDomain = new IndependentDictionaryTupleDomain(2, arrayPool, policy);
            int capacity = DistinctKeySet.capacity(expectedSize);
            allocate(capacity);
        }

        private void allocate(int capacity)
        {
            firstKeys = arrayPool.borrowLongs(capacity);
            secondKeys = arrayPool.borrowLongs(capacity);
            if (policy.taggedLongPairHash()) {
                tags = arrayPool.borrowBytes(capacity);
                Arrays.fill(tags, (byte) 0);
                occupied = null;
            }
            else {
                occupied = arrayPool.borrowBooleans(capacity);
                Arrays.fill(occupied, false);
                tags = null;
            }
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            if (adaptiveDelegate != null) {
                adaptiveDelegate.reserveAdditional(additionalEntries);
                return;
            }
            pendingAdditional = Math.max(0, additionalEntries);
            // Once this pair has earned migration, sizing the old table for the next batch would immediately
            // allocate, rehash, and release a representation that will not process that batch. Scalar adds still
            // retain their exact grow-on-demand guard in addKey().
            if (adaptiveCompactCandidate && batchCount + 1 >= policy.adaptiveCompactLongPairStartBatch()) {
                return;
            }
            ensureCapacity(size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (adaptiveDelegate != null) {
                return adaptiveDelegate.add(values, nulls, position);
            }
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            int index = findSlot(first, second);
            if (isOccupied(index)) {
                return false;
            }

            firstKeys[index] = first;
            secondKeys[index] = second;
            markOccupied(index, first, second);
            size++;
            if (size >= maxFill) {
                rehash(firstKeys.length * 2);
            }
            return true;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            if (adaptiveDelegate != null) {
                return adaptiveDelegate.addBatch(values, nulls, mask, distinctPositions);
            }
            int dictionaryPositionCount = independentDictionaryTupleDomain.filter(
                    values, nulls, mask, distinctPositions);
            if (dictionaryPositionCount >= 0) {
                VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
                VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
                int count = 0;
                for (int index = 0; index < dictionaryPositionCount; index++) {
                    int position = distinctPositions[index];
                    boolean added = policy.taggedLongPairHash()
                            ? addTaggedKey(firstValues.value(position), secondValues.value(position))
                            : addBooleanKey(firstValues.value(position), secondValues.value(position));
                    if (added) {
                        distinctPositions[count++] = position;
                    }
                }
                return count;
            }
            if (migrateForNextBatch()) {
                return adaptiveDelegate.addBatch(values, nulls, mask, distinctPositions);
            }
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            boolean nullFree = policy.longPairNullFreeBatch() &&
                    VectorAccess.isAllFalseNulls(nulls[0]) &&
                    VectorAccess.isAllFalseNulls(nulls[1]);
            if (policy.taggedLongPairHash()) {
                return addTaggedBatch(firstValues, secondValues, firstNulls, secondNulls, mask, distinctPositions, nullFree);
            }
            return addBooleanBatch(firstValues, secondValues, firstNulls, secondNulls, mask, distinctPositions, nullFree);
        }

        private int addTaggedBatch(
                VectorAccess.LongValues firstValues,
                VectorAccess.LongValues secondValues,
                VectorAccess.BooleanValues firstNulls,
                VectorAccess.BooleanValues secondNulls,
                Mask mask,
                int[] distinctPositions,
                boolean nullFree)
        {
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                if (nullFree) {
                    for (int position = 0; position < size; position++) {
                        if (addTaggedKey(firstValues.value(position), secondValues.value(position))) {
                            distinctPositions[count++] = position;
                        }
                    }
                    return count;
                }
                for (int position = 0; position < size; position++) {
                    if (firstNulls.value(position) || secondNulls.value(position)) {
                        continue;
                    }
                    if (addTaggedKey(firstValues.value(position), secondValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                if (nullFree) {
                    for (int position : mask) {
                        if (addTaggedKey(firstValues.value(position), secondValues.value(position))) {
                            distinctPositions[count++] = position;
                        }
                    }
                    return count;
                }
                for (int position : mask) {
                    if (firstNulls.value(position) || secondNulls.value(position)) {
                        continue;
                    }
                    if (addTaggedKey(firstValues.value(position), secondValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        private int addBooleanBatch(
                VectorAccess.LongValues firstValues,
                VectorAccess.LongValues secondValues,
                VectorAccess.BooleanValues firstNulls,
                VectorAccess.BooleanValues secondNulls,
                Mask mask,
                int[] distinctPositions,
                boolean nullFree)
        {
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                if (nullFree) {
                    for (int position = 0; position < size; position++) {
                        if (addBooleanKey(firstValues.value(position), secondValues.value(position))) {
                            distinctPositions[count++] = position;
                        }
                    }
                    return count;
                }
                for (int position = 0; position < size; position++) {
                    if (firstNulls.value(position) || secondNulls.value(position)) {
                        continue;
                    }
                    if (addBooleanKey(firstValues.value(position), secondValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                if (nullFree) {
                    for (int position : mask) {
                        if (addBooleanKey(firstValues.value(position), secondValues.value(position))) {
                            distinctPositions[count++] = position;
                        }
                    }
                    return count;
                }
                for (int position : mask) {
                    if (firstNulls.value(position) || secondNulls.value(position)) {
                        continue;
                    }
                    if (addBooleanKey(firstValues.value(position), secondValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        @Override
        public int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            if (migrateForNextBatch()) {
                return adaptiveDelegate.addNonNullBatch(values, nulls, positions, positionCount, distinctPositions);
            }
            return DistinctIndex.super.addNonNullBatch(values, nulls, positions, positionCount, distinctPositions);
        }

        @Override
        public int addNonNullDenseBatch(Vector[] values, Vector[] nulls, int positionCount, int[] positions, int[] distinctPositions)
        {
            if (migrateForNextBatch()) {
                return adaptiveDelegate.addNonNullDenseBatch(values, nulls, positionCount, positions, distinctPositions);
            }
            return DistinctIndex.super.addNonNullDenseBatch(values, nulls, positionCount, positions, distinctPositions);
        }

        private boolean migrateForNextBatch()
        {
            if (adaptiveDelegate != null) {
                return true;
            }
            batchCount++;
            if (!adaptiveCompactCandidate || batchCount < policy.adaptiveCompactLongPairStartBatch()) {
                pendingAdditional = 0;
                return false;
            }
            adaptiveDelegate = new AdaptiveMultiLongDistinctIndex(
                    2,
                    Math.max(16, size + pendingAdditional),
                    arrayPool,
                    codeGeneration,
                    policy,
                    adaptiveLongGroupingPolicy);
            adaptiveDelegate.importPairs(this);
            releaseTableBuffers();
            pendingAdditional = 0;
            if (policy.debugDistinctShapes()) {
                System.err.printf("[adaptive-pair-distinct-migrate] batch=%d keys=%d%n", batchCount, size);
            }
            return true;
        }

        private boolean addKey(long first, long second)
        {
            int index = findSlot(first, second);
            if (isOccupied(index)) {
                return false;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            markOccupied(index, first, second);
            size++;
            if (size >= maxFill) {
                rehash(firstKeys.length * 2);
            }
            return true;
        }

        private boolean addTaggedKey(long first, long second)
        {
            long hash = hash64(first, second);
            byte tag = hashTag(hash);
            int index = ((int) hash) & mask;
            while (tags[index] != 0) {
                if (tags[index] == tag && firstKeys[index] == first && secondKeys[index] == second) {
                    return false;
                }
                index = (index + 1) & mask;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            tags[index] = tag;
            size++;
            if (size >= maxFill) {
                rehashTagged(firstKeys.length * 2);
            }
            return true;
        }

        private boolean addBooleanKey(long first, long second)
        {
            int index = mix(first, second) & mask;
            while (occupied[index]) {
                if (firstKeys[index] == first && secondKeys[index] == second) {
                    return false;
                }
                index = (index + 1) & mask;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            occupied[index] = true;
            size++;
            if (size >= maxFill) {
                rehashBoolean(firstKeys.length * 2);
            }
            return true;
        }

        private int findSlot(long first, long second)
        {
            if (policy.taggedLongPairHash()) {
                long hash = hash64(first, second);
                byte tag = hashTag(hash);
                int index = ((int) hash) & mask;
                while (tags[index] != 0) {
                    if (tags[index] == tag && firstKeys[index] == first && secondKeys[index] == second) {
                        return index;
                    }
                    index = (index + 1) & mask;
                }
                return index;
            }
            int index = mix(first, second) & mask;
            while (occupied[index] && (firstKeys[index] != first || secondKeys[index] != second)) {
                index = (index + 1) & mask;
            }
            return index;
        }

        private void ensureCapacity(int expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = firstKeys.length;
            while (expectedSize >= (int) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash(int capacity)
        {
            if (policy.taggedLongPairHash()) {
                rehashTagged(capacity);
            }
            else {
                rehashBoolean(capacity);
            }
        }

        private void rehashTagged(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            byte[] previousTags = tags;

            allocate(capacity);
            size = 0;

            for (int index = 0; index < previousFirstKeys.length; index++) {
                if (previousTags[index] == 0) {
                    continue;
                }
                long first = previousFirstKeys[index];
                long second = previousSecondKeys[index];
                long hash = hash64(first, second);
                byte tag = hashTag(hash);
                int newIndex = ((int) hash) & mask;
                while (tags[newIndex] != 0) {
                    newIndex = (newIndex + 1) & mask;
                }
                firstKeys[newIndex] = first;
                secondKeys[newIndex] = second;
                tags[newIndex] = tag;
                size++;
            }
            arrayPool.release(previousFirstKeys);
            arrayPool.release(previousSecondKeys);
            arrayPool.release(previousTags);
        }

        private void rehashBoolean(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            boolean[] previousOccupied = occupied;

            allocate(capacity);
            size = 0;

            for (int index = 0; index < previousFirstKeys.length; index++) {
                if (!previousOccupied[index]) {
                    continue;
                }
                long first = previousFirstKeys[index];
                long second = previousSecondKeys[index];
                int newIndex = mix(first, second) & mask;
                while (occupied[newIndex]) {
                    newIndex = (newIndex + 1) & mask;
                }
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                occupied[newIndex] = true;
                size++;
            }
            arrayPool.release(previousFirstKeys);
            arrayPool.release(previousSecondKeys);
            arrayPool.release(previousOccupied);
        }

        private boolean isOccupied(int index)
        {
            return policy.taggedLongPairHash() ? tags[index] != 0 : occupied[index];
        }

        private void markOccupied(int index, long first, long second)
        {
            if (policy.taggedLongPairHash()) {
                tags[index] = hashTag(hash64(first, second));
            }
            else {
                occupied[index] = true;
            }
        }

        @Override
        public void releaseBuffers()
        {
            if (policy.debugDistinctShapes()) {
                System.err.printf("[adaptive-pair-distinct-final] batches=%d keys=%d migrated=%s%n", batchCount, size, adaptiveDelegate != null);
            }
            if (adaptiveDelegate != null) {
                adaptiveDelegate.releaseBuffers();
                adaptiveDelegate = null;
            }
            independentDictionaryTupleDomain.releaseBuffers();
            releaseTableBuffers();
        }

        private void releaseTableBuffers()
        {
            arrayPool.release(firstKeys);
            arrayPool.release(secondKeys);
            arrayPool.release(occupied);
            arrayPool.release(tags);
            firstKeys = null;
            secondKeys = null;
            occupied = null;
            tags = null;
        }

        @Override
        public long retainedBytes()
        {
            if (adaptiveDelegate != null) {
                return adaptiveDelegate.retainedBytes();
            }
            return (firstKeys == null ? 0 : (long) firstKeys.length * Long.BYTES) +
                    (secondKeys == null ? 0 : (long) secondKeys.length * Long.BYTES) +
                    (occupied == null ? 0 : occupied.length) +
                    (tags == null ? 0 : tags.length) +
                    independentDictionaryTupleDomain.retainedBytes();
        }

        private static int mix(long first, long second)
        {
            return (int) hash64(first, second);
        }

        private static long hash64(long first, long second)
        {
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return hash;
        }

        private static byte hashTag(long hash)
        {
            return (byte) ((hash >>> 56) | 0x80L);
        }
    }

    private static final class GroupedLongDistinctIndex
            implements DistinctIndex
    {
        private static final int MAX_RESERVED_BATCH_GROUPS = 1 << 16;
        private static final float LOAD_FACTOR = 0.75f;
        private static final int INITIAL_TABLE_SIZE = 16;
        private static final int INLINE_CAPACITY = 4;
        private static final int INLINE_GROUPS_PER_CHUNK_SHIFT = 15;
        private static final int INLINE_GROUPS_PER_CHUNK = 1 << INLINE_GROUPS_PER_CHUNK_SHIFT;
        private static final int INLINE_GROUPS_PER_CHUNK_MASK = INLINE_GROUPS_PER_CHUNK - 1;
        private static final int INLINE_CHUNK_LONGS = INLINE_GROUPS_PER_CHUNK * INLINE_CAPACITY;
        private final PrimitiveArrayPool arrayPool;
        private final DistinctKeySetPolicy policy;

        private long[][] tables = new long[16][];
        private int[] sizes = new int[16];
        private boolean[] containsZero = new boolean[16];
        private long[][] inlineChunks = new long[16][];
        private boolean inlineSmallGroups;

        private GroupedLongDistinctIndex(PrimitiveArrayPool arrayPool, DistinctKeySetPolicy policy)
        {
            this.arrayPool = arrayPool;
            this.policy = policy;
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls[1], position)) {
                return false;
            }
            return addKey(toIntExact(OperatorVectorSupport.longValue(values[0], position)), OperatorVectorSupport.longValue(values[1], position));
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues groupValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values[1]);
            boolean nullFree = VectorAccess.isAllFalseNulls(nulls[1]);
            VectorAccess.BooleanValues keyNulls = nullFree ? null : VectorAccess.booleanValues(nulls[1]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if ((nullFree || !keyNulls.value(position)) && addKey(toIntExact(groupValues.value(position)), keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
                return count;
            }
            for (int position : mask) {
                if ((nullFree || !keyNulls.value(position)) && addKey(toIntExact(groupValues.value(position)), keyValues.value(position))) {
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        @Override
        public int addGroupedBatch(Vector[] values, Vector[] nulls, Mask mask, int groupCount, int[] distinctPositions)
        {
            // A dense chunk replaces one small Java array per group. Wait until the observed group domain is
            // large enough for those object headers and allocation sites to dominate the fixed chunk cost.
            // Admission is monotonic: representation selection disappears from the steady-state row loop.
            if (policy.inlineSmallGroupedLong() && groupCount > MAX_RESERVED_BATCH_GROUPS) {
                inlineSmallGroups = true;
            }
            // With millions of groups, eagerly growing the three group-metadata arrays at every batch boundary
            // loses locality versus the ordinary incremental path. Small/stable group domains amortize the
            // reservation and let the hot row loop omit both conversion and capacity guards.
            if (groupCount > MAX_RESERVED_BATCH_GROUPS) {
                return addBatch(values, nulls, mask, distinctPositions);
            }
            ensureGroupCapacity(groupCount);
            VectorAccess.LongValues groupValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values[1]);
            boolean nullFree = VectorAccess.isAllFalseNulls(nulls[1]);
            VectorAccess.BooleanValues keyNulls = nullFree ? null : VectorAccess.booleanValues(nulls[1]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if ((nullFree || !keyNulls.value(position)) && addReservedKey((int) groupValues.value(position), keyValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
                return count;
            }
            for (int position : mask) {
                if ((nullFree || !keyNulls.value(position)) && addReservedKey((int) groupValues.value(position), keyValues.value(position))) {
                    distinctPositions[count++] = position;
                }
            }
            return count;
        }

        private boolean addKey(int group, long key)
        {
            if (group < 0) {
                throw new IllegalArgumentException("group is negative: " + group);
            }
            ensureGroupCapacity(group + 1);
            return addReservedKey(group, key);
        }

        private boolean addReservedKey(int group, long key)
        {
            if (key == 0) {
                if (containsZero[group]) {
                    return false;
                }
                containsZero[group] = true;
                sizes[group]++;
                return true;
            }

            long[] table = tables[group];
            if (inlineSmallGroups && table == null) {
                return addInlineKey(group, key);
            }
            if (table == null) {
                table = allocateTable(INITIAL_TABLE_SIZE);
                tables[group] = table;
            }
            if (sizes[group] + 1 >= (int) (table.length * LOAD_FACTOR)) {
                table = growTable(table);
                tables[group] = table;
            }

            int hash = GroupingState.hashLong(key);
            int slot = hash & (table.length - 1);
            while (table[slot] != 0) {
                if (table[slot] == key) {
                    return false;
                }
                slot = (slot + 1) & (table.length - 1);
            }
            table[slot] = key;
            sizes[group]++;
            return true;
        }

        private boolean addInlineKey(int group, long key)
        {
            int nonzeroSize = sizes[group] - (containsZero[group] ? 1 : 0);
            long[] chunk = inlineChunk(group);
            int offset = (group & INLINE_GROUPS_PER_CHUNK_MASK) * INLINE_CAPACITY;
            for (int index = 0; index < nonzeroSize; index++) {
                if (chunk[offset + index] == key) {
                    return false;
                }
            }
            if (nonzeroSize < INLINE_CAPACITY) {
                chunk[offset + nonzeroSize] = key;
                sizes[group]++;
                return true;
            }

            long[] table = allocateTable(INITIAL_TABLE_SIZE);
            tables[group] = table;
            int mask = table.length - 1;
            for (int index = 0; index < INLINE_CAPACITY; index++) {
                long existing = chunk[offset + index];
                int slot = GroupingState.hashLong(existing) & mask;
                while (table[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                table[slot] = existing;
            }
            return addTableKey(group, key, table);
        }

        private boolean addTableKey(int group, long key, long[] table)
        {
            if (sizes[group] + 1 >= (int) (table.length * LOAD_FACTOR)) {
                table = growTable(table);
                tables[group] = table;
            }
            int slot = GroupingState.hashLong(key) & (table.length - 1);
            while (table[slot] != 0) {
                if (table[slot] == key) {
                    return false;
                }
                slot = (slot + 1) & (table.length - 1);
            }
            table[slot] = key;
            sizes[group]++;
            return true;
        }

        private long[] inlineChunk(int group)
        {
            int chunkIndex = group >>> INLINE_GROUPS_PER_CHUNK_SHIFT;
            if (chunkIndex >= inlineChunks.length) {
                inlineChunks = Arrays.copyOf(inlineChunks, Integer.highestOneBit(chunkIndex) << 1);
            }
            long[] chunk = inlineChunks[chunkIndex];
            if (chunk == null) {
                chunk = arrayPool.borrowLongs(INLINE_CHUNK_LONGS);
                Arrays.fill(chunk, 0);
                inlineChunks[chunkIndex] = chunk;
            }
            return chunk;
        }

        private void ensureGroupCapacity(int needed)
        {
            if (needed <= tables.length) {
                return;
            }
            int capacity = Integer.highestOneBit(needed - 1) << 1;
            tables = Arrays.copyOf(tables, capacity);
            sizes = Arrays.copyOf(sizes, capacity);
            containsZero = Arrays.copyOf(containsZero, capacity);
        }

        private long[] allocateTable(int capacity)
        {
            long[] table = arrayPool.borrowLongs(capacity);
            Arrays.fill(table, 0);
            return table;
        }

        private long[] growTable(long[] previous)
        {
            long[] table = allocateTable(previous.length * 2);
            int mask = table.length - 1;
            for (long key : previous) {
                if (key == 0) {
                    continue;
                }
                int hash = GroupingState.hashLong(key);
                int slot = hash & mask;
                while (table[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                table[slot] = key;
            }
            arrayPool.release(previous);
            return table;
        }

        @Override
        public void releaseBuffers()
        {
            for (long[] table : tables) {
                arrayPool.release(table);
            }
            for (long[] chunk : inlineChunks) {
                arrayPool.release(chunk);
            }
            tables = new long[0][];
            sizes = new int[0];
            containsZero = new boolean[0];
            inlineChunks = new long[0][];
        }

        @Override
        public long retainedBytes()
        {
            long bytes = (long) sizes.length * Integer.BYTES + containsZero.length;
            for (long[] table : tables) {
                bytes += table == null ? 0 : (long) table.length * Long.BYTES;
            }
            for (long[] chunk : inlineChunks) {
                bytes += chunk == null ? 0 : (long) chunk.length * Long.BYTES;
            }
            return bytes;
        }
    }

    private static final class LongTripleDistinctIndex
            implements DistinctIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] thirdKeys;
        private boolean[] occupied;
        private int mask;
        private int maxFill;
        private int size;

        private LongTripleDistinctIndex(int expectedSize)
        {
            int capacity = capacity(expectedSize);
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            ensureCapacity(size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            long third = OperatorVectorSupport.longValue(values[2], position);
            return addKey(first, second, third);
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        private boolean addKey(long first, long second, long third)
        {
            int index = findSlot(first, second, third);
            if (occupied[index]) {
                return false;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            thirdKeys[index] = third;
            occupied[index] = true;
            size++;
            if (size >= maxFill) {
                rehash(occupied.length * 2);
            }
            return true;
        }

        private int findSlot(long first, long second, long third)
        {
            int index = mix(first, second, third) & mask;
            while (occupied[index] && (firstKeys[index] != first || secondKeys[index] != second || thirdKeys[index] != third)) {
                index = (index + 1) & mask;
            }
            return index;
        }

        private void ensureCapacity(int expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = occupied.length;
            while (expectedSize >= (int) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousThirdKeys = thirdKeys;
            boolean[] previousOccupied = occupied;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousOccupied.length; index++) {
                if (!previousOccupied[index]) {
                    continue;
                }
                int newIndex = findSlot(previousFirstKeys[index], previousSecondKeys[index], previousThirdKeys[index]);
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                thirdKeys[newIndex] = previousThirdKeys[index];
                occupied[newIndex] = true;
                size++;
            }
        }

        private static int mix(long first, long second, long third)
        {
            long hash = first * 0x9E3779B97F4A7C15L + second * 0xC4CEB9FE1A85EC53L + third * 0x94D049BB133111EBL;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return (int) hash;
        }

        @Override
        public void releaseBuffers()
        {
            firstKeys = null;
            secondKeys = null;
            thirdKeys = null;
            occupied = null;
        }

        @Override
        public long retainedBytes()
        {
            return (firstKeys == null ? 0 : (long) firstKeys.length * Long.BYTES) +
                    (secondKeys == null ? 0 : (long) secondKeys.length * Long.BYTES) +
                    (thirdKeys == null ? 0 : (long) thirdKeys.length * Long.BYTES) +
                    (occupied == null ? 0 : occupied.length);
        }
    }

    private static final class LongQuadDistinctIndex
            implements DistinctIndex
    {
        private static final float LOAD_FACTOR = 0.75f;

        private long[] firstKeys;
        private long[] secondKeys;
        private long[] thirdKeys;
        private long[] fourthKeys;
        private boolean[] occupied;
        private int mask;
        private int maxFill;
        private int size;

        private LongQuadDistinctIndex(int expectedSize)
        {
            int capacity = capacity(expectedSize);
            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            fourthKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            ensureCapacity(size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            if (hasNull(nulls, position)) {
                return false;
            }

            long first = OperatorVectorSupport.longValue(values[0], position);
            long second = OperatorVectorSupport.longValue(values[1], position);
            long third = OperatorVectorSupport.longValue(values[2], position);
            long fourth = OperatorVectorSupport.longValue(values[3], position);
            return addKey(first, second, third, fourth);
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            VectorAccess.LongValues firstValues = VectorAccess.longValues(values[0]);
            VectorAccess.LongValues secondValues = VectorAccess.longValues(values[1]);
            VectorAccess.LongValues thirdValues = VectorAccess.longValues(values[2]);
            VectorAccess.LongValues fourthValues = VectorAccess.longValues(values[3]);
            VectorAccess.BooleanValues firstNulls = VectorAccess.booleanValues(nulls[0]);
            VectorAccess.BooleanValues secondNulls = VectorAccess.booleanValues(nulls[1]);
            VectorAccess.BooleanValues thirdNulls = VectorAccess.booleanValues(nulls[2]);
            VectorAccess.BooleanValues fourthNulls = VectorAccess.booleanValues(nulls[3]);
            int count = 0;
            if (mask.all()) {
                int size = mask.size();
                for (int position = 0; position < size; position++) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position) || fourthNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position), fourthValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (firstNulls.value(position) || secondNulls.value(position) || thirdNulls.value(position) || fourthNulls.value(position)) {
                        continue;
                    }
                    if (addKey(firstValues.value(position), secondValues.value(position), thirdValues.value(position), fourthValues.value(position))) {
                        distinctPositions[count++] = position;
                    }
                }
            }
            return count;
        }

        private boolean addKey(long first, long second, long third, long fourth)
        {
            int index = findSlot(first, second, third, fourth);
            if (occupied[index]) {
                return false;
            }
            firstKeys[index] = first;
            secondKeys[index] = second;
            thirdKeys[index] = third;
            fourthKeys[index] = fourth;
            occupied[index] = true;
            size++;
            if (size >= maxFill) {
                rehash(occupied.length * 2);
            }
            return true;
        }

        private int findSlot(long first, long second, long third, long fourth)
        {
            int index = mix(first, second, third, fourth) & mask;
            while (occupied[index] && (firstKeys[index] != first || secondKeys[index] != second || thirdKeys[index] != third || fourthKeys[index] != fourth)) {
                index = (index + 1) & mask;
            }
            return index;
        }

        private void ensureCapacity(int expectedSize)
        {
            if (expectedSize < maxFill) {
                return;
            }

            int capacity = occupied.length;
            while (expectedSize >= (int) (capacity * LOAD_FACTOR)) {
                capacity <<= 1;
            }
            rehash(capacity);
        }

        private void rehash(int capacity)
        {
            long[] previousFirstKeys = firstKeys;
            long[] previousSecondKeys = secondKeys;
            long[] previousThirdKeys = thirdKeys;
            long[] previousFourthKeys = fourthKeys;
            boolean[] previousOccupied = occupied;

            firstKeys = new long[capacity];
            secondKeys = new long[capacity];
            thirdKeys = new long[capacity];
            fourthKeys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
            maxFill = (int) (capacity * LOAD_FACTOR);
            size = 0;

            for (int index = 0; index < previousOccupied.length; index++) {
                if (!previousOccupied[index]) {
                    continue;
                }
                int newIndex = findSlot(previousFirstKeys[index], previousSecondKeys[index], previousThirdKeys[index], previousFourthKeys[index]);
                firstKeys[newIndex] = previousFirstKeys[index];
                secondKeys[newIndex] = previousSecondKeys[index];
                thirdKeys[newIndex] = previousThirdKeys[index];
                fourthKeys[newIndex] = previousFourthKeys[index];
                occupied[newIndex] = true;
                size++;
            }
        }

        private static int mix(long first, long second, long third, long fourth)
        {
            long hash = first * 0x9E3779B97F4A7C15L
                    + second * 0xC4CEB9FE1A85EC53L
                    + third * 0x94D049BB133111EBL
                    + fourth * 0xBF58476D1CE4E5B9L;
            hash ^= hash >>> 33;
            hash *= 0xFF51AFD7ED558CCDL;
            hash ^= hash >>> 33;
            hash *= 0xC4CEB9FE1A85EC53L;
            hash ^= hash >>> 33;
            return (int) hash;
        }

        @Override
        public void releaseBuffers()
        {
            firstKeys = null;
            secondKeys = null;
            thirdKeys = null;
            fourthKeys = null;
            occupied = null;
        }

        @Override
        public long retainedBytes()
        {
            return (firstKeys == null ? 0 : (long) firstKeys.length * Long.BYTES) +
                    (secondKeys == null ? 0 : (long) secondKeys.length * Long.BYTES) +
                    (thirdKeys == null ? 0 : (long) thirdKeys.length * Long.BYTES) +
                    (fourthKeys == null ? 0 : (long) fourthKeys.length * Long.BYTES) +
                    (occupied == null ? 0 : occupied.length);
        }
    }

    private static final class FixedWidthDistinctIndex
            implements DistinctIndex
    {
        private final boolean retainNulls;
        private final AbstractFixedWidthKeyTable table;
        private final FixedWidthKeyBatchBindings bindings;
        private final int[] singlePosition = new int[1];
        private final int[] singleDistinct = new int[1];

        private FixedWidthDistinctIndex(
                ResolvedFixedWidthKeyLayout layout,
                boolean retainNulls,
                int expectedSize,
                PrimitiveArrayPool arrayPool,
                OperatorCodeGenerationResources codeGeneration,
                AdaptiveLongGroupingPolicy policy)
        {
            this.retainNulls = retainNulls;
            table = codeGeneration.fixedWidthKeyTables().createDistinct(
                    FixedWidthKeyTableLayout.from(layout),
                    Math.max(16, expectedSize),
                    arrayPool,
                    policy);
            bindings = new FixedWidthKeyBatchBindings(layout, arrayPool);
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            table.ensureCapacity(table.size + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            singlePosition[0] = position;
            return addPositions(values, nulls, singlePosition, 1, singleDistinct) == 1;
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            int[] positions = mask.all() ? null : mask.selectedPositions();
            return addPositions(values, nulls, positions, mask.count(), distinctPositions);
        }

        @Override
        public int addNonNullBatch(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            return addPositions(values, nulls, positions, positionCount, distinctPositions, false);
        }

        @Override
        public int addNonNullDenseBatch(Vector[] values, Vector[] nulls, int positionCount, int[] positions, int[] distinctPositions)
        {
            return addPositions(values, nulls, null, positionCount, distinctPositions, false);
        }

        @Override
        public int addRetainingNullBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            int[] positions = mask.all() ? null : mask.selectedPositions();
            return addPositions(values, nulls, positions, mask.count(), distinctPositions, true);
        }

        private int addPositions(Vector[] values, Vector[] nulls, int[] positions, int positionCount, int[] distinctPositions)
        {
            return addPositions(values, nulls, positions, positionCount, distinctPositions, retainNulls);
        }

        private int addPositions(
                Vector[] values,
                Vector[] nulls,
                int[] positions,
                int positionCount,
                int[] distinctPositions,
                boolean retainNullKeys)
        {
            bindings.bind(values, nulls);
            try {
                table.ensureCapacity(table.size + positionCount);
                if (retainNullKeys) {
                    return table.assignPhysicalDistinctRetainingNullBatch(
                            bindings.keyArrays(),
                            bindings.keyMappings(),
                            bindings.keyMappingOffsets(),
                            bindings.keyBaseOffsets(),
                            bindings.nullArrays(),
                            bindings.nullMappings(),
                            bindings.nullMappingOffsets(),
                            bindings.nullBaseOffsets(),
                            positions,
                            positionCount,
                            distinctPositions,
                            table.size);
                }
                return table.assignPhysicalDistinctBatch(
                        bindings.keyArrays(),
                        bindings.keyMappings(),
                        bindings.keyMappingOffsets(),
                        bindings.keyBaseOffsets(),
                        bindings.nullArrays(),
                        bindings.nullMappings(),
                        bindings.nullMappingOffsets(),
                        bindings.nullBaseOffsets(),
                        positions,
                        positionCount,
                        distinctPositions,
                        table.size);
            }
            finally {
                bindings.release();
            }
        }

        @Override
        public long retainedBytes()
        {
            return table.retainedBytes();
        }

        @Override
        public void releaseBuffers()
        {
            bindings.release();
            table.releaseBuffers();
        }
    }

    private static final class ObjectDistinctIndex
            implements DistinctIndex
    {
        private final ObjectOpenHashSet<Object> keys = new ObjectOpenHashSet<>();
        private final OperatorKeySemantics.Key[] probeKeys;
        private final OperatorKeySemantics.CompositeProbeKey compositeProbeKey;

        private ObjectDistinctIndex(int keyCount)
        {
            this.probeKeys = new OperatorKeySemantics.Key[keyCount];
            this.compositeProbeKey = keyCount > 1 ? OperatorKeySemantics.reusableCompositeProbeKey(keyCount) : null;
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            keys.ensureCapacity(keys.size() + Math.max(0, additionalEntries));
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            OperatorKeySemantics.Key key = keyForPosition(values, nulls, position);
            if (key == null) {
                return false;
            }
            if (keys.contains(key)) {
                return false;
            }
            keys.add(OperatorKeySemantics.ownedKey(key));
            return true;
        }

        private OperatorKeySemantics.Key keyForPosition(Vector[] values, Vector[] nulls, int position)
        {
            for (int keyIndex = 0; keyIndex < values.length; keyIndex++) {
                if (probeKeys[keyIndex] == null) {
                    probeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key key = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, probeKeys[keyIndex]);
                if (key == null) {
                    return null;
                }
                probeKeys[keyIndex] = key;
            }
            if (probeKeys.length == 1) {
                return probeKeys[0];
            }
            return OperatorKeySemantics.probeCompositeKey(Arrays.copyOf(probeKeys, probeKeys.length), compositeProbeKey);
        }
    }

    /**
     * Wraps a fast {@link DistinctIndex} to add SQL {@code DISTINCT}/{@code UNION} null handling. Fully non-null
     * batches are forwarded unchanged to the delegate (preserving its specialized batch path); a row with any
     * NULL key column is routed to a separate null-aware set keyed by a {@link OperatorKeySemantics.CompositeKey}
     * whose null columns are represented by {@code null} entries, so equal-null rows collapse to one survivor
     * while staying distinct from every concrete-valued row.
     */
    private static final class RetainNullsDistinctIndex
            implements DistinctIndex
    {
        private static final int[] EMPTY_POSITIONS = new int[0];

        private final DistinctIndex delegate;
        private final int keyCount;
        private final OperatorKeySemantics.Key[] probeKeys;
        private final VectorAccess.BooleanValues[] nullAccessors;
        private final ObjectOpenHashSet<Object> nullContainingKeys = new ObjectOpenHashSet<>();
        private int[] nonNullDistinctPositions = EMPTY_POSITIONS;
        private boolean debugAdaptiveRetainNullBatchPrinted;

        private final PrimitiveArrayPool arrayPool;
        private final DistinctKeySetPolicy policy;

        private RetainNullsDistinctIndex(DistinctIndex delegate, int keyCount, PrimitiveArrayPool arrayPool, DistinctKeySetPolicy policy)
        {
            this.delegate = delegate;
            this.keyCount = keyCount;
            this.arrayPool = arrayPool;
            this.policy = policy;
            this.probeKeys = new OperatorKeySemantics.Key[keyCount];
            this.nullAccessors = new VectorAccess.BooleanValues[keyCount];
        }

        @Override
        public void reserveAdditional(int additionalEntries)
        {
            delegate.reserveAdditional(additionalEntries);
        }

        @Override
        public boolean add(Vector[] values, Vector[] nulls, int position)
        {
            prepareNullAccessors(nulls);
            if (!hasPreparedNull(position)) {
                return delegate.add(values, nulls, position);
            }
            return nullContainingKeys.add(buildNullAwareKey(values, nulls, position));
        }

        @Override
        public int addBatch(Vector[] values, Vector[] nulls, Mask mask, int[] distinctPositions)
        {
            if (!hasNullStream(nulls)) {
                return delegate.addBatch(values, nulls, mask, distinctPositions);
            }
            if (policy.adaptiveRetainNullsBatch()) {
                int distinctCount = delegate.addRetainingNullBatch(values, nulls, mask, distinctPositions);
                if (distinctCount >= 0) {
                    if (policy.debugDistinctShapes() && !debugAdaptiveRetainNullBatchPrinted) {
                        System.err.printf("[adaptive-retain-null-distinct] keys=%d delegate=%s%n",
                                keyCount, delegate.getClass().getSimpleName());
                        debugAdaptiveRetainNullBatchPrinted = true;
                    }
                    return distinctCount;
                }
            }
            prepareNullAccessors(nulls);
            int selectedCount = mask.selectedCount();
            if (nonNullDistinctPositions.length < selectedCount) {
                int[] previous = nonNullDistinctPositions;
                nonNullDistinctPositions = arrayPool.borrowInts(selectedCount);
                arrayPool.release(previous);
            }

            // Partition once: keep the relatively rare null-containing keys in the generic SQL-null set,
            // and pass the proven-non-null position vector to the delegate's specialized batch kernel.
            int nullCount = 0;
            int nonNullPositionCount = 0;
            for (int position : mask) {
                if (hasPreparedNull(position)) {
                    if (nullContainingKeys.add(buildNullAwareKey(values, nulls, position))) {
                        distinctPositions[nullCount++] = position;
                    }
                }
                else {
                    nonNullDistinctPositions[nonNullPositionCount++] = position;
                }
            }
            int nonNullCount = nonNullPositionCount == selectedCount && mask.all()
                    ? delegate.addNonNullDenseBatch(values, nulls, nonNullPositionCount, nonNullDistinctPositions, nonNullDistinctPositions)
                    : delegate.addNonNullBatch(values, nulls, nonNullDistinctPositions, nonNullPositionCount, nonNullDistinctPositions);

            // Both lists preserve mask order. Merge backward so the null list can remain in the caller's
            // output buffer without another scratch allocation.
            int nullIndex = nullCount - 1;
            int nonNullIndex = nonNullCount - 1;
            int outputIndex = nullCount + nonNullCount - 1;
            while (nullIndex >= 0 && nonNullIndex >= 0) {
                if (distinctPositions[nullIndex] > nonNullDistinctPositions[nonNullIndex]) {
                    distinctPositions[outputIndex--] = distinctPositions[nullIndex--];
                }
                else {
                    distinctPositions[outputIndex--] = nonNullDistinctPositions[nonNullIndex--];
                }
            }
            while (nonNullIndex >= 0) {
                distinctPositions[outputIndex--] = nonNullDistinctPositions[nonNullIndex--];
            }
            return nullCount + nonNullCount;
        }

        private OperatorKeySemantics.Key buildNullAwareKey(Vector[] values, Vector[] nulls, int position)
        {
            OperatorKeySemantics.Key[] keys = new OperatorKeySemantics.Key[keyCount];
            for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                VectorAccess.BooleanValues nullAccessor = nullAccessors[keyIndex];
                if (nullAccessor != null && nullAccessor.value(position)) {
                    keys[keyIndex] = null;
                    continue;
                }
                if (probeKeys[keyIndex] == null) {
                    probeKeys[keyIndex] = OperatorKeySemantics.reusableProbeKey(values[keyIndex]);
                }
                OperatorKeySemantics.Key probe = OperatorKeySemantics.probeKey(values[keyIndex], nulls[keyIndex], position, probeKeys[keyIndex]);
                probeKeys[keyIndex] = probe;
                keys[keyIndex] = OperatorKeySemantics.ownedKey(probe);
            }
            return new OperatorKeySemantics.CompositeKey(keys);
        }

        private void prepareNullAccessors(Vector[] nulls)
        {
            for (int index = 0; index < keyCount; index++) {
                Vector nullVector = nulls[index];
                nullAccessors[index] = VectorAccess.isAllFalseNulls(nullVector) ? null : VectorAccess.booleanValues(nullVector);
            }
        }

        private boolean hasPreparedNull(int position)
        {
            for (VectorAccess.BooleanValues nullAccessor : nullAccessors) {
                if (nullAccessor != null && nullAccessor.value(position)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void releaseBuffers()
        {
            delegate.releaseBuffers();
            arrayPool.release(nonNullDistinctPositions);
            nonNullDistinctPositions = EMPTY_POSITIONS;
            Arrays.fill(nullAccessors, null);
        }

        @Override
        public long retainedBytes()
        {
            return delegate.retainedBytes() + (long) nonNullDistinctPositions.length * Integer.BYTES;
        }

        private static boolean hasNullStream(Vector[] nulls)
        {
            for (Vector nullsVector : nulls) {
                if (!VectorAccess.isAllFalseNulls(nullsVector)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static boolean isIntegerVector(Vector vector)
    {
        return switch (OperatorVectorSupport.flatten(vector)) {
            case I32Vector _ -> true;
            case I64Vector _ -> true;
            default -> false;
        };
    }

    private static boolean allIntegerVectors(Vector[] vectors)
    {
        for (Vector vector : vectors) {
            if (!isIntegerVector(vector)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasNull(Vector[] nulls, int position)
    {
        for (Vector nullsVector : nulls) {
            if (OperatorVectorSupport.isNull(nullsVector, position)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sameDictionaryMapping(DictionaryVector left, DictionaryVector right)
    {
        return left.length() == right.length() &&
                (left.ids() == right.ids() ||
                        Arrays.mismatch(left.ids(), 0, left.length(), right.ids(), 0, right.length()) < 0);
    }

    private static int capacity(int expectedSize)
    {
        int capacity = 16;
        while (capacity < expectedSize / LOAD_FACTOR) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static final float LOAD_FACTOR = 0.75f;
}
