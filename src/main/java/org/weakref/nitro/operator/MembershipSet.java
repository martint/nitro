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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Exact, null-rejecting membership state selected from the physical key shape. Operators use this contract instead
 * of abusing a grouping table and retaining group ids/reverse keys they never consume.
 */
final class MembershipSet
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final OperatorResources operatorResources;
    private final MembershipSetPolicy policy;
    private final Optional<TypeBinding> keyType;
    private final StructuralKeyKernel keyKernel;
    private Index index;

    MembershipSet(
            Allocator allocator,
            Allocator.Context allocationContext,
            OperatorResources operatorResources,
            MembershipSetPolicy policy,
            Optional<TypeBinding> keyType)
    {
        this.allocator = allocator;
        this.allocationContext = allocationContext;
        this.operatorResources = operatorResources;
        this.policy = requireNonNull(policy, "policy is null");
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.keyKernel = operatorResources.codeGeneration().structuralTypes().key(
                keyType.orElseGet(() -> Schema.unspecified(1).field(0).type()));
    }

    void addBatch(Vector values, Vector nulls, Mask mask)
    {
        validateKeyType(values);
        if (index == null) {
            if (!keyKernel.allowsLegacyPhysicalShortcuts()) {
                index = new StructuralIndex(allocator, allocationContext, keyKernel);
            }
            else {
                FlatTypeHandler handler = FlatTypeHandlers.forVector(values);
                LongIndex longIndex = handler != null && handler.kind() == FlatTypeHandler.Kind.LONG
                        ? LongIndex.tryCreate(values, nulls, mask, allocator.primitiveArrays(), policy)
                        : null;
                index = longIndex != null ? longIndex : new GroupingIndex(allocator, allocationContext, operatorResources, keyType);
            }
        }
        index.addBatch(values, nulls, mask);
    }

    boolean allowsLegacyPhysicalShortcuts()
    {
        return keyKernel.allowsLegacyPhysicalShortcuts();
    }

    void beginProbeBatch(Vector values, Vector nulls)
    {
        validateKeyType(values);
        if (index == null) {
            // An empty build has no schema-bearing batch. The probe still needs an exact always-false index.
            index = new EmptyIndex();
        }
        index.beginProbeBatch(values, nulls);
    }

    private void validateKeyType(Vector values)
    {
        keyType.filter(TypeBinding::isSpecified)
                .filter(type -> !type.supportsVector(values))
                .ifPresent(type -> {
                    throw new IllegalArgumentException("Membership key vector is not supported by type " + type.identity());
                });
    }

    boolean contains(int position)
    {
        return index.contains(position);
    }

    int selectPositions(Vector values, Vector nulls, Mask mask, boolean includeMatches, int[] positions)
    {
        return index.selectPositions(values, nulls, mask, includeMatches, positions);
    }

    void writeMatches(Vector values, Vector nulls, Mask mask, boolean[] matches)
    {
        index.writeMatches(values, nulls, mask, matches);
    }

    void endProbeBatch()
    {
        index.endProbeBatch();
    }

    void releaseBuffers()
    {
        if (index != null) {
            index.releaseBuffers();
            index = null;
        }
    }

    private interface Index
    {
        void addBatch(Vector values, Vector nulls, Mask mask);

        void beginProbeBatch(Vector values, Vector nulls);

        boolean contains(int position);

        default int selectPositions(Vector values, Vector nulls, Mask mask, boolean includeMatches, int[] positions)
        {
            int selected = 0;
            beginProbeBatch(values, nulls);
            try {
                for (int position : mask) {
                    if (contains(position) == includeMatches) {
                        positions[selected++] = position;
                    }
                }
            }
            finally {
                endProbeBatch();
            }
            return selected;
        }

        default void writeMatches(Vector values, Vector nulls, Mask mask, boolean[] matches)
        {
            beginProbeBatch(values, nulls);
            try {
                for (int position : mask) {
                    matches[position] = contains(position);
                }
            }
            finally {
                endProbeBatch();
            }
        }

        default void endProbeBatch() {}

        default void releaseBuffers() {}
    }

    private static final class EmptyIndex
            implements Index
    {
        @Override
        public void addBatch(Vector values, Vector nulls, Mask mask)
        {
            throw new IllegalStateException("Cannot add after probing an empty membership set");
        }

        @Override
        public void beginProbeBatch(Vector values, Vector nulls) {}

        @Override
        public boolean contains(int position)
        {
            return false;
        }

        @Override
        public int selectPositions(Vector values, Vector nulls, Mask mask, boolean includeMatches, int[] positions)
        {
            if (includeMatches) {
                return 0;
            }
            int count = 0;
            for (int position : mask) {
                positions[count++] = position;
            }
            return count;
        }

        @Override
        public void writeMatches(Vector values, Vector nulls, Mask mask, boolean[] matches)
        {
            for (int position : mask) {
                matches[position] = false;
            }
        }
    }

    private static final class GroupingIndex
            implements Index
    {
        private final Allocator allocator;
        private final Allocator.Context allocationContext;
        private final GroupingState grouping;
        private I64Vector scratch;
        private Vector probeValues;
        private Vector probeNulls;

        private GroupingIndex(
                Allocator allocator,
                Allocator.Context allocationContext,
                OperatorResources operatorResources,
                Optional<TypeBinding> keyType)
        {
            this.allocator = allocator;
            this.allocationContext = allocationContext;
            this.grouping = new GroupingState(
                    allocator.primitiveArrays(),
                    operatorResources.codeGeneration(),
                    operatorResources.grouping(),
                    operatorResources.adaptiveLongGroupingPolicy(),
                    operatorResources.flatKeyTablePolicy(),
                    keyType.stream().toList(),
                    allocator,
                    allocationContext);
        }

        @Override
        public void addBatch(Vector values, Vector nulls, Mask mask)
        {
            scratch = allocator.allocateOrGrow(
                    allocationContext,
                    scratch,
                    I64Vector.class,
                    values.length(),
                    I64Vector::new);
            grouping.assignGroups(values, nulls, mask, scratch);
        }

        @Override
        public void beginProbeBatch(Vector values, Vector nulls)
        {
            probeValues = values;
            probeNulls = nulls;
            grouping.beginContainsBatch(values, nulls);
        }

        @Override
        public boolean contains(int position)
        {
            return grouping.contains(probeValues, probeNulls, position);
        }

        @Override
        public void endProbeBatch()
        {
            grouping.endContainsBatch();
            probeValues = null;
            probeNulls = null;
        }

        @Override
        public void releaseBuffers()
        {
            grouping.releaseBuffers();
            scratch = null;
        }
    }

    private static final class StructuralIndex
            implements Index
    {
        private final Allocator allocator;
        private final Allocator.Context allocationContext;
        private final StructuralKeyKernel kernel;
        private final Set<StructuralMembershipKey> keys = new HashSet<>();
        private final StructuralMembershipKey probeKey;

        private StructuralIndex(
                Allocator allocator,
                Allocator.Context allocationContext,
                StructuralKeyKernel kernel)
        {
            this.allocator = allocator;
            this.allocationContext = allocationContext;
            this.kernel = kernel;
            this.probeKey = new StructuralMembershipKey(kernel);
        }

        @Override
        public void addBatch(Vector values, Vector nulls, Mask mask)
        {
            Streams copied = allocator.copyStreams(
                    allocationContext,
                    Streams.of(values, nulls, null),
                    mask);
            Vector copiedValues = copied.values();
            Vector copiedNulls = copied.getOrNull(Stream.NULLS);
            for (int position = 0; position < copiedValues.length(); position++) {
                if (!OperatorVectorSupport.isNull(copiedNulls, position)) {
                    keys.add(new StructuralMembershipKey(kernel, copiedValues, copiedNulls, position));
                }
            }
        }

        @Override
        public void beginProbeBatch(Vector values, Vector nulls)
        {
            probeKey.set(values, nulls, 0);
        }

        @Override
        public boolean contains(int position)
        {
            if (OperatorVectorSupport.isNull(probeKey.nulls, position)) {
                return false;
            }
            probeKey.position = position;
            return keys.contains(probeKey);
        }

        @Override
        public void endProbeBatch()
        {
            probeKey.clear();
        }

        @Override
        public void releaseBuffers()
        {
            keys.clear();
            probeKey.clear();
        }
    }

    private static final class StructuralMembershipKey
    {
        private final StructuralKeyKernel kernel;
        private Vector values;
        private Vector nulls;
        private int position;

        private StructuralMembershipKey(StructuralKeyKernel kernel)
        {
            this.kernel = kernel;
        }

        private StructuralMembershipKey(
                StructuralKeyKernel kernel,
                Vector values,
                Vector nulls,
                int position)
        {
            this.kernel = kernel;
            set(values, nulls, position);
        }

        private void set(Vector values, Vector nulls, int position)
        {
            this.values = requireNonNull(values, "values is null");
            this.nulls = nulls;
            this.position = position;
        }

        private void clear()
        {
            values = null;
            nulls = null;
            position = 0;
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(kernel.hash(values, nulls, position));
        }

        @Override
        public boolean equals(Object object)
        {
            return object instanceof StructuralMembershipKey other &&
                    kernel == other.kernel &&
                    kernel.identical(values, nulls, position, other.values, other.nulls, other.position);
        }
    }

    private static final class LongIndex
            implements Index
    {
        private static final int WORD_SHIFT = 6;
        private static final int WORD_BITS = Long.SIZE;
        private final PrimitiveArrayPool arrayPool;
        private final MembershipSetPolicy policy;
        private long[] bits;
        private long base;
        private int capacityBits;
        private LongOpenHashSet hash;
        private VectorAccess.LongValues probeValues;
        private VectorAccess.BooleanValues probeNulls;

        private LongIndex(long base, int capacityBits, PrimitiveArrayPool arrayPool, MembershipSetPolicy policy)
        {
            this.arrayPool = arrayPool;
            this.policy = policy;
            allocateBits(base, capacityBits);
        }

        private static LongIndex tryCreate(
                Vector values,
                Vector nulls,
                Mask mask,
                PrimitiveArrayPool arrayPool,
                MembershipSetPolicy policy)
        {
            if (mask.none()) {
                return null;
            }
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values);
            VectorAccess.BooleanValues keyNulls = VectorAccess.booleanValues(nulls);
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            int observed = 0;
            for (int position : mask) {
                if (keyNulls.value(position)) {
                    continue;
                }
                long value = keyValues.value(position);
                min = Math.min(min, value);
                max = Math.max(max, value);
                observed++;
            }
            if (observed == 0) {
                return null;
            }
            long alignedMin = Math.floorDiv(min, WORD_BITS) * WORD_BITS;
            long required = safeSpan(alignedMin, max);
            if (required <= 0 ||
                    required > policy.denseLongMaxCapacityBits() ||
                    required > (long) observed * policy.denseLongMaxBitsPerObservedKey()) {
                return null;
            }
            return new LongIndex(alignedMin, bitCapacity(required, policy.denseLongMinCapacityBits()), arrayPool, policy);
        }

        @Override
        public void addBatch(Vector values, Vector nulls, Mask mask)
        {
            if (mask.none()) {
                return;
            }
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values);
            VectorAccess.BooleanValues keyNulls = VectorAccess.booleanValues(nulls);
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    if (!keyNulls.value(position)) {
                        add(keyValues.value(position));
                    }
                }
                return;
            }
            for (int position : mask) {
                if (!keyNulls.value(position)) {
                    add(keyValues.value(position));
                }
            }
        }

        private void add(long key)
        {
            if (hash != null) {
                hash.add(key);
                return;
            }
            long ordinal = key - base;
            if (ordinal < 0 || ordinal >= capacityBits) {
                if (!expandToInclude(key)) {
                    hash.add(key);
                    return;
                }
                ordinal = key - base;
            }
            int word = (int) (ordinal >>> WORD_SHIFT);
            long bit = 1L << ordinal;
            // Membership does not consume an insertion result or distinct count.  Rewriting an already-set bit is
            // exact and avoids a poorly-predicted duplicate branch on large build sides.
            bits[word] |= bit;
        }

        private boolean expandToInclude(long key)
        {
            long currentMax;
            try {
                currentMax = Math.addExact(base, capacityBits - 1L);
            }
            catch (ArithmeticException _) {
                convertToHash();
                return false;
            }
            long min = Math.min(base, key);
            long max = Math.max(currentMax, key);
            long newBase = Math.floorDiv(min, WORD_BITS) * WORD_BITS;
            long required = safeSpan(newBase, max);
            if (required <= 0 || required > policy.denseLongMaxCapacityBits() ||
                    required > (long) (bitCardinality() + 1) * policy.denseLongMaxBitsPerObservedKey()) {
                convertToHash();
                return false;
            }
            int newCapacity = bitCapacity(required, policy.denseLongMinCapacityBits());
            long[] expanded = arrayPool.borrowLongs(newCapacity >>> WORD_SHIFT);
            Arrays.fill(expanded, 0);
            int wordOffset = toIntExact((base - newBase) >>> WORD_SHIFT);
            System.arraycopy(bits, 0, expanded, wordOffset, bits.length);
            arrayPool.release(bits);
            bits = expanded;
            base = newBase;
            capacityBits = newCapacity;
            return true;
        }

        private void convertToHash()
        {
            // Conversion is a cold representation boundary. Count the exact population here so the hash table is
            // sized once without imposing distinct-tracking work on every bit insertion.
            LongOpenHashSet converted = new LongOpenHashSet(Math.max(16, bitCardinality()));
            for (int wordIndex = 0; wordIndex < bits.length; wordIndex++) {
                long word = bits[wordIndex];
                while (word != 0) {
                    int bit = Long.numberOfTrailingZeros(word);
                    converted.add(base + ((long) wordIndex << WORD_SHIFT) + bit);
                    word &= word - 1;
                }
            }
            arrayPool.release(bits);
            bits = null;
            capacityBits = 0;
            hash = converted;
        }

        private int bitCardinality()
        {
            int cardinality = 0;
            for (long word : bits) {
                cardinality += Long.bitCount(word);
            }
            return cardinality;
        }

        private void allocateBits(long newBase, int newCapacityBits)
        {
            base = newBase;
            capacityBits = newCapacityBits;
            bits = arrayPool.borrowLongs(newCapacityBits >>> WORD_SHIFT);
            Arrays.fill(bits, 0);
        }

        private static long safeSpan(long min, long max)
        {
            try {
                return Math.addExact(Math.subtractExact(max, min), 1);
            }
            catch (ArithmeticException _) {
                return -1;
            }
        }

        private static int bitCapacity(long required, int minimumCapacity)
        {
            int capacity = minimumCapacity;
            while (capacity < required) {
                capacity <<= 1;
            }
            return capacity;
        }

        @Override
        public void beginProbeBatch(Vector values, Vector nulls)
        {
            probeValues = VectorAccess.longValues(values);
            probeNulls = VectorAccess.booleanValues(nulls);
        }

        @Override
        public boolean contains(int position)
        {
            if (probeNulls.value(position)) {
                return false;
            }
            long key = probeValues.value(position);
            if (hash != null) {
                return hash.contains(key);
            }
            long ordinal = key - base;
            return ordinal >= 0 && ordinal < capacityBits &&
                    (bits[(int) (ordinal >>> WORD_SHIFT)] & (1L << ordinal)) != 0;
        }

        @Override
        public int selectPositions(Vector values, Vector nulls, Mask mask, boolean includeMatches, int[] positions)
        {
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values);
            int selected = 0;
            if (VectorAccess.isAllFalseNulls(nulls)) {
                if (mask.all()) {
                    for (int position = 0; position < mask.size(); position++) {
                        if (containsKey(keyValues.value(position)) == includeMatches) {
                            positions[selected++] = position;
                        }
                    }
                    return selected;
                }
                for (int position : mask) {
                    if (containsKey(keyValues.value(position)) == includeMatches) {
                        positions[selected++] = position;
                    }
                }
                return selected;
            }
            VectorAccess.BooleanValues keyNulls = VectorAccess.booleanValues(nulls);
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    if ((!keyNulls.value(position) && containsKey(keyValues.value(position))) == includeMatches) {
                        positions[selected++] = position;
                    }
                }
                return selected;
            }
            for (int position : mask) {
                if ((!keyNulls.value(position) && containsKey(keyValues.value(position))) == includeMatches) {
                    positions[selected++] = position;
                }
            }
            return selected;
        }

        @Override
        public void writeMatches(Vector values, Vector nulls, Mask mask, boolean[] matches)
        {
            VectorAccess.LongValues keyValues = VectorAccess.longValues(values);
            if (VectorAccess.isAllFalseNulls(nulls)) {
                if (mask.all()) {
                    for (int position = 0; position < mask.size(); position++) {
                        matches[position] = containsKey(keyValues.value(position));
                    }
                    return;
                }
                for (int position : mask) {
                    matches[position] = containsKey(keyValues.value(position));
                }
                return;
            }
            VectorAccess.BooleanValues keyNulls = VectorAccess.booleanValues(nulls);
            if (mask.all()) {
                for (int position = 0; position < mask.size(); position++) {
                    matches[position] = !keyNulls.value(position) && containsKey(keyValues.value(position));
                }
                return;
            }
            for (int position : mask) {
                matches[position] = !keyNulls.value(position) && containsKey(keyValues.value(position));
            }
        }

        private boolean containsKey(long key)
        {
            if (hash != null) {
                return hash.contains(key);
            }
            long ordinal = key - base;
            return ordinal >= 0 && ordinal < capacityBits &&
                    (bits[(int) (ordinal >>> WORD_SHIFT)] & (1L << ordinal)) != 0;
        }

        @Override
        public void endProbeBatch()
        {
            probeValues = null;
            probeNulls = null;
        }

        @Override
        public void releaseBuffers()
        {
            arrayPool.release(bits);
            bits = null;
            hash = null;
            capacityBits = 0;
        }
    }
}
