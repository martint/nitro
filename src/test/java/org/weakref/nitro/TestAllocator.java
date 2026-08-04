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
package org.weakref.nitro;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.AllocatorPolicy;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.ConcatenatedBooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MinUtf8StateVector;
import org.weakref.nitro.data.NativeBufferAdvice;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAllocator
{
    @Test
    void testSparseVariableWidthCopyIsCompactAndIndependent()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("sparse-copy");
            BinaryVector source = new BinaryVector(5, 32);
            source.setBytes(0, "ignored-0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.setBytes(1, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.setBytes(2, "ignored-2".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.setBytes(3, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            BinaryVector copy = (BinaryVector) allocator.copyVector(context, source, Mask.sparse(new int[] {1, 3}, 5));

            assertThat(copy.length()).isEqualTo(2);
            assertThat(copy.offsets()).containsExactly(0, 5, 9);
            assertThat(copy.data()).containsExactly("alphabeta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            source.data()[source.startOffset(1)] = 'x';
            assertThat(copy.data()).containsExactly("alphabeta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void testEngineResourcesAreIsolatedAndExplicitlyClosed()
    {
        EngineResources first = EngineResources.createDefault();
        EngineResources second = EngineResources.createDefault();
        PrimitiveArrayPool firstArrays = first.primitiveArrays();

        int[] retained = new int[1 << 16];
        assertThat(firstArrays.retain(int[].class, retained.length, (long) retained.length * Integer.BYTES, retained)).isTrue();
        assertThat(firstArrays.retainedBytes()).isEqualTo((long) retained.length * Integer.BYTES);
        assertThat(second.primitiveArrays().retainedBytes()).isZero();

        first.close();
        assertThatThrownBy(first::primitiveArrays)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Engine resources are closed");
        assertThatThrownBy(() -> firstArrays.release(new int[1]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Primitive array pool is closed");
        assertThat(second.primitiveArrays().retainedBytes()).isZero();
        second.close();
    }

    @Test
    void testResidentMemoryIncludesRetainedPool()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("test");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            I64Vector vector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            long bytes = vector.retainedBytes();

            assertThat(allocator.residentBytes()).isEqualTo(bytes);
            assertThat(memory.reservedBytes()).isEqualTo(bytes);

            allocator.release(context, vector);
            assertThat(allocator.residentBytes()).isEqualTo(bytes);
            assertThat(memory.reservedBytes()).isEqualTo(bytes);

            I64Vector reused = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            assertThat(reused).isSameAs(vector);
            assertThat(memory.reservedBytes()).isEqualTo(bytes);

            allocator.discard(context, reused);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testReleasesPooledMemoryWithoutDiscardingInUseVectors()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("test");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            I64Vector idle = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
            I64Vector inUse = allocator.allocate(context, I64Vector.class, 16, I64Vector::new);
            long inUseBytes = inUse.retainedBytes();
            allocator.release(context, idle);

            allocator.releasePooledMemory();

            assertThat(allocator.residentBytes()).isEqualTo(inUseBytes);
            assertThat(memory.reservedBytes()).isEqualTo(inUseBytes);
            allocator.discard(context, inUse);
            assertThat(allocator.residentBytes()).isZero();
        }
    }

    @Test
    void testTracksInPlaceVectorRetainedSizeChanges()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("variable-state");
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            MinUtf8StateVector state = allocator.allocate(
                    context,
                    MinUtf8StateVector.class,
                    2,
                    MinUtf8StateVector::new);
            long initialBytes = state.retainedBytes();

            long previousBytes = state.retainedBytes();
            state.setValue(0, new byte[17]);
            allocator.retainedBytesChanged(context, state, previousBytes);
            assertThat(allocator.residentBytes()).isEqualTo(initialBytes + 17);
            assertThat(memory.reservedBytes()).isEqualTo(initialBytes + 17);

            previousBytes = state.retainedBytes();
            state.setValue(0, new byte[5]);
            allocator.retainedBytesChanged(context, state, previousBytes);
            assertThat(allocator.residentBytes()).isEqualTo(initialBytes + 5);
            assertThat(memory.reservedBytes()).isEqualTo(initialBytes + 5);

            allocator.discard(context, state);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testTracksNonVectorRetainedStateByOwner()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("index-state");
        Object firstIndex = new Object();
        Object secondIndex = new Object();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            allocator.setRetainedBytes(context, firstIndex, 100);
            allocator.setRetainedBytes(context, secondIndex, 25);
            assertThat(allocator.residentBytes()).isEqualTo(125);
            assertThat(allocator.currentBytes(context)).isEqualTo(125);
            assertThat(memory.reservedBytes()).isEqualTo(125);

            allocator.setRetainedBytes(context, firstIndex, 60);
            allocator.setRetainedBytes(context, secondIndex, 0);
            assertThat(allocator.residentBytes()).isEqualTo(60);
            assertThat(allocator.currentBytes(context)).isEqualTo(60);
            assertThat(memory.reservedBytes()).isEqualTo(60);

            allocator.release(context);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(allocator.currentBytes(context)).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testRejectedRetainedStateReservationDoesNotCorruptTeardownAccounting()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator.Context context = new Allocator.Context("rejected-index-state");
        Object index = new Object();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            allocator.setRetainedBytes(context, index, 100);
            memory.failure = new IllegalStateException("memory limit exceeded");

            assertThatThrownBy(() -> allocator.setRetainedBytes(context, index, 250))
                    .isSameAs(memory.failure);

            assertThat(allocator.residentBytes()).isEqualTo(100);
            assertThat(allocator.currentBytes(context)).isEqualTo(100);
            assertThat(memory.reservedBytes()).isEqualTo(100);
            memory.failure = null;
            allocator.release(context);
            assertThat(allocator.residentBytes()).isZero();
            assertThat(memory.reservedBytes()).isZero();
        }
    }

    @Test
    void testClosingAllocatorReleasesResidentConstantsAndPools()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        Allocator allocator = new Allocator(EngineResources.createDefault(), memory);
        Allocator.Context context = new Allocator.Context("test");

        allocator.borrowAllFalseBoolean(context, 32);
        I64Vector vector = allocator.allocate(context, I64Vector.class, 8, I64Vector::new);
        allocator.release(context, vector);
        assertThat(memory.reservedBytes()).isPositive();

        allocator.close();
        assertThat(memory.reservedBytes()).isZero();
        assertThat(allocator.residentBytes()).isZero();
        allocator.close();
    }

    @Test
    void testBlockedMemoryContinuationIsExposed()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        memory.blocked = new CompletableFuture<>();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            allocator.allocate(new Allocator.Context("test"), I64Vector.class, 8, I64Vector::new);

            assertThat(allocator.memoryBlocked()).contains(memory.blocked);
            memory.blocked.complete(null);
            assertThat(allocator.memoryBlocked()).isEmpty();
        }
    }

    @Test
    void testCompatibleMaskPoolCanSpillEntireLocalBucket()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Object compatibilityGroup = new Object();
        Allocator.Context producer = new Allocator.Context("producer", new Object(), compatibilityGroup);
        Allocator.Context consumer = new Allocator.Context("consumer", new Object(), compatibilityGroup);
        Allocator.Context third = new Allocator.Context("third", new Object(), compatibilityGroup);
        allocator.register(producer);
        allocator.register(consumer);
        allocator.register(third);
        allocator.beginExecution();

        Mask produced = allocator.allocateRangeMask(producer, 1, 4);
        allocator.release(producer, produced);

        Mask borrowed = allocator.allocateRangeMask(consumer, 2, 4);
        assertThat(borrowed.size()).isEqualTo(6);
        allocator.release(consumer, borrowed);
    }

    @Test
    void testMaskCannotBeReleasedThroughAnotherContext()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context owner = new Allocator.Context("owner");
            Allocator.Context other = new Allocator.Context("other");
            Mask mask = allocator.allocateRangeMask(owner, 1, 10_000);
            long residentBytes = allocator.residentBytes();

            allocator.release(other, mask);
            assertThat(allocator.residentBytes()).isEqualTo(residentBytes);

            allocator.release(owner, mask);
            assertThat(allocator.allocateRangeMask(owner, 2, 10_000)).isSameAs(mask);
        }
    }

    @Test
    void testResidentMemoryTracksInPlaceMaskCapacityGrowth()
    {
        TestingMemoryReservation memory = new TestingMemoryReservation();
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            Allocator.Context context = new Allocator.Context("test");
            Mask mask = allocator.allocateAllMask(context, 10_000);
            assertThat(allocator.residentBytes()).isZero();

            mask.retainIf(position -> (position & 1) == 0);
            assertThat(allocator.residentBytes()).isEqualTo(40_000);
            assertThat(memory.reservedBytes()).isEqualTo(40_000);

            allocator.release(context, mask);
            assertThat(allocator.residentBytes()).isEqualTo(40_000);
        }
        assertThat(memory.reservedBytes()).isZero();
    }

    @Test
    void testUninitializedSparseMaskSupportsAllRowsSelected()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            Allocator.Context context = new Allocator.Context("test");
            Mask mask = allocator.allocateUninitializedSparseMask(context, 4, 4);
            int[] positions = mask.positionsArrayForOverwrite(4);
            for (int position = 0; position < 4; position++) {
                positions[position] = position;
            }

            assertThat(mask.all()).isTrue();
            assertThat(mask).containsExactly(0, 1, 2, 3);

            allocator.release(context, mask);
            Mask reused = allocator.allocateUninitializedSparseMask(context, 4, 4);
            assertThat(reused).isSameAs(mask);
            assertThat(reused.all()).isTrue();
        }
    }

    @Test
    void testCompatibleVectorPoolRequiresThreeRegisteredGroups()
    {
        Object compatibilityGroup = new Object();
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context first = new Allocator.Context("first", new Object(), compatibilityGroup);
        Allocator.Context second = new Allocator.Context("second", new Object(), compatibilityGroup);
        allocator.register(first);
        allocator.register(second);
        allocator.beginExecution();

        I64Vector isolated = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
        allocator.release(first, isolated);
        I64Vector secondVector = allocator.allocate(second, I64Vector.class, 8, I64Vector::new);
        assertThat(secondVector).isNotSameAs(isolated);
        allocator.release(second, secondVector);

        allocator = new Allocator(EngineResources.createDefault());
        first = new Allocator.Context("first", new Object(), compatibilityGroup);
        second = new Allocator.Context("second", new Object(), compatibilityGroup);
        Allocator.Context third = new Allocator.Context("third", new Object(), compatibilityGroup);
        allocator.register(first);
        allocator.register(second);
        allocator.register(third);
        allocator.beginExecution();
        I64Vector shared = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
        allocator.release(first, shared);
        assertThat(allocator.allocate(third, I64Vector.class, 8, I64Vector::new)).isSameAs(shared);
    }

    @Test
    void testCompatibilityAdmissionDoesNotAccumulateAcrossExecutions()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Object compatibilityGroup = new Object();
        Allocator.Context first = new Allocator.Context("first", new Object(), compatibilityGroup);
        Allocator.Context second = new Allocator.Context("second", new Object(), compatibilityGroup);
        Allocator.Context third = new Allocator.Context("third", new Object(), compatibilityGroup);
        allocator.register(first);
        allocator.register(second);
        allocator.register(third);
        allocator.beginExecution();

        I64Vector shared = allocator.allocate(first, I64Vector.class, 8, I64Vector::new);
        allocator.release(first, shared);
        assertThat(allocator.allocate(third, I64Vector.class, 8, I64Vector::new)).isSameAs(shared);

        Allocator.Context laterFirst = new Allocator.Context("laterFirst", new Object(), compatibilityGroup);
        Allocator.Context laterSecond = new Allocator.Context("laterSecond", new Object(), compatibilityGroup);
        allocator.register(laterFirst);
        allocator.register(laterSecond);
        allocator.beginExecution();

        I64Vector laterIsolated = allocator.allocate(laterFirst, I64Vector.class, 8, I64Vector::new);
        allocator.release(laterFirst, laterIsolated);
        assertThat(allocator.allocate(laterSecond, I64Vector.class, 8, I64Vector::new)).isNotSameAs(laterIsolated);
    }

    @Test
    void testSharedResourceClosesAfterLastLease()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Object key = new Object();
        AtomicInteger closes = new AtomicInteger();

        Allocator.SharedResource<TestResource> first = allocator.acquireSharedResource(key, () -> new TestResource(closes));
        Allocator.SharedResource<TestResource> second = allocator.acquireSharedResource(key, () -> {
            throw new AssertionError("resource constructed twice");
        });

        assertThat(second.value()).isSameAs(first.value());
        first.close();
        assertThat(closes).hasValue(0);
        first.close();
        assertThat(closes).hasValue(0);
        second.close();
        assertThat(closes).hasValue(1);
    }

    @Test
    void testSharedResourceLeaseCanCloseAfterAllocator()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        AtomicInteger closes = new AtomicInteger();
        Allocator.SharedResource<TestResource> lease =
                allocator.acquireSharedResource(new Object(), () -> new TestResource(closes));

        allocator.close();
        assertThat(closes).hasValue(1);

        lease.close();
        lease.close();
        assertThat(closes).hasValue(1);
    }

    @Test
    void testAllFalseBooleanConstantIsSharedAndUnowned()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("test");

        Vector first = allocator.borrowAllFalseBoolean(context, 5);
        Vector sameLength = allocator.borrowAllFalseBoolean(context, 5);
        Vector differentLength = allocator.borrowAllFalseBoolean(context, 3);

        assertThat(first).isInstanceOf(BooleanVector.class);
        assertThat(sameLength).isSameAs(first);
        assertThat(differentLength).isNotSameAs(first);
        assertThat(first.length()).isEqualTo(5);
        assertThat(differentLength.length()).isEqualTo(3);
        for (int position = 0; position < first.length(); position++) {
            assertThat(VectorAccess.booleanValues(first).value(position)).isFalse();
        }

        allocator.release(context, first);
        allocator.release(context, sameLength);
        assertThat(allocator.borrowAllFalseBoolean(context, 5)).isSameAs(first);
        assertThat(allocator.totalBytes(context)).isZero();
    }

    @Test
    void testAllocatorUsesOwnerSuppliedPolicy()
    {
        AllocatorPolicy policy = new AllocatorPolicy(
                true,
                false,
                false,
                new AllocatorPolicy.BooleanCopies(false, false, false),
                new AllocatorPolicy.MaskFiltering(false, false),
                4,
                false,
                true,
                true,
                true,
                64L << 20,
                64L << 20,
                AllocatorPolicy.AggregateStateVectorRetention.defaults(),
                1,
                0,
                3,
                0,
                true,
                false,
                true,
                true);
        NativeBufferAdvice nativeBufferAdvice = NativeBufferAdvice.disabled();
        try (AllocationResources resources = new AllocationResources(
                new PrimitiveArrayPool(1 << 20, 0),
                new PrimitiveArrayPool(1 << 20, 0),
                policy,
                nativeBufferAdvice);
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("test");

            assertThat(allocator.nativeBufferAdvice()).isSameAs(nativeBufferAdvice);

            Vector first = allocator.borrowAllFalseBoolean(context, 5);
            Vector second = allocator.borrowAllFalseBoolean(context, 5);

            assertThat(first).isNotSameAs(second);

            I64Vector values = new I64Vector(5);
            BooleanVector nulls = new BooleanVector(5);
            Streams existing = Streams.ofValuesAndNulls(values, nulls);
            assertThat(allocator.reuseValuesAndNulls(existing, values, nulls)).isNotSameAs(existing);

            I32Vector ids = allocator.allocate(context, I32Vector.class, 5, I32Vector::new);
            DictionaryVector dictionary = DictionaryVector.wrapOwnedIds(ids, 5, values);
            dictionary.prepareBufferTransfer(allocator, context);
            allocator.release(context, ids);
            assertThat(allocator.allocate(context, I32Vector.class, 5, I32Vector::new)).isSameAs(ids);

            boolean[] copiedFlags = new boolean[3];
            allocator.copyBooleanValues(
                    new BooleanVector(new boolean[] {true, false, true}),
                    Mask.all(3),
                    copiedFlags);
            assertThat(copiedFlags).containsExactly(true, false, true);

            Mask integerRange = allocator.allocateAllMask(context, 4);
            integerRange.retainConstantRange(new int[] {-5, 0, 5, 10}, 0, 10, null);
            assertThat(integerRange).containsExactly(2);

            Mask doubleComparison = allocator.allocateAllMask(context, 4);
            doubleComparison.retainConstantComparison(
                    new double[] {3.0, -1.0, 2.0, 1.5},
                    2.0,
                    Mask.ComparisonOperator.LESS_THAN);
            assertThat(doubleComparison).containsExactly(1, 3);

            ConcatenatedBooleanVector concatenated = new ConcatenatedBooleanVector(new Vector[] {
                    new BooleanVector(new boolean[] {false, true}),
                    new BooleanVector(new boolean[] {true, false})});
            BooleanVector copiedPositions = (BooleanVector) concatenated.copyPositionsInto(
                    allocator,
                    context,
                    null,
                    new int[] {3, 0, 2},
                    3,
                    0,
                    3);
            assertThat(copiedPositions.values()).containsExactly(false, false, true);
        }
    }

    @Test
    void testAllocatorUsesOwnerSuppliedAggregateStateRetention()
    {
        AllocatorPolicy policy = AllocatorPolicy.defaults()
                .withAggregateStateVectorRetention(new AllocatorPolicy.AggregateStateVectorRetention(1, 18, 18));
        try (AllocationResources resources = new AllocationResources(
                new PrimitiveArrayPool(1 << 20, 0),
                new PrimitiveArrayPool(1 << 20, 0),
                policy);
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("aggregate-state-retention");

            SumStateVector retained = allocator.allocate(context, SumStateVector.class, 2, SumStateVector::new);
            allocator.release(context, retained);
            assertThat(allocator.allocate(context, SumStateVector.class, 2, SumStateVector::new)).isSameAs(retained);

            SumStateVector discarded = allocator.allocate(context, SumStateVector.class, 3, SumStateVector::new);
            allocator.release(context, discarded);
            assertThat(allocator.allocate(context, SumStateVector.class, 3, SumStateVector::new)).isNotSameAs(discarded);
        }
    }

    private record TestResource(AtomicInteger closes)
            implements AutoCloseable
    {
        @Override
        public void close()
        {
            closes.incrementAndGet();
        }
    }

    private static final class TestingMemoryReservation
            implements org.weakref.nitro.core.execution.MemoryReservation
    {
        private long reserved;
        private CompletableFuture<Void> blocked = CompletableFuture.completedFuture(null);
        private RuntimeException failure;

        @Override
        public CompletionStage<Void> reserve(long bytes)
        {
            if (failure != null) {
                throw failure;
            }
            reserved += bytes;
            return blocked;
        }

        @Override
        public void release(long bytes)
        {
            reserved -= bytes;
        }

        @Override
        public long reservedBytes()
        {
            return reserved;
        }
    }
}
