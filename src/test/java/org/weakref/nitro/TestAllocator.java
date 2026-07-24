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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAllocator
{
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

        @Override
        public CompletionStage<Void> reserve(long bytes)
        {
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
