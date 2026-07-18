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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TestAllocator
{
    @Test
    void testCompatibleMaskPoolCanSpillEntireLocalBucket()
    {
        Allocator allocator = new Allocator();
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
        Allocator allocator = new Allocator();
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

        allocator = new Allocator();
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
        Allocator allocator = new Allocator();
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
        Allocator allocator = new Allocator();
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
        Allocator allocator = new Allocator();
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
}
