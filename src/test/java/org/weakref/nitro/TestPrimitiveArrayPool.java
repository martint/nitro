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
import org.weakref.nitro.data.PrimitiveArrayPool;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPrimitiveArrayPool
{
    @Test
    void testTryBorrowIntsDoesNotAllocateOnMiss()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);

        assertThat(pool.tryBorrowInts(32)).isNull();
        int[] retained = new int[32];
        pool.release(retained);
        assertThat(pool.tryBorrowInts(32)).isSameAs(retained);
        assertThat(pool.tryBorrowInts(32)).isNull();
    }

    @Test
    public void testExactCapacityAndTypeReuse()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        int[] ints = new int[64];
        long[] longs = new long[64];
        pool.release(ints);
        pool.release(longs);

        assertThat(pool.borrowInts(64)).isSameAs(ints);
        assertThat(pool.borrowLongs(64)).isSameAs(longs);
        assertThat(pool.borrowInts(63)).isNotSameAs(ints);
        assertThat(pool.retainedBytes()).isZero();
    }

    @Test
    public void testBooleanArrayReuse()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        boolean[] booleans = new boolean[64];
        pool.release(booleans);

        assertThat(pool.borrowBooleans(64)).isSameAs(booleans);
    }

    @Test
    public void testDuplicateReleaseCannotCreateConcurrentLeases()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        byte[] bytes = new byte[64];
        pool.release(bytes);
        pool.release(bytes);

        assertThat(pool.retainedBytes()).isEqualTo(64);
        assertThat(pool.borrowBytes(64)).isSameAs(bytes);
        assertThat(pool.borrowBytes(64)).isNotSameAs(bytes);
        assertThat(pool.retainedBytes()).isZero();
    }

    @Test
    public void testSemanticFamiliesDoNotExchangeBuffers()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        Object fixedRecords = new Object();
        Object variableWidth = new Object();
        byte[] bytes = new byte[64];
        assertThat(pool.retain(fixedRecords, bytes.length, bytes.length, bytes)).isTrue();

        assertThat(pool.borrow(variableWidth, bytes.length, byte[].class)).isNull();
        assertThat(pool.borrow(fixedRecords, bytes.length, byte[].class)).isSameAs(bytes);
    }

    @Test
    public void testCeilingCapacityReuseChoosesSmallestSufficientBuffer()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        Object family = new Object();
        byte[] small = new byte[64];
        byte[] large = new byte[128];
        assertThat(pool.retain(family, small.length, small.length, small)).isTrue();
        assertThat(pool.retain(family, large.length, large.length, large)).isTrue();

        assertThat(pool.borrowAtLeast(family, 65, byte[].class)).isSameAs(large);
        assertThat(pool.borrowAtLeast(family, 32, byte[].class)).isSameAs(small);
        assertThat(pool.borrowAtLeast(family, 129, byte[].class)).isNull();
    }

    @Test
    public void testHardByteCeilingEvictsOldest()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(512, 0);
        int[] oldest = new int[64];
        int[] newest = new int[96];
        pool.release(oldest);
        pool.release(newest);

        assertThat(pool.retainedBytes()).isEqualTo(384);
        assertThat(pool.borrowInts(96)).isSameAs(newest);
        assertThat(pool.borrowInts(64)).isNotSameAs(oldest);
    }

    @Test
    public void testMinimumAndOversizedArraysAreNotRetained()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(512, 128);
        assertThat(pool.minRetainedBytes()).isEqualTo(128);
        pool.release(new int[16]);
        pool.release(new long[128]);

        assertThat(pool.retainedBytes()).isZero();
    }
}
