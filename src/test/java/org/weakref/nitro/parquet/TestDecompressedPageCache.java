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
package org.weakref.nitro.parquet;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDecompressedPageCache
{
    @Test
    void testRequiresTwoConsumersAndRecyclesSlab()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        DecompressedPageCache.Source source = new DecompressedPageCache.Source(Path.of("table.parquet"), "column");

        try (DecompressedPageCache cache = new DecompressedPageCache(pool, 128, 1)) {
            cache.register(source);
            assertThat(cache.reserve(source, 11, 7, 16, 8)).isNull();

            cache.register(source);
            assertThat(cache.lookup(source, 11, 7, 16)).isNull();
            DecompressedPageCache.Reservation reservation = cache.reserve(source, 11, 7, 16, 8);
            assertThat(reservation).isNotNull();
            reservation.segment().set(ValueLayout.JAVA_BYTE, 0, (byte) 37);
            cache.commit(reservation);

            assertThat(cache.lookup(source, 11, 7, 16).get(ValueLayout.JAVA_BYTE, 0)).isEqualTo((byte) 37);
            assertThat(cache.lookup(source, 12, 7, 16)).isNull();
            assertThat(cache.cachedPageCount()).isOne();
            assertThat(cache.usedBytes()).isEqualTo(24);
        }

        assertThat(pool.retainedBytes()).isEqualTo(128);
        long reusedBefore = pool.reusedBytes();
        DecompressedPageCache recycled = new DecompressedPageCache(pool, 128, 1);
        recycled.register(source);
        recycled.register(source);
        assertThat(recycled.lookup(source, 12, 7, 16)).isNull();
        assertThat(recycled.reserve(source, 12, 7, 16, 8)).isNotNull();
        assertThat(pool.reusedBytes()).isEqualTo(reusedBefore + 128);
        recycled.close();
        assertThatThrownBy(() -> recycled.register(source)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testShortSourceDoesNotAcquireSlab()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        DecompressedPageCache.Source source = new DecompressedPageCache.Source(Path.of("table.parquet"), "column");

        try (DecompressedPageCache cache = new DecompressedPageCache(pool, 128, 2)) {
            cache.register(source);
            cache.register(source);
            assertThat(cache.lookup(source, 11, 7, 16)).isNull();
            assertThat(cache.reserve(source, 11, 7, 16, 8)).isNull();
        }

        assertThat(pool.retainedBytes()).isZero();
    }

    @Test
    void testSourceWorkingSetMustFitBudget()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        DecompressedPageCache.Source first = new DecompressedPageCache.Source(Path.of("table.parquet"), "first");

        try (DecompressedPageCache cache = new DecompressedPageCache(pool, 128, 1, 64)) {
            for (String column : new String[] {"first", "second", "third"}) {
                DecompressedPageCache.Source source = new DecompressedPageCache.Source(Path.of("table.parquet"), column);
                cache.register(source);
                cache.register(source);
            }
            assertThat(cache.lookup(first, 11, 7, 16)).isNull();
            assertThat(cache.reserve(first, 11, 7, 16, 8)).isNull();
        }

        assertThat(pool.retainedBytes()).isZero();
    }
}
