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

import jdk.incubator.vector.LongVector;
import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestPooledLongHashSet
{
    @Test
    void testScalarBatchesPreserveFirstPositionsAcrossGrowth()
    {
        long[] values = new long[4099];
        Random random = new Random(8191);
        for (int position = 3; position < values.length; position++) {
            values[position] = switch (position % 7) {
                case 0 -> 0;
                case 1 -> Long.MIN_VALUE;
                case 2 -> Long.MAX_VALUE;
                case 3 -> values[position - 3];
                default -> random.nextLong();
            };
        }
        for (int batchSize : new int[] {1, 7, 64, 257}) {
            PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
            PooledLongHashSet set = new PooledLongHashSet(
                    1,
                    pool,
                    new PooledLongHashSetPolicy(0.75f, false, false, 5, 50, 128, 256, batchSize, false));
            Set<Long> expected = new HashSet<>();
            int[] expectedPositions = new int[values.length];
            int expectedCount = 0;
            for (int position = 3; position < values.length; position++) {
                if (expected.add(values[position])) {
                    expectedPositions[expectedCount++] = position;
                }
            }
            int[] positions = new int[values.length];
            int count = set.addScalarBatch(position -> {
                assertThat(position).isBetween(3, values.length - 1);
                return values[position];
            }, 3, values.length, positions);
            assertThat(count).isEqualTo(expectedCount);
            assertThat(positions).containsExactly(expectedPositions);
            assertThat(set.size()).isEqualTo(expected.size());
            expected.forEach(value -> assertThat(set.contains(value)).isTrue());
            long allocatedBytes = pool.allocatedBytes();
            long retainedBytes = set.retainedBytes();
            assertThat(set.addScalarBatch(position -> values[position], 3, values.length, positions)).isZero();
            assertThat(set.addScalarBatch(position -> { throw new AssertionError("empty input was read"); }, 3, 3, positions)).isZero();
            assertThat(pool.allocatedBytes()).isEqualTo(allocatedBytes);
            assertThat(set.retainedBytes()).isEqualTo(retainedBytes);
            set.releaseBuffers();
            assertThat(set.retainedBytes()).isZero();
        }
    }

    @Test
    void testScalarBatchContract()
    {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new PooledLongHashSetPolicy(0.75f, false, false, 5, 50, 128, 256, 0, false));
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
        PooledLongHashSet set = new PooledLongHashSet(
                16,
                pool,
                new PooledLongHashSetPolicy(0.75f, true, false, 5, 50, 128, 256, 64, false));
        assertThatIllegalStateException().isThrownBy(() -> set.addScalarBatch(_ -> 1, 0, 1, new int[1]));
        set.releaseBuffers();
    }

    @Test
    void testDefaultsUseVectorKeysForFourOrMoreHardwareLanes()
    {
        PooledLongHashSetPolicy policy = PooledLongHashSetPolicy.defaults();

        assertThat(policy.keyGroupBits()).isEqualTo(Math.clamp(LongVector.SPECIES_PREFERRED.vectorBitSize(), 64, 512));
        assertThat(policy.vectorKeys()).isEqualTo(LongVector.SPECIES_PREFERRED.length() >= 4);
        assertThat(policy.vectorTags()).isFalse();
    }

    @Test
    void testInsertResizeAndReuse()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(16L << 20, 0);
        Set<Long> expected = new HashSet<>();
        PooledLongHashSet first = new PooledLongHashSet(1, pool, PooledLongHashSetPolicy.defaults());
        for (long value = -10_000; value <= 10_000; value++) {
            assertThat(first.add(value)).isEqualTo(expected.add(value));
            assertThat(first.add(value)).isFalse();
        }
        assertThat(first.size()).isEqualTo(expected.size());
        expected.forEach(value -> assertThat(first.contains(value)).isTrue());
        assertThat(first.contains(Long.MIN_VALUE)).isFalse();
        first.releaseBuffers();

        long reusedBefore = pool.reusedBytes();
        PooledLongHashSet second = new PooledLongHashSet(20_001, pool, PooledLongHashSetPolicy.defaults());
        assertThat(pool.reusedBytes()).isGreaterThan(reusedBefore);
        assertThat(second.add(0)).isTrue();
        assertThat(second.add(0)).isFalse();
        second.releaseBuffers();
    }

    @Test
    void testVectorTagPolicyIsInstanceScoped()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
        PooledLongHashSet scalar = new PooledLongHashSet(
                16,
                pool,
                new PooledLongHashSetPolicy(0.5f, false, false, 100, 100, 64, 256, 64, false));
        PooledLongHashSet vector = new PooledLongHashSet(
                16,
                pool,
                new PooledLongHashSetPolicy(0.9f, true, true, 0, 100, 256, 256, 64, false));

        assertThat(scalar.vectorTagsEnabled()).isFalse();
        assertThat(vector.vectorTagsEnabled()).isTrue();

        scalar.releaseBuffers();
        vector.releaseBuffers();
    }

    @Test
    void testObservedNoveltySelectsScalarVectorKeysOrTags()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
        PooledLongHashSetPolicy policy = new PooledLongHashSetPolicy(0.75f, true, true, 5, 50, 128, 256, 64, false);

        PooledLongHashSet lowNovelty = new PooledLongHashSet(16, pool, policy, false);
        for (int key = 1; key <= 2; key++) {
            lowNovelty.add(key);
        }
        lowNovelty.enableVectorTags(100);
        assertThat(lowNovelty.vectorTagsEnabled()).isFalse();

        PooledLongHashSet moderateNovelty = new PooledLongHashSet(16, pool, policy, false);
        for (int key = 1; key <= 10; key++) {
            moderateNovelty.add(key);
        }
        // Admission uses the owning index's input-attempt count. This preserves observed novelty when an adaptive
        // bitmap replays only its unique keys while transferring into the hash representation.
        moderateNovelty.enableVectorTags(100);
        assertThat(moderateNovelty.vectorTagsEnabled()).isTrue();
        assertThat(moderateNovelty.vectorKeysEnabled()).isTrue();

        PooledLongHashSet highNovelty = new PooledLongHashSet(128, pool, policy, false);
        for (int call = 0; call < 100; call++) {
            highNovelty.add(call + 1);
        }
        highNovelty.enableVectorTags(100);
        assertThat(highNovelty.vectorTagsEnabled()).isTrue();
        assertThat(highNovelty.vectorKeysEnabled()).isFalse();
        for (int call = 0; call < 100; call++) {
            assertThat(highNovelty.contains(call + 1)).isTrue();
        }

        lowNovelty.releaseBuffers();
        moderateNovelty.releaseBuffers();
        highNovelty.releaseBuffers();
    }

    @Test
    void testVectorKeyWidthsPreserveSetSemantics()
    {
        for (int keyGroupBits : new int[] {64, 128, 256, 512}) {
            PrimitiveArrayPool pool = new PrimitiveArrayPool(1 << 20, 0);
            PooledLongHashSet set = new PooledLongHashSet(
                    16,
                    pool,
                    new PooledLongHashSetPolicy(0.75f, true, true, 0, 100, 128, keyGroupBits, 64, false));

            for (long value = -1_000; value <= 1_000; value++) {
                assertThat(set.add(value)).isTrue();
                assertThat(set.add(value)).isFalse();
            }
            for (long value = -1_000; value <= 1_000; value++) {
                assertThat(set.contains(value)).isTrue();
            }
            set.releaseBuffers();
        }
    }
}
