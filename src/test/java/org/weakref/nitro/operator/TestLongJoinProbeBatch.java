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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLongJoinProbeBatch
{
    @Test
    void admissionHasHysteresisAndReadaptsAfterDistributionChanges()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        LongJoinProbeBatch probe = new LongJoinProbeBatch(new HashJoinProbeBatchPolicy(64, 4, 4, 75, 50, 2), pool);
        assertThat(probe.eligible(32, 4)).isFalse();
        assertThat(probe.statistics().decision()).isEqualTo(HashJoinProbeStatistics.Decision.TABLE_TOO_SMALL);
        assertThat(probe.eligible(64, 3)).isFalse();
        assertThat(probe.statistics().decision()).isEqualTo(HashJoinProbeStatistics.Decision.BATCH_TOO_SMALL);
        assertThat(probe.eligible(64, 4)).isTrue();
        long[] high = {0, 1, 2, -1};
        long[] middle = {0, 1, -1, -1};
        long[] low = {0, -1, -1, -1};
        probe.observe(high, 4, -1, false);
        assertThat(probe.batching()).isFalse();
        assertThat(probe.statistics().decision()).isEqualTo(HashJoinProbeStatistics.Decision.ACCUMULATING_HIGH_HIT_EVIDENCE);
        probe.observe(high, 4, -1, false);
        assertThat(probe.batching()).isTrue();
        assertThat(probe.statistics().decision()).isEqualTo(HashJoinProbeStatistics.Decision.HIGH_HIT_RATE);
        probe.observe(middle, 4, -1, true);
        assertThat(probe.batching()).isTrue();
        assertThat(probe.statistics().decision()).isEqualTo(HashJoinProbeStatistics.Decision.HYSTERESIS_BAND);
        probe.observe(low, 4, -1, true);
        assertThat(probe.batching()).isFalse();
        assertThat(probe.statistics().decision()).isEqualTo(HashJoinProbeStatistics.Decision.LOW_HIT_RATE);
        for (int repetition = 0; repetition < 10; repetition++) {
            probe.observe(high, 4, -1, false);
            probe.observe(low, 4, -1, false);
            assertThat(probe.batching()).isFalse();
        }
        probe.observe(high, 4, -1, false);
        probe.observe(high, 4, -1, false);
        assertThat(probe.batching()).isTrue();
        assertThat(probe.statistics().transitions()).isEqualTo(3);
        assertThat(probe.statistics().batchedCalls()).isEqualTo(2);
        assertThat(probe.statistics().sampledPositions()).isEqualTo(104);
        assertThat(probe.retainedBytes()).isZero();
        probe.release();
        assertThat(probe.batching()).isFalse();
    }

    @Test
    void maskedNullAwareLookupOwnsAndReusesProbeScratch()
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        LongJoinHashTable table = new LongJoinHashTable(pool, 64, true, true, -1);
        JoinRowStore rows = new JoinRowStore(pool, 16, false, false, false, false, -1);
        SparseLongRangeMembership membership = new SparseLongRangeMembership(HashJoinIndexPolicy.defaults(), pool);
        LongJoinProbeBatch first = new LongJoinProbeBatch(HashJoinProbeBatchPolicy.defaults(), pool);
        LongJoinProbeBatch second = new LongJoinProbeBatch(HashJoinProbeBatchPolicy.defaults(), pool);
        try {
            long[] keys = {Long.MIN_VALUE, 0, Long.MAX_VALUE};
            for (int index = 0; index < keys.length; index++) {
                table.initialize(table.findSlot(keys[index]), keys[index], index);
                rows.append(index, 100 + index);
            }
            membership.build(table, Long.MIN_VALUE, Long.MAX_VALUE, keys.length);
            int[] selected = {7, 4, 2, 9, 5};
            long[] references = new long[6];
            references[5] = 12345;
            for (int repetition = 0; repetition < 3; repetition++) {
                first.lookup(table, rows, membership, position -> switch (position) {
                    case 7 -> Long.MAX_VALUE;
                    case 2 -> Long.MIN_VALUE;
                    case 9 -> 42;
                    case 5 -> 0;
                    default -> throw new AssertionError("Read an unselected or null value: " + position);
                }, position -> position == 4, selected, selected.length, references, -1);
                assertThat(references).containsExactly(102, -1, 100, -1, 101, 12345);
                long retained = first.retainedBytes();
                assertThat(retained).isPositive();
                second.lookup(table, rows, membership, position -> 0, null, new int[] {5}, 1, new long[1], -1);
                second.release();
                assertThat(second.retainedBytes()).isZero();
                assertThat(first.retainedBytes()).isEqualTo(retained);
            }
            first.release();
            assertThat(first.retainedBytes()).isZero();
            assertThat(pool.retainedBytes()).isPositive();
            assertThat(table.head(table.findSlot(Long.MAX_VALUE))).isEqualTo(2);
        }
        finally {
            first.release();
            second.release();
            membership.release();
            rows.release();
            table.release();
        }
    }

    @Test
    void rejectsInvalidAdmissionPolicies()
    {
        assertThatThrownBy(() -> new HashJoinProbeBatchPolicy(0, 4, 4, 75, 50, 2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HashJoinProbeBatchPolicy(64, 4, 4, 50, 50, 2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HashJoinProbeBatchPolicy(64, 4, 4, 101, 50, 2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentProbeViewsDoNotMutateSharedBuild()
            throws Exception
    {
        PrimitiveArrayPool pool = new PrimitiveArrayPool(1024, 0);
        LongJoinHashTable table = new LongJoinHashTable(pool, 64, true, true, -1);
        JoinRowStore rows = new JoinRowStore(pool, 16, false, false, false, false, -1);
        SparseLongRangeMembership membership = new SparseLongRangeMembership(HashJoinIndexPolicy.defaults(), pool);
        try {
            for (int index = 0; index < 16; index++) {
                table.initialize(table.findSlot(index * 100L), index * 100L, index);
                rows.append(index, 1000 + index);
            }
            membership.build(table, 0, 1500, 16);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var first = executor.submit(() -> probeRepeatedly(table, rows, membership, pool, 2));
                var second = executor.submit(() -> probeRepeatedly(table, rows, membership, pool, 13));
                first.get();
                second.get();
            }
            assertThat(table.head(table.findSlot(200))).isEqualTo(2);
            assertThat(table.head(table.findSlot(1300))).isEqualTo(13);
        }
        finally {
            membership.release();
            rows.release();
            table.release();
        }
    }

    private static void probeRepeatedly(LongJoinHashTable table, JoinRowStore rows, SparseLongRangeMembership membership, PrimitiveArrayPool pool, int keyOrdinal)
    {
        LongJoinProbeBatch probe = new LongJoinProbeBatch(HashJoinProbeBatchPolicy.defaults(), pool);
        try {
            int[] positions = {2, 4, 9};
            long[] references = new long[3];
            for (int repetition = 0; repetition < 100; repetition++) {
                probe.lookup(table, rows, membership, position -> keyOrdinal * 100L,
                        null, positions, positions.length, references, -1);
                assertThat(references).containsOnly(1000L + keyOrdinal);
                // A sibling may return and reuse pool entries while this probe still owns its arrays.
                if (repetition % 3 == 0) {
                    probe.release();
                }
            }
        }
        finally {
            probe.release();
        }
    }
}
