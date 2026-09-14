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

import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.VectorAccess;

import static java.util.Objects.requireNonNull;

/// Probe-local admission and pooled scratch. No mutable state is shared with another prepared-build consumer.
final class LongJoinProbeBatch
{
    private final HashJoinProbeBatchPolicy policy;
    private final PrimitiveArrayPool pool;
    private long[] keys;
    private long[] candidateKeys;
    private int[] slots;
    private int[] outputIndexes;
    private boolean batching;
    private int highHitSamples;
    private long batchedCalls;
    private long scalarCalls;
    private long transitions;
    private long sampledPositions;
    private long sampledMatches;
    private HashJoinProbeStatistics.Decision decision = HashJoinProbeStatistics.Decision.NOT_OBSERVED;

    LongJoinProbeBatch(HashJoinProbeBatchPolicy policy, PrimitiveArrayPool pool)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.pool = requireNonNull(pool, "pool is null");
    }

    boolean eligible(int tableCapacity, int positionCount)
    {
        if (tableCapacity < policy.minimumTableCapacity()) {
            decision = HashJoinProbeStatistics.Decision.TABLE_TOO_SMALL;
            return false;
        }
        if (positionCount < policy.minimumBatchSize()) {
            decision = HashJoinProbeStatistics.Decision.BATCH_TOO_SMALL;
            return false;
        }
        return true;
    }

    boolean batching()
    {
        return batching;
    }

    void observe(long[] references, int count, long noMatch, boolean usedBatching)
    {
        if (usedBatching) {
            batchedCalls++;
        }
        else {
            scalarCalls++;
        }
        int samples = Math.min(count, policy.sampleSize());
        int matches = 0;
        for (int sample = 0; sample < samples; sample++) {
            int index = (int) ((long) sample * count / samples);
            matches += references[index] == noMatch ? 0 : 1;
        }
        sampledPositions += samples;
        sampledMatches += matches;
        boolean previous = batching;
        if (samples == 0 || (long) matches * 100 < (long) samples * policy.exitHitPercent()) {
            batching = false;
            highHitSamples = 0;
            decision = HashJoinProbeStatistics.Decision.LOW_HIT_RATE;
        }
        else if ((long) matches * 100 >= (long) samples * policy.enterHitPercent()) {
            if (highHitSamples < policy.consecutiveHighHitSamples()) {
                highHitSamples++;
            }
            if (highHitSamples == policy.consecutiveHighHitSamples()) {
                batching = true;
            }
            decision = batching ? HashJoinProbeStatistics.Decision.HIGH_HIT_RATE : HashJoinProbeStatistics.Decision.ACCUMULATING_HIGH_HIT_EVIDENCE;
        }
        else {
            highHitSamples = 0;
            decision = HashJoinProbeStatistics.Decision.HYSTERESIS_BAND;
        }
        if (batching != previous) {
            transitions++;
        }
    }

    void lookup(
            LongJoinHashTable table,
            JoinRowStore rows,
            SparseLongRangeMembership membership,
            VectorAccess.LongValues values,
            VectorAccess.BooleanValues nulls,
            int[] positions,
            int count,
            long[] references,
            long noMatch)
    {
        if (count == 0) {
            return;
        }
        ensureCapacity(count);
        int admitted = 0;
        for (int index = 0; index < count; index++) {
            int position = positions[index];
            if (nulls != null && nulls.value(position)) {
                references[index] = noMatch;
                continue;
            }
            long key = values.value(position);
            if (!membership.contains(key)) {
                references[index] = noMatch;
                continue;
            }
            keys[admitted] = key;
            outputIndexes[admitted] = index;
            admitted++;
        }
        table.findSlots(keys, admitted, slots, candidateKeys);
        for (int index = 0; index < admitted; index++) {
            int slot = slots[index];
            references[outputIndexes[index]] = slot < 0 ? noMatch : rows.referenceAt(table.head(slot));
        }
    }

    private void ensureCapacity(int count)
    {
        if (keys != null && keys.length >= count) {
            return;
        }
        releaseArrays();
        keys = pool.borrowLongs(count);
        candidateKeys = pool.borrowLongs(count);
        slots = pool.borrowInts(count);
        outputIndexes = pool.borrowInts(count);
    }

    long retainedBytes()
    {
        if (keys == null) {
            return 0;
        }
        return ((long) keys.length + candidateKeys.length) * Long.BYTES +
                ((long) slots.length + outputIndexes.length) * Integer.BYTES;
    }

    HashJoinProbeStatistics statistics()
    {
        return new HashJoinProbeStatistics(scalarCalls, batchedCalls, transitions, sampledPositions, sampledMatches, batching, decision);
    }

    void release()
    {
        releaseArrays();
        batching = false;
        highHitSamples = 0;
        decision = HashJoinProbeStatistics.Decision.NOT_OBSERVED;
    }

    private void releaseArrays()
    {
        pool.release(keys);
        pool.release(candidateKeys);
        pool.release(slots);
        pool.release(outputIndexes);
        keys = null;
        candidateKeys = null;
        slots = null;
        outputIndexes = null;
    }
}
