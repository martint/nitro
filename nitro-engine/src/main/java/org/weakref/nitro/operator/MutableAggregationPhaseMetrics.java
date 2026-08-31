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

final class MutableAggregationPhaseMetrics
{
    private long fusedNanos;
    private long groupingNanos;
    private long statePreparationNanos;
    private long accumulationNanos;
    private long initialKeyNanos;
    private long initialAggregationNanos;
    private long initialFlatKeyNanos;
    private long initialEncodedKeyNanos;
    private long encodedKeyDomainBatches;
    private long authoritativeHashDomainBatches;
    private long computedHashDomainBatches;
    private long authoritativeHashRowBatches;
    private long computedHashRowBatches;

    public void recordFused(long nanos)
    {
        fusedNanos += nanos;
    }

    public void recordGrouping(long nanos)
    {
        groupingNanos += nanos;
    }

    public void recordStatePreparation(long nanos)
    {
        statePreparationNanos += nanos;
    }

    public void recordAccumulation(long nanos)
    {
        accumulationNanos += nanos;
    }

    public void recordInitialKey(long nanos)
    {
        initialKeyNanos += nanos;
    }

    public void recordInitialAggregation(long nanos)
    {
        initialAggregationNanos += nanos;
    }

    public void recordInitialFlatKey(long nanos)
    {
        initialFlatKeyNanos += nanos;
    }

    public void recordInitialEncodedKey(long nanos)
    {
        initialEncodedKeyNanos += nanos;
    }

    public void recordEncodedKeyDomain(boolean authoritativeHash, boolean computedHash)
    {
        encodedKeyDomainBatches++;
        authoritativeHashDomainBatches += authoritativeHash ? 1 : 0;
        computedHashDomainBatches += computedHash ? 1 : 0;
    }

    public void recordAuthoritativeHashRowBatch()
    {
        authoritativeHashRowBatches++;
    }

    public void recordComputedHashRowBatch()
    {
        computedHashRowBatches++;
    }

    public AggregationPhaseMetrics snapshot()
    {
        return new AggregationPhaseMetrics(
                fusedNanos,
                groupingNanos,
                statePreparationNanos,
                accumulationNanos,
                initialKeyNanos,
                initialAggregationNanos,
                initialFlatKeyNanos,
                initialEncodedKeyNanos,
                encodedKeyDomainBatches,
                authoritativeHashDomainBatches,
                computedHashDomainBatches,
                authoritativeHashRowBatches,
                computedHashRowBatches);
    }
}
