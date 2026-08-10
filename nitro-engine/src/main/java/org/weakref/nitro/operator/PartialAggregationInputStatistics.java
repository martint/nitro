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

import java.util.Arrays;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/** Bounded, format-neutral observation of physical grouping-key reuse in one input batch. */
public final class PartialAggregationInputStatistics
{
    private final int sampledRows;
    private final long[] distinctKeyHashes;

    public PartialAggregationInputStatistics(int sampledRows, long[] distinctKeyHashes)
    {
        if (sampledRows < 0) {
            throw new IllegalArgumentException("sampledRows is negative");
        }
        requireNonNull(distinctKeyHashes, "distinctKeyHashes is null");
        if (distinctKeyHashes.length > sampledRows) {
            throw new IllegalArgumentException("distinctKeyHashes is larger than the sampled row count");
        }
        this.sampledRows = sampledRows;
        this.distinctKeyHashes = distinctKeyHashes.clone();
    }

    public int sampledRows()
    {
        return sampledRows;
    }

    public int distinctKeyHashes()
    {
        return distinctKeyHashes.length;
    }

    /** Supplies the immutable primitive hash sample without exposing its backing array. */
    public void forEachDistinctKeyHash(LongConsumer consumer)
    {
        requireNonNull(consumer, "consumer is null");
        for (long hash : distinctKeyHashes) {
            consumer.accept(hash);
        }
    }

    @Override
    public boolean equals(Object object)
    {
        return object == this ||
                (object instanceof PartialAggregationInputStatistics other &&
                        sampledRows == other.sampledRows &&
                        Arrays.equals(distinctKeyHashes, other.distinctKeyHashes));
    }

    @Override
    public int hashCode()
    {
        return 31 * sampledRows + Arrays.hashCode(distinctKeyHashes);
    }

    @Override
    public String toString()
    {
        return "PartialAggregationInputStatistics[sampledRows=" + sampledRows +
                ", distinctKeyHashes=" + distinctKeyHashes.length + ']';
    }
}
