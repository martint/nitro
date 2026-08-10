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

import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

final class GroupingCardinalitySampler
{
    private GroupingCardinalitySampler() {}

    public static PartialAggregationInputStatistics sample(
            Batch batch,
            List<Integer> groupByColumns,
            int maximumSampleSize,
            PrimitiveArrayPool arrays)
    {
        requireNonNull(batch, "batch is null");
        requireNonNull(groupByColumns, "groupByColumns is null");
        requireNonNull(arrays, "arrays is null");
        if (maximumSampleSize <= 0) {
            throw new IllegalArgumentException("maximumSampleSize must be positive");
        }

        Mask mask = batch.borrowMask();
        int sampledRows = Math.min(mask.count(), maximumSampleSize);
        if (sampledRows == 0) {
            return new PartialAggregationInputStatistics(0, 0);
        }
        if (groupByColumns.isEmpty()) {
            return new PartialAggregationInputStatistics(sampledRows, 1);
        }

        Vector[] values = new Vector[groupByColumns.size()];
        Vector[] nulls = new Vector[groupByColumns.size()];
        try {
            for (int key = 0; key < groupByColumns.size(); key++) {
                Output output = batch.output(groupByColumns.get(key));
                values[key] = output.borrow(Stream.VALUES);
                nulls[key] = output.borrowOrNull(Stream.NULLS);
            }
        }
        catch (IllegalArgumentException ignored) {
            // Some structural representations require their type-authored grouping kernel. Do not substitute a
            // different equality model merely for admission; zero sampled rows means no observation is available.
            return new PartialAggregationInputStatistics(0, 0);
        }

        long[] hashes = arrays.borrowLongs(sampledRows);
        try {
            for (int sample = 0; sample < sampledRows; sample++) {
                int ordinal = (int) ((long) sample * mask.count() / sampledRows);
                int position = mask.position(ordinal);
                long hash = 0x9E3779B97F4A7C15L;
                for (int key = 0; key < values.length; key++) {
                    hash = Long.rotateLeft(hash, 27) * 0xC2B2AE3D27D4EB4FL +
                            OperatorKeySemantics.hash(values[key], nulls[key], position);
                }
                hashes[sample] = hash;
            }
            Arrays.sort(hashes, 0, sampledRows);
            int distinct = 1;
            for (int sample = 1; sample < sampledRows; sample++) {
                if (hashes[sample] != hashes[sample - 1]) {
                    distinct++;
                }
            }
            return new PartialAggregationInputStatistics(sampledRows, distinct);
        }
        catch (IllegalArgumentException ignored) {
            return new PartialAggregationInputStatistics(0, 0);
        }
        finally {
            arrays.release(hashes);
        }
    }
}
