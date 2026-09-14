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

/// Immutable admission for overlapping independent lookups into large, completed hash tables.
public record HashJoinProbeBatchPolicy(
        int minimumTableCapacity,
        int minimumBatchSize,
        int sampleSize,
        int enterHitPercent,
        int exitHitPercent,
        int consecutiveHighHitSamples)
{
    public HashJoinProbeBatchPolicy
    {
        if (minimumTableCapacity < 1 || minimumBatchSize < 1 || sampleSize < 1 ||
                exitHitPercent < 0 || enterHitPercent > 100 || enterHitPercent <= exitHitPercent ||
                consecutiveHighHitSamples < 1) {
            throw new IllegalArgumentException("Invalid probe batching admission");
        }
    }

    public static HashJoinProbeBatchPolicy defaults()
    {
        return new HashJoinProbeBatchPolicy(1 << 20, 4096, 32, 75, 50, 2);
    }
}
