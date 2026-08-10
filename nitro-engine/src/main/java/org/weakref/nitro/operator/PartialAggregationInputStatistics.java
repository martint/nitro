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

/** Bounded, format-neutral observation of physical grouping-key reuse in one input batch. */
public record PartialAggregationInputStatistics(int sampledRows, int distinctKeyHashes)
{
    public PartialAggregationInputStatistics
    {
        if (sampledRows < 0) {
            throw new IllegalArgumentException("sampledRows is negative");
        }
        if (distinctKeyHashes < 0 || distinctKeyHashes > sampledRows) {
            throw new IllegalArgumentException("distinctKeyHashes is outside the sampled row range");
        }
    }
}
