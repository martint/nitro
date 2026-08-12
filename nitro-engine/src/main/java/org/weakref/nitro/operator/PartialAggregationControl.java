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

/**
 * Constructed host control for adaptive partial aggregation.
 *
 * <p>The host owns the admission state and memory policy. Nitro reports format-neutral row and
 * byte observations and only chooses between hash aggregation and planner-authored initial
 * aggregation rows.
 */
public interface PartialAggregationControl
{
    boolean aggregationEnabled();

    /**
     * Maximum number of rows Nitro should inspect before deciding how to consume the current input. Zero disables
     * the observation. Sampling may remain enabled while aggregation is bypassed so that a host can reconsider a
     * provisional decision when later input has different cardinality. The host owns both this bound and the
     * admission decision.
     */
    default int inputCardinalitySampleSize()
    {
        return 0;
    }

    /**
     * Decides whether the current empty aggregation should consume input after a bounded grouping-key sample.
     * This can be called while aggregation is provisionally bypassed. Hash collisions can only undercount distinct
     * keys; the decision changes physical partial aggregation only.
     */
    default boolean aggregationEnabled(PartialAggregationInputStatistics inputStatistics)
    {
        return aggregationEnabled();
    }

    void onAggregatedFlush(long inputBytes, long inputRows, long outputRows);

    void onPassthroughFlush(long inputBytes, long inputRows);
}
