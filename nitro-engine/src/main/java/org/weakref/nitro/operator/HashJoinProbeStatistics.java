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

/// Snapshot of eligible probe calls and bounded hit-rate observations; never elapsed time or row totals.
/// `batching` describes the next eligible batch. `decision` describes the latest admission check, including a
/// small-batch bypass that does not reset previously learned admission. Call counts describe actual execution.
public record HashJoinProbeStatistics(
        long scalarCalls,
        long batchedCalls,
        long transitions,
        long sampledPositions,
        long sampledMatches,
        boolean batching,
        Decision decision)
{
    public enum Decision
    {
        NOT_OBSERVED,
        TABLE_TOO_SMALL,
        BATCH_TOO_SMALL,
        LOW_HIT_RATE,
        ACCUMULATING_HIGH_HIT_EVIDENCE,
        HIGH_HIT_RATE,
        HYSTERESIS_BAND,
    }
}
