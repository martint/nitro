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

import static java.util.Objects.requireNonNull;

/// Engine-selected batching, scratch, and row-reference execution policy for hash joins.
///
/// The property-backed factory is a standalone composition adapter. Hash joins receive one immutable policy from
/// their resource owner and never consult process-global configuration for these decisions.
public record HashJoinExecutionPolicy(
        int maxBatchRows,
        boolean poolScratch,
        int duplicateListInitialCapacity,
        boolean lazyDuplicateSlotState,
        boolean implicitSequentialBuildRowReferences,
        HashJoinProbeBatchPolicy probeBatchPolicy)
{
    public HashJoinExecutionPolicy
    {
        probeBatchPolicy = requireNonNull(probeBatchPolicy, "probeBatchPolicy is null");
    }

    public HashJoinExecutionPolicy(
            int maxBatchRows,
            boolean poolScratch,
            int duplicateListInitialCapacity,
            boolean lazyDuplicateSlotState,
            boolean implicitSequentialBuildRowReferences)
    {
        this(maxBatchRows, poolScratch, duplicateListInitialCapacity, lazyDuplicateSlotState,
                implicitSequentialBuildRowReferences, HashJoinProbeBatchPolicy.defaults());
    }

    public static HashJoinExecutionPolicy defaults()
    {
        return new HashJoinExecutionPolicy(10_000, true, 2, false, true);
    }

    public static HashJoinExecutionPolicy fromSystemProperties()
    {
        HashJoinExecutionPolicy defaults = defaults();
        return new HashJoinExecutionPolicy(
                Integer.getInteger("nitro.hash.join.maxBatchRows", defaults.maxBatchRows()),
                booleanProperty("nitro.hash.join.poolScratch", defaults.poolScratch()),
                Integer.getInteger("nitro.hash.join.duplicateListInitialCapacity", defaults.duplicateListInitialCapacity()),
                booleanProperty("nitro.join.lazyDuplicateSlotState", defaults.lazyDuplicateSlotState()),
                booleanProperty("nitro.join.implicitSequentialBuildRowReferences", defaults.implicitSequentialBuildRowReferences()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
