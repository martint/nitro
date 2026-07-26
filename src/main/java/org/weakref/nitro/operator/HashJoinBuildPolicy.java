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

/// Engine-selected ingestion and initial-admission policy for hash-join builds.
///
/// The property-backed factory is a standalone composition adapter. Hash joins receive one immutable policy from
/// their resource owner and never consult process-global configuration for these decisions.
public record HashJoinBuildPolicy(
        boolean exactStreamingCardinality,
        int maxBuildBatchRows,
        boolean pruneZeroBitwiseOverlapRows,
        boolean batchSingleLongBuild,
        boolean batchLongPairBuild,
        boolean streamUnusedPayload,
        boolean capDuplicatePairHash,
        long maxInitialPairHashBytes)
{
    public static HashJoinBuildPolicy defaults()
    {
        return new HashJoinBuildPolicy(
                true,
                1 << 16,
                true,
                true,
                true,
                true,
                true,
                512L << 20);
    }

    public static HashJoinBuildPolicy fromSystemProperties()
    {
        HashJoinBuildPolicy defaults = defaults();
        return new HashJoinBuildPolicy(
                booleanProperty("nitro.join.exactStreamingBuildCardinality", defaults.exactStreamingCardinality()),
                Math.min(1 << 16, Integer.getInteger("nitro.hash.join.maxBuildBatchRows", defaults.maxBuildBatchRows())),
                booleanProperty("nitro.hash.join.pruneZeroBitwiseOverlapBuildRows", defaults.pruneZeroBitwiseOverlapRows()),
                booleanProperty("nitro.hash.join.batchSingleLongBuild", defaults.batchSingleLongBuild()),
                booleanProperty("nitro.hash.join.batchLongPairBuild", defaults.batchLongPairBuild()),
                booleanProperty("nitro.hash.join.streamUnusedBuildPayload", defaults.streamUnusedPayload()),
                booleanProperty("nitro.join.capDuplicatePairHash", defaults.capDuplicatePairHash()),
                Long.getLong("nitro.join.maxInitialPairHashBytes", defaults.maxInitialPairHashBytes()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
