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

/// Shared physical buffering and coalescing policy for join build inputs.
///
/// Coalescing bounds the one-time copy that creates a single addressable batch for zero-copy dictionary output.
/// Post-load and direct-build limits remain separate: an exact cardinality can write directly into the final layout,
/// while ordinary large or selective builds remain paged. Automatic direct admission additionally uses a row floor
/// and the first batch's physical variable-width shape; explicit plan hints retain the bounded escape hatch.
public record BufferedJoinInputPolicy(
        int maxCoalescedRows,
        int maxPostLoadCoalescedRows,
        boolean coalesceRangeSelection,
        boolean bufferedDensePositionsCache,
        boolean closeCopiedBatches,
        boolean sharedCompactionPositions,
        boolean recycledCompactionMappings,
        boolean releaseCopiedCoalesceSources,
        boolean directExactCoalesce,
        boolean automaticDirectExactCoalesce,
        int minAutomaticDirectExactRows,
        boolean directBoundedCoalesce)
{
    public static BufferedJoinInputPolicy defaults()
    {
        return new BufferedJoinInputPolicy(
                4_000_000,
                1 << 20,
                true,
                true,
                true,
                true,
                true,
                true,
                false,
                true,
                1 << 18,
                true);
    }

    public static BufferedJoinInputPolicy fromSystemProperties()
    {
        BufferedJoinInputPolicy defaults = defaults();
        return new BufferedJoinInputPolicy(
                Integer.getInteger("nitro.hash.join.maxCoalescedInnerRows", defaults.maxCoalescedRows()),
                Integer.getInteger(
                        "nitro.hash.join.maxPostLoadCoalescedInnerRows",
                        defaults.maxPostLoadCoalescedRows()),
                booleanProperty("nitro.hash.join.coalesceRangeSelection", defaults.coalesceRangeSelection()),
                booleanProperty(
                        "nitro.hash.join.bufferedDensePositionsCache",
                        defaults.bufferedDensePositionsCache()),
                booleanProperty("nitro.hash.join.closeCopiedBuildBatches", defaults.closeCopiedBatches()),
                booleanProperty(
                        "nitro.hash.join.sharedCompactionPositions",
                        defaults.sharedCompactionPositions()),
                booleanProperty(
                        "nitro.hash.join.recycledCompactionMappings",
                        defaults.recycledCompactionMappings()),
                booleanProperty(
                        "nitro.hash.join.releaseCopiedCoalesceSources",
                        defaults.releaseCopiedCoalesceSources()),
                booleanProperty("nitro.hash.join.directExactCoalesce", defaults.directExactCoalesce()),
                booleanProperty(
                        "nitro.hash.join.automaticDirectExactCoalesce",
                        defaults.automaticDirectExactCoalesce()),
                Integer.getInteger(
                        "nitro.hash.join.minAutomaticDirectExactRows",
                        defaults.minAutomaticDirectExactRows()),
                booleanProperty("nitro.hash.join.directBoundedCoalesce", defaults.directBoundedCoalesce()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
