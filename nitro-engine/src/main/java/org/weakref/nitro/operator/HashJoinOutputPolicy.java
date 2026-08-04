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

/// Engine-selected output representation and materialization policy for hash joins.
///
/// The property-backed factory is a standalone composition adapter. Hash joins receive one immutable policy from
/// their resource owner and never consult process-global configuration for these decisions.
public record HashJoinOutputPolicy(
        boolean directOuterJoinNullStream,
        boolean rleAllUnmatchedOuterJoinOutput,
        boolean debugRleAllUnmatchedOuterJoinOutput,
        boolean directCompactedRangeOutput,
        int buildDictionarySparseRatio,
        int buildDictionaryMinDistinctValues,
        int buildDictionaryMaxDistinctPercent,
        boolean wrapNonRetainedFixedWidthBuildValues,
        boolean cacheInnerDictionaryIds,
        boolean wrapEncodedOuterDictionaries,
        int composeEncodedOuterDictionaryDepth,
        boolean adaptiveTinyDictionaryComposition,
        int adaptiveComposeMaxRows,
        int adaptiveComposeDepth,
        boolean cacheComposedOuterDictionaryIds,
        boolean directDenseSingleMatchRangeOutput,
        boolean forwardIdentityReborrowOuter,
        boolean debugIdentityReborrowOuter,
        boolean rleRunIndexHint,
        boolean poolBuildDictionaryIds,
        boolean gatherMultiRunRetainedFixedWidthValues)
{
    public static HashJoinOutputPolicy defaults()
    {
        return new HashJoinOutputPolicy(
                true,
                true,
                false,
                true,
                8,
                16,
                50,
                true,
                true,
                true,
                Integer.MAX_VALUE,
                true,
                1024,
                4,
                true,
                true,
                true,
                false,
                true,
                true,
                true);
    }

    public static HashJoinOutputPolicy fromSystemProperties()
    {
        HashJoinOutputPolicy defaults = defaults();
        return new HashJoinOutputPolicy(
                booleanProperty("nitro.join.directOuterJoinNullStream", defaults.directOuterJoinNullStream()),
                booleanProperty("nitro.join.rleAllUnmatchedOuterJoinOutput", defaults.rleAllUnmatchedOuterJoinOutput()),
                Boolean.getBoolean("nitro.join.debugRleAllUnmatchedOuterJoinOutput"),
                booleanProperty("nitro.join.directCompactedRangeOutput", defaults.directCompactedRangeOutput()),
                Integer.getInteger("nitro.hash.join.buildDictionarySparseRatio", defaults.buildDictionarySparseRatio()),
                Integer.getInteger(
                        "nitro.hash.join.buildDictionaryMinDistinctValues",
                        defaults.buildDictionaryMinDistinctValues()),
                Integer.getInteger(
                        "nitro.hash.join.buildDictionaryMaxDistinctPercent",
                        defaults.buildDictionaryMaxDistinctPercent()),
                booleanProperty("nitro.hash.join.wrapNonRetainedFixedWidthBuildValues", defaults.wrapNonRetainedFixedWidthBuildValues()),
                booleanProperty("nitro.hash.join.cacheInnerDictionaryIds", defaults.cacheInnerDictionaryIds()),
                booleanProperty("nitro.hash.join.wrapEncodedOuterDictionaries", defaults.wrapEncodedOuterDictionaries()),
                Integer.getInteger("nitro.hash.join.composeEncodedOuterDictionaryDepth", defaults.composeEncodedOuterDictionaryDepth()),
                booleanProperty("nitro.hash.join.adaptiveTinyDictionaryComposition", defaults.adaptiveTinyDictionaryComposition()),
                Integer.getInteger("nitro.hash.join.adaptiveComposeMaxRows", defaults.adaptiveComposeMaxRows()),
                Integer.getInteger("nitro.hash.join.adaptiveComposeDepth", defaults.adaptiveComposeDepth()),
                booleanProperty("nitro.hash.join.cacheComposedOuterDictionaryIds", defaults.cacheComposedOuterDictionaryIds()),
                booleanProperty("nitro.join.directDenseSingleMatchRangeOutput", defaults.directDenseSingleMatchRangeOutput()),
                booleanProperty("nitro.join.forwardIdentityReborrowOuter", defaults.forwardIdentityReborrowOuter()),
                Boolean.getBoolean("nitro.debug.identityReborrowOuter"),
                booleanProperty("nitro.join.rleRunIndexHint", defaults.rleRunIndexHint()),
                booleanProperty("nitro.hash.join.poolBuildDictionaryIds", defaults.poolBuildDictionaryIds()),
                booleanProperty("nitro.hash.join.gatherMultiRunRetainedFixedWidthValues", defaults.gatherMultiRunRetainedFixedWidthValues()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
