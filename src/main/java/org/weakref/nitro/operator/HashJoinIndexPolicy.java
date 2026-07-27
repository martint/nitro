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

/// Engine-selected representation and admission policy for hash join indexes.
///
/// The property-backed factory is a standalone composition adapter. Hash joins receive one immutable policy from
/// their resource owner and never consult process-global configuration for these decisions.
public record HashJoinIndexPolicy(
        int initialHashExpectedCap,
        boolean denseBuildFastPath,
        boolean compactDirectRowReferences,
        boolean compactChainRowReferences,
        boolean preSizeCappedRowStorage,
        boolean computeDenseSingleBatchRowReferences,
        boolean denseSingleBatchProbeSpecialization,
        boolean denseSingleBatchMatchPositions,
        boolean denseSingleBatchRangeProbe,
        boolean compactDenseSingleMatchReferences,
        boolean denseDictionaryProbeCache,
        boolean groupedLongHashTable,
        boolean sparseAwareLongHashLayout,
        int sparseAwareScalarMinRows,
        boolean lazyUniqueChainState,
        boolean sparseDirectDuplicateState,
        boolean compressedDirectBuildBatchLoop,
        int sparseDirectDuplicateMinExpectedDomainRatio,
        int sparseDirectDuplicateMinExpectedRows,
        boolean debugDirectDuplicateState,
        boolean debugCompressedDirectRange,
        boolean compressedDirectRange,
        int compressedDirectRangeMinKeys,
        int compressedDirectRangeMaxEntries,
        int compressedDirectRangeMaxRatio,
        boolean sparseLongRangeMembership,
        int sparseLongRangeMinRatio,
        int rangeAdmissionSampleRows,
        int rangeAdmissionMinSampleRows,
        int directRangeMaxCardinalityRatio,
        boolean keyOnlyDirectRangeBuild,
        int keyOnlyDirectRangeMinRows,
        int maxDirectBuildKey,
        int maxArrayRange,
        boolean compactChains,
        int compactChainsMinProbeRows,
        boolean compressKeyOnlyDuplicates,
        boolean sizeCompressedRowsByDistinctKeys,
        boolean flatPrimitiveSingleRows,
        boolean flatDictionaryProbeCache,
        int flatDictionaryProbeCacheMaxCardinality,
        int flatDictionaryProbeCacheMinRowsPerEntry,
        boolean compactLongPairKeys,
        boolean compactKeyOnlyLongPairBuild,
        boolean denseCompactPairEntries,
        int denseCompactPairMinCapacity,
        boolean denseCompactSparsePairEntries,
        int denseCompactSparsePairMinCapacity,
        boolean compactDensePairSingleBatchRowReferences,
        boolean preSizeDensePairDuplicateRows,
        boolean guardPairTagMaskConversion,
        boolean compactCompletedDirectRangeBuild,
        int compactCompletedDirectRangeMinSize,
        boolean denseUnusedBuildMembership,
        int denseUnusedBuildMembershipMinKeys,
        boolean debugJoinIndex)
{
    public static HashJoinIndexPolicy defaults()
    {
        return new HashJoinIndexPolicy(
                1 << 18,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                1 << 20,
                true,
                true,
                true,
                4,
                1 << 20,
                false,
                false,
                true,
                1 << 20,
                1 << 24,
                6,
                true,
                4,
                4096,
                16,
                2,
                true,
                1 << 20,
                1 << 26,
                1 << 26,
                true,
                256,
                true,
                true,
                true,
                true,
                1 << 16,
                2,
                true,
                true,
                true,
                1 << 25,
                true,
                1 << 23,
                true,
                true,
                true,
                true,
                256,
                true,
                1 << 12,
                true);
    }

    public static HashJoinIndexPolicy fromSystemProperties()
    {
        HashJoinIndexPolicy defaults = defaults();
        return new HashJoinIndexPolicy(
                Integer.getInteger("nitro.join.initialHashExpectedCap", defaults.initialHashExpectedCap()),
                booleanProperty("nitro.join.denseBuildFastPath", defaults.denseBuildFastPath()),
                booleanProperty("nitro.join.compactDirectRowReferences", defaults.compactDirectRowReferences()),
                booleanProperty("nitro.join.compactChainRowReferences", defaults.compactChainRowReferences()),
                booleanProperty("nitro.join.preSizeCappedRowStorage", defaults.preSizeCappedRowStorage()),
                booleanProperty(
                        "nitro.join.computeDenseSingleBatchRowReferences",
                        defaults.computeDenseSingleBatchRowReferences()),
                booleanProperty(
                        "nitro.join.denseSingleBatchProbeSpecialization",
                        defaults.denseSingleBatchProbeSpecialization()),
                booleanProperty(
                        "nitro.join.denseSingleBatchMatchPositions",
                        defaults.denseSingleBatchMatchPositions()),
                booleanProperty("nitro.join.denseSingleBatchRangeProbe", defaults.denseSingleBatchRangeProbe()),
                booleanProperty(
                        "nitro.join.compactDenseSingleMatchRefs",
                        defaults.compactDenseSingleMatchReferences()),
                booleanProperty("nitro.join.denseDictionaryProbeCache", defaults.denseDictionaryProbeCache()),
                booleanProperty("nitro.join.groupedLongHashTable", defaults.groupedLongHashTable()),
                booleanProperty("nitro.join.sparseAwareLongHashLayout", defaults.sparseAwareLongHashLayout()),
                Integer.getInteger("nitro.join.sparseAwareScalarMinRows", defaults.sparseAwareScalarMinRows()),
                booleanProperty("nitro.join.lazyUniqueChainState", defaults.lazyUniqueChainState()),
                booleanProperty("nitro.join.sparseDirectDuplicateState", defaults.sparseDirectDuplicateState()),
                booleanProperty(
                        "nitro.join.compressedDirectBuildBatchLoop",
                        defaults.compressedDirectBuildBatchLoop()),
                Integer.getInteger(
                        "nitro.join.sparseDirectDuplicateMinExpectedDomainRatio",
                        defaults.sparseDirectDuplicateMinExpectedDomainRatio()),
                Integer.getInteger(
                        "nitro.join.sparseDirectDuplicateMinExpectedRows",
                        defaults.sparseDirectDuplicateMinExpectedRows()),
                Boolean.getBoolean("nitro.join.debugDirectDuplicateState"),
                Boolean.getBoolean("nitro.join.debugCompressedDirectRange"),
                booleanProperty("nitro.join.compressedDirectRange", defaults.compressedDirectRange()),
                Integer.getInteger(
                        "nitro.join.compressedDirectRangeMinKeys",
                        defaults.compressedDirectRangeMinKeys()),
                Integer.getInteger(
                        "nitro.join.compressedDirectRangeMaxEntries",
                        defaults.compressedDirectRangeMaxEntries()),
                Integer.getInteger(
                        "nitro.join.compressedDirectRangeMaxRatio",
                        defaults.compressedDirectRangeMaxRatio()),
                booleanProperty("nitro.join.sparseLongRangeMembership", defaults.sparseLongRangeMembership()),
                Integer.getInteger("nitro.join.sparseLongRangeMinRatio", defaults.sparseLongRangeMinRatio()),
                Integer.getInteger("nitro.join.rangeAdmissionSampleRows", defaults.rangeAdmissionSampleRows()),
                Integer.getInteger(
                        "nitro.join.rangeAdmissionMinSampleRows",
                        defaults.rangeAdmissionMinSampleRows()),
                Integer.getInteger(
                        "nitro.join.directRangeMaxCardinalityRatio",
                        defaults.directRangeMaxCardinalityRatio()),
                booleanProperty("nitro.join.keyOnlyDirectRangeBuild", defaults.keyOnlyDirectRangeBuild()),
                Integer.getInteger(
                        "nitro.join.keyOnlyDirectRangeMinRows",
                        defaults.keyOnlyDirectRangeMinRows()),
                Integer.getInteger("nitro.join.maxDirectBuildKey", defaults.maxDirectBuildKey()),
                Integer.getInteger("nitro.join.maxArrayRange", defaults.maxArrayRange()),
                booleanProperty("nitro.join.compactChains", defaults.compactChains()),
                Integer.getInteger("nitro.join.compactChainsMinProbeRows", defaults.compactChainsMinProbeRows()),
                booleanProperty("nitro.join.compressKeyOnlyDuplicates", defaults.compressKeyOnlyDuplicates()),
                booleanProperty(
                        "nitro.join.sizeCompressedRowsByDistinctKeys",
                        defaults.sizeCompressedRowsByDistinctKeys()),
                booleanProperty("nitro.join.flatPrimitiveSingleRows", defaults.flatPrimitiveSingleRows()),
                booleanProperty("nitro.join.flatDictionaryProbeCache", defaults.flatDictionaryProbeCache()),
                Integer.getInteger(
                        "nitro.join.flatDictionaryProbeCacheMaxCardinality",
                        defaults.flatDictionaryProbeCacheMaxCardinality()),
                Integer.getInteger(
                        "nitro.join.flatDictionaryProbeCacheMinRowsPerEntry",
                        defaults.flatDictionaryProbeCacheMinRowsPerEntry()),
                booleanProperty("nitro.join.compactLongPairKeys", defaults.compactLongPairKeys()),
                booleanProperty(
                        "nitro.join.compactKeyOnlyLongPairBuild",
                        defaults.compactKeyOnlyLongPairBuild()),
                booleanProperty("nitro.join.denseCompactPairEntries", defaults.denseCompactPairEntries()),
                Integer.getInteger(
                        "nitro.join.denseCompactPairMinCapacity",
                        defaults.denseCompactPairMinCapacity()),
                booleanProperty(
                        "nitro.join.denseCompactSparsePairEntries",
                        defaults.denseCompactSparsePairEntries()),
                Integer.getInteger(
                        "nitro.join.denseCompactSparsePairMinCapacity",
                        defaults.denseCompactSparsePairMinCapacity()),
                booleanProperty(
                        "nitro.join.compactDensePairSingleBatchRowReferences",
                        defaults.compactDensePairSingleBatchRowReferences()),
                booleanProperty(
                        "nitro.join.preSizeDensePairDuplicateRows",
                        defaults.preSizeDensePairDuplicateRows()),
                booleanProperty("nitro.join.guardPairTagMaskConversion", defaults.guardPairTagMaskConversion()),
                booleanProperty(
                        "nitro.hash.join.compactCompletedDirectRangeBuild",
                        defaults.compactCompletedDirectRangeBuild()),
                Integer.getInteger(
                        "nitro.hash.join.compactCompletedDirectRangeMinSize",
                        defaults.compactCompletedDirectRangeMinSize()),
                booleanProperty(
                        "nitro.hash.join.denseUnusedBuildMembership",
                        defaults.denseUnusedBuildMembership()),
                Integer.getInteger(
                        "nitro.hash.join.denseUnusedBuildMembershipMinKeys",
                        defaults.denseUnusedBuildMembershipMinKeys()),
                Boolean.getBoolean("nitro.debug.joinIndex"));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
