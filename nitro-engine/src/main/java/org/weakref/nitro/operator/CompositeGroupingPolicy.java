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

/// Engine-selected representation and admission policy for composite grouping.
///
/// The property-backed factory is a standalone composition adapter. Grouping state receives one immutable policy
/// from its resource owner and never consults process-global configuration.
public record CompositeGroupingPolicy(
        boolean debugGroupingShapes,
        boolean debugFlatPackedIdentity,
        boolean sharedDictionaryComposite,
        int sharedDictionaryMaxFields,
        int sharedDictionarySampleSize,
        int sharedDictionaryMaxDistinctPercent,
        boolean sharedDictionaryFlatBacking,
        int sharedDictionaryFlatBackingMinFields,
        int sharedDictionaryFlatBackingMinRows,
        boolean packedIntPair,
        boolean packedIntTriple,
        boolean packedIntTripleCombinedControl,
        boolean packedIntTriplePackedTail,
        boolean adaptiveCompactLong,
        boolean generatedCompactLongPair,
        int generatedCompactLongMinArity,
        boolean earlyRejectMixedComposite,
        boolean flatSingleKeyRecordIdentity,
        int flatSingleKeyRecordIdentityMinBatchRows,
        int flatSingleKeyRecordIdentitySampleSize,
        int flatSingleKeyRecordIdentityMinDistinctPercent,
        boolean packedFlatIdentitySlots,
        int packedFlatIdentityMaxFields,
        int packedFlatIdentityMinBatchRows,
        int packedFlatIdentityBlockingMinBatchRows,
        int packedFlatIdentityMinDistinctPercent,
        boolean fullWidthPairPackedIdentity,
        int fullWidthPairPackedIdentityMinBatchRows,
        boolean adaptiveFlatLookahead,
        int adaptiveFlatLookaheadStartBatch,
        int adaptiveFlatLookaheadBatches,
        int adaptiveFlatLookaheadMinRows,
        int adaptiveFlatLookaheadMinNewPercent)
{
    public CompositeGroupingPolicy
    {
        if (sharedDictionaryMaxFields <= 0 ||
                sharedDictionarySampleSize < 0 ||
                sharedDictionaryMaxDistinctPercent < 0 ||
                sharedDictionaryFlatBackingMinFields <= 0 ||
                sharedDictionaryFlatBackingMinRows < 0 ||
                generatedCompactLongMinArity <= 0 ||
                flatSingleKeyRecordIdentityMinBatchRows < 0 ||
                flatSingleKeyRecordIdentitySampleSize <= 0 ||
                flatSingleKeyRecordIdentityMinDistinctPercent < 0 ||
                packedFlatIdentityMaxFields <= 0 ||
                packedFlatIdentityMinBatchRows < 0 ||
                packedFlatIdentityBlockingMinBatchRows < 0 ||
                packedFlatIdentityMinDistinctPercent < 0 ||
                fullWidthPairPackedIdentityMinBatchRows < 0 ||
                adaptiveFlatLookaheadStartBatch <= 0 ||
                adaptiveFlatLookaheadBatches <= 0 ||
                adaptiveFlatLookaheadMinRows < 0 ||
                adaptiveFlatLookaheadMinNewPercent < 0) {
            throw new IllegalArgumentException("Composite grouping thresholds are invalid");
        }
    }

    public static CompositeGroupingPolicy defaults()
    {
        return new CompositeGroupingPolicy(
                false,
                false,
                true,
                3,
                128,
                75,
                true,
                3,
                1 << 12,
                true,
                true,
                true,
                true,
                false,
                true,
                4,
                true,
                true,
                1 << 12,
                256,
                90,
                true,
                2,
                1 << 12,
                10_000,
                80,
                true,
                1 << 10,
                true,
                4,
                64,
                1 << 13,
                20);
    }

    public CompositeGroupingPolicy withAdaptiveFlatLookaheadBatches(int batches)
    {
        return new CompositeGroupingPolicy(
                debugGroupingShapes,
                debugFlatPackedIdentity,
                sharedDictionaryComposite,
                sharedDictionaryMaxFields,
                sharedDictionarySampleSize,
                sharedDictionaryMaxDistinctPercent,
                sharedDictionaryFlatBacking,
                sharedDictionaryFlatBackingMinFields,
                sharedDictionaryFlatBackingMinRows,
                packedIntPair,
                packedIntTriple,
                packedIntTripleCombinedControl,
                packedIntTriplePackedTail,
                adaptiveCompactLong,
                generatedCompactLongPair,
                generatedCompactLongMinArity,
                earlyRejectMixedComposite,
                flatSingleKeyRecordIdentity,
                flatSingleKeyRecordIdentityMinBatchRows,
                flatSingleKeyRecordIdentitySampleSize,
                flatSingleKeyRecordIdentityMinDistinctPercent,
                packedFlatIdentitySlots,
                packedFlatIdentityMaxFields,
                packedFlatIdentityMinBatchRows,
                packedFlatIdentityBlockingMinBatchRows,
                packedFlatIdentityMinDistinctPercent,
                fullWidthPairPackedIdentity,
                fullWidthPairPackedIdentityMinBatchRows,
                adaptiveFlatLookahead,
                adaptiveFlatLookaheadStartBatch,
                batches,
                adaptiveFlatLookaheadMinRows,
                adaptiveFlatLookaheadMinNewPercent);
    }

    /**
     * Grouping-only aggregation has no accumulator state indexed by group id, so the identity-record layout does
     * not recover its extra record indirection through cheaper accumulator access. Keep full-width pairs on the
     * generated adaptive table for that semantic shape while preserving every other composite-grouping policy.
     */
    public CompositeGroupingPolicy forGroupingOnlyAggregation()
    {
        if (!fullWidthPairPackedIdentity) {
            return this;
        }
        return new CompositeGroupingPolicy(
                debugGroupingShapes,
                debugFlatPackedIdentity,
                sharedDictionaryComposite,
                sharedDictionaryMaxFields,
                sharedDictionarySampleSize,
                sharedDictionaryMaxDistinctPercent,
                sharedDictionaryFlatBacking,
                sharedDictionaryFlatBackingMinFields,
                sharedDictionaryFlatBackingMinRows,
                packedIntPair,
                packedIntTriple,
                packedIntTripleCombinedControl,
                packedIntTriplePackedTail,
                adaptiveCompactLong,
                generatedCompactLongPair,
                generatedCompactLongMinArity,
                earlyRejectMixedComposite,
                flatSingleKeyRecordIdentity,
                flatSingleKeyRecordIdentityMinBatchRows,
                flatSingleKeyRecordIdentitySampleSize,
                flatSingleKeyRecordIdentityMinDistinctPercent,
                packedFlatIdentitySlots,
                packedFlatIdentityMaxFields,
                packedFlatIdentityMinBatchRows,
                packedFlatIdentityBlockingMinBatchRows,
                packedFlatIdentityMinDistinctPercent,
                false,
                fullWidthPairPackedIdentityMinBatchRows,
                adaptiveFlatLookahead,
                adaptiveFlatLookaheadStartBatch,
                adaptiveFlatLookaheadBatches,
                adaptiveFlatLookaheadMinRows,
                adaptiveFlatLookaheadMinNewPercent);
    }

    public static CompositeGroupingPolicy fromSystemProperties()
    {
        CompositeGroupingPolicy defaults = defaults();
        return new CompositeGroupingPolicy(
                Boolean.getBoolean("nitro.debug.groupingShapes"),
                Boolean.getBoolean("nitro.debug.flatPackedIdentity"),
                booleanProperty("nitro.group.sharedDictionaryComposite", defaults.sharedDictionaryComposite()),
                Integer.getInteger("nitro.group.sharedDictionaryMaxFields", defaults.sharedDictionaryMaxFields()),
                Integer.getInteger("nitro.group.sharedDictionarySampleSize", defaults.sharedDictionarySampleSize()),
                Integer.getInteger(
                        "nitro.group.sharedDictionaryMaxDistinctPercent",
                        defaults.sharedDictionaryMaxDistinctPercent()),
                booleanProperty("nitro.group.sharedDictionaryFlatBacking", defaults.sharedDictionaryFlatBacking()),
                Integer.getInteger(
                        "nitro.group.sharedDictionaryFlatBackingMinFields",
                        defaults.sharedDictionaryFlatBackingMinFields()),
                Integer.getInteger(
                        "nitro.group.sharedDictionaryFlatBackingMinRows",
                        defaults.sharedDictionaryFlatBackingMinRows()),
                booleanProperty("nitro.group.packedIntPair", defaults.packedIntPair()),
                booleanProperty("nitro.group.packedIntTriple", defaults.packedIntTriple()),
                booleanProperty(
                        "nitro.group.packedIntTripleCombinedControl",
                        defaults.packedIntTripleCombinedControl()),
                booleanProperty("nitro.group.packedIntTriplePackedTail", defaults.packedIntTriplePackedTail()),
                booleanProperty("nitro.group.adaptiveCompactLong", defaults.adaptiveCompactLong()),
                booleanProperty("nitro.group.generatedCompactLongPair", defaults.generatedCompactLongPair()),
                Integer.getInteger(
                        "nitro.group.generatedCompactLongMinArity",
                        defaults.generatedCompactLongMinArity()),
                booleanProperty("nitro.group.earlyRejectMixedComposite", defaults.earlyRejectMixedComposite()),
                booleanProperty(
                        "nitro.group.flatSingleKeyRecordIdentity",
                        defaults.flatSingleKeyRecordIdentity()),
                Integer.getInteger(
                        "nitro.group.flatSingleKeyRecordIdentityMinBatchRows",
                        defaults.flatSingleKeyRecordIdentityMinBatchRows()),
                Integer.getInteger(
                        "nitro.group.flatSingleKeyRecordIdentitySampleSize",
                        defaults.flatSingleKeyRecordIdentitySampleSize()),
                Integer.getInteger(
                        "nitro.group.flatSingleKeyRecordIdentityMinDistinctPercent",
                        defaults.flatSingleKeyRecordIdentityMinDistinctPercent()),
                booleanProperty(
                        "nitro.flatGrouping.packedHashRecordSlots",
                        defaults.packedFlatIdentitySlots()),
                Integer.getInteger(
                        "nitro.flatGrouping.packedHashRecordSlotsMaxFields",
                        defaults.packedFlatIdentityMaxFields()),
                Integer.getInteger(
                        "nitro.flatGrouping.packedHashRecordSlotsMinBatchRows",
                        defaults.packedFlatIdentityMinBatchRows()),
                Integer.getInteger(
                        "nitro.flatGrouping.packedHashRecordSlotsBlockingMinBatchRows",
                        defaults.packedFlatIdentityBlockingMinBatchRows()),
                Integer.getInteger(
                        "nitro.flatGrouping.packedHashRecordSlotsMinDistinctPercent",
                        defaults.packedFlatIdentityMinDistinctPercent()),
                booleanProperty(
                        "nitro.group.fullWidthPairPackedIdentity",
                        defaults.fullWidthPairPackedIdentity()),
                Integer.getInteger(
                        "nitro.group.fullWidthPairPackedIdentityMinBatchRows",
                        defaults.fullWidthPairPackedIdentityMinBatchRows()),
                booleanProperty("nitro.group.adaptiveFlatLookahead", defaults.adaptiveFlatLookahead()),
                Integer.getInteger(
                        "nitro.group.adaptiveFlatLookaheadStartBatch",
                        defaults.adaptiveFlatLookaheadStartBatch()),
                Integer.getInteger(
                        "nitro.group.adaptiveFlatLookaheadBatches",
                        defaults.adaptiveFlatLookaheadBatches()),
                Integer.getInteger(
                        "nitro.group.adaptiveFlatLookaheadMinRows",
                        defaults.adaptiveFlatLookaheadMinRows()),
                Integer.getInteger(
                        "nitro.group.adaptiveFlatLookaheadMinNewPercent",
                        defaults.adaptiveFlatLookaheadMinNewPercent()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
