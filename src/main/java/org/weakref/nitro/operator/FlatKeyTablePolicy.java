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

/// Engine-selected physical policy shared by flat-key layouts and tables.
///
/// Grouping, joins, and distinct execution receive the same immutable instance from their operator-resource owner.
/// The property-backed factory is used only by the standalone composition root.
public record FlatKeyTablePolicy(Layout layout, Table table, ValueIds valueIds)
{
    public FlatKeyTablePolicy
    {
        requireNonNull(layout, "layout is null");
        requireNonNull(table, "table is null");
        requireNonNull(valueIds, "valueIds is null");
    }

    public static FlatKeyTablePolicy defaults()
    {
        return new FlatKeyTablePolicy(Layout.defaults(), Table.defaults(), ValueIds.defaults());
    }

    public static FlatKeyTablePolicy fromSystemProperties()
    {
        return new FlatKeyTablePolicy(
                Layout.fromSystemProperties(),
                Table.fromSystemProperties(),
                ValueIds.fromSystemProperties());
    }

    public record Layout(
            boolean poolScratch,
            boolean stableDictionaryValueHash,
            boolean reuseDictionaryEntryHashes,
            boolean generatedDictionaryHashBatch,
            int generatedDictionaryHashBatchMinRows,
            int generatedHybridHashBatchMinFields,
            int generatedHybridHashBatchMinAccessorFields,
            int generatedDictionaryHashBatchNullFreePairMinRows,
            int generatedDictionaryHashProbeTileRows,
            boolean generatedDictionaryRecordEquality,
            boolean debugGeneratedDictionaryHashBatch,
            boolean mixedCompositeIds,
            int mixedCompositeMaxFields,
            boolean fastMixedComposite3,
            boolean fastConstantNullMixedComposite3,
            boolean fastMixedComposite3Batch,
            boolean singleRunBinaryAccessor,
            boolean singleRunBinaryIdOnly,
            boolean debugConstantNullMixedComposite3,
            boolean debugMixedComposite,
            boolean earlyRejectMixedComposite,
            boolean fastNullFreeLongBinary,
            boolean fastNullFreeSingleBinary,
            boolean debugNullFreeSingleBinary,
            boolean allNullBatchMetadata,
            boolean trackDictionaryEmptySentinel,
            int dictionarySentinelSampleSize,
            int dictionarySentinelMinPercent,
            boolean fastNullFreeLongBinaryComposite,
            boolean packedRecordDictionaryIds,
            boolean idOnlyBinaryRecords,
            boolean embedIdOnlyBinaryIds,
            boolean compactEmbeddedBinaryRecords,
            int compactBinaryMinFields,
            int compactBinaryMinRows,
            int compactBinaryMinReusableFields,
            int compactBinaryReusePercent,
            long compactBinaryMinDistinctProduct,
            int compactBinaryMinDiscriminatingDistinct,
            boolean precomputeCompactBinaryPositionIds,
            boolean debugCompactBinaryPositionIds,
            boolean ownGroupedDictionaryIds,
            boolean composeNestedDictionaries,
            boolean resolveDictionaryNulls,
            boolean composeNestedDictionaryNulls,
            int dictionaryNullResolutionMinFields,
            boolean adaptiveDiscriminatingFieldHash,
            boolean normalizedIntKey,
            int discriminatingFieldHashMinFields,
            int discriminatingFieldHashSampleSize,
            int discriminatingFieldHashMinDistinctPercent,
            int valueIdCeiling)
    {
        public Layout
        {
            if (generatedDictionaryHashProbeTileRows <= 0) {
                throw new IllegalArgumentException("Generated dictionary hash probe tile rows must be positive");
            }
        }

        public static Layout defaults()
        {
            return new Layout(
                    true,
                    true,
                    true,
                    true,
                    128,
                    5,
                    2,
                    2048,
                    72,
                    true,
                    false,
                    true,
                    6,
                    true,
                    true,
                    true,
                    true,
                    true,
                    false,
                    false,
                    true,
                    true,
                    true,
                    false,
                    true,
                    true,
                    128,
                    25,
                    true,
                    true,
                    true,
                    true,
                    true,
                    5,
                    1024,
                    3,
                    75,
                    1024L,
                    32,
                    true,
                    false,
                    true,
                    true,
                    true,
                    true,
                    5,
                    true,
                    true,
                    4,
                    128,
                    90,
                    1 << 16);
        }

        public static Layout fromSystemProperties()
        {
            Layout defaults = defaults();
            return new Layout(
                    booleanProperty("nitro.flatKeyLayout.poolScratch", defaults.poolScratch()),
                    booleanProperty("nitro.group.stableDictionaryValueHash", defaults.stableDictionaryValueHash()),
                    booleanProperty("nitro.group.reuseDictionaryEntryHashes", defaults.reuseDictionaryEntryHashes()),
                    booleanProperty("nitro.group.generatedDictionaryHashBatch", defaults.generatedDictionaryHashBatch()),
                    Integer.getInteger(
                            "nitro.group.generatedDictionaryHashBatchMinRows",
                            defaults.generatedDictionaryHashBatchMinRows()),
                    Integer.getInteger(
                            "nitro.group.generatedHybridHashBatchMinFields",
                            defaults.generatedHybridHashBatchMinFields()),
                    Integer.getInteger(
                            "nitro.group.generatedHybridHashBatchMinAccessorFields",
                            defaults.generatedHybridHashBatchMinAccessorFields()),
                    Integer.getInteger(
                            "nitro.group.generatedDictionaryHashBatchNullFreePairMinRows",
                            defaults.generatedDictionaryHashBatchNullFreePairMinRows()),
                    Math.max(
                            1,
                            Integer.getInteger(
                                    "nitro.group.generatedDictionaryHashProbeTileRows",
                                    defaults.generatedDictionaryHashProbeTileRows())),
                    booleanProperty(
                            "nitro.group.generatedDictionaryRecordEquality",
                            defaults.generatedDictionaryRecordEquality()),
                    Boolean.getBoolean("nitro.debug.generatedDictionaryHashBatch"),
                    booleanProperty("nitro.group.mixedCompositeIds", defaults.mixedCompositeIds()),
                    Integer.getInteger("nitro.group.mixedCompositeMaxFields", defaults.mixedCompositeMaxFields()),
                    booleanProperty("nitro.group.fastMixedComposite3", defaults.fastMixedComposite3()),
                    booleanProperty(
                            "nitro.group.fastConstantNullMixedComposite3",
                            defaults.fastConstantNullMixedComposite3()),
                    booleanProperty("nitro.group.fastMixedComposite3Batch", defaults.fastMixedComposite3Batch()),
                    booleanProperty("nitro.flatGrouping.singleRunBinaryAccessor", defaults.singleRunBinaryAccessor()),
                    booleanProperty("nitro.flatGrouping.singleRunBinaryIdOnly", defaults.singleRunBinaryIdOnly()),
                    Boolean.getBoolean("nitro.debug.constantNullMixedComposite3"),
                    Boolean.getBoolean("nitro.debug.mixedComposite"),
                    booleanProperty("nitro.group.earlyRejectMixedComposite", defaults.earlyRejectMixedComposite()),
                    booleanProperty("nitro.group.fastNullFreeLongBinary", defaults.fastNullFreeLongBinary()),
                    booleanProperty("nitro.group.fastNullFreeSingleBinary", defaults.fastNullFreeSingleBinary()),
                    Boolean.getBoolean("nitro.debug.nullFreeSingleBinary"),
                    booleanProperty("nitro.flatGrouping.allNullBatchMetadata", defaults.allNullBatchMetadata()),
                    booleanProperty(
                            "nitro.group.trackDictionaryEmptySentinel",
                            defaults.trackDictionaryEmptySentinel()),
                    Integer.getInteger(
                            "nitro.group.dictionarySentinelSampleSize",
                            defaults.dictionarySentinelSampleSize()),
                    Integer.getInteger(
                            "nitro.group.dictionarySentinelMinPercent",
                            defaults.dictionarySentinelMinPercent()),
                    booleanProperty(
                            "nitro.group.fastNullFreeLongBinaryComposite",
                            defaults.fastNullFreeLongBinaryComposite()),
                    booleanProperty("nitro.group.packedRecordDictionaryIds", defaults.packedRecordDictionaryIds()),
                    booleanProperty("nitro.group.idOnlyBinaryRecords", defaults.idOnlyBinaryRecords()),
                    booleanProperty("nitro.group.embedIdOnlyBinaryIds", defaults.embedIdOnlyBinaryIds()),
                    booleanProperty(
                            "nitro.group.compactEmbeddedBinaryRecords",
                            defaults.compactEmbeddedBinaryRecords()),
                    Integer.getInteger("nitro.group.compactBinaryMinFields", defaults.compactBinaryMinFields()),
                    Integer.getInteger("nitro.group.compactBinaryMinRows", defaults.compactBinaryMinRows()),
                    Integer.getInteger(
                            "nitro.group.compactBinaryMinReusableFields",
                            defaults.compactBinaryMinReusableFields()),
                    Integer.getInteger(
                            "nitro.group.compactBinaryReusePercent",
                            defaults.compactBinaryReusePercent()),
                    Long.getLong(
                            "nitro.group.compactBinaryMinDistinctProduct",
                            defaults.compactBinaryMinDistinctProduct()),
                    Integer.getInteger(
                            "nitro.group.compactBinaryMinDiscriminatingDistinct",
                            defaults.compactBinaryMinDiscriminatingDistinct()),
                    booleanProperty(
                            "nitro.group.precomputeCompactBinaryPositionIds",
                            defaults.precomputeCompactBinaryPositionIds()),
                    Boolean.getBoolean("nitro.debug.compactBinaryPositionIds"),
                    booleanProperty("nitro.group.ownedGroupedDictionaryIds", defaults.ownGroupedDictionaryIds()),
                    booleanProperty("nitro.group.composeNestedDictionaries", defaults.composeNestedDictionaries()),
                    booleanProperty("nitro.group.resolveDictionaryNulls", defaults.resolveDictionaryNulls()),
                    booleanProperty(
                            "nitro.group.composeNestedDictionaryNulls",
                            defaults.composeNestedDictionaryNulls()),
                    Integer.getInteger(
                            "nitro.group.dictionaryNullResolutionMinFields",
                            defaults.dictionaryNullResolutionMinFields()),
                    booleanProperty(
                            "nitro.group.adaptiveDiscriminatingFieldHash",
                            defaults.adaptiveDiscriminatingFieldHash()),
                    booleanProperty("nitro.group.normalizedIntKey", defaults.normalizedIntKey()),
                    Integer.getInteger(
                            "nitro.group.discriminatingFieldHashMinFields",
                            defaults.discriminatingFieldHashMinFields()),
                    Integer.getInteger(
                            "nitro.group.discriminatingFieldHashSampleSize",
                            defaults.discriminatingFieldHashSampleSize()),
                    Integer.getInteger(
                            "nitro.group.discriminatingFieldHashMinDistinctPercent",
                            defaults.discriminatingFieldHashMinDistinctPercent()),
                    Integer.getInteger("nitro.group.valueIdCeiling", defaults.valueIdCeiling()));
        }
    }

    public record Table(
            boolean debugNormalizedIntKey,
            boolean debugSparseCompositeGroupCache,
            boolean poolSizedRecordChunks,
            boolean singleDictionaryGroupCache,
            boolean sparseCompositeGroupCache,
            boolean generatedDictionaryHashProbeBatch,
            int singleDictionaryGroupCacheMaxCardinalityAmplification,
            int singleDictionaryGroupCacheMaxCardinality,
            boolean identityGroupIds)
    {
        public static Table defaults()
        {
            return new Table(false, false, true, true, true, true, 2, 1 << 16, true);
        }

        public static Table fromSystemProperties()
        {
            Table defaults = defaults();
            return new Table(
                    Boolean.getBoolean("nitro.debug.normalizedIntKey"),
                    Boolean.getBoolean("nitro.debug.sparseCompositeGroupCache"),
                    booleanProperty("nitro.flatGrouping.poolSizedRecordChunks", defaults.poolSizedRecordChunks()),
                    booleanProperty(
                            "nitro.flatGrouping.singleDictionaryGroupCache",
                            defaults.singleDictionaryGroupCache()),
                    booleanProperty(
                            "nitro.flatGrouping.sparseCompositeGroupCache",
                            defaults.sparseCompositeGroupCache()),
                    booleanProperty(
                            "nitro.group.generatedDictionaryHashProbeBatch",
                            defaults.generatedDictionaryHashProbeBatch()),
                    Integer.getInteger(
                            "nitro.flatGrouping.singleDictionaryGroupCacheMaxCardinalityAmplification",
                            defaults.singleDictionaryGroupCacheMaxCardinalityAmplification()),
                    Integer.getInteger(
                            "nitro.flatGrouping.singleDictionaryGroupCacheMaxCardinality",
                            defaults.singleDictionaryGroupCacheMaxCardinality()),
                    booleanProperty("nitro.flatGrouping.identityGroupIds", defaults.identityGroupIds()));
        }
    }

    public record ValueIds(boolean recognizeEmptyAfterOverflow)
    {
        public static ValueIds defaults()
        {
            return new ValueIds(true);
        }

        public static ValueIds fromSystemProperties()
        {
            ValueIds defaults = defaults();
            return new ValueIds(booleanProperty(
                    "nitro.group.recognizeEmptyAfterOverflow",
                    defaults.recognizeEmptyAfterOverflow()));
        }
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
