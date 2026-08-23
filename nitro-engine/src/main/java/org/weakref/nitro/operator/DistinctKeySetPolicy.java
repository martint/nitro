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

/**
 * Engine-selected strategy policy for exact distinct-key storage.
 *
 * <p>The property-backed factory belongs at the standalone composition boundary. Distinct operators receive an
 * immutable instance and never consult process-global configuration.
 */
public record DistinctKeySetPolicy(
        PooledLongHashSetPolicy pooledLongHashSetPolicy,
        boolean debugDistinctShapes,
        boolean sharedDictionaryPositionResolver,
        boolean sharedDictionaryNullResolver,
        boolean sharedDictionaryBasePositionCache,
        boolean adaptiveCompactMultiLong,
        boolean adaptiveRetainNullsBatch,
        boolean adaptiveCompactLongPair,
        int adaptiveCompactLongPairSampleSize,
        int adaptiveCompactMultiLongMinArity,
        boolean adaptivePagedLongBitmap,
        int pagedLongBitmapMinKeys,
        long pagedLongBitmapMaxBitsPerKey,
        boolean emptyBinaryFastPath,
        boolean filterSentinelBeforeHash,
        boolean adaptiveDirectBatch,
        boolean taggedLongPairHash,
        boolean longPairNullFreeBatch,
        int adaptiveCompactLongPairStartBatch,
        boolean inlineSmallGroupedLong,
        boolean keyOnlyDictionaryDomain,
        int keyOnlyDictionaryDomainMinimumReduction,
        int keyOnlySparseRetentionMinPercent)
{
    public DistinctKeySetPolicy
    {
        requireNonNull(pooledLongHashSetPolicy, "pooledLongHashSetPolicy is null");
        if (adaptiveCompactLongPairSampleSize <= 0) {
            throw new IllegalArgumentException("adaptiveCompactLongPairSampleSize must be positive");
        }
        if (adaptiveCompactMultiLongMinArity <= 0) {
            throw new IllegalArgumentException("adaptiveCompactMultiLongMinArity must be positive");
        }
        if (adaptiveCompactLongPairStartBatch <= 0) {
            throw new IllegalArgumentException("adaptiveCompactLongPairStartBatch must be positive");
        }
        if (pagedLongBitmapMinKeys <= 0 || pagedLongBitmapMaxBitsPerKey <= 0) {
            throw new IllegalArgumentException("Paged-long bitmap admission values must be positive");
        }
        if (keyOnlyDictionaryDomainMinimumReduction <= 0) {
            throw new IllegalArgumentException("keyOnlyDictionaryDomainMinimumReduction must be positive");
        }
        if (keyOnlySparseRetentionMinPercent <= 0 || keyOnlySparseRetentionMinPercent > 100) {
            throw new IllegalArgumentException("keyOnlySparseRetentionMinPercent must be in [1, 100]");
        }
    }

    public static DistinctKeySetPolicy defaults()
    {
        return new DistinctKeySetPolicy(
                PooledLongHashSetPolicy.defaults(),
                false,
                true,
                true,
                true,
                true,
                true,
                true,
                256,
                3,
                true,
                4_096,
                64,
                true,
                true,
                true,
                true,
                true,
                4,
                true,
                true,
                4,
                75);
    }

    public static DistinctKeySetPolicy fromSystemProperties()
    {
        return new DistinctKeySetPolicy(
                PooledLongHashSetPolicy.fromSystemProperties(),
                Boolean.getBoolean("nitro.debug.distinctShapes"),
                booleanProperty("nitro.distinct.sharedDictionaryPositionResolver", true),
                booleanProperty("nitro.distinct.sharedDictionaryNullResolver", true),
                booleanProperty("nitro.distinct.sharedDictionaryBasePositionCache", true),
                booleanProperty("nitro.distinct.adaptiveCompactMultiLong", true),
                booleanProperty("nitro.distinct.adaptiveRetainNullsBatch", true),
                booleanProperty("nitro.distinct.adaptiveCompactLongPair", true),
                Integer.getInteger("nitro.distinct.adaptiveCompactLongPairSampleSize", 256),
                Integer.getInteger("nitro.distinct.adaptiveCompactMultiLongMinArity", 3),
                booleanProperty("nitro.distinct.adaptivePagedLongBitmap", true),
                Integer.getInteger("nitro.distinct.pagedLongBitmapMinKeys", 4_096),
                Long.getLong("nitro.distinct.pagedLongBitmapMaxBitsPerKey", 64L),
                booleanProperty("nitro.distinct.emptyBinaryFastPath", true),
                booleanProperty("nitro.distinct.filterSentinelBeforeHash", true),
                booleanProperty("nitro.distinct.adaptiveDirectBatch", true),
                booleanProperty("nitro.distinct.taggedLongPairHash", true),
                booleanProperty("nitro.distinct.longPairNullFreeBatch", true),
                Integer.getInteger("nitro.distinct.adaptiveCompactLongPairStartBatch", 4),
                booleanProperty("nitro.distinct.inlineSmallGroupedLong", true),
                booleanProperty("nitro.distinct.keyOnlyDictionaryDomain", true),
                Integer.getInteger("nitro.distinct.keyOnlyDictionaryDomainMinimumReduction", 4),
                Integer.getInteger("nitro.distinct.keyOnlySparseRetentionMinPercent", 75));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
