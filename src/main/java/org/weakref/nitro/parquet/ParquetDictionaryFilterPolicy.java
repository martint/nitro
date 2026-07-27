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
package org.weakref.nitro.parquet;

import static java.util.Objects.requireNonNull;

/**
 * Immutable mechanics and admission thresholds for predicate-over-dictionary filtering.
 */
public record ParquetDictionaryFilterPolicy(
        boolean fusedFilter,
        boolean vectorFilter,
        Compaction compaction,
        NullableFilter nullableFilter,
        ZeroAcceptedPageSkip zeroAcceptedPageSkip)
{
    public ParquetDictionaryFilterPolicy
    {
        requireNonNull(compaction, "compaction is null");
        requireNonNull(nullableFilter, "nullableFilter is null");
        requireNonNull(zeroAcceptedPageSkip, "zeroAcceptedPageSkip is null");
    }

    public static ParquetDictionaryFilterPolicy defaults()
    {
        return new ParquetDictionaryFilterPolicy(
                true,
                false,
                new Compaction(9, true, 16, 12, 256, true, 1L << 16),
                new NullableFilter(true, true, 100, 10),
                new ZeroAcceptedPageSkip(true, 1L << 20));
    }

    public static ParquetDictionaryFilterPolicy fromSystemProperties()
    {
        return new ParquetDictionaryFilterPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.fusedDictFilter", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.vectorDictFilter", "false")) &&
                        VectorDictFilter.supported(),
                new Compaction(
                        Integer.getInteger("nitro.parquet.branchlessCompactionDenominator", 9),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.bitmaskDictionaryCompaction", "true")),
                        Integer.getInteger("nitro.parquet.bitmaskDictionaryCompactionLowerDenominator", 16),
                        Integer.getInteger("nitro.parquet.bitmaskDictionaryCompactionUpperDenominator", 12),
                        Integer.getInteger("nitro.parquet.bitmaskDictionaryCompactionMinDictionarySize", 256),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.adaptiveBranchlessCompaction", "true")),
                        Long.getLong("nitro.parquet.adaptiveBranchlessCompactionMinRows", 1L << 16)),
                new NullableFilter(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.streamNullableDictionaryFilter", "true")),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.directNullableDictionaryFilter", "true")),
                        Integer.getInteger("nitro.parquet.directNullableDictionaryFilterMinAcceptedDenominator", 100),
                        Integer.getInteger("nitro.parquet.directNullableDictionaryFilterMinIdBitWidth", 10)),
                new ZeroAcceptedPageSkip(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.zeroAcceptedDictionaryPageSkip", "true")),
                        Long.getLong("nitro.parquet.zeroAcceptedDictionaryMinObservedRows", 1L << 20)));
    }

    public record Compaction(
            int branchlessDenominator,
            boolean bitmask,
            int bitmaskLowerDenominator,
            int bitmaskUpperDenominator,
            int bitmaskMinDictionarySize,
            boolean adaptiveBranchless,
            long adaptiveBranchlessMinRows) {}

    public record NullableFilter(
            boolean stream,
            boolean direct,
            int directMinAcceptedDenominator,
            int directMinIdBitWidth) {}

    public record ZeroAcceptedPageSkip(boolean enabled, long minObservedRows) {}
}
