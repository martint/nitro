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
 * Immutable filter ordering, admission, and nullable-mask evaluation choices for Parquet scans.
 */
public record ParquetFilterEvaluationPolicy(
        Ordering ordering,
        NonSelectiveElision nonSelectiveElision,
        DirectNullMask directNullMask,
        StaticBinarySourceFilter staticBinarySourceFilter)
{
    public ParquetFilterEvaluationPolicy
    {
        requireNonNull(ordering, "ordering is null");
        requireNonNull(nonSelectiveElision, "nonSelectiveElision is null");
        requireNonNull(directNullMask, "directNullMask is null");
        requireNonNull(staticBinarySourceFilter, "staticBinarySourceFilter is null");
    }

    public static ParquetFilterEvaluationPolicy defaults()
    {
        return new ParquetFilterEvaluationPolicy(
                new Ordering(true, true, 8_192),
                new NonSelectiveElision(true, true),
                new DirectNullMask(true, true),
                new StaticBinarySourceFilter(8_192));
    }

    public static ParquetFilterEvaluationPolicy fromSystemProperties()
    {
        return new ParquetFilterEvaluationPolicy(
                new Ordering(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.selectivityFilterOrder", "true")),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.rangeDensityFilterOrder", "true")),
                        Integer.getInteger("nitro.parquet.exactDictionaryOrderMaxEntries", 8_192)),
                new NonSelectiveElision(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.dropNonSelectiveFilters", "true")),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.exactDynamicFilterDictionaryCoverage", "true"))),
                new DirectNullMask(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.directNullMaskReader", "true")),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.directNullMaskCompaction", "true"))),
                new StaticBinarySourceFilter(
                        Integer.getInteger("nitro.parquet.staticBinaryFilterMaxDictionaryEntries", 8_192)));
    }

    public record Ordering(boolean selectivity, boolean rangeDensity, int exactDictionaryMaxEntries)
    {
        public Ordering
        {
            if (exactDictionaryMaxEntries < 0) {
                throw new IllegalArgumentException("exactDictionaryMaxEntries is negative");
            }
        }
    }

    public record NonSelectiveElision(boolean enabled, boolean exactDictionaryCoverage) {}

    public record DirectNullMask(boolean reader, boolean compaction) {}

    public record StaticBinarySourceFilter(int maxDictionaryEntries)
    {
        public StaticBinarySourceFilter
        {
            if (maxDictionaryEntries < 0) {
                throw new IllegalArgumentException("maxDictionaryEntries is negative");
            }
        }

        public boolean admits(int dictionaryEntries)
        {
            return dictionaryEntries > 0 && dictionaryEntries <= maxDictionaryEntries;
        }
    }
}
