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
package org.weakref.nitro.operator.source.compatibility.parquet;

import static java.util.Objects.requireNonNull;

/**
 * Immutable filter ordering, admission, and nullable-mask evaluation choices for Parquet scans.
 */
public record ParquetFilterEvaluationPolicy(
        Ordering ordering,
        NonSelectiveElision nonSelectiveElision,
        DirectNullMask directNullMask)
{
    public ParquetFilterEvaluationPolicy
    {
        requireNonNull(ordering, "ordering is null");
        requireNonNull(nonSelectiveElision, "nonSelectiveElision is null");
        requireNonNull(directNullMask, "directNullMask is null");
    }

    public static ParquetFilterEvaluationPolicy defaults()
    {
        return new ParquetFilterEvaluationPolicy(
                new Ordering(true, true),
                new NonSelectiveElision(true, true),
                new DirectNullMask(true, true));
    }

    public static ParquetFilterEvaluationPolicy fromSystemProperties()
    {
        return new ParquetFilterEvaluationPolicy(
                new Ordering(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.selectivityFilterOrder", "true")),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.rangeDensityFilterOrder", "true"))),
                new NonSelectiveElision(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.dropNonSelectiveFilters", "true")),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.exactDynamicFilterDictionaryCoverage", "true"))),
                new DirectNullMask(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.directNullMaskReader", "true")),
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.directNullMaskCompaction", "true"))));
    }

    public record Ordering(boolean selectivity, boolean rangeDensity) {}

    public record NonSelectiveElision(boolean enabled, boolean exactDictionaryCoverage) {}

    public record DirectNullMask(boolean reader, boolean compaction) {}
}
