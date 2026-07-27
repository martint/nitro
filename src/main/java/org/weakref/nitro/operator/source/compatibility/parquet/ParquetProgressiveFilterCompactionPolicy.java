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
 * Immutable admission and reporting choices for compacting progressively filtered scan columns.
 */
public record ParquetProgressiveFilterCompactionPolicy(
        boolean enabled,
        int maxPercent,
        long minObservedRows,
        int maxColumns,
        Prospective prospective,
        Fused fused,
        boolean diagnostics)
{
    public ParquetProgressiveFilterCompactionPolicy
    {
        requireNonNull(prospective, "prospective is null");
        requireNonNull(fused, "fused is null");
    }

    public static ParquetProgressiveFilterCompactionPolicy defaults()
    {
        return new ParquetProgressiveFilterCompactionPolicy(
                true,
                12,
                1L << 20,
                4,
                new Prospective(true, 1L << 20, 50, 1L << 18),
                new Fused(true, 60, 2, 4),
                false);
    }

    public static ParquetProgressiveFilterCompactionPolicy fromSystemProperties(
            ParquetLateMaterializationPolicy lateMaterializationPolicy)
    {
        requireNonNull(lateMaterializationPolicy, "lateMaterializationPolicy is null");
        return new ParquetProgressiveFilterCompactionPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.progressiveFilterCompaction", "true")),
                Integer.getInteger("nitro.parquet.progressiveFilterCompactionMaxPercent", 12),
                Integer.getInteger("nitro.parquet.progressiveFilterCompactionMinObservedRows", 1 << 20),
                Integer.getInteger("nitro.parquet.progressiveFilterCompactionMaxColumns", 4),
                new Prospective(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.prospectiveProgressiveFilterCompaction", "true")),
                        Integer.getInteger(
                                "nitro.parquet.prospectiveProgressiveFilterCompactionMinRawRows", 1 << 20),
                        Integer.getInteger(
                                "nitro.parquet.prospectiveProgressiveFilterCompactionMinObservedPercent", 50),
                        Integer.getInteger(
                                "nitro.parquet.prospectiveProgressiveFilterCompactionMinProjectedRows", 1 << 18)),
                new Fused(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.fusedProgressiveFilterCompaction", "true")),
                        Integer.getInteger("nitro.parquet.fusedProgressiveFilterCompactionMaxPercent", 60),
                        Integer.getInteger("nitro.parquet.fusedProgressiveFilterCompactionMinAlignedColumns", 2),
                        Integer.getInteger(
                                "nitro.parquet.fusedProgressiveFilterCompactionMinAverageRun",
                                lateMaterializationPolicy.skipDecode().minAverageRun())),
                Boolean.getBoolean("nitro.debug.progressiveFilterCompaction"));
    }

    public record Prospective(
            boolean enabled,
            long minRawRows,
            int minObservedPercent,
            long minProjectedRows) {}

    public record Fused(
            boolean enabled,
            int maxPercent,
            int minAlignedColumns,
            int minAverageRun) {}
}
