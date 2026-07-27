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
 * Immutable window sizing and scratch-allocation choices for dynamic-filter scans.
 */
public record ParquetFilterWindowPolicy(
        int rows,
        AdaptiveNarrow adaptiveNarrow,
        boolean eagerScratch)
{
    public ParquetFilterWindowPolicy
    {
        requireNonNull(adaptiveNarrow, "adaptiveNarrow is null");
    }

    public static ParquetFilterWindowPolicy defaults()
    {
        return new ParquetFilterWindowPolicy(
                1 << 19,
                new AdaptiveNarrow(true, 3, 1 << 24, 38, false),
                false);
    }

    public static ParquetFilterWindowPolicy fromSystemProperties()
    {
        return new ParquetFilterWindowPolicy(
                Integer.getInteger("nitro.parquet.scan.filterWindow", 1 << 19),
                new AdaptiveNarrow(
                        Boolean.parseBoolean(System.getProperty(
                                "nitro.parquet.scan.adaptiveNarrowFilterWindow", "true")),
                        Integer.getInteger("nitro.parquet.scan.narrowFilterWindowMaxColumns", 3),
                        Integer.getInteger("nitro.parquet.scan.narrowFilterWindow", 1 << 24),
                        Integer.getInteger("nitro.parquet.scan.narrowFilterWindowMaxExecutionScans", 38),
                        Boolean.getBoolean("nitro.debug.narrowFilterWindow")),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.parquet.scan.eagerFilterWindowScratch", "false")));
    }

    public record AdaptiveNarrow(
            boolean enabled,
            int maxColumns,
            int rows,
            int maxExecutionScans,
            boolean diagnostics) {}
}
