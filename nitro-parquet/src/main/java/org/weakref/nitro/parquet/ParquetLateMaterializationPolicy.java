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
 * Immutable late-materialization and constrained-column decode choices for a native Parquet scan.
 */
public record ParquetLateMaterializationPolicy(
        boolean enabled,
        SkipDecode skipDecode,
        boolean deferEmptyConstrainedDecode)
{
    public ParquetLateMaterializationPolicy
    {
        requireNonNull(skipDecode, "skipDecode is null");
    }

    public static ParquetLateMaterializationPolicy defaults()
    {
        return new ParquetLateMaterializationPolicy(
                true,
                new SkipDecode(
                        20,
                        4,
                        true,
                        new FragmentedNumeric(true, 6, 128, 2, 128),
                        1,
                        false),
                true);
    }

    public static ParquetLateMaterializationPolicy fromSystemProperties()
    {
        return new ParquetLateMaterializationPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.lateMaterialization", "true")),
                new SkipDecode(
                        Integer.getInteger("nitro.parquet.skipMaxSurvivorPercent", 20),
                        Integer.getInteger("nitro.parquet.skipMinAverageRun", 4),
                        Boolean.parseBoolean(System.getProperty("nitro.parquet.lazyNumericSkipDecode", "true")),
                        new FragmentedNumeric(
                                Boolean.parseBoolean(System.getProperty(
                                        "nitro.parquet.lazyFragmentedNumericSkipDecode", "true")),
                                Integer.getInteger(
                                        "nitro.parquet.lazyFragmentedNumericSkipMaxSurvivorPercent", 6),
                                Integer.getInteger("nitro.parquet.lazyFragmentedNumericSkipMaxScanColumns", 128),
                                Integer.getInteger("nitro.parquet.lazyFragmentedNumericSkipMinPayloadColumns", 2),
                                Integer.getInteger("nitro.parquet.lazyFragmentedNumericSkipMaxPayloadColumns", 128)),
                        Integer.getInteger("nitro.parquet.lazyNumericSkipMinDictionarySize", 1),
                        Boolean.getBoolean("nitro.debug.lazyNumericSkip")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.deferEmptyConstrainedDecode", "true")));
    }

    public record SkipDecode(
            int maxSurvivorPercent,
            int minAverageRun,
            boolean numeric,
            FragmentedNumeric fragmentedNumeric,
            int numericMinDictionarySize,
            boolean diagnostics)
    {
        public SkipDecode
        {
            requireNonNull(fragmentedNumeric, "fragmentedNumeric is null");
        }
    }

    public record FragmentedNumeric(
            boolean enabled,
            int maxSurvivorPercent,
            int maxScanColumns,
            int minPayloadColumns,
            int maxPayloadColumns)
    {
        public boolean admits(int selected, int total, int scanColumns, int payloadColumns)
        {
            return enabled &&
                    total > 0 &&
                    (long) selected * 100 <= (long) total * maxSurvivorPercent &&
                    scanColumns <= maxScanColumns &&
                    payloadColumns >= minPayloadColumns &&
                    payloadColumns <= maxPayloadColumns;
        }
    }
}
