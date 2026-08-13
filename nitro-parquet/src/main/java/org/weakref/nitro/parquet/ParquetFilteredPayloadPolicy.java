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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Immutable decode-path and deferral choices for payload columns after dynamic filtering.
 */
public record ParquetFilteredPayloadPolicy(
        int bulkMinSurvivorPercent,
        int fragmentedBulkMinSurvivorPercent,
        int minAverageSurvivorRun,
        Deferred deferred)
{
    public ParquetFilteredPayloadPolicy
    {
        checkArgument(fragmentedBulkMinSurvivorPercent >= 0 && fragmentedBulkMinSurvivorPercent <= 100,
                "fragmentedBulkMinSurvivorPercent must be between 0 and 100");
        checkArgument(bulkMinSurvivorPercent >= fragmentedBulkMinSurvivorPercent && bulkMinSurvivorPercent <= 100,
                "bulkMinSurvivorPercent must be between fragmentedBulkMinSurvivorPercent and 100");
        checkArgument(minAverageSurvivorRun > 0, "minAverageSurvivorRun must be positive");
        requireNonNull(deferred, "deferred is null");
    }

    public boolean useBulkDecode(int[] positions, int selected, int total)
    {
        checkArgument(selected >= 0 && selected <= total, "selected must be between 0 and total");
        checkArgument(positions.length >= selected, "positions does not contain selected entries");
        if (selected > (long) total * bulkMinSurvivorPercent / 100) {
            return true;
        }
        if (selected <= (long) total * fragmentedBulkMinSurvivorPercent / 100) {
            return false;
        }
        int runs = selected == 0 ? 0 : 1;
        for (int index = 1; index < selected; index++) {
            if (positions[index] != positions[index - 1] + 1) {
                runs++;
            }
        }
        return selected < (long) runs * minAverageSurvivorRun;
    }

    public static ParquetFilteredPayloadPolicy defaults()
    {
        return new ParquetFilteredPayloadPolicy(
                50,
                20,
                4,
                new Deferred(
                        true,
                        20,
                        4,
                        new BoundedWindow(false, 163_840, 8, false)));
    }

    public static ParquetFilteredPayloadPolicy fromSystemProperties()
    {
        return new ParquetFilteredPayloadPolicy(
                Integer.getInteger("nitro.parquet.dfPayloadBulkPercent", 50),
                Integer.getInteger("nitro.parquet.dfPayloadFragmentedBulkPercent", 20),
                Integer.getInteger("nitro.parquet.dfPayloadMinAverageRun", 4),
                new Deferred(
                        Boolean.parseBoolean(System.getProperty("nitro.parquet.deferFilteredPayload", "true")),
                        Integer.getInteger("nitro.parquet.deferredPayloadMaxSurvivorPercent", 20),
                        Integer.getInteger("nitro.parquet.deferredPayloadMinColumns", 4),
                        new BoundedWindow(
                                Boolean.parseBoolean(System.getProperty(
                                        "nitro.parquet.boundedDeferredPayloadWindow", "false")),
                                Integer.getInteger("nitro.parquet.boundedDeferredPayloadWindowRows", 163_840),
                                Integer.getInteger("nitro.parquet.boundedDeferredPayloadMaxColumns", 8),
                                Boolean.getBoolean("nitro.debug.boundedDeferredPayloadWindow"))));
    }

    public record Deferred(
            boolean enabled,
            int maxSurvivorPercent,
            int minColumns,
            BoundedWindow boundedWindow)
    {
        public Deferred
        {
            requireNonNull(boundedWindow, "boundedWindow is null");
        }
    }

    public record BoundedWindow(
            boolean enabled,
            int rows,
            int maxScanColumns,
            boolean diagnostics) {}
}
