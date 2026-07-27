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
 * Immutable decode-path and deferral choices for payload columns after dynamic filtering.
 */
public record ParquetFilteredPayloadPolicy(
        int bulkMinSurvivorPercent,
        Deferred deferred)
{
    public ParquetFilteredPayloadPolicy
    {
        requireNonNull(deferred, "deferred is null");
    }

    public static ParquetFilteredPayloadPolicy defaults()
    {
        return new ParquetFilteredPayloadPolicy(
                50,
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
