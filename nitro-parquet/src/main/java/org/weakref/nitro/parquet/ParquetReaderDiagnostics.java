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

/**
 * Immutable diagnostic choices for a Parquet column reader.
 */
public record ParquetReaderDiagnostics(
        boolean dictionaryFilterSummary,
        boolean directNumericBatchDecode,
        boolean decompression,
        boolean versionedDictionaryPredicates)
{
    public static ParquetReaderDiagnostics disabled()
    {
        return new ParquetReaderDiagnostics(false, false, false, false);
    }

    public static ParquetReaderDiagnostics fromSystemProperties()
    {
        return new ParquetReaderDiagnostics(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.debugDictionaryFilterSummary", "false")),
                Boolean.getBoolean("nitro.debug.directNumericBatchDecode"),
                Boolean.getBoolean("nitro.debug.decompression") || Boolean.getBoolean("nitro.parquet.decompressionStats"),
                Boolean.getBoolean("nitro.debug.versionedDictionaryPredicates"));
    }
}
