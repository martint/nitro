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
 * Immutable reader-local policies selected by the Parquet connector composition root.
 */
public record ParquetReaderPolicy(
        RleReaderPolicy rle,
        ParquetPageNavigationPolicy pageNavigation,
        ParquetReaderDiagnostics diagnostics,
        ParquetMaterializationPolicy materialization,
        ParquetNumericDecodePolicy numericDecode,
        ParquetDictionaryFilterPolicy dictionaryFilter)
{
    public ParquetReaderPolicy
    {
        requireNonNull(rle, "rle is null");
        requireNonNull(pageNavigation, "pageNavigation is null");
        requireNonNull(diagnostics, "diagnostics is null");
        requireNonNull(materialization, "materialization is null");
        requireNonNull(numericDecode, "numericDecode is null");
        requireNonNull(dictionaryFilter, "dictionaryFilter is null");
    }

    public static ParquetReaderPolicy defaults()
    {
        return new ParquetReaderPolicy(
                RleReaderPolicy.defaults(),
                ParquetPageNavigationPolicy.defaults(),
                ParquetReaderDiagnostics.disabled(),
                ParquetMaterializationPolicy.defaults(),
                ParquetNumericDecodePolicy.defaults(),
                ParquetDictionaryFilterPolicy.defaults());
    }

    public static ParquetReaderPolicy fromSystemProperties()
    {
        return new ParquetReaderPolicy(
                RleReaderPolicy.fromSystemProperties(),
                ParquetPageNavigationPolicy.fromSystemProperties(),
                ParquetReaderDiagnostics.fromSystemProperties(),
                ParquetMaterializationPolicy.fromSystemProperties(),
                ParquetNumericDecodePolicy.fromSystemProperties(),
                ParquetDictionaryFilterPolicy.fromSystemProperties());
    }
}
