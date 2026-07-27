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
 * Immutable scan-shape admission and reporting choices for direct numeric page decoding.
 */
public record ParquetNumericDecodeAdmissionPolicy(
        int minIntColumns,
        long minRows,
        boolean requireConstraintForAllNumeric,
        boolean repeatedSourceDecode,
        int repeatedSourceAnchorMinConsumers,
        int repeatedSourceAnchorMinColumns,
        long repeatedSourceMinRows,
        boolean diagnostics)
{
    public static ParquetNumericDecodeAdmissionPolicy defaults()
    {
        return new ParquetNumericDecodeAdmissionPolicy(3, 1L << 24, true, true, 3, 2, 1L << 25, false);
    }

    public static ParquetNumericDecodeAdmissionPolicy fromSystemProperties()
    {
        return new ParquetNumericDecodeAdmissionPolicy(
                Integer.getInteger("nitro.parquet.directNumericBatchDecodeMinIntColumns", 3),
                Long.getLong("nitro.parquet.directNumericBatchDecodeMinRows", 1L << 24),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.parquet.directNumericBatchDecodeRequireConstraintForAllNumeric", "true")),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.parquet.directNumericRepeatedSourceDecode", "true")),
                Integer.getInteger("nitro.parquet.directNumericRepeatedSourceAnchorMinConsumers", 3),
                Integer.getInteger("nitro.parquet.directNumericRepeatedSourceAnchorMinColumns", 2),
                Long.getLong("nitro.parquet.directNumericRepeatedSourceMinRows", 1L << 25),
                Boolean.getBoolean("nitro.debug.directNumericBatchDecode"));
    }
}
