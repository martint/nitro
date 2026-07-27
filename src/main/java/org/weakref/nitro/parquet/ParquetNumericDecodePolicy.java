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
 * Immutable choices for decoding fixed-width numeric pages into output batches.
 */
public record ParquetNumericDecodePolicy(
        boolean directBatchDecode,
        boolean directDictionaryBatchDecode,
        boolean directPlainBatchDecode)
{
    public static ParquetNumericDecodePolicy defaults()
    {
        return new ParquetNumericDecodePolicy(true, true, true);
    }

    public static ParquetNumericDecodePolicy fromSystemProperties()
    {
        boolean directBatchDecode = Boolean.parseBoolean(
                System.getProperty("nitro.parquet.directNumericBatchDecode", "true"));
        return new ParquetNumericDecodePolicy(
                directBatchDecode,
                Boolean.parseBoolean(System.getProperty(
                        "nitro.parquet.directNumericDictionaryBatchDecode",
                        Boolean.toString(directBatchDecode))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.parquet.directNumericPlainBatchDecode",
                        Boolean.toString(directBatchDecode))));
    }
}
