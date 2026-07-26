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
 * Immutable choices for materializing decoded Parquet values and retaining reusable scratch.
 */
public record ParquetMaterializationPolicy(
        boolean ownedDictionaryIds,
        boolean reuseNumericDictionaryScratch,
        boolean recycleBinaryDictionaryScratch,
        boolean directSelectedBinary,
        boolean directFlatBinaryOutput,
        boolean directOwnedDictionaryIds,
        long directOwnedDictionaryIdsMinObservedRows,
        boolean presizePlainBinaryPage,
        int plainCopyLoopMaxValues,
        boolean bulkSelectedNumericDictionaryIds,
        boolean bulkSelectedNumericDictionaryIdsRequirePageReuse,
        boolean binaryDictionary)
{
    public static ParquetMaterializationPolicy defaults()
    {
        return new ParquetMaterializationPolicy(
                true, true, true, true, true, true, 1L << 20, true, 16, true, true, true);
    }

    public static ParquetMaterializationPolicy fromSystemProperties()
    {
        return new ParquetMaterializationPolicy(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.ownedDictionaryIds", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.reuseNumericDictionaryScratch", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.recycleBinaryDictionaryScratch", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.directSelectedBinary", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.directFlatBinaryOutput", "true")),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.directOwnedDictionaryIds", "true")),
                Long.getLong("nitro.parquet.directOwnedDictionaryIdsMinObservedRows", 1L << 20),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.presizePlainBinaryPage", "true")),
                Integer.getInteger("nitro.parquet.plainCopyLoopMaxValues", 16),
                Boolean.parseBoolean(System.getProperty("nitro.parquet.bulkSelectedNumericDictionaryIds", "true")),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.parquet.bulkSelectedNumericDictionaryIdsRequirePageReuse", "true")),
                !Boolean.getBoolean("nitro.parquet.disableBinaryDictionary"));
    }
}
