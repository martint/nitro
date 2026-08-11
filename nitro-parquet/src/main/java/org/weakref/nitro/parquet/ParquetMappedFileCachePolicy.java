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

/** Bounds connector-owned reuse of immutable memory-mapped Parquet files. */
public record ParquetMappedFileCachePolicy(int maxEntries, long maxMappedBytes)
{
    public ParquetMappedFileCachePolicy
    {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        if (maxMappedBytes <= 0) {
            throw new IllegalArgumentException("maxMappedBytes must be positive");
        }
    }

    public static ParquetMappedFileCachePolicy defaults()
    {
        return new ParquetMappedFileCachePolicy(256, 64L * 1024 * 1024 * 1024);
    }
}
