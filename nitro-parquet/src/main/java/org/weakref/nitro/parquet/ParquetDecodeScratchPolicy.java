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

/** Immutable bounds for connector-owned, cross-thread Parquet decode scratch. */
public record ParquetDecodeScratchPolicy(long maxRetainedBytes, int maxRetainedBufferBytes)
{
    public ParquetDecodeScratchPolicy
    {
        if (maxRetainedBytes < 0 || maxRetainedBufferBytes < 0) {
            throw new IllegalArgumentException("Decode scratch bounds must be non-negative");
        }
    }

    public static ParquetDecodeScratchPolicy defaults()
    {
        return new ParquetDecodeScratchPolicy(256L << 20, 64 << 20);
    }

    public static ParquetDecodeScratchPolicy fromSystemProperties()
    {
        ParquetDecodeScratchPolicy defaults = defaults();
        return new ParquetDecodeScratchPolicy(
                Long.getLong("nitro.parquet.decodeScratch.maxRetainedBytes", defaults.maxRetainedBytes()),
                Integer.getInteger(
                        "nitro.parquet.decodeScratch.maxRetainedBufferBytes",
                        defaults.maxRetainedBufferBytes()));
    }
}
