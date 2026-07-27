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

public record DecompressedPageCachePolicy(
        boolean enabled,
        int capacity,
        int minSourcePages,
        int minBytesPerSource,
        boolean debug)
{
    public DecompressedPageCachePolicy
    {
        checkArgument(capacity > 0, "capacity must be positive");
        checkArgument(minSourcePages > 0, "minSourcePages must be positive");
        checkArgument(minBytesPerSource > 0, "minBytesPerSource must be positive");
    }

    public static DecompressedPageCachePolicy defaults()
    {
        return new DecompressedPageCachePolicy(true, 256 << 20, 16, 20 << 20, false);
    }

    public static DecompressedPageCachePolicy fromSystemProperties()
    {
        DecompressedPageCachePolicy defaults = defaults();
        return new DecompressedPageCachePolicy(
                Boolean.parseBoolean(System.getProperty("nitro.parquet.sharedDecompressedPages", "true")),
                Integer.getInteger("nitro.parquet.sharedDecompressedPageBytes", defaults.capacity()),
                Integer.getInteger("nitro.parquet.sharedDecompressedPageMinSourcePages", defaults.minSourcePages()),
                Integer.getInteger("nitro.parquet.sharedDecompressedPageMinBytesPerSource", defaults.minBytesPerSource()),
                Boolean.getBoolean("nitro.debug.sharedDecompressedPages"));
    }
}
