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

import org.weakref.nitro.parquet.DecompressedPageCachePolicy;
import org.weakref.nitro.parquet.RleReaderPolicy;

import static java.util.Objects.requireNonNull;

/**
 * Connector-owned sharing domains for native Parquet scan instances.
 *
 * <p>A query or connector factory creates one owner and passes it to every scan that should coordinate batch-buffer
 * recycling, decompressed-page reuse, and direct-decode admission. Separate owners are completely isolated. The
 * identities remain private to the Parquet implementation and are never exposed through Nitro's engine resources or
 * connector SPI.
 */
public final class NitroParquetScanResources
{
    private final Object batchBufferPool = new Object();
    private final Object decompressedPageCache = new Object();
    private final Object directNumericBatchDecodeAdmission = new Object();
    private final DecompressedPageCachePolicy decompressedPageCachePolicy;
    private final RleReaderPolicy rleReaderPolicy;

    public NitroParquetScanResources(
            DecompressedPageCachePolicy decompressedPageCachePolicy,
            RleReaderPolicy rleReaderPolicy)
    {
        this.decompressedPageCachePolicy = requireNonNull(decompressedPageCachePolicy, "decompressedPageCachePolicy is null");
        this.rleReaderPolicy = requireNonNull(rleReaderPolicy, "rleReaderPolicy is null");
    }

    public static NitroParquetScanResources createDefault()
    {
        return new NitroParquetScanResources(
                DecompressedPageCachePolicy.fromSystemProperties(),
                RleReaderPolicy.fromSystemProperties());
    }

    Object batchBufferPool()
    {
        return batchBufferPool;
    }

    Object decompressedPageCache()
    {
        return decompressedPageCache;
    }

    Object directNumericBatchDecodeAdmission()
    {
        return directNumericBatchDecodeAdmission;
    }

    DecompressedPageCachePolicy decompressedPageCachePolicy()
    {
        return decompressedPageCachePolicy;
    }

    RleReaderPolicy rleReaderPolicy()
    {
        return rleReaderPolicy;
    }
}
