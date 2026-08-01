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
    private final ParquetMetadataCache metadataCache;
    private final DecompressedPageCachePolicy decompressedPageCachePolicy;
    private final ParquetReaderPolicy readerPolicy;
    private final ParquetNumericDecodeAdmissionPolicy numericDecodeAdmissionPolicy;
    private final ParquetLateMaterializationPolicy lateMaterializationPolicy;
    private final ParquetProgressiveFilterCompactionPolicy progressiveFilterCompactionPolicy;
    private final ParquetFilteredPayloadPolicy filteredPayloadPolicy;
    private final ParquetFilterWindowPolicy filterWindowPolicy;
    private final ParquetFilterEvaluationPolicy filterEvaluationPolicy;
    private final ParquetScanDiagnostics diagnostics;
    private final ParquetScanBatchPolicy batchPolicy;
    private final ParquetArenaPolicy arenaPolicy;

    public NitroParquetScanResources(
            DecompressedPageCachePolicy decompressedPageCachePolicy,
            ParquetReaderPolicy readerPolicy,
            ParquetNumericDecodeAdmissionPolicy numericDecodeAdmissionPolicy,
            ParquetLateMaterializationPolicy lateMaterializationPolicy,
            ParquetProgressiveFilterCompactionPolicy progressiveFilterCompactionPolicy,
            ParquetFilteredPayloadPolicy filteredPayloadPolicy,
            ParquetFilterWindowPolicy filterWindowPolicy,
            ParquetFilterEvaluationPolicy filterEvaluationPolicy,
            ParquetScanDiagnostics diagnostics,
            ParquetScanBatchPolicy batchPolicy)
    {
        this(
                decompressedPageCachePolicy,
                readerPolicy,
                numericDecodeAdmissionPolicy,
                lateMaterializationPolicy,
                progressiveFilterCompactionPolicy,
                filteredPayloadPolicy,
                filterWindowPolicy,
                filterEvaluationPolicy,
                diagnostics,
                batchPolicy,
                ParquetArenaPolicy.confined(),
                ParquetMetadataCachePolicy.defaults());
    }

    public NitroParquetScanResources(
            DecompressedPageCachePolicy decompressedPageCachePolicy,
            ParquetReaderPolicy readerPolicy,
            ParquetNumericDecodeAdmissionPolicy numericDecodeAdmissionPolicy,
            ParquetLateMaterializationPolicy lateMaterializationPolicy,
            ParquetProgressiveFilterCompactionPolicy progressiveFilterCompactionPolicy,
            ParquetFilteredPayloadPolicy filteredPayloadPolicy,
            ParquetFilterWindowPolicy filterWindowPolicy,
            ParquetFilterEvaluationPolicy filterEvaluationPolicy,
            ParquetScanDiagnostics diagnostics,
            ParquetScanBatchPolicy batchPolicy,
            ParquetArenaPolicy arenaPolicy)
    {
        this(
                decompressedPageCachePolicy,
                readerPolicy,
                numericDecodeAdmissionPolicy,
                lateMaterializationPolicy,
                progressiveFilterCompactionPolicy,
                filteredPayloadPolicy,
                filterWindowPolicy,
                filterEvaluationPolicy,
                diagnostics,
                batchPolicy,
                arenaPolicy,
                ParquetMetadataCachePolicy.defaults());
    }

    public NitroParquetScanResources(
            DecompressedPageCachePolicy decompressedPageCachePolicy,
            ParquetReaderPolicy readerPolicy,
            ParquetNumericDecodeAdmissionPolicy numericDecodeAdmissionPolicy,
            ParquetLateMaterializationPolicy lateMaterializationPolicy,
            ParquetProgressiveFilterCompactionPolicy progressiveFilterCompactionPolicy,
            ParquetFilteredPayloadPolicy filteredPayloadPolicy,
            ParquetFilterWindowPolicy filterWindowPolicy,
            ParquetFilterEvaluationPolicy filterEvaluationPolicy,
            ParquetScanDiagnostics diagnostics,
            ParquetScanBatchPolicy batchPolicy,
            ParquetArenaPolicy arenaPolicy,
            ParquetMetadataCachePolicy metadataCachePolicy)
    {
        this.decompressedPageCachePolicy = requireNonNull(decompressedPageCachePolicy, "decompressedPageCachePolicy is null");
        this.readerPolicy = requireNonNull(readerPolicy, "readerPolicy is null");
        this.numericDecodeAdmissionPolicy = requireNonNull(numericDecodeAdmissionPolicy, "numericDecodeAdmissionPolicy is null");
        this.lateMaterializationPolicy = requireNonNull(lateMaterializationPolicy, "lateMaterializationPolicy is null");
        this.progressiveFilterCompactionPolicy = requireNonNull(
                progressiveFilterCompactionPolicy, "progressiveFilterCompactionPolicy is null");
        this.filteredPayloadPolicy = requireNonNull(filteredPayloadPolicy, "filteredPayloadPolicy is null");
        this.filterWindowPolicy = requireNonNull(filterWindowPolicy, "filterWindowPolicy is null");
        this.filterEvaluationPolicy = requireNonNull(filterEvaluationPolicy, "filterEvaluationPolicy is null");
        this.diagnostics = requireNonNull(diagnostics, "diagnostics is null");
        this.batchPolicy = requireNonNull(batchPolicy, "batchPolicy is null");
        this.arenaPolicy = requireNonNull(arenaPolicy, "arenaPolicy is null");
        this.metadataCache = new ParquetMetadataCache(requireNonNull(metadataCachePolicy, "metadataCachePolicy is null"));
    }

    public static NitroParquetScanResources createDefault()
    {
        return createDefault(ParquetArenaPolicy.confined());
    }

    public static NitroParquetScanResources createDefault(ParquetArenaPolicy arenaPolicy)
    {
        ParquetLateMaterializationPolicy lateMaterializationPolicy =
                ParquetLateMaterializationPolicy.fromSystemProperties();
        return new NitroParquetScanResources(
                DecompressedPageCachePolicy.fromSystemProperties(),
                ParquetReaderPolicy.fromSystemProperties(),
                ParquetNumericDecodeAdmissionPolicy.fromSystemProperties(),
                lateMaterializationPolicy,
                ParquetProgressiveFilterCompactionPolicy.fromSystemProperties(lateMaterializationPolicy),
                ParquetFilteredPayloadPolicy.fromSystemProperties(),
                ParquetFilterWindowPolicy.fromSystemProperties(),
                ParquetFilterEvaluationPolicy.fromSystemProperties(),
                ParquetScanDiagnostics.fromSystemProperties(),
                ParquetScanBatchPolicy.fromSystemProperties(),
                arenaPolicy);
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

    ParquetMetadataCache metadataCache()
    {
        return metadataCache;
    }

    DecompressedPageCachePolicy decompressedPageCachePolicy()
    {
        return decompressedPageCachePolicy;
    }

    ParquetReaderPolicy readerPolicy()
    {
        return readerPolicy;
    }

    ParquetNumericDecodeAdmissionPolicy numericDecodeAdmissionPolicy()
    {
        return numericDecodeAdmissionPolicy;
    }

    ParquetLateMaterializationPolicy lateMaterializationPolicy()
    {
        return lateMaterializationPolicy;
    }

    ParquetProgressiveFilterCompactionPolicy progressiveFilterCompactionPolicy()
    {
        return progressiveFilterCompactionPolicy;
    }

    ParquetFilteredPayloadPolicy filteredPayloadPolicy()
    {
        return filteredPayloadPolicy;
    }

    ParquetFilterWindowPolicy filterWindowPolicy()
    {
        return filterWindowPolicy;
    }

    ParquetFilterEvaluationPolicy filterEvaluationPolicy()
    {
        return filterEvaluationPolicy;
    }

    ParquetScanDiagnostics diagnostics()
    {
        return diagnostics;
    }

    ParquetScanBatchPolicy batchPolicy()
    {
        return batchPolicy;
    }

    ParquetArenaPolicy arenaPolicy()
    {
        return arenaPolicy;
    }
}
