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

import org.weakref.nitro.data.AllocationResourcesOwner;
import org.weakref.nitro.data.Allocator;

import java.util.IdentityHashMap;
import java.util.Map;

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
    private final ParquetRuntimeFilterPolicy runtimeFilterPolicy;
    private final ParquetScanDiagnostics diagnostics;
    private final ParquetScanBatchPolicy batchPolicy;
    private final ParquetArenaPolicy arenaPolicy;
    private final Map<AllocationResourcesOwner, DecompressedPageCacheState> decompressedPageCaches = new IdentityHashMap<>();

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
                ParquetMetadataCachePolicy.defaults(),
                ParquetRuntimeFilterPolicy.defaults());
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
                ParquetMetadataCachePolicy.defaults(),
                ParquetRuntimeFilterPolicy.defaults());
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
                metadataCachePolicy,
                ParquetRuntimeFilterPolicy.defaults());
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
            ParquetMetadataCachePolicy metadataCachePolicy,
            ParquetRuntimeFilterPolicy runtimeFilterPolicy)
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
        this.runtimeFilterPolicy = requireNonNull(runtimeFilterPolicy, "runtimeFilterPolicy is null");
    }

    public static NitroParquetScanResources createDefault()
    {
        return createDefault(ParquetArenaPolicy.confined());
    }

    public static NitroParquetScanResources createDefault(ParquetArenaPolicy arenaPolicy)
    {
        return createDefault(arenaPolicy, ParquetRuntimeFilterPolicy.defaults());
    }

    public static NitroParquetScanResources createDefault(
            ParquetArenaPolicy arenaPolicy,
            ParquetScanBatchPolicy batchPolicy)
    {
        return createDefault(arenaPolicy, ParquetRuntimeFilterPolicy.defaults(), batchPolicy);
    }

    public static NitroParquetScanResources createDefault(
            ParquetArenaPolicy arenaPolicy,
            ParquetRuntimeFilterPolicy runtimeFilterPolicy)
    {
        return createDefault(
                arenaPolicy,
                runtimeFilterPolicy,
                ParquetScanBatchPolicy.fromSystemProperties());
    }

    public static NitroParquetScanResources createDefault(
            ParquetArenaPolicy arenaPolicy,
            ParquetRuntimeFilterPolicy runtimeFilterPolicy,
            ParquetScanBatchPolicy batchPolicy)
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
                requireNonNull(batchPolicy, "batchPolicy is null"),
                arenaPolicy,
                ParquetMetadataCachePolicy.defaults(),
                runtimeFilterPolicy);
    }

    Object batchBufferPool()
    {
        return batchBufferPool;
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

    ParquetRuntimeFilterPolicy runtimeFilterPolicy()
    {
        return runtimeFilterPolicy;
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

    synchronized DecompressedPageCacheLease acquireDecompressedPageCache(Allocator allocator)
    {
        requireNonNull(allocator, "allocator is null");
        AllocationResourcesOwner owner = allocator.resourcesOwner();
        DecompressedPageCacheState state = decompressedPageCaches.get(owner);
        if (state == null) {
            state = new DecompressedPageCacheState(new DecompressedPageCache(
                    allocator.nativeBuffers(),
                    decompressedPageCachePolicy,
                    allocator.nativeBufferAdvice()));
            decompressedPageCaches.put(owner, state);
        }
        state.references++;
        return new DecompressedPageCacheLease(this, owner, state.cache);
    }

    private synchronized void releaseDecompressedPageCache(AllocationResourcesOwner owner)
    {
        DecompressedPageCacheState state = decompressedPageCaches.get(owner);
        if (state == null || state.references <= 0) {
            throw new IllegalStateException("Decompressed page cache is not acquired");
        }
        state.references--;
        if (state.references == 0) {
            decompressedPageCaches.remove(owner);
            state.cache.close();
        }
    }

    static final class DecompressedPageCacheLease
            implements AutoCloseable
    {
        private final NitroParquetScanResources resources;
        private final AllocationResourcesOwner owner;
        private final DecompressedPageCache cache;
        private boolean closed;

        private DecompressedPageCacheLease(
                NitroParquetScanResources resources,
                AllocationResourcesOwner owner,
                DecompressedPageCache cache)
        {
            this.resources = resources;
            this.owner = owner;
            this.cache = cache;
        }

        DecompressedPageCache value()
        {
            if (closed) {
                throw new IllegalStateException("Decompressed page cache lease is closed");
            }
            return cache;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            resources.releaseDecompressedPageCache(owner);
        }
    }

    private static final class DecompressedPageCacheState
    {
        private final DecompressedPageCache cache;
        private int references;

        private DecompressedPageCacheState(DecompressedPageCache cache)
        {
            this.cache = cache;
        }
    }
}
