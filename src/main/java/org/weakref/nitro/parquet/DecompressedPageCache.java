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

import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.data.NativeBufferAdvice.collapseHugePages;
import static org.weakref.nitro.data.NativeBufferAdvice.preferHugePages;

/**
 * Query-local immutable page reuse for physical columns consumed by more than one reader.
 *
 * <p>The cache owns one bounded direct slab. The slab itself participates in the embedding engine's primitive-buffer
 * budget, so subsequent queries can recycle the same native storage while cache metadata and page identities remain
 * query-local. A source becomes eligible only after two independent readers register it and it demonstrates enough
 * page misses to amortize retaining the stream. Single-scan and short repeated columns retain the ordinary one-page
 * scratch lifecycle, and the slab is acquired lazily only after some source earns admission.
 */
public final class DecompressedPageCache
        implements AutoCloseable
{
    private static final int DEFAULT_CAPACITY = 256 << 20;
    private static final int DEFAULT_MIN_SOURCE_PAGES = 16;
    private static final int DEFAULT_MIN_BYTES_PER_SOURCE = 20 << 20;
    private static final boolean DEBUG = Boolean.getBoolean("nitro.debug.sharedDecompressedPages");

    private final PrimitiveArrayPool pool;
    private final int capacity;
    private final int minSourcePages;
    private final int maxReusableSources;
    private final Map<Source, Integer> consumers = new HashMap<>();
    private final Map<Source, Set<PageKey>> candidatePages = new HashMap<>();
    private final Set<Source> admittedSources = new HashSet<>();
    private final Map<PageKey, MemorySegment> pages = new HashMap<>();
    private Slab slabStorage;
    private MemorySegment slab;
    private int nextOffset;
    private int hits;
    private int misses;
    private int capacityBypasses;
    private int reusableSourceCount;
    private boolean closed;

    public DecompressedPageCache(PrimitiveArrayPool pool)
    {
        this(
                pool,
                Integer.getInteger("nitro.parquet.sharedDecompressedPageBytes", DEFAULT_CAPACITY),
                Integer.getInteger("nitro.parquet.sharedDecompressedPageMinSourcePages", DEFAULT_MIN_SOURCE_PAGES),
                Integer.getInteger("nitro.parquet.sharedDecompressedPageMinBytesPerSource", DEFAULT_MIN_BYTES_PER_SOURCE));
    }

    DecompressedPageCache(PrimitiveArrayPool pool, int capacity)
    {
        this(pool, capacity, DEFAULT_MIN_SOURCE_PAGES, DEFAULT_MIN_BYTES_PER_SOURCE);
    }

    DecompressedPageCache(PrimitiveArrayPool pool, int capacity, int minSourcePages)
    {
        this(pool, capacity, minSourcePages, DEFAULT_MIN_BYTES_PER_SOURCE);
    }

    DecompressedPageCache(PrimitiveArrayPool pool, int capacity, int minSourcePages, int minBytesPerSource)
    {
        this.pool = requireNonNull(pool, "pool is null");
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        if (minSourcePages <= 0) {
            throw new IllegalArgumentException("minSourcePages must be positive");
        }
        if (minBytesPerSource <= 0) {
            throw new IllegalArgumentException("minBytesPerSource must be positive");
        }
        this.capacity = capacity;
        this.minSourcePages = minSourcePages;
        this.maxReusableSources = Math.max(1, capacity / minBytesPerSource);
    }

    public void register(Source source)
    {
        checkOpen();
        int count = consumers.merge(requireNonNull(source, "source is null"), 1, Integer::sum);
        if (count == 2) {
            reusableSourceCount++;
        }
    }

    /**
     * Returns whether this physical column is consumed by multiple independently constructed readers in the current
     * query. Consumers may use this execution-time fact to select a representation whose setup is amortized by the
     * repeated source; page identity and reader state remain private to each consumer.
     */
    public boolean hasMultipleConsumers(Source source)
    {
        return consumerCount(source) >= 2;
    }

    public int consumerCount(Source source)
    {
        checkOpen();
        return consumers.getOrDefault(requireNonNull(source, "source is null"), 0);
    }

    public MemorySegment lookup(Source source, long offset, int compressedSize, int uncompressedSize)
    {
        checkOpen();
        if (!isReusable(source)) {
            return null;
        }
        PageKey key = new PageKey(source, offset, compressedSize, uncompressedSize);
        MemorySegment page = pages.get(key);
        if (page == null) {
            misses++;
            observeCandidate(key);
        }
        else {
            hits++;
        }
        return page;
    }

    public Reservation reserve(Source source, long offset, int compressedSize, int uncompressedSize, int slack)
    {
        checkOpen();
        if (!isReusable(source)) {
            return null;
        }
        if (!admittedSources.contains(source)) {
            return null;
        }
        PageKey key = new PageKey(source, offset, compressedSize, uncompressedSize);
        if (pages.containsKey(key)) {
            throw new IllegalStateException("Page is already cached");
        }
        int alignedOffset = (nextOffset + 7) & ~7;
        int length = Math.addExact(uncompressedSize, slack);
        if (length > capacity - alignedOffset) {
            capacityBypasses++;
            return null;
        }
        ensureSlab();
        nextOffset = alignedOffset + length;
        return new Reservation(key, slab.asSlice(alignedOffset, length));
    }

    public void commit(Reservation reservation)
    {
        checkOpen();
        requireNonNull(reservation, "reservation is null");
        MemorySegment previous = pages.putIfAbsent(reservation.key, reservation.segment);
        if (previous != null) {
            throw new IllegalStateException("Page was committed twice");
        }
    }

    int cachedPageCount()
    {
        return pages.size();
    }

    int usedBytes()
    {
        return nextOffset;
    }

    private boolean isReusable(Source source)
    {
        return reusableSourceCount <= maxReusableSources && hasMultipleConsumers(source);
    }

    private void observeCandidate(PageKey key)
    {
        Source source = key.source();
        if (!isReusable(source) || admittedSources.contains(source)) {
            return;
        }
        Set<PageKey> candidates = candidatePages.computeIfAbsent(source, ignored -> new HashSet<>());
        candidates.add(key);
        if (candidates.size() >= minSourcePages) {
            candidatePages.remove(source);
            admittedSources.add(source);
        }
    }

    private void ensureSlab()
    {
        if (slab != null) {
            return;
        }
        Slab borrowed = pool.borrow(Slab.class, capacity, Slab.class);
        slabStorage = borrowed == null ? new Slab(capacity) : borrowed;
        slab = slabStorage.segment;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Cache is closed");
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (DEBUG) {
            System.err.printf("[shared-decompressed-pages] sources=%d pages=%d hits=%d misses=%d capacityBypasses=%d used=%d capacity=%d%n",
                    reusableSourceCount, pages.size(), hits, misses, capacityBypasses, nextOffset, capacity);
        }
        pages.clear();
        consumers.clear();
        candidatePages.clear();
        admittedSources.clear();
        if (slabStorage != null) {
            slabStorage.finishUse(nextOffset);
            pool.retain(Slab.class, capacity, capacity, slabStorage);
        }
    }

    public record Source(Path path, String column)
    {
        public Source
        {
            path = requireNonNull(path, "path is null").toAbsolutePath().normalize();
            column = requireNonNull(column, "column is null");
        }
    }

    public static final class Reservation
    {
        private final PageKey key;
        private final MemorySegment segment;

        private Reservation(PageKey key, MemorySegment segment)
        {
            this.key = key;
            this.segment = segment;
        }

        public MemorySegment segment()
        {
            return segment;
        }
    }

    private record PageKey(Source source, long offset, int compressedSize, int uncompressedSize) {}

    private static final class Slab
    {
        private static final int HUGE_PAGE_BYTES = 2 << 20;

        private final java.nio.ByteBuffer buffer;
        private final MemorySegment segment;
        private int preparedBytes;

        private Slab(int capacity)
        {
            buffer = java.nio.ByteBuffer.allocateDirect(capacity);
            buffer.clear();
            segment = MemorySegment.ofBuffer(buffer);
            preferHugePages(segment);
        }

        private void finishUse(int usedBytes)
        {
            int prepareThrough = usedBytes & -HUGE_PAGE_BYTES;
            if (prepareThrough <= preparedBytes) {
                return;
            }
            collapseHugePages(segment.asSlice(preparedBytes, prepareThrough - preparedBytes));
            // A failed or unsupported advisory must not become a steady-state tax.
            preparedBytes = prepareThrough;
        }
    }
}
