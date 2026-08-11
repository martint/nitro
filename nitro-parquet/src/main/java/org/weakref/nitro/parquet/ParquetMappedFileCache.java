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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

final class ParquetMappedFileCache
        implements AutoCloseable
{
    private final ParquetMetadataCache metadataCache;
    private final int maxEntries;
    private final long maxMappedBytes;
    private final Map<Key, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private long mappedBytes;
    private boolean closed;

    ParquetMappedFileCache(ParquetMetadataCache metadataCache, ParquetMappedFileCachePolicy policy)
    {
        this.metadataCache = requireNonNull(metadataCache, "metadataCache is null");
        policy = requireNonNull(policy, "policy is null");
        maxEntries = policy.maxEntries();
        maxMappedBytes = policy.maxMappedBytes();
    }

    Lease acquire(Path path)
    {
        requireNonNull(path, "path is null");
        Key key = key(path);
        return acquire(key);
    }

    private synchronized Lease acquire(Key key)
    {
        if (closed) {
            throw new IllegalStateException("Mapped file cache is closed");
        }
        Entry entry = entries.get(key);
        if (entry == null) {
            ParquetFile file = ParquetFile.open(key.path(), ParquetArenaPolicy.shared(), metadataCache);
            entry = new Entry(file, key.size());
            entry.references++;
            entries.put(key, entry);
            mappedBytes = Math.addExact(mappedBytes, key.size());
            evict();
            return new Lease(this, entry);
        }
        entry.references++;
        return new Lease(this, entry);
    }

    private void evict()
    {
        var iterator = entries.entrySet().iterator();
        while ((entries.size() > maxEntries || mappedBytes > maxMappedBytes) && iterator.hasNext()) {
            Entry entry = iterator.next().getValue();
            if (entry.references != 0) {
                continue;
            }
            iterator.remove();
            mappedBytes -= entry.bytes;
            entry.file.close();
        }
    }

    private synchronized void release(Entry entry)
    {
        if (entry.references <= 0) {
            throw new IllegalStateException("Mapped file is not acquired");
        }
        entry.references--;
        evict();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        if (entries.values().stream().anyMatch(entry -> entry.references != 0)) {
            throw new IllegalStateException("Mapped file cache closed with active readers");
        }
        closed = true;
        entries.values().forEach(entry -> entry.file.close());
        entries.clear();
        mappedBytes = 0;
    }

    private static Key key(Path path)
    {
        Path normalized = path.toAbsolutePath().normalize();
        try {
            BasicFileAttributes attributes = Files.readAttributes(normalized, BasicFileAttributes.class);
            return new Key(normalized, attributes.size(), attributes.lastModifiedTime());
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect Parquet file: " + path, exception);
        }
    }

    static final class Lease
            implements AutoCloseable
    {
        private final ParquetMappedFileCache cache;
        private Entry entry;

        private Lease(ParquetMappedFileCache cache, Entry entry)
        {
            this.cache = cache;
            this.entry = entry;
        }

        ParquetFile file()
        {
            if (entry == null) {
                throw new IllegalStateException("Mapped file lease is closed");
            }
            return entry.file;
        }

        @Override
        public void close()
        {
            Entry acquired = entry;
            if (acquired == null) {
                return;
            }
            entry = null;
            cache.release(acquired);
        }
    }

    private static final class Entry
    {
        private final ParquetFile file;
        private final long bytes;
        private int references;

        private Entry(ParquetFile file, long bytes)
        {
            this.file = requireNonNull(file, "file is null");
            this.bytes = bytes;
        }
    }

    private record Key(Path path, long size, FileTime modifiedTime) {}
}
