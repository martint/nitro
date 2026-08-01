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
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.Objects.requireNonNull;

final class ParquetMetadataCache
{
    private final int maxEntries;
    private final Map<Key, CompletableFuture<ParquetFile.Metadata>> entries = new LinkedHashMap<>(16, 0.75f, true);

    ParquetMetadataCache(ParquetMetadataCachePolicy policy)
    {
        maxEntries = requireNonNull(policy, "policy is null").maxEntries();
    }

    ParquetFile.Metadata get(Path path, long size, FileTime modifiedTime, Loader loader)
            throws IOException
    {
        Key key = new Key(path.toAbsolutePath().normalize(), size, requireNonNull(modifiedTime, "modifiedTime is null"));
        CompletableFuture<ParquetFile.Metadata> future;
        boolean load = false;
        synchronized (this) {
            future = entries.get(key);
            if (future == null) {
                future = new CompletableFuture<>();
                entries.put(key, future);
                load = true;
                while (entries.size() > maxEntries) {
                    entries.remove(entries.keySet().iterator().next());
                }
            }
        }

        if (load) {
            try {
                future.complete(requireNonNull(loader, "loader is null").load());
            }
            catch (IOException | RuntimeException | Error failure) {
                future.completeExceptionally(failure);
                synchronized (this) {
                    entries.remove(key, future);
                }
            }
        }
        try {
            return future.join();
        }
        catch (CompletionException failure) {
            if (failure.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw failure;
        }
    }

    @FunctionalInterface
    interface Loader
    {
        ParquetFile.Metadata load()
                throws IOException;
    }

    private record Key(Path path, long size, FileTime modifiedTime) {}
}
