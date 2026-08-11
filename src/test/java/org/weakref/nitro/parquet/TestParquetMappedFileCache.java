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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.weakref.nitro.clickbench.ClickBenchHitsSupport;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TestParquetMappedFileCache
{
    @TempDir
    Path temporaryDirectory;

    @Test
    void testReusesMappingAndEvictsIdleLeastRecentlyUsedFile()
            throws Exception
    {
        Path firstPath = ClickBenchHitsSupport.writeHitsFixture(temporaryDirectory.resolve("first.parquet"), 3);
        Path secondPath = ClickBenchHitsSupport.writeHitsFixture(temporaryDirectory.resolve("second.parquet"), 3);
        ParquetMetadataCache metadata = new ParquetMetadataCache(ParquetMetadataCachePolicy.defaults());

        try (ParquetMappedFileCache cache = new ParquetMappedFileCache(
                metadata,
                new ParquetMappedFileCachePolicy(1, Long.MAX_VALUE))) {
            ParquetFile first;
            try (ParquetMappedFileCache.Lease initial = cache.acquire(firstPath);
                    ParquetMappedFileCache.Lease shared = cache.acquire(firstPath)) {
                first = initial.file();
                assertThat(shared.file()).isSameAs(first);
            }

            try (ParquetMappedFileCache.Lease ignored = cache.acquire(secondPath)) {
                assertThat(ignored.file()).isNotSameAs(first);
            }
            try (ParquetMappedFileCache.Lease reloaded = cache.acquire(firstPath)) {
                assertThat(reloaded.file()).isNotSameAs(first);
            }
        }
    }
}
