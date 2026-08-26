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

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TestParquetMetadataCache
{
    @Test
    void testReusesMatchingFileIdentityAndEvictsLeastRecentlyUsed()
            throws Exception
    {
        ParquetMetadataCache cache = new ParquetMetadataCache(new ParquetMetadataCachePolicy(2));
        AtomicInteger loads = new AtomicInteger();
        FileTime firstVersion = FileTime.fromMillis(1);

        ParquetFile.Metadata first = cache.get(Path.of("first"), 10, firstVersion, () -> metadata(loads.incrementAndGet()));
        assertThat(cache.get(Path.of("first"), 10, firstVersion, () -> metadata(loads.incrementAndGet()))).isSameAs(first);
        cache.get(Path.of("second"), 20, firstVersion, () -> metadata(loads.incrementAndGet()));
        cache.get(Path.of("third"), 30, firstVersion, () -> metadata(loads.incrementAndGet()));
        cache.get(Path.of("first"), 10, firstVersion, () -> metadata(loads.incrementAndGet()));

        assertThat(loads).hasValue(4);
    }

    @Test
    void testFileVersionIsPartOfIdentity()
            throws Exception
    {
        ParquetMetadataCache cache = new ParquetMetadataCache(new ParquetMetadataCachePolicy(2));
        AtomicInteger loads = new AtomicInteger();

        cache.get(Path.of("file"), 10, FileTime.fromMillis(1), () -> metadata(loads.incrementAndGet()));
        cache.get(Path.of("file"), 10, FileTime.fromMillis(2), () -> metadata(loads.incrementAndGet()));

        assertThat(loads).hasValue(2);
    }

    private static ParquetFile.Metadata metadata(int rows)
    {
        FileMetaData footer = new FileMetaData(1, List.of(), rows, List.of());
        return new ParquetFile.Metadata(
                footer,
                ParquetSchema.parse(List.of(new SchemaElement("root").setNum_children(0))),
                List.of(),
                Map.of());
    }
}
