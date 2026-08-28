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
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
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

    @Test
    void testReusesMatchingConnectorInputIdentity()
            throws Exception
    {
        ParquetMetadataCache cache = new ParquetMetadataCache(new ParquetMetadataCachePolicy(2));
        AtomicInteger loads = new AtomicInteger();

        ParquetFile.Metadata first = cache.get("s3://bucket/file", 10, "etag-1", () -> metadata(loads.incrementAndGet()));
        assertThat(cache.get("s3://bucket/file", 10, "etag-1", () -> metadata(loads.incrementAndGet())))
                .isSameAs(first);
        cache.get("s3://bucket/file", 10, "etag-2", () -> metadata(loads.incrementAndGet()));

        assertThat(loads).hasValue(2);
    }

    @Test
    void testVersionedInputReusesParsedFooter()
            throws Exception
    {
        ParquetMetadataCache cache = new ParquetMetadataCache(new ParquetMetadataCachePolicy(2));
        byte[] data = parquetFile();
        CountingInput firstInput = new CountingInput(data);
        CountingInput secondInput = new CountingInput(data);

        try (ParquetFile ignored = ParquetFile.open(firstInput, cache)) {
            assertThat(firstInput.reads()).isEqualTo(2);
        }
        try (ParquetFile ignored = ParquetFile.open(secondInput, cache)) {
            assertThat(secondInput.reads()).isZero();
        }
    }

    private static byte[] parquetFile()
            throws Exception
    {
        FileMetaData metadata = new FileMetaData(
                1,
                List.of(new SchemaElement("root").setNum_children(0)),
                0,
                List.of());
        ByteArrayOutputStream footer = new ByteArrayOutputStream();
        Util.writeFileMetaData(metadata, footer);
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.write(footer.toByteArray());
        file.write(ByteBuffer.allocate(Integer.BYTES).order(LITTLE_ENDIAN).putInt(footer.size()).array());
        file.write(new byte[] {'P', 'A', 'R', '1'});
        return file.toByteArray();
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

    private static final class CountingInput
            implements ParquetInput
    {
        private final byte[] data;
        private final AtomicInteger reads = new AtomicInteger();

        private CountingInput(byte[] data)
        {
            this.data = data;
        }

        @Override
        public String id()
        {
            return "test://versioned.parquet";
        }

        @Override
        public long size()
        {
            return data.length;
        }

        @Override
        public Optional<String> metadataVersion()
        {
            return Optional.of("version-1");
        }

        @Override
        public ParquetInputRange readRange(long offset, int length)
        {
            reads.incrementAndGet();
            return ParquetInputRange.retained(MemorySegment.ofArray(data).asSlice(offset, length));
        }

        @Override
        public void close() {}

        private int reads()
        {
            return reads.get();
        }
    }
}
