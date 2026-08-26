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

import io.airlift.compress.v3.snappy.SnappyDecompressor;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageType;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Forward-only native reader for one nested physical leaf and its repetition/definition event stream. */
final class NestedLeafReader
        implements NestedLeafCursor
{
    private record Chunk(ParquetFile file, ColumnMetaData metadata) {}

    private static final int DECOMPRESSION_SLACK = 32;

    private final ParquetSchema.Primitive leaf;
    private final NestedPageDecoder decoder;
    private final PhysicalValueDecoder valueDecoder;
    private final List<Chunk> chunks = new ArrayList<>();
    private final FastPageHeaderReader header = new FastPageHeaderReader();
    private final SnappyDecompressor snappy = SnappyDecompressor.create();
    private final Arena scratchArena = Arena.ofConfined();

    private int chunkIndex = -1;
    private ParquetInputRange chunkRange;
    private MemorySegment chunkData;
    private long pagePosition;
    private long chunkEnd;
    private MemorySegment decompressionBuffer;
    private long decompressionCapacity;
    private int eventIndex;
    private int currentEvent = -1;
    private boolean closed;

    NestedLeafReader(ParquetSchema.Primitive leaf, RleReaderPolicy rlePolicy)
    {
        this.leaf = requireNonNull(leaf, "leaf is null");
        this.decoder = new NestedPageDecoder(
                leaf.maximumRepetitionLevel(),
                leaf.maximumDefinitionLevel(),
                requireNonNull(rlePolicy, "rlePolicy is null"));
        this.valueDecoder = PhysicalValueDecoders.create(leaf);
    }

    void addChunk(ParquetFile file, ColumnMetaData metadata)
    {
        checkOpen();
        chunks.add(new Chunk(requireNonNull(file, "file is null"), requireNonNull(metadata, "metadata is null")));
    }

    @Override
    public boolean next()
    {
        checkOpen();
        while (eventIndex >= decoder.eventCount()) {
            if (!decodeNextPage()) {
                currentEvent = -1;
                return false;
            }
            eventIndex = 0;
        }
        currentEvent = eventIndex++;
        return true;
    }

    @Override
    public int repetitionLevel()
    {
        return decoder.repetitionLevel(currentEvent());
    }

    @Override
    public int definitionLevel()
    {
        return decoder.definitionLevel(currentEvent());
    }

    @Override
    public boolean hasValue()
    {
        return decoder.hasValue(currentEvent());
    }

    @Override
    public PhysicalValueDecoder valueDecoder()
    {
        return valueDecoder;
    }

    @Override
    public int valueOrdinal()
    {
        return decoder.valueOrdinal(currentEvent());
    }

    @Override
    public int dictionaryId()
    {
        int ordinal = decoder.valueOrdinal(currentEvent());
        return decoder.dictionaryId(ordinal);
    }

    private boolean decodeNextPage()
    {
        while (true) {
            if (chunkData == null || pagePosition >= chunkEnd) {
                if (!advanceChunk()) {
                    return false;
                }
            }
            long bodyPosition = header.read(chunkData, pagePosition, chunkEnd);
            int compressedSize = header.compressedSize();
            long nextPage = bodyPosition + compressedSize;
            if (nextPage > chunkEnd) {
                throw new IllegalArgumentException("Parquet page exceeds nested leaf chunk '" + String.join(".", leaf.path()) + "'");
            }
            MemorySegment body = decompress(
                    chunkData,
                    bodyPosition,
                    compressedSize,
                    header.uncompressedSize(),
                    chunks.get(chunkIndex).metadata().codec);
            pagePosition = nextPage;
            if (header.type() == PageType.DICTIONARY_PAGE.getValue()) {
                valueDecoder.decodeDictionary(body, header.valueCount(), Encoding.findByValue(header.encoding()));
                continue;
            }
            if (header.type() == PageType.DATA_PAGE.getValue()) {
                long valueOffset = decoder.decodeDataPageV1(
                        body,
                        header.valueCount(),
                        Encoding.findByValue(header.encoding()),
                        valueDecoder.dictionarySize());
                if (valueOffset >= 0) {
                    valueDecoder.decodePlain(body, valueOffset, decoder.physicalValueCount());
                }
                return true;
            }
            throw new UnsupportedParquetFeatureException(
                    "Native nested Parquet reader does not support page type " + header.type() + " at '" + String.join(".", leaf.path()) + "'");
        }
    }

    private boolean advanceChunk()
    {
        closeChunk();
        chunkIndex++;
        if (chunkIndex >= chunks.size()) {
            return false;
        }
        Chunk chunk = chunks.get(chunkIndex);
        ColumnMetaData metadata = chunk.metadata();
        long start = metadata.dictionary_page_offset > 0 && metadata.dictionary_page_offset < metadata.data_page_offset
                ? metadata.dictionary_page_offset
                : metadata.data_page_offset;
        chunkRange = chunk.file().readRange(start, metadata.total_compressed_size);
        chunkData = chunkRange.data();
        pagePosition = 0;
        chunkEnd = chunkData.byteSize();
        valueDecoder.resetDictionary();
        return true;
    }

    private MemorySegment decompress(
            MemorySegment input,
            long offset,
            int compressedSize,
            int uncompressedSize,
            CompressionCodec codec)
    {
        if (compressedSize < 0 || uncompressedSize < 0) {
            throw new IllegalArgumentException("Negative nested Parquet page size");
        }
        if (codec == CompressionCodec.UNCOMPRESSED) {
            return input.asSlice(offset, compressedSize);
        }
        if (codec != CompressionCodec.SNAPPY) {
            throw new UnsupportedParquetFeatureException(
                    "Native nested Parquet reader does not support compression codec " + codec + " at '" + String.join(".", leaf.path()) + "'");
        }
        long required = (long) uncompressedSize + DECOMPRESSION_SLACK;
        if (decompressionCapacity < required) {
            decompressionBuffer = scratchArena.allocate(required);
            decompressionCapacity = required;
        }
        snappy.decompress(input.asSlice(offset, compressedSize), decompressionBuffer.asSlice(0, uncompressedSize));
        return decompressionBuffer.asSlice(0, required);
    }

    private int currentEvent()
    {
        if (currentEvent < 0) {
            throw new IllegalStateException("Nested leaf reader is not positioned on an event");
        }
        return currentEvent;
    }

    private void closeChunk()
    {
        if (chunkRange != null) {
            chunkRange.close();
            chunkRange = null;
            chunkData = null;
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Nested leaf reader is closed");
        }
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            closeChunk();
            scratchArena.close();
        }
    }
}
