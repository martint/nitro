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
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/** Forward-only native reader for one nested physical leaf and its repetition/definition event stream. */
final class NestedLeafReader
        implements NestedLeafEventSource
{
    private record Chunk(ParquetFile file, ColumnMetaData metadata) {}

    private static final int DECOMPRESSION_SLACK = 32;

    private final ParquetSchema.Primitive leaf;
    private final NestedPageDecoder decoder;
    private final PhysicalValueDecoder valueDecoder;
    private final boolean decodeValues;
    private final List<Chunk> chunks = new ArrayList<>();
    private final FastPageHeaderReader header = new FastPageHeaderReader();
    private final NestedEventWindow eventWindow = new NestedEventWindow();
    private final Map<Thread, SnappyDecompressor> snappyByThread = new ConcurrentHashMap<>();
    private final Arena scratchArena = Arena.ofShared();

    private int chunkIndex = -1;
    private ParquetInputRange chunkRange;
    private MemorySegment chunkData;
    private long pagePosition;
    private long chunkEnd;
    private MemorySegment decompressionBuffer;
    private long decompressionCapacity;
    private int eventIndex;
    private int currentEvent = -1;
    private long consumedPageBytes;
    private boolean closed;

    NestedLeafReader(ParquetSchema.Primitive leaf, RleReaderPolicy rlePolicy, PrimitiveArrayPool arrayPool)
    {
        this(leaf, rlePolicy, arrayPool, true);
    }

    NestedLeafReader(ParquetSchema.Primitive leaf, RleReaderPolicy rlePolicy, PrimitiveArrayPool arrayPool, boolean decodeValues)
    {
        this(leaf, rlePolicy, arrayPool, decodeValues, true);
    }

    NestedLeafReader(
            ParquetSchema.Primitive leaf,
            RleReaderPolicy rlePolicy,
            PrimitiveArrayPool arrayPool,
            boolean decodeValues,
            boolean decodeRepetitionLevels)
    {
        this.leaf = requireNonNull(leaf, "leaf is null");
        this.decoder = new NestedPageDecoder(
                leaf.maximumRepetitionLevel(),
                leaf.maximumDefinitionLevel(),
                requireNonNull(rlePolicy, "rlePolicy is null"),
                requireNonNull(arrayPool, "arrayPool is null"),
                decodeRepetitionLevels);
        this.valueDecoder = PhysicalValueDecoders.create(leaf, requireNonNull(arrayPool, "arrayPool is null"));
        this.decodeValues = decodeValues;
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

    void appendFlatRows(NestedValueAccumulator accumulator, int rowCount)
    {
        if (leaf.maximumRepetitionLevel() != 0) {
            throw new IllegalStateException("Bulk nested append requires a non-repeated leaf");
        }
        int remaining = rowCount;
        while (remaining > 0) {
            while (eventIndex >= decoder.eventCount()) {
                if (!decodeNextPage()) {
                    throw new IllegalArgumentException("Nested primitive event stream ended before requested rows");
                }
                eventIndex = 0;
            }
            int count = Math.min(remaining, decoder.eventCount() - eventIndex);
            decoder.appendEvents(accumulator, valueDecoder, eventIndex, count);
            eventIndex += count;
            currentEvent = -1;
            remaining -= count;
        }
    }

    @Override
    public NestedEventWindow eventWindow()
    {
        while (eventIndex >= decoder.eventCount()) {
            if (!decodeNextPage()) {
                return null;
            }
            eventIndex = 0;
        }
        decoder.resetWindow(eventWindow, valueDecoder, eventIndex);
        currentEvent = -1;
        return eventWindow;
    }

    @Override
    public boolean hasRepetitionLevels()
    {
        return decoder.hasRepetitionLevels();
    }

    @Override
    public void advanceEvents(int count)
    {
        if (count < 0 || count > decoder.eventCount() - eventIndex) {
            throw new IndexOutOfBoundsException("Invalid nested event advance: " + count);
        }
        eventIndex += count;
        currentEvent = -1;
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
            consumedPageBytes = Math.addExact(consumedPageBytes, compressedSize);
            pagePosition = nextPage;
            if (header.type() == PageType.DICTIONARY_PAGE.getValue()) {
                if (decodeValues) {
                    valueDecoder.decodeDictionary(body, header.valueCount(), Encoding.findByValue(header.encoding()));
                }
                continue;
            }
            if (header.type() == PageType.DATA_PAGE.getValue()) {
                if (!decodeValues) {
                    decoder.decodeLevelsDataPageV1(body, header.valueCount());
                    return true;
                }
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
        snappyByThread.computeIfAbsent(Thread.currentThread(), _ -> SnappyDecompressor.create())
                .decompress(input.asSlice(offset, compressedSize), decompressionBuffer.asSlice(0, uncompressedSize));
        return decompressionBuffer.asSlice(0, required);
    }

    private int currentEvent()
    {
        if (currentEvent < 0) {
            throw new IllegalStateException("Nested leaf reader is not positioned on an event");
        }
        return currentEvent;
    }

    long consumedPageBytes()
    {
        return consumedPageBytes;
    }

    boolean pageExhausted()
    {
        return eventIndex >= decoder.eventCount();
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
            decoder.close();
            valueDecoder.close();
            snappyByThread.clear();
            scratchArena.close();
        }
    }
}
