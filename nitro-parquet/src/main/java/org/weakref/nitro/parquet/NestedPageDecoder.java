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

import org.apache.parquet.format.Encoding;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.parquet.ParquetFile.LE_INT;

/**
 * Allocation-stable decoder for repetition/definition levels and physical-value ordinals in a nested V1 page.
 * Physical values and ARRAY/MAP/ROW reconstruction are separate layers: all physical types and logical shapes consume
 * this same event stream.
 */
final class NestedPageDecoder
        implements AutoCloseable
{
    private final int maximumRepetitionLevel;
    private final int maximumDefinitionLevel;
    private final boolean decodeRepetitionLevels;
    private final PrimitiveArrayPool arrayPool;
    private final RleReader repetitionReader;
    private final RleReader definitionReader;
    private final RleReader dictionaryIdReader;

    private int[] repetitionLevels;
    private int[] definitionLevels;
    private int[] valueOrdinals;
    private int[] dictionaryIds;
    private int eventCount;
    private int physicalValueCount;
    private boolean dictionaryEncoded;
    private boolean constantDefinitionLevel;
    private int definitionLevel;

    NestedPageDecoder(
            int maximumRepetitionLevel,
            int maximumDefinitionLevel,
            RleReaderPolicy rlePolicy,
            PrimitiveArrayPool arrayPool)
    {
        this(maximumRepetitionLevel, maximumDefinitionLevel, rlePolicy, arrayPool, true);
    }

    NestedPageDecoder(
            int maximumRepetitionLevel,
            int maximumDefinitionLevel,
            RleReaderPolicy rlePolicy,
            PrimitiveArrayPool arrayPool,
            boolean decodeRepetitionLevels)
    {
        this.maximumRepetitionLevel = maximumRepetitionLevel;
        this.maximumDefinitionLevel = maximumDefinitionLevel;
        this.decodeRepetitionLevels = decodeRepetitionLevels;
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        this.repetitionReader = new RleReader(rlePolicy);
        this.definitionReader = new RleReader(rlePolicy);
        this.dictionaryIdReader = new RleReader(rlePolicy);
    }

    long decodeDataPageV1(MemorySegment body, int valueCount, Encoding encoding, int dictionarySize)
    {
        long offset = decodeLevels(body, valueCount, true);

        dictionaryEncoded = encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY;
        if (dictionaryEncoded) {
            if (dictionarySize == 0 && physicalValueCount != 0) {
                throw new UnsupportedParquetFeatureException("Nested dictionary data page precedes its dictionary page");
            }
            int bitWidth = body.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
            offset++;
            dictionaryIds = grow(dictionaryIds, physicalValueCount);
            dictionaryIdReader.init(body, offset, bitWidth);
            dictionaryIdReader.read(dictionaryIds, 0, physicalValueCount);
            for (int index = 0; index < physicalValueCount; index++) {
                if (dictionaryIds[index] < 0 || dictionaryIds[index] >= dictionarySize) {
                    throw new IllegalArgumentException("Nested Parquet dictionary id is outside dictionary: " + dictionaryIds[index]);
                }
            }
            return -1;
        }
        if (encoding != Encoding.PLAIN) {
            throw new UnsupportedParquetFeatureException("Native nested Parquet reader does not support data encoding " + encoding);
        }
        return offset;
    }

    void decodeLevelsDataPageV1(MemorySegment body, int valueCount)
    {
        decodeLevels(body, valueCount, false);
        dictionaryEncoded = false;
    }

    private long decodeLevels(MemorySegment body, int valueCount, boolean decodeValueOrdinals)
    {
        requireNonNull(body, "body is null");
        eventCount = valueCount;
        if (decodeRepetitionLevels) {
            repetitionLevels = grow(repetitionLevels, valueCount);
        }
        long offset = 0;
        if (maximumRepetitionLevel == 0) {
            if (decodeRepetitionLevels) {
                Arrays.fill(repetitionLevels, 0, valueCount, 0);
            }
        }
        else {
            int length = levelStreamLength(body, offset, "repetition");
            offset += Integer.BYTES;
            if (decodeRepetitionLevels) {
                repetitionReader.init(body.asSlice(0, offset + length), offset, bitWidth(maximumRepetitionLevel));
                repetitionReader.read(repetitionLevels, 0, valueCount);
            }
            offset += length;
        }

        if (maximumDefinitionLevel == 0) {
            constantDefinitionLevel = true;
            definitionLevel = 0;
        }
        else {
            int length = levelStreamLength(body, offset, "definition");
            offset += Integer.BYTES;
            definitionReader.init(body.asSlice(0, offset + length), offset, bitWidth(maximumDefinitionLevel));
            definitionLevel = definitionReader.consumeSingleRleValue(valueCount);
            constantDefinitionLevel = definitionLevel >= 0;
            if (!constantDefinitionLevel) {
                definitionLevels = grow(definitionLevels, valueCount);
                definitionReader.init(body.asSlice(0, offset + length), offset, bitWidth(maximumDefinitionLevel));
                definitionReader.read(definitionLevels, 0, valueCount);
            }
            offset += length;
        }

        if (constantDefinitionLevel) {
            physicalValueCount = definitionLevel == maximumDefinitionLevel ? valueCount : 0;
        }
        else if (decodeValueOrdinals) {
            valueOrdinals = grow(valueOrdinals, valueCount);
            physicalValueCount = 0;
            for (int event = 0; event < valueCount; event++) {
                valueOrdinals[event] = definitionLevels[event] == maximumDefinitionLevel ? physicalValueCount++ : -1;
            }
        }
        else {
            physicalValueCount = 0;
        }

        return offset;
    }

    int eventCount()
    {
        return eventCount;
    }

    int repetitionLevel(int event)
    {
        if (!decodeRepetitionLevels) {
            throw new IllegalStateException("Repetition levels are supplied by the shared nested structure");
        }
        return repetitionLevels[event];
    }

    int definitionLevel(int event)
    {
        return constantDefinitionLevel ? definitionLevel : definitionLevels[event];
    }

    boolean hasValue(int event)
    {
        return constantDefinitionLevel ? definitionLevel == maximumDefinitionLevel : valueOrdinals[event] >= 0;
    }

    int physicalValueCount()
    {
        return physicalValueCount;
    }

    int valueOrdinal(int event)
    {
        int ordinal = constantDefinitionLevel && definitionLevel == maximumDefinitionLevel ? event :
                constantDefinitionLevel ? -1 : valueOrdinals[event];
        if (ordinal < 0) {
            throw new IllegalStateException("Nested Parquet event has no physical value");
        }
        return ordinal;
    }

    int dictionaryId(int ordinal)
    {
        return dictionaryEncoded ? dictionaryIds[ordinal] : -1;
    }

    void appendEvents(
            NestedValueAccumulator accumulator,
            PhysicalValueDecoder valueDecoder,
            int eventOffset,
            int count)
    {
        if (eventOffset < 0 || count < 0 || eventOffset > eventCount - count) {
            throw new IndexOutOfBoundsException("Invalid nested event window: " + eventOffset + ", " + count);
        }
        if (constantDefinitionLevel) {
            if (definitionLevel == maximumDefinitionLevel) {
                if (dictionaryEncoded) {
                    accumulator.appendDictionaryRun(valueDecoder, dictionaryIds, eventOffset, count);
                }
                else {
                    accumulator.appendPlainRun(valueDecoder, eventOffset, count);
                }
            }
            else {
                for (int index = 0; index < count; index++) {
                    accumulator.appendNull();
                }
            }
            return;
        }
        accumulator.appendEvents(
                valueDecoder,
                valueOrdinals,
                dictionaryEncoded ? dictionaryIds : null,
                eventOffset,
                count);
    }

    void resetWindow(NestedEventWindow window, PhysicalValueDecoder valueDecoder, int eventOffset)
    {
        if (eventOffset < 0 || eventOffset >= eventCount) {
            throw new IndexOutOfBoundsException("Invalid nested event offset: " + eventOffset);
        }
        if (constantDefinitionLevel) {
            window.resetConstantDefinitionLevel(
                    valueDecoder,
                    repetitionLevels,
                    definitionLevel,
                    definitionLevel == maximumDefinitionLevel,
                    dictionaryEncoded ? dictionaryIds : null,
                    eventOffset,
                    eventCount - eventOffset);
        }
        else {
            window.reset(
                    valueDecoder,
                    repetitionLevels,
                    definitionLevels,
                    valueOrdinals,
                    dictionaryEncoded ? dictionaryIds : null,
                    eventOffset,
                    eventCount - eventOffset);
        }
    }

    boolean hasRepetitionLevels()
    {
        return decodeRepetitionLevels;
    }

    private static int levelStreamLength(MemorySegment body, long offset, String kind)
    {
        if (offset > body.byteSize() - Integer.BYTES) {
            throw new IllegalArgumentException("Truncated nested Parquet " + kind + " levels");
        }
        int length = body.get(LE_INT, offset);
        if (length < 0 || length > body.byteSize() - offset - Integer.BYTES) {
            throw new IllegalArgumentException("Invalid nested Parquet " + kind + " level length: " + length);
        }
        return length;
    }

    private static int bitWidth(int maximumLevel)
    {
        return Integer.SIZE - Integer.numberOfLeadingZeros(maximumLevel);
    }

    private int[] grow(int[] values, int required)
    {
        if (values != null && values.length >= required) {
            return values;
        }
        int capacity = Math.max(required, Math.max(16, values == null ? 0 : values.length * 2));
        int[] replacement = arrayPool.borrowInts(capacity);
        arrayPool.release(values);
        return replacement;
    }

    @Override
    public void close()
    {
        arrayPool.release(repetitionLevels);
        arrayPool.release(definitionLevels);
        arrayPool.release(valueOrdinals);
        arrayPool.release(dictionaryIds);
        repetitionLevels = null;
        definitionLevels = null;
        valueOrdinals = null;
        dictionaryIds = null;
    }
}
