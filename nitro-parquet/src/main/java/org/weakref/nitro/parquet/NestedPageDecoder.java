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
{
    private static final int[] EMPTY_INTS = new int[0];
    private final int maximumRepetitionLevel;
    private final int maximumDefinitionLevel;
    private final RleReader repetitionReader;
    private final RleReader definitionReader;
    private final RleReader dictionaryIdReader;

    private int[] repetitionLevels = EMPTY_INTS;
    private int[] definitionLevels = EMPTY_INTS;
    private int[] valueOrdinals = EMPTY_INTS;
    private int[] dictionaryIds = EMPTY_INTS;
    private int eventCount;
    private int physicalValueCount;
    private boolean dictionaryEncoded;

    NestedPageDecoder(int maximumRepetitionLevel, int maximumDefinitionLevel, RleReaderPolicy rlePolicy)
    {
        this.maximumRepetitionLevel = maximumRepetitionLevel;
        this.maximumDefinitionLevel = maximumDefinitionLevel;
        this.repetitionReader = new RleReader(rlePolicy);
        this.definitionReader = new RleReader(rlePolicy);
        this.dictionaryIdReader = new RleReader(rlePolicy);
    }

    long decodeDataPageV1(MemorySegment body, int valueCount, Encoding encoding, int dictionarySize)
    {
        long offset = decodeLevels(body, valueCount);

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
        decodeLevels(body, valueCount);
        dictionaryEncoded = false;
    }

    private long decodeLevels(MemorySegment body, int valueCount)
    {
        requireNonNull(body, "body is null");
        eventCount = valueCount;
        repetitionLevels = grow(repetitionLevels, valueCount);
        definitionLevels = grow(definitionLevels, valueCount);
        valueOrdinals = grow(valueOrdinals, valueCount);

        long offset = 0;
        if (maximumRepetitionLevel == 0) {
            Arrays.fill(repetitionLevels, 0, valueCount, 0);
        }
        else {
            int length = levelStreamLength(body, offset, "repetition");
            offset += Integer.BYTES;
            repetitionReader.init(body.asSlice(0, offset + length), offset, bitWidth(maximumRepetitionLevel));
            repetitionReader.read(repetitionLevels, 0, valueCount);
            offset += length;
        }

        if (maximumDefinitionLevel == 0) {
            Arrays.fill(definitionLevels, 0, valueCount, 0);
        }
        else {
            int length = levelStreamLength(body, offset, "definition");
            offset += Integer.BYTES;
            definitionReader.init(body.asSlice(0, offset + length), offset, bitWidth(maximumDefinitionLevel));
            definitionReader.read(definitionLevels, 0, valueCount);
            offset += length;
        }

        physicalValueCount = 0;
        for (int event = 0; event < valueCount; event++) {
            valueOrdinals[event] = definitionLevels[event] == maximumDefinitionLevel ? physicalValueCount++ : -1;
        }

        return offset;
    }

    int eventCount()
    {
        return eventCount;
    }

    int repetitionLevel(int event)
    {
        return repetitionLevels[event];
    }

    int definitionLevel(int event)
    {
        return definitionLevels[event];
    }

    boolean hasValue(int event)
    {
        return valueOrdinals[event] >= 0;
    }

    int physicalValueCount()
    {
        return physicalValueCount;
    }

    int valueOrdinal(int event)
    {
        int ordinal = valueOrdinals[event];
        if (ordinal < 0) {
            throw new IllegalStateException("Nested Parquet event has no physical value");
        }
        return ordinal;
    }

    int dictionaryId(int ordinal)
    {
        return dictionaryEncoded ? dictionaryIds[ordinal] : -1;
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

    private static int[] grow(int[] values, int required)
    {
        return values.length >= required ? values : new int[Math.max(required, Math.max(16, values.length * 2))];
    }
}
