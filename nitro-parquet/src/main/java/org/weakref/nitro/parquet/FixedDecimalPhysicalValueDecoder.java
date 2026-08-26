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

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.parquet.ParquetFile.BE_LONG;

/** Signed big-endian FIXED_LEN_BYTE_ARRAY decoder for decimals whose unscaled value fits in a long. */
final class FixedDecimalPhysicalValueDecoder
        implements LongValueDecoder
{
    private static final long[] EMPTY_LONGS = new long[0];

    private final int typeLength;
    private final PrimitiveArrayPool arrayPool;
    private long[] dictionary = EMPTY_LONGS;
    private int dictionarySize;
    private MemorySegment values;
    private long valuesOffset;
    private int valueCount;

    FixedDecimalPhysicalValueDecoder(int typeLength, PrimitiveArrayPool arrayPool)
    {
        if (typeLength <= 0 || typeLength > Long.BYTES) {
            throw new IllegalArgumentException("Short decimal byte width is outside [1, 8]: " + typeLength);
        }
        this.typeLength = typeLength;
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
    }

    @Override
    public void decodeDictionary(MemorySegment body, int valueCount, Encoding encoding)
    {
        if (encoding != Encoding.PLAIN && encoding != Encoding.PLAIN_DICTIONARY) {
            throw new UnsupportedParquetFeatureException("Native Parquet reader requires a plain dictionary page, got " + encoding);
        }
        long required = Math.multiplyExact((long) valueCount, typeLength);
        if (required > body.byteSize()) {
            throw new IllegalArgumentException("Truncated fixed-width decimal dictionary payload");
        }
        if (dictionary.length < valueCount) {
            long[] replacement = arrayPool.borrowLongs(Math.max(valueCount, Math.max(16, dictionary.length * 2)));
            arrayPool.release(dictionary);
            dictionary = replacement;
        }
        for (int index = 0; index < valueCount; index++) {
            dictionary[index] = bigEndianSignedLong(body, (long) index * typeLength);
        }
        dictionarySize = valueCount;
    }

    @Override
    public void decodePlain(MemorySegment body, long offset, int valueCount)
    {
        long required = Math.multiplyExact((long) valueCount, typeLength);
        if (offset < 0 || required > body.byteSize() - offset) {
            throw new IllegalArgumentException("Truncated fixed-width decimal payload");
        }
        this.values = body;
        this.valuesOffset = offset;
        this.valueCount = valueCount;
    }

    @Override
    public long value(int ordinal, int dictionaryId)
    {
        if (dictionaryId >= 0) {
            return dictionary[dictionaryId];
        }
        if (ordinal < 0 || ordinal >= valueCount) {
            throw new IndexOutOfBoundsException("Fixed decimal ordinal is outside decoded values: " + ordinal);
        }
        return bigEndianSignedLong(values, valuesOffset + (long) ordinal * typeLength);
    }

    @Override
    public void copyPlain(int ordinal, long[] output, int outputOffset, int count)
    {
        if (ordinal < 0 || count < 0 || ordinal > valueCount - count) {
            throw new IndexOutOfBoundsException("Fixed decimal run is outside decoded values");
        }
        long offset = valuesOffset + (long) ordinal * typeLength;
        int end = outputOffset + count;
        while (outputOffset < end && offset <= values.byteSize() - Long.BYTES) {
            int shift = Long.SIZE - typeLength * Byte.SIZE;
            output[outputOffset++] = values.get(BE_LONG, offset) >> shift;
            offset += typeLength;
        }
        while (outputOffset < end) {
            output[outputOffset++] = bigEndianSignedLong(values, offset);
            offset += typeLength;
        }
    }

    @Override
    public void resetDictionary()
    {
        values = null;
        valueCount = 0;
        dictionarySize = 0;
    }

    @Override
    public int dictionarySize()
    {
        return dictionarySize;
    }

    private long bigEndianSignedLong(MemorySegment body, long offset)
    {
        int shift = Long.SIZE - typeLength * Byte.SIZE;
        if (offset <= body.byteSize() - Long.BYTES) {
            return body.get(BE_LONG, offset) >> shift;
        }
        long value = 0;
        for (int index = 0; index < typeLength; index++) {
            value = (value << Byte.SIZE) | (body.get(ValueLayout.JAVA_BYTE, offset + index) & 0xFFL);
        }
        return value << shift >> shift;
    }

    @Override
    public void close()
    {
        arrayPool.release(dictionary);
        dictionary = EMPTY_LONGS;
        dictionarySize = 0;
        values = null;
        valueCount = 0;
    }
}
