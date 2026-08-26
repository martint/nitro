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
import org.apache.parquet.format.Type;
import org.weakref.nitro.data.PrimitiveArrayPool;

import java.lang.foreign.MemorySegment;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.parquet.ParquetFile.LE_INT;
import static org.weakref.nitro.parquet.ParquetFile.LE_LONG;

/** Physical INT32/INT64 decoder. Logical interpretation remains the vector/type adapter's responsibility. */
final class LongPhysicalValueDecoder
        implements LongValueDecoder
{
    private static final long[] EMPTY_LONGS = new long[0];

    private final Type physicalType;
    private final PrimitiveArrayPool arrayPool;
    private long[] dictionary = EMPTY_LONGS;
    private long[] values = EMPTY_LONGS;
    private int dictionarySize;
    private int dictionaryGeneration;

    LongPhysicalValueDecoder(Type physicalType, PrimitiveArrayPool arrayPool)
    {
        this.physicalType = requireNonNull(physicalType, "physicalType is null");
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
        if (physicalType != Type.INT32 && physicalType != Type.INT64) {
            throw new IllegalArgumentException("Not an integer physical type: " + physicalType);
        }
    }

    @Override
    public void decodeDictionary(MemorySegment body, int valueCount, Encoding encoding)
    {
        requirePlainDictionary(encoding);
        dictionary = grow(dictionary, valueCount);
        if (physicalType == Type.INT32) {
            for (int index = 0; index < valueCount; index++) {
                dictionary[index] = body.get(LE_INT, (long) index * Integer.BYTES);
            }
        }
        else {
            MemorySegment.copy(body, LE_LONG, 0, dictionary, 0, valueCount);
        }
        dictionarySize = valueCount;
        dictionaryGeneration++;
    }

    @Override
    public void decodePlain(MemorySegment body, long offset, int valueCount)
    {
        values = grow(values, valueCount);
        if (physicalType == Type.INT32) {
            for (int index = 0; index < valueCount; index++) {
                values[index] = body.get(LE_INT, offset + (long) index * Integer.BYTES);
            }
        }
        else {
            MemorySegment.copy(body, LE_LONG, offset, values, 0, valueCount);
        }
    }

    @Override
    public long value(int ordinal, int dictionaryId)
    {
        return dictionaryId >= 0 ? dictionary[dictionaryId] : values[ordinal];
    }

    @Override
    public void copyPlain(int ordinal, long[] output, int outputOffset, int count)
    {
        System.arraycopy(values, ordinal, output, outputOffset, count);
    }

    @Override
    public void copyDictionary(int[] dictionaryIds, int ordinal, long[] output, int outputOffset, int count)
    {
        int end = ordinal + count;
        for (int index = ordinal; index < end; index++) {
            output[outputOffset++] = dictionary[dictionaryIds[index]];
        }
    }

    @Override
    public int dictionaryGeneration()
    {
        return dictionaryGeneration;
    }

    @Override
    public void copyDictionaryValues(long[] output, int outputOffset)
    {
        System.arraycopy(dictionary, 0, output, outputOffset, dictionarySize);
    }

    @Override
    public void resetDictionary()
    {
        dictionarySize = 0;
    }

    @Override
    public int dictionarySize()
    {
        return dictionarySize;
    }

    private static void requirePlainDictionary(Encoding encoding)
    {
        if (encoding != Encoding.PLAIN && encoding != Encoding.PLAIN_DICTIONARY) {
            throw new UnsupportedParquetFeatureException("Native Parquet reader requires a plain dictionary page, got " + encoding);
        }
    }

    @Override
    public void close()
    {
        arrayPool.release(dictionary);
        arrayPool.release(values);
        dictionary = EMPTY_LONGS;
        values = EMPTY_LONGS;
        dictionarySize = 0;
    }

    private long[] grow(long[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        long[] replacement = arrayPool.borrowLongs(Math.max(required, Math.max(16, current.length * 2)));
        arrayPool.release(current);
        return replacement;
    }
}
