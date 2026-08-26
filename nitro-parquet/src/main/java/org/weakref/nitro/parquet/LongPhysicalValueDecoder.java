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

import java.lang.foreign.MemorySegment;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.parquet.ParquetFile.LE_INT;
import static org.weakref.nitro.parquet.ParquetFile.LE_LONG;

/** Physical INT32/INT64 decoder. Logical interpretation remains the vector/type adapter's responsibility. */
final class LongPhysicalValueDecoder
        implements PhysicalValueDecoder
{
    private static final long[] EMPTY_LONGS = new long[0];

    private final Type physicalType;
    private long[] dictionary = EMPTY_LONGS;
    private long[] values = EMPTY_LONGS;
    private int dictionarySize;

    LongPhysicalValueDecoder(Type physicalType)
    {
        this.physicalType = requireNonNull(physicalType, "physicalType is null");
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

    long value(int ordinal, int dictionaryId)
    {
        return dictionaryId >= 0 ? dictionary[dictionaryId] : values[ordinal];
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

    private static long[] grow(long[] values, int required)
    {
        return values.length >= required ? values : new long[Math.max(required, Math.max(16, values.length * 2))];
    }
}
