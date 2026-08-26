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

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.parquet.ParquetFile.LE_DOUBLE;

/** Physical DOUBLE decoder. Logical interpretation remains the vector/type adapter's responsibility. */
final class DoublePhysicalValueDecoder
        implements DoubleValueDecoder
{
    private static final double[] EMPTY_DOUBLES = new double[0];

    private final PrimitiveArrayPool arrayPool;
    private double[] dictionary = EMPTY_DOUBLES;
    private double[] values = EMPTY_DOUBLES;
    private int dictionarySize;

    DoublePhysicalValueDecoder(PrimitiveArrayPool arrayPool)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
    }

    @Override
    public void decodeDictionary(MemorySegment body, int valueCount, Encoding encoding)
    {
        requirePlainDictionary(encoding);
        dictionary = grow(dictionary, valueCount);
        MemorySegment.copy(body, LE_DOUBLE, 0, dictionary, 0, valueCount);
        dictionarySize = valueCount;
    }

    @Override
    public void decodePlain(MemorySegment body, long offset, int valueCount)
    {
        values = grow(values, valueCount);
        MemorySegment.copy(body, LE_DOUBLE, offset, values, 0, valueCount);
    }

    public double value(int ordinal, int dictionaryId)
    {
        return dictionaryId >= 0 ? dictionary[dictionaryId] : values[ordinal];
    }

    @Override
    public void copyPlain(int ordinal, double[] output, int outputOffset, int count)
    {
        System.arraycopy(values, ordinal, output, outputOffset, count);
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
        dictionary = EMPTY_DOUBLES;
        values = EMPTY_DOUBLES;
        dictionarySize = 0;
    }

    private double[] grow(double[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        double[] replacement = arrayPool.borrowDoubles(Math.max(required, Math.max(16, current.length * 2)));
        arrayPool.release(current);
        return replacement;
    }
}
