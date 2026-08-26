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

/** Physical BOOLEAN decoder for Parquet's least-significant-bit-first PLAIN representation. */
final class BooleanPhysicalValueDecoder
        implements BooleanValueDecoder
{
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final PrimitiveArrayPool arrayPool;
    private boolean[] values = EMPTY_BOOLEANS;

    BooleanPhysicalValueDecoder(PrimitiveArrayPool arrayPool)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
    }

    @Override
    public void decodeDictionary(MemorySegment body, int valueCount, Encoding encoding)
    {
        throw new UnsupportedParquetFeatureException("Parquet BOOLEAN values cannot use dictionary encoding");
    }

    @Override
    public void decodePlain(MemorySegment body, long offset, int valueCount)
    {
        long requiredBytes = (valueCount + 7L) / 8;
        if (offset < 0 || requiredBytes > body.byteSize() - offset) {
            throw new IllegalArgumentException("Truncated nested Parquet BOOLEAN values");
        }
        values = grow(values, valueCount);
        for (int index = 0; index < valueCount; index++) {
            int packed = body.get(ValueLayout.JAVA_BYTE, offset + (index >>> 3)) & 0xFF;
            values[index] = ((packed >>> (index & 7)) & 1) != 0;
        }
    }

    public boolean value(int ordinal)
    {
        return values[ordinal];
    }

    @Override
    public void copyPlain(int ordinal, boolean[] output, int outputOffset, int count)
    {
        System.arraycopy(values, ordinal, output, outputOffset, count);
    }

    @Override
    public void resetDictionary() {}

    @Override
    public int dictionarySize()
    {
        return 0;
    }

    @Override
    public void close()
    {
        arrayPool.release(values);
        values = EMPTY_BOOLEANS;
    }

    private boolean[] grow(boolean[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        boolean[] replacement = arrayPool.borrowBooleans(Math.max(required, Math.max(16, current.length * 2)));
        arrayPool.release(current);
        return replacement;
    }
}
