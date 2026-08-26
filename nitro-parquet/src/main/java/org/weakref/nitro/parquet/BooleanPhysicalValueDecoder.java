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

/** Physical BOOLEAN decoder for Parquet's least-significant-bit-first PLAIN representation. */
final class BooleanPhysicalValueDecoder
        implements PhysicalValueDecoder
{
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private boolean[] values = EMPTY_BOOLEANS;

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

    boolean value(int ordinal)
    {
        return values[ordinal];
    }

    @Override
    public void resetDictionary() {}

    @Override
    public int dictionarySize()
    {
        return 0;
    }

    private static boolean[] grow(boolean[] values, int required)
    {
        return values.length >= required ? values : Arrays.copyOf(values, Math.max(required, Math.max(16, values.length * 2)));
    }
}
