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
import static org.weakref.nitro.parquet.ParquetFile.LE_INT;

/** Physical BYTE_ARRAY decoder. UTF-8 and binary logical semantics are intentionally outside this layer. */
final class BinaryPhysicalValueDecoder
        implements BinaryValueDecoder
{
    private static final int[] EMPTY_INTS = new int[0];
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final PrimitiveArrayPool arrayPool;
    private final DeltaBinaryPackedIntDecoder deltaLengthDecoder = new DeltaBinaryPackedIntDecoder();
    private int[] dictionaryOffsets = EMPTY_INTS;
    private byte[] dictionaryData = EMPTY_BYTES;
    private int[] offsets = EMPTY_INTS;
    private byte[] data = EMPTY_BYTES;
    private int dictionarySize;
    private int dictionaryGeneration;

    BinaryPhysicalValueDecoder(PrimitiveArrayPool arrayPool)
    {
        this.arrayPool = requireNonNull(arrayPool, "arrayPool is null");
    }

    @Override
    public void decodeDictionary(MemorySegment body, int valueCount, Encoding encoding)
    {
        requireNonNull(body, "body is null");
        if (encoding != Encoding.PLAIN && encoding != Encoding.PLAIN_DICTIONARY) {
            throw new UnsupportedParquetFeatureException("Native Parquet reader requires a plain dictionary page, got " + encoding);
        }
        dictionaryOffsets = grow(dictionaryOffsets, valueCount + 1);
        dictionaryData = decode(body, 0, valueCount, dictionaryOffsets, dictionaryData);
        dictionarySize = valueCount;
        dictionaryGeneration++;
    }

    @Override
    public void decodePlain(MemorySegment body, long offset, int valueCount)
    {
        offsets = grow(offsets, valueCount + 1);
        data = decode(body, offset, valueCount, offsets, data);
    }

    @Override
    public void decodeData(MemorySegment body, long offset, int valueCount, Encoding encoding)
    {
        if (encoding == Encoding.PLAIN) {
            decodePlain(body, offset, valueCount);
            return;
        }
        if (encoding != Encoding.DELTA_LENGTH_BYTE_ARRAY) {
            throw new UnsupportedParquetFeatureException("Native Parquet binary reader does not support encoding " + encoding);
        }

        offsets = grow(offsets, valueCount + 1);
        deltaLengthDecoder.reset(body, offset);
        deltaLengthDecoder.decode(offsets, valueCount);
        long dataOffset = deltaLengthDecoder.position();

        int bytes = 0;
        for (int index = 0; index < valueCount; index++) {
            int length = offsets[index];
            if (length < 0 || bytes > Integer.MAX_VALUE - length) {
                throw new IllegalArgumentException("Invalid Parquet binary length: " + length);
            }
            offsets[index] = bytes;
            bytes += length;
        }
        offsets[valueCount] = bytes;
        if (dataOffset > body.byteSize() - bytes) {
            throw new IllegalArgumentException("Truncated Parquet delta-length binary payload");
        }
        data = grow(data, bytes);
        MemorySegment.copy(body, ValueLayout.JAVA_BYTE, dataOffset, data, 0, bytes);
    }

    @Override
    public int length(int ordinal, int dictionaryId)
    {
        int[] selectedOffsets = dictionaryId >= 0 ? dictionaryOffsets : offsets;
        int index = dictionaryId >= 0 ? dictionaryId : ordinal;
        return selectedOffsets[index + 1] - selectedOffsets[index];
    }

    @Override
    public void copy(int ordinal, int dictionaryId, byte[] output, int outputOffset)
    {
        int[] selectedOffsets = dictionaryId >= 0 ? dictionaryOffsets : offsets;
        byte[] selectedData = dictionaryId >= 0 ? dictionaryData : data;
        int index = dictionaryId >= 0 ? dictionaryId : ordinal;
        System.arraycopy(selectedData, selectedOffsets[index], output, outputOffset, selectedOffsets[index + 1] - selectedOffsets[index]);
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

    @Override
    public int dictionaryGeneration()
    {
        return dictionaryGeneration;
    }

    @Override
    public int dictionaryByteSize()
    {
        return dictionaryOffsets[dictionarySize];
    }

    @Override
    public void copyDictionary(int[] offsets, byte[] data)
    {
        System.arraycopy(dictionaryOffsets, 0, offsets, 0, dictionarySize + 1);
        System.arraycopy(dictionaryData, 0, data, 0, dictionaryByteSize());
    }

    private byte[] decode(MemorySegment body, long offset, int valueCount, int[] offsets, byte[] data)
    {
        long cursor = offset;
        int bytes = 0;
        for (int index = 0; index < valueCount; index++) {
            int length = body.get(LE_INT, cursor);
            if (length < 0 || length > body.byteSize() - cursor - Integer.BYTES) {
                throw new IllegalArgumentException("Invalid Parquet binary length: " + length);
            }
            offsets[index] = bytes;
            bytes += length;
            cursor += Integer.BYTES + length;
        }
        offsets[valueCount] = bytes;
        data = grow(data, bytes);
        cursor = offset;
        int output = 0;
        for (int index = 0; index < valueCount; index++) {
            int length = body.get(LE_INT, cursor);
            cursor += Integer.BYTES;
            MemorySegment.copy(body, ValueLayout.JAVA_BYTE, cursor, data, output, length);
            cursor += length;
            output += length;
        }
        return data;
    }

    @Override
    public void close()
    {
        arrayPool.release(dictionaryOffsets);
        arrayPool.release(dictionaryData);
        arrayPool.release(offsets);
        arrayPool.release(data);
        dictionaryOffsets = EMPTY_INTS;
        dictionaryData = EMPTY_BYTES;
        offsets = EMPTY_INTS;
        data = EMPTY_BYTES;
        dictionarySize = 0;
    }

    private int[] grow(int[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        int[] replacement = arrayPool.borrowInts(Math.max(required, Math.max(16, current.length * 2)));
        arrayPool.release(current);
        return replacement;
    }

    private byte[] grow(byte[] current, int required)
    {
        if (current.length >= required) {
            return current;
        }
        byte[] replacement = arrayPool.borrowBytes(Math.max(required, Math.max(16, current.length * 2)));
        arrayPool.release(current);
        return replacement;
    }
}
