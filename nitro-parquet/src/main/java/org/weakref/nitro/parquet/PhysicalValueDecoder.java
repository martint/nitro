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

/**
 * Decodes the physical values of a Parquet leaf independently of repetition and definition levels.
 * Implementations own reusable high-water buffers and expose a carrier appropriate for the physical type.
 */
interface PhysicalValueDecoder
        extends AutoCloseable
{
    void decodeDictionary(MemorySegment body, int valueCount, Encoding encoding);

    void decodePlain(MemorySegment body, long offset, int valueCount);

    default void decodeData(MemorySegment body, long offset, int valueCount, Encoding encoding)
    {
        if (encoding != Encoding.PLAIN) {
            throw new UnsupportedParquetFeatureException("Native Parquet reader does not support physical encoding " + encoding);
        }
        decodePlain(body, offset, valueCount);
    }

    void resetDictionary();

    int dictionarySize();

    @Override
    void close();
}
