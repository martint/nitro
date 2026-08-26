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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Streams;

/** Schema-selected bridge from a physical leaf decoder to allocator-owned Nitro child vectors. */
interface NestedValueAccumulator
        extends AutoCloseable
{
    void reset(Allocator allocator);

    default void reset(Allocator allocator, Allocator.Context context, int exactSize)
    {
        reset(allocator);
    }

    void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId);

    default void appendPlainRun(PhysicalValueDecoder decoder, int ordinal, int count)
    {
        for (int index = 0; index < count; index++) {
            append(decoder, ordinal + index, -1);
        }
    }

    void appendNull();

    int size();

    Streams materialize(Allocator allocator, Allocator.Context context);

    @Override
    void close();
}
