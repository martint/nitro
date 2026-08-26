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

    /**
     * Appends a contiguous window of decoded nested events. The physical decoder owns type interpretation; the
     * page decoder supplies only schema-level presence, physical ordinals, and optional dictionary ids. Typed
     * accumulators can replace the generic event loop with a bulk transfer without coupling page decoding to a
     * logical type.
     */
    default void appendEvents(
            PhysicalValueDecoder decoder,
            int[] valueOrdinals,
            int[] dictionaryIds,
            int eventOffset,
            int eventCount)
    {
        int end = eventOffset + eventCount;
        if (eventCount > 0) {
            int firstOrdinal = valueOrdinals[eventOffset];
            int lastOrdinal = valueOrdinals[end - 1];
            // NestedPageDecoder assigns consecutive ordinals to present values. Matching endpoints therefore
            // prove that the whole event window is present, without rescanning it for nulls.
            if (firstOrdinal >= 0 && lastOrdinal == firstOrdinal + eventCount - 1) {
                if (dictionaryIds == null) {
                    appendPlainRun(decoder, firstOrdinal, eventCount);
                }
                else {
                    appendDictionaryRun(decoder, dictionaryIds, firstOrdinal, eventCount);
                }
                return;
            }
        }
        if (dictionaryIds == null) {
            int runOrdinal = -1;
            int runCount = 0;
            for (int event = eventOffset; event < end; event++) {
                int ordinal = valueOrdinals[event];
                if (ordinal >= 0 && (runCount == 0 || ordinal == runOrdinal + runCount)) {
                    if (runCount == 0) {
                        runOrdinal = ordinal;
                    }
                    runCount++;
                    continue;
                }
                if (runCount != 0) {
                    appendPlainRun(decoder, runOrdinal, runCount);
                    runCount = 0;
                }
                if (ordinal < 0) {
                    appendNull();
                }
                else {
                    append(decoder, ordinal, -1);
                }
            }
            if (runCount != 0) {
                appendPlainRun(decoder, runOrdinal, runCount);
            }
            return;
        }
        for (int event = eventOffset; event < end; event++) {
            int ordinal = valueOrdinals[event];
            if (ordinal < 0) {
                appendNull();
            }
            else {
                append(decoder, ordinal, dictionaryIds[ordinal]);
            }
        }
    }

    default void appendDictionaryRun(
            PhysicalValueDecoder decoder,
            int[] dictionaryIds,
            int ordinal,
            int count)
    {
        for (int index = 0; index < count; index++) {
            append(decoder, ordinal + index, dictionaryIds[ordinal + index]);
        }
    }

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
