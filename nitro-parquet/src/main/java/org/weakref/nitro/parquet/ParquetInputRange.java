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

import java.lang.foreign.MemorySegment;

import static java.util.Objects.requireNonNull;

/** A bounded input range whose storage may be returned to its connector when the range is closed. */
public final class ParquetInputRange
        implements AutoCloseable
{
    private final MemorySegment data;
    private final Runnable releaser;
    private boolean closed;

    public static ParquetInputRange retained(MemorySegment data)
    {
        return new ParquetInputRange(data, () -> {});
    }

    public ParquetInputRange(MemorySegment data, Runnable releaser)
    {
        this.data = requireNonNull(data, "data is null");
        this.releaser = requireNonNull(releaser, "releaser is null");
    }

    public MemorySegment data()
    {
        if (closed) {
            throw new IllegalStateException("range is closed");
        }
        return data;
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            releaser.run();
        }
    }
}
