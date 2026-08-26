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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.util.Objects.requireNonNull;

final class MappedParquetInput
        implements ParquetInput
{
    private final Path path;
    private final Arena arena;
    private final boolean ownsArena;
    private final MemorySegment data;
    private boolean closed;

    static MappedParquetInput open(Path path, Arena arena, boolean ownsArena)
            throws IOException
    {
        requireNonNull(path, "path is null");
        requireNonNull(arena, "arena is null");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return new MappedParquetInput(path, arena, ownsArena, channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena));
        }
    }

    private MappedParquetInput(Path path, Arena arena, boolean ownsArena, MemorySegment data)
    {
        this.path = path.toAbsolutePath().normalize();
        this.arena = arena;
        this.ownsArena = ownsArena;
        this.data = data;
    }

    @Override
    public String id()
    {
        return path.toString();
    }

    @Override
    public long size()
    {
        return data.byteSize();
    }

    @Override
    public MemorySegment readRange(long offset, int length)
    {
        if (offset < 0 || length < 0 || offset > data.byteSize() - length) {
            throw new IndexOutOfBoundsException("Invalid Parquet range: offset=" + offset + ", length=" + length + ", size=" + data.byteSize());
        }
        return data.asSlice(offset, length);
    }

    @Override
    public void close()
    {
        if (!closed && ownsArena) {
            arena.close();
        }
        closed = true;
    }
}
