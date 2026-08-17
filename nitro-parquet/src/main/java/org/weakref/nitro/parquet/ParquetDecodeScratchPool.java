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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/** Connector-owned pool of cross-thread direct buffers used for transient page decompression. */
final class ParquetDecodeScratchPool
        implements AutoCloseable
{
    private final ParquetDecodeScratchPolicy policy;
    private final NavigableMap<Integer, ArrayDeque<ByteBuffer>> buffers = new TreeMap<>();
    private long retainedBytes;
    private boolean closed;

    ParquetDecodeScratchPool(ParquetDecodeScratchPolicy policy)
    {
        this.policy = requireNonNull(policy, "policy is null");
    }

    synchronized Lease borrow(int minimumBytes)
    {
        if (closed) {
            throw new IllegalStateException("Decode scratch pool is closed");
        }
        if (minimumBytes < 0) {
            throw new IllegalArgumentException("minimumBytes is negative");
        }
        var entry = buffers.ceilingEntry(minimumBytes);
        ByteBuffer buffer;
        if (entry == null) {
            buffer = ByteBuffer.allocateDirect(allocationSize(minimumBytes));
        }
        else {
            ArrayDeque<ByteBuffer> available = entry.getValue();
            buffer = available.removeFirst();
            if (available.isEmpty()) {
                buffers.remove(entry.getKey());
            }
            retainedBytes -= buffer.capacity();
        }
        buffer.clear();
        return new Lease(this, buffer);
    }

    private static int allocationSize(int minimumBytes)
    {
        if (minimumBytes <= 1) {
            return 1;
        }
        int highest = Integer.highestOneBit(minimumBytes - 1);
        return highest > (Integer.MAX_VALUE >>> 1) ? minimumBytes : highest << 1;
    }

    private synchronized void release(ByteBuffer buffer)
    {
        int bytes = buffer.capacity();
        if (closed ||
                bytes > policy.maxRetainedBufferBytes() ||
                retainedBytes + bytes > policy.maxRetainedBytes()) {
            return;
        }
        buffer.clear();
        buffers.computeIfAbsent(bytes, _ -> new ArrayDeque<>()).addFirst(buffer);
        retainedBytes += bytes;
    }

    @Override
    public synchronized void close()
    {
        closed = true;
        buffers.clear();
        retainedBytes = 0;
    }

    static final class Lease
            implements AutoCloseable
    {
        private final ParquetDecodeScratchPool pool;
        private ByteBuffer buffer;
        private MemorySegment segment;

        private Lease(ParquetDecodeScratchPool pool, ByteBuffer buffer)
        {
            this.pool = pool;
            this.buffer = buffer;
            this.segment = MemorySegment.ofBuffer(buffer);
        }

        MemorySegment segment()
        {
            if (buffer == null) {
                throw new IllegalStateException("Decode scratch lease is closed");
            }
            return segment;
        }

        @Override
        public void close()
        {
            ByteBuffer acquired = buffer;
            if (acquired == null) {
                return;
            }
            buffer = null;
            segment = null;
            pool.release(acquired);
        }
    }
}
