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
package org.weakref.nitro.data;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * A process-wide, bounded lease for large primitive work arrays.
 *
 * <p>The vector allocator's pools are intentionally scoped to an allocator instance. Query-state arrays need a
 * longer-lived owner so a fresh query can reuse the previous query's build storage, but that owner must not turn
 * transient peak memory into an unbounded steady-state tax. This pool therefore uses exact capacity buckets and a
 * hard FIFO byte ceiling. Arrays smaller than the configured minimum are left to the normal allocator/GC path.
 */
public final class PrimitiveArrayPool
{
    private static final long MIN_DEFAULT_MAX_RETAINED_BYTES = 512L << 20;
    private static final long MAX_DEFAULT_MAX_RETAINED_BYTES = 1L << 30;
    private static final long DEFAULT_MIN_RETAINED_BYTES = 256L << 10;
    private static final long DEFAULT_MAX_RETAINED_NATIVE_BYTES = 256L << 20;

    private static final PrimitiveArrayPool SHARED = new PrimitiveArrayPool(
            Long.getLong("nitro.primitiveArrayPool.maxRetainedBytes", defaultMaxRetainedBytes()),
            Long.getLong("nitro.primitiveArrayPool.minRetainedBytes", DEFAULT_MIN_RETAINED_BYTES));
    private static final PrimitiveArrayPool SHARED_NATIVE_BUFFERS = new PrimitiveArrayPool(
            Long.getLong("nitro.nativeBufferPool.maxRetainedBytes", DEFAULT_MAX_RETAINED_NATIVE_BYTES),
            Long.getLong("nitro.nativeBufferPool.minRetainedBytes", DEFAULT_MIN_RETAINED_BYTES));

    private final long maxRetainedBytes;
    private final long minRetainedBytes;
    private final Map<Key, ArrayDeque<Entry>> buckets = new HashMap<>();
    private final Map<Object, Entry> retainedBuffers = new IdentityHashMap<>();
    private Entry oldest;
    private Entry newest;
    private long retainedBytes;
    private long borrowedBytes;
    private long reusedBytes;

    private static long defaultMaxRetainedBytes()
    {
        // Keep the original bounded footprint on small heaps, but let large analytic-query heaps retain enough of
        // their actual primitive working set to avoid recreating it every invocation. The hard upper bound remains
        // below one tenth of the 12 GiB publication heap, and the explicit property remains authoritative.
        return Math.min(
                MAX_DEFAULT_MAX_RETAINED_BYTES,
                Math.max(MIN_DEFAULT_MAX_RETAINED_BYTES, Runtime.getRuntime().maxMemory() / 12));
    }

    public PrimitiveArrayPool(long maxRetainedBytes, long minRetainedBytes)
    {
        if (maxRetainedBytes < 0) {
            throw new IllegalArgumentException("maxRetainedBytes is negative");
        }
        if (minRetainedBytes < 0) {
            throw new IllegalArgumentException("minRetainedBytes is negative");
        }
        this.maxRetainedBytes = maxRetainedBytes;
        this.minRetainedBytes = minRetainedBytes;
    }

    public static PrimitiveArrayPool shared()
    {
        return SHARED;
    }

    /**
     * Returns the process-wide bounded retention domain for native buffers.
     *
     * <p>Native workspaces have an independent budget so retaining one cannot evict reusable Java primitive arrays
     * and turn an off-heap optimization into downstream heap allocation. The same generic family/capacity API is
     * available to any reader or operator that owns a recyclable native buffer.
     */
    public static PrimitiveArrayPool sharedNativeBuffers()
    {
        return SHARED_NATIVE_BUFFERS;
    }

    public synchronized int[] borrowInts(int length)
    {
        int[] array = borrow(int[].class, length, int[].class);
        return array != null ? array : new int[length];
    }

    /**
     * Acquires an exactly sized retained array when one is already available, without allocating on a miss.
     * Capacity planners can use this to choose a wider reusable generation only when it does not increase the
     * execution's primitive working set.
     */
    public synchronized int[] tryBorrowInts(int length)
    {
        return borrow(int[].class, length, int[].class);
    }

    public synchronized byte[] borrowBytes(int length)
    {
        byte[] array = borrow(byte[].class, length, byte[].class);
        return array != null ? array : new byte[length];
    }

    public synchronized long[] borrowLongs(int length)
    {
        long[] array = borrow(long[].class, length, long[].class);
        return array != null ? array : new long[length];
    }

    public synchronized boolean[] borrowBooleans(int length)
    {
        boolean[] array = borrow(boolean[].class, length, boolean[].class);
        return array != null ? array : new boolean[length];
    }

    public synchronized void release(int[] array)
    {
        if (array != null) {
            retain(int[].class, array.length, (long) array.length * Integer.BYTES, array);
        }
    }

    public synchronized void release(byte[] array)
    {
        if (array != null) {
            retain(byte[].class, array.length, array.length, array);
        }
    }

    public synchronized void release(long[] array)
    {
        if (array != null) {
            retain(long[].class, array.length, (long) array.length * Long.BYTES, array);
        }
    }

    public synchronized void release(boolean[] array)
    {
        if (array != null) {
            retain(boolean[].class, array.length, array.length, array);
        }
    }

    public synchronized <T> T borrow(Object family, int capacity, Class<T> type)
    {
        Key key = new Key(family, capacity);
        ArrayDeque<Entry> bucket = buckets.get(key);
        if (bucket == null) {
            return null;
        }
        Entry entry = bucket.removeLast();
        if (bucket.isEmpty()) {
            buckets.remove(key);
        }
        unlink(entry);
        if (entry.buffer.getClass().isArray()) {
            retainedBuffers.remove(entry.buffer);
        }
        retainedBytes -= entry.bytes;
        borrowedBytes += entry.bytes;
        reusedBytes += entry.bytes;
        return type.cast(entry.buffer);
    }

    /**
     * Offers a reusable buffer to the shared budget. Returns false when the buffer is below the pooling threshold or
     * cannot fit under the hard ceiling, allowing the caller to keep it in a cheaper query-local pool instead.
     */
    public synchronized boolean retain(Object family, int capacity, long bytes, Object buffer)
    {
        if (bytes < minRetainedBytes || bytes > maxRetainedBytes) {
            return false;
        }
        // Release is intentionally idempotent. Without identity deduplication, releasing the same array twice puts
        // two entries for one object in the pool; two later borrowers can then mutate the same backing storage.
        if (buffer.getClass().isArray()) {
            if (retainedBuffers.containsKey(buffer)) {
                return true;
            }
        }
        Entry entry = new Entry(new Key(family, capacity), buffer, bytes);
        if (buffer.getClass().isArray()) {
            retainedBuffers.put(buffer, entry);
        }
        buckets.computeIfAbsent(entry.key, _ -> new ArrayDeque<>()).addLast(entry);
        if (newest == null) {
            oldest = entry;
        }
        else {
            newest.next = entry;
            entry.previous = newest;
        }
        newest = entry;
        retainedBytes += bytes;
        while (retainedBytes > maxRetainedBytes) {
            evictOldest();
        }
        return true;
    }

    public synchronized long retainedBytes()
    {
        return retainedBytes;
    }

    public synchronized long borrowedBytes()
    {
        return borrowedBytes;
    }

    public synchronized long reusedBytes()
    {
        return reusedBytes;
    }

    /** Returns whether a buffer of this size can participate in this pool, without acquiring the pool lock. */
    public boolean isRetainable(long bytes)
    {
        return bytes >= minRetainedBytes && bytes <= maxRetainedBytes;
    }

    /** Minimum buffer size that participates in this pool; useful for choosing reusable chunk geometry. */
    public long minRetainedBytes()
    {
        return minRetainedBytes;
    }

    private void evictOldest()
    {
        Entry entry = oldest;
        unlink(entry);
        ArrayDeque<Entry> bucket = buckets.get(entry.key);
        bucket.remove(entry);
        if (bucket.isEmpty()) {
            buckets.remove(entry.key);
        }
        if (entry.buffer.getClass().isArray()) {
            retainedBuffers.remove(entry.buffer);
        }
        retainedBytes -= entry.bytes;
    }

    private void unlink(Entry entry)
    {
        if (entry.previous == null) {
            oldest = entry.next;
        }
        else {
            entry.previous.next = entry.next;
        }
        if (entry.next == null) {
            newest = entry.previous;
        }
        else {
            entry.next.previous = entry.previous;
        }
        entry.previous = null;
        entry.next = null;
    }

    private record Key(Object family, int capacity) {}

    private static final class Entry
    {
        private final Key key;
        private final Object buffer;
        private final long bytes;
        private Entry previous;
        private Entry next;

        private Entry(Key key, Object buffer, long bytes)
        {
            this.key = key;
            this.buffer = buffer;
            this.bytes = bytes;
        }
    }
}
