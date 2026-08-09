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
 * An explicitly owned, bounded lease for large primitive work arrays.
 *
 * <p>The vector allocator's pools are intentionally scoped to an allocator instance. Query-state arrays can use a
 * longer-lived owner so a fresh query can reuse the previous query's build storage, but that lifetime is selected by
 * the embedding engine and passed through {@link AllocationResources}; it is never process-global. This pool uses exact
 * capacity buckets and a hard FIFO byte ceiling. Arrays smaller than the configured minimum are left to the normal
 * allocator/GC path. Primitive arrays use exact-capacity lookup; allocator-owned variable-width vectors can request
 * the smallest retained capacity that satisfies their next batch.
 */
public final class PrimitiveArrayPool
        implements AutoCloseable
{
    private final long maxRetainedBytes;
    private final long minRetainedBytes;
    private final Map<Key, ArrayDeque<Entry>> buckets = new HashMap<>();
    private final Map<Object, Entry> retainedBuffers = new IdentityHashMap<>();
    private Entry oldest;
    private Entry newest;
    private long retainedBytes;
    private long borrowedBytes;
    private long reusedBytes;
    private boolean closed;

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

    public int[] borrowInts(int length)
    {
        int[] array = tryBorrowInts(length);
        return array != null ? array : new int[length];
    }

    /**
     * Acquires an exactly sized retained array when one is already available, without allocating on a miss.
     * Capacity planners can use this to choose a wider reusable generation only when it does not increase the
     * execution's primitive working set.
     */
    public synchronized int[] tryBorrowInts(int length)
    {
        checkOpen();
        return borrow(int[].class, length, int[].class);
    }

    public byte[] borrowBytes(int length)
    {
        byte[] array = borrow(byte[].class, length, byte[].class);
        return array != null ? array : new byte[length];
    }

    public long[] borrowLongs(int length)
    {
        long[] array = borrow(long[].class, length, long[].class);
        return array != null ? array : new long[length];
    }

    public boolean[] borrowBooleans(int length)
    {
        boolean[] array = borrow(boolean[].class, length, boolean[].class);
        return array != null ? array : new boolean[length];
    }

    public synchronized void release(int[] array)
    {
        checkOpen();
        if (array != null) {
            retain(int[].class, array.length, (long) array.length * Integer.BYTES, array);
        }
    }

    public synchronized void release(byte[] array)
    {
        checkOpen();
        if (array != null) {
            retain(byte[].class, array.length, array.length, array);
        }
    }

    public synchronized void release(long[] array)
    {
        checkOpen();
        if (array != null) {
            retain(long[].class, array.length, (long) array.length * Long.BYTES, array);
        }
    }

    public synchronized void release(boolean[] array)
    {
        checkOpen();
        if (array != null) {
            retain(boolean[].class, array.length, array.length, array);
        }
    }

    public synchronized <T> T borrow(Object family, int capacity, Class<T> type)
    {
        checkOpen();
        return borrow(new Key(family, capacity), type);
    }

    /** Borrows the smallest retained buffer in {@code family} whose capacity satisfies {@code minimumCapacity}. */
    public synchronized <T> T borrowAtLeast(Object family, int minimumCapacity, Class<T> type)
    {
        checkOpen();
        Key candidate = null;
        for (Key key : buckets.keySet()) {
            if (key.family().equals(family) &&
                    key.capacity() >= minimumCapacity &&
                    (candidate == null || key.capacity() < candidate.capacity())) {
                candidate = key;
            }
        }
        return candidate == null ? null : borrow(candidate, type);
    }

    private <T> T borrow(Key key, Class<T> type)
    {
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
     * Offers a reusable buffer to this owner's budget. Returns false when the buffer is below the pooling threshold or
     * cannot fit under the hard ceiling, allowing the caller to keep it in a cheaper query-local pool instead.
     */
    public synchronized boolean retain(Object family, int capacity, long bytes, Object buffer)
    {
        checkOpen();
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

    public synchronized void clear()
    {
        buckets.clear();
        retainedBuffers.clear();
        oldest = null;
        newest = null;
        retainedBytes = 0;
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        clear();
        closed = true;
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

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Primitive array pool is closed");
        }
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
