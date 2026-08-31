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

import static java.util.Objects.requireNonNull;

/**
 * Storage resources shared by allocators without exposing engine services.
 *
 * <p>The embedding chooses this object's lifetime. Connector-facing allocation can therefore share vector and native
 * storage while remaining independent of operators, function registries, and code-generation services.
 */
public final class AllocationResources
        implements AllocationResourcesOwner, AutoCloseable
{
    private static final long MIN_DEFAULT_MAX_RETAINED_BYTES = 512L << 20;
    private static final long MAX_DEFAULT_MAX_RETAINED_BYTES = 1L << 30;
    // Scan masks and null vectors commonly occupy 32-256 KiB. Retaining them removes a bounded but recurring
    // steady-state allocation floor while the pool's byte ceiling still bounds total storage and entry count.
    private static final long DEFAULT_MIN_RETAINED_BYTES = 32L << 10;
    private static final long DEFAULT_MAX_RETAINED_NATIVE_BYTES = 256L << 20;

    private final PrimitiveArrayPool primitiveArrays;
    private final PrimitiveArrayPool nativeBuffers;
    private final AllocatorPolicy allocatorPolicy;
    private final NativeBufferAdvice nativeBufferAdvice;
    private boolean closed;

    public AllocationResources(PrimitiveArrayPool primitiveArrays, PrimitiveArrayPool nativeBuffers)
    {
        this(primitiveArrays, nativeBuffers, AllocatorPolicy.defaults(), NativeBufferAdvice.defaults());
    }

    public AllocationResources(
            PrimitiveArrayPool primitiveArrays,
            PrimitiveArrayPool nativeBuffers,
            AllocatorPolicy allocatorPolicy)
    {
        this(primitiveArrays, nativeBuffers, allocatorPolicy, NativeBufferAdvice.defaults());
    }

    public AllocationResources(
            PrimitiveArrayPool primitiveArrays,
            PrimitiveArrayPool nativeBuffers,
            AllocatorPolicy allocatorPolicy,
            NativeBufferAdvice nativeBufferAdvice)
    {
        this.primitiveArrays = requireNonNull(primitiveArrays, "primitiveArrays is null");
        this.nativeBuffers = requireNonNull(nativeBuffers, "nativeBuffers is null");
        this.allocatorPolicy = requireNonNull(allocatorPolicy, "allocatorPolicy is null");
        this.nativeBufferAdvice = requireNonNull(nativeBufferAdvice, "nativeBufferAdvice is null");
    }

    /**
     * Constructs a new, isolated storage owner using Nitro's standalone retention defaults.
     *
     * <p>This is a factory for a fresh owner, not a shared resource accessor.
     */
    public static AllocationResources createDefault()
    {
        return createDefault(Long.getLong("nitro.primitiveArrayPool.maxRetainedBytes", defaultMaxRetainedBytes()));
    }

    /**
     * Constructs a fresh storage owner with an embedding-selected primitive-array retention budget.
     * Other storage policies retain Nitro's standalone defaults.
     */
    public static AllocationResources createDefault(long primitiveArrayMaxRetainedBytes)
    {
        return new AllocationResources(
                new PrimitiveArrayPool(
                        primitiveArrayMaxRetainedBytes,
                        Long.getLong("nitro.primitiveArrayPool.minRetainedBytes", DEFAULT_MIN_RETAINED_BYTES)),
                new PrimitiveArrayPool(
                        Long.getLong("nitro.nativeBufferPool.maxRetainedBytes", DEFAULT_MAX_RETAINED_NATIVE_BYTES),
                        Long.getLong("nitro.nativeBufferPool.minRetainedBytes", DEFAULT_MIN_RETAINED_BYTES)),
                AllocatorPolicy.fromSystemProperties(),
                NativeBufferAdvice.fromSystemProperties());
    }

    public PrimitiveArrayPool primitiveArrays()
    {
        checkOpen();
        return primitiveArrays;
    }

    public PrimitiveArrayPool nativeBuffers()
    {
        checkOpen();
        return nativeBuffers;
    }

    public AllocatorPolicy allocatorPolicy()
    {
        checkOpen();
        return allocatorPolicy;
    }

    public NativeBufferAdvice nativeBufferAdvice()
    {
        checkOpen();
        return nativeBufferAdvice;
    }

    @Override
    public AllocationResources allocationResources()
    {
        checkOpen();
        return this;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        primitiveArrays.close();
        if (nativeBuffers != primitiveArrays) {
            nativeBuffers.close();
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Allocation resources are closed");
        }
    }

    private static long defaultMaxRetainedBytes()
    {
        // Keep the original bounded footprint on small heaps, but let large analytic-query heaps retain enough of
        // their actual primitive working set to avoid recreating it every invocation. The hard upper bound remains
        // below one tenth of the 12 GiB publication heap, and the explicit property remains authoritative.
        return Math.min(
                MAX_DEFAULT_MAX_RETAINED_BYTES,
                Math.max(MIN_DEFAULT_MAX_RETAINED_BYTES, Runtime.getRuntime().maxMemory() / 12));
    }
}
