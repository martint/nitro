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

import java.util.List;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/// Explicitly constructed [VectorAllocator] backed by one engine allocation context.
public final class ScopedVectorAllocator
        implements VectorAllocator
{
    private final Allocator allocator;
    private final Allocator.Context context;
    private boolean closed;

    public ScopedVectorAllocator(Allocator allocator, Allocator.Context context)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public <T extends Vector> T allocate(Class<T> vectorType, int size, IntFunction<T> factory)
    {
        checkOpen();
        return allocator.allocate(
                context,
                requireNonNull(vectorType, "vectorType is null"),
                size,
                requireNonNull(factory, "factory is null"));
    }

    @Override
    public <T extends Vector> T allocatePooled(
            Object poolFamily,
            int minimumPoolCapacity,
            boolean exactCapacityMatch,
            Class<T> vectorType,
            Supplier<T> factory)
    {
        checkOpen();
        return allocator.allocatePooled(
                context,
                requireNonNull(poolFamily, "poolFamily is null"),
                minimumPoolCapacity,
                exactCapacityMatch,
                requireNonNull(vectorType, "vectorType is null"),
                requireNonNull(factory, "factory is null"));
    }

    @Override
    public <T extends Vector> T adopt(T vector)
    {
        checkOpen();
        return allocator.adopt(context, requireNonNull(vector, "vector is null"));
    }

    @Override
    public DictionaryVector dictionary(int[] ids, int length, Vector values)
    {
        checkOpen();
        return allocator.allocateDictionary(
                context,
                requireNonNull(ids, "ids is null"),
                length,
                requireNonNull(values, "values is null"));
    }

    @Override
    public RleVector runLength(int[] counts, Vector values)
    {
        checkOpen();
        return allocator.allocateRle(
                context,
                requireNonNull(counts, "counts is null"),
                requireNonNull(values, "values is null"));
    }

    @Override
    public Streams interleave(List<Streams> columns, int positionCount)
    {
        checkOpen();
        return allocator.interleaveStreams(
                context,
                List.copyOf(requireNonNull(columns, "columns is null")),
                positionCount);
    }

    @Override
    public <T extends Vector> T transfer(T vector)
    {
        checkOpen();
        return allocator.transfer(context, requireNonNull(vector, "vector is null"));
    }

    @Override
    public void release(Vector vector)
    {
        checkOpen();
        allocator.release(context, requireNonNull(vector, "vector is null"));
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            allocator.release(context);
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("vector allocator is closed");
        }
    }
}
