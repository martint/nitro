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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/** Reusable accumulation for BOOLEAN physical leaves. */
final class BooleanNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean nullable;
    private PrimitiveArrayPool arrayPool;
    private boolean[] values = EMPTY_BOOLEANS;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private BooleanVector directValues;
    private BooleanVector directNulls;
    private int size;

    BooleanNestedValueAccumulator(boolean nullable)
    {
        this.nullable = nullable;
    }

    @Override
    public void reset(Allocator allocator)
    {
        PrimitiveArrayPool requestedPool = requireNonNull(allocator, "allocator is null").primitiveArrays();
        if (arrayPool != null && arrayPool != requestedPool) {
            throw new IllegalArgumentException("Nested accumulator cannot change allocator ownership");
        }
        arrayPool = requestedPool;
        size = 0;
    }

    @Override
    public void reset(Allocator allocator, Allocator.Context context, int exactSize)
    {
        reset(allocator);
        directValues = allocator.allocate(context, BooleanVector.class, exactSize, BooleanVector::new);
        directNulls = nullable
                ? allocator.allocate(context, BooleanVector.class, exactSize, BooleanVector::new)
                : null;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof BooleanPhysicalValueDecoder booleans)) {
            throw new IllegalArgumentException("Boolean accumulator requires a boolean physical decoder");
        }
        if (dictionaryId >= 0) {
            throw new IllegalArgumentException("Parquet BOOLEAN values cannot use dictionary encoding");
        }
        if (directValues == null) {
            ensureCapacity(size + 1);
            values[size] = booleans.value(ordinal);
        }
        else {
            directValues.values()[size] = booleans.value(ordinal);
        }
        if (nullable) {
            nullValues()[size] = false;
        }
        size++;
    }

    @Override
    public void appendPlainRun(PhysicalValueDecoder decoder, int ordinal, int count)
    {
        if (!(decoder instanceof BooleanValueDecoder booleans)) {
            throw new IllegalArgumentException("Boolean accumulator requires a boolean physical decoder");
        }
        if (directValues == null) {
            ensureCapacity(size + count);
            booleans.copyPlain(ordinal, values, size, count);
        }
        else {
            booleans.copyPlain(ordinal, directValues.values(), size, count);
        }
        if (nullable) {
            Arrays.fill(nullValues(), size, size + count, false);
        }
        size += count;
    }

    @Override
    public void appendNull()
    {
        if (!nullable) {
            throw new IllegalArgumentException("Required nested value is missing");
        }
        if (directValues == null) {
            ensureCapacity(size + 1);
            values[size] = false;
        }
        nullValues()[size] = true;
        size++;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public Streams materialize(Allocator allocator, Allocator.Context context)
    {
        if (directValues != null) {
            BooleanVector result = directValues;
            BooleanVector resultNulls = directNulls;
            directValues = null;
            directNulls = null;
            return resultNulls == null ? Streams.ofValues(result) : Streams.ofValuesAndNulls(result, resultNulls);
        }
        BooleanVector result = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
        System.arraycopy(values, 0, result.values(), 0, size);
        if (!nullable) {
            return Streams.ofValues(result);
        }
        BooleanVector resultNulls = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
        System.arraycopy(nulls, 0, resultNulls.values(), 0, size);
        return Streams.ofValuesAndNulls(result, resultNulls);
    }

    private void ensureCapacity(int required)
    {
        if (values.length >= required) {
            return;
        }
        int capacity = Math.max(required, Math.max(16, values.length * 2));
        boolean[] replacement = arrayPool.borrowBooleans(capacity);
        System.arraycopy(values, 0, replacement, 0, size);
        arrayPool.release(values);
        values = replacement;
        if (nullable) {
            boolean[] replacementNulls = arrayPool.borrowBooleans(capacity);
            System.arraycopy(nulls, 0, replacementNulls, 0, size);
            arrayPool.release(nulls);
            nulls = replacementNulls;
        }
    }

    private boolean[] nullValues()
    {
        return directNulls == null ? nulls : directNulls.values();
    }

    @Override
    public void close()
    {
        if (arrayPool != null) {
            arrayPool.release(values);
            arrayPool.release(nulls);
            values = EMPTY_BOOLEANS;
            nulls = EMPTY_BOOLEANS;
            arrayPool = null;
            size = 0;
            directValues = null;
            directNulls = null;
        }
    }
}
