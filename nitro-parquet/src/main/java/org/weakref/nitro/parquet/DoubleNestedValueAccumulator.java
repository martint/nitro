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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/** Reusable accumulation for DOUBLE physical leaves. */
final class DoubleNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean nullable;
    private PrimitiveArrayPool arrayPool;
    private double[] values = EMPTY_DOUBLES;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private F64Vector directValues;
    private BooleanVector directNulls;
    private int size;

    DoubleNestedValueAccumulator(boolean nullable)
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
        directValues = F64Vector.allocate(allocator, context, exactSize);
        directNulls = nullable
                ? allocator.allocate(context, BooleanVector.class, exactSize, BooleanVector::new)
                : null;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof DoublePhysicalValueDecoder doubles)) {
            throw new IllegalArgumentException("Double accumulator requires a double physical decoder");
        }
        if (directValues == null) {
            ensureCapacity(size + 1);
            values[size] = doubles.value(ordinal, dictionaryId);
        }
        else {
            directValues.values()[size] = doubles.value(ordinal, dictionaryId);
        }
        if (nullable) {
            nullValues()[size] = false;
        }
        size++;
    }

    @Override
    public void appendNull()
    {
        if (!nullable) {
            throw new IllegalArgumentException("Required nested value is missing");
        }
        if (directValues == null) {
            ensureCapacity(size + 1);
            values[size] = 0;
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
            F64Vector result = directValues;
            BooleanVector resultNulls = directNulls;
            directValues = null;
            directNulls = null;
            return resultNulls == null ? Streams.ofValues(result) : Streams.ofValuesAndNulls(result, resultNulls);
        }
        F64Vector result = F64Vector.allocate(allocator, context, size);
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
        double[] replacement = arrayPool.borrowDoubles(capacity);
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
            values = EMPTY_DOUBLES;
            nulls = EMPTY_BOOLEANS;
            arrayPool = null;
            size = 0;
            directValues = null;
            directNulls = null;
        }
    }
}
