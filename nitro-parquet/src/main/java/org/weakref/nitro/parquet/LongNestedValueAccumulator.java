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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/** Reusable accumulation for integer-backed physical leaves. */
final class LongNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final long[] EMPTY_LONGS = new long[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean outputInt32;
    private final boolean nullable;
    private PrimitiveArrayPool arrayPool;
    private long[] values = EMPTY_LONGS;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private Vector directValues;
    private BooleanVector directNulls;
    private int size;

    LongNestedValueAccumulator(boolean outputInt32, boolean nullable)
    {
        this.outputInt32 = outputInt32;
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
        directValues = outputInt32
                ? I32Vector.allocate(allocator, context, exactSize)
                : I64Vector.allocate(allocator, context, exactSize);
        directNulls = nullable
                ? allocator.allocate(context, BooleanVector.class, exactSize, BooleanVector::new)
                : null;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof LongValueDecoder longs)) {
            throw new IllegalArgumentException("Integer accumulator requires an integer physical decoder");
        }
        long value = longs.value(ordinal, dictionaryId);
        if (directValues instanceof I32Vector integers) {
            integers.values()[size] = toIntExact(value);
        }
        else if (directValues instanceof I64Vector longValues) {
            longValues.values()[size] = value;
        }
        else {
            ensureCapacity(size + 1);
            values[size] = value;
        }
        if (nullable) {
            nullValues()[size] = false;
        }
        size++;
    }

    @Override
    public void appendPlainRun(PhysicalValueDecoder decoder, int ordinal, int count)
    {
        if (!(decoder instanceof LongValueDecoder longs)) {
            throw new IllegalArgumentException("Integer accumulator requires an integer physical decoder");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count is negative");
        }
        if (directValues instanceof I64Vector longValues) {
            longs.copyPlain(ordinal, longValues.values(), size, count);
        }
        else if (directValues instanceof I32Vector integers) {
            for (int index = 0; index < count; index++) {
                integers.values()[size + index] = toIntExact(longs.value(ordinal + index, -1));
            }
        }
        else {
            ensureCapacity(size + count);
            longs.copyPlain(ordinal, values, size, count);
        }
        if (nullable) {
            java.util.Arrays.fill(nullValues(), size, size + count, false);
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
            Vector result = directValues;
            BooleanVector resultNulls = directNulls;
            directValues = null;
            directNulls = null;
            return resultNulls == null ? Streams.ofValues(result) : Streams.ofValuesAndNulls(result, resultNulls);
        }
        Vector result;
        if (outputInt32) {
            I32Vector integers = I32Vector.allocate(allocator, context, size);
            for (int index = 0; index < size; index++) {
                integers.values()[index] = toIntExact(values[index]);
            }
            result = integers;
        }
        else {
            I64Vector longs = I64Vector.allocate(allocator, context, size);
            System.arraycopy(values, 0, longs.values(), 0, size);
            result = longs;
        }
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
        long[] replacement = arrayPool.borrowLongs(capacity);
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
            values = EMPTY_LONGS;
            nulls = EMPTY_BOOLEANS;
            arrayPool = null;
            size = 0;
            directValues = null;
            directNulls = null;
        }
    }
}
