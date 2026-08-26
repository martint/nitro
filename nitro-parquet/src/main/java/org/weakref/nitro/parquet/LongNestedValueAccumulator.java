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
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

/** Reusable accumulation for integer-backed physical leaves. */
final class LongNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final long[] EMPTY_LONGS = new long[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean outputInt32;
    private final boolean nullable;
    private long[] values = EMPTY_LONGS;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private int size;

    LongNestedValueAccumulator(boolean outputInt32, boolean nullable)
    {
        this.outputInt32 = outputInt32;
        this.nullable = nullable;
    }

    @Override
    public void reset()
    {
        size = 0;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof LongPhysicalValueDecoder longs)) {
            throw new IllegalArgumentException("Integer accumulator requires an integer physical decoder");
        }
        ensureCapacity(size + 1);
        values[size] = longs.value(ordinal, dictionaryId);
        if (nullable) {
            nulls[size] = false;
        }
        size++;
    }

    @Override
    public void appendNull()
    {
        if (!nullable) {
            throw new IllegalArgumentException("Required nested value is missing");
        }
        ensureCapacity(size + 1);
        values[size] = 0;
        nulls[size] = true;
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
        values = Arrays.copyOf(values, capacity);
        if (nullable) {
            nulls = Arrays.copyOf(nulls, capacity);
        }
    }
}
