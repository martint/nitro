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
import org.weakref.nitro.data.Streams;

import java.util.Arrays;

/** Reusable accumulation for DOUBLE physical leaves. */
final class DoubleNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean nullable;
    private double[] values = EMPTY_DOUBLES;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private int size;

    DoubleNestedValueAccumulator(boolean nullable)
    {
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
        if (!(decoder instanceof DoublePhysicalValueDecoder doubles)) {
            throw new IllegalArgumentException("Double accumulator requires a double physical decoder");
        }
        ensureCapacity(size + 1);
        values[size] = doubles.value(ordinal, dictionaryId);
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
        values = Arrays.copyOf(values, capacity);
        if (nullable) {
            nulls = Arrays.copyOf(nulls, capacity);
        }
    }
}
