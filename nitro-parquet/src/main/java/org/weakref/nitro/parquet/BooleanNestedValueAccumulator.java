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
import org.weakref.nitro.data.Streams;

import java.util.Arrays;

/** Reusable accumulation for BOOLEAN physical leaves. */
final class BooleanNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean nullable;
    private boolean[] values = EMPTY_BOOLEANS;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private int size;

    BooleanNestedValueAccumulator(boolean nullable)
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
        if (!(decoder instanceof BooleanPhysicalValueDecoder booleans)) {
            throw new IllegalArgumentException("Boolean accumulator requires a boolean physical decoder");
        }
        if (dictionaryId >= 0) {
            throw new IllegalArgumentException("Parquet BOOLEAN values cannot use dictionary encoding");
        }
        ensureCapacity(size + 1);
        values[size] = booleans.value(ordinal);
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
        values[size] = false;
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
        values = Arrays.copyOf(values, capacity);
        if (nullable) {
            nulls = Arrays.copyOf(nulls, capacity);
        }
    }
}
