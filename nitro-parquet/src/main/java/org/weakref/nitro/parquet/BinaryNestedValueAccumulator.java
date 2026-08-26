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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;
import static org.weakref.nitro.data.Utf8Traits.UTF8_STRING;

/** Reusable accumulation for BYTE_ARRAY leaves, preserving the schema's UTF-8 annotation. */
final class BinaryNestedValueAccumulator
        implements NestedValueAccumulator
{
    private static final int[] EMPTY_INTS = new int[0];
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];

    private final boolean utf8;
    private final boolean nullable;
    private PrimitiveArrayPool arrayPool;
    private int[] offsets = EMPTY_INTS;
    private byte[] data = EMPTY_BYTES;
    private boolean[] nulls = EMPTY_BOOLEANS;
    private int size;
    private int bytes;

    BinaryNestedValueAccumulator(boolean utf8, boolean nullable)
    {
        this.utf8 = utf8;
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
        bytes = 0;
    }

    @Override
    public void append(PhysicalValueDecoder decoder, int ordinal, int dictionaryId)
    {
        if (!(decoder instanceof BinaryPhysicalValueDecoder binary)) {
            throw new IllegalArgumentException("Binary accumulator requires a binary physical decoder");
        }
        int length = binary.length(ordinal, dictionaryId);
        ensurePositionCapacity(size + 1);
        ensureByteCapacity(bytes + length);
        offsets[size] = bytes;
        binary.copy(ordinal, dictionaryId, data, bytes);
        bytes += length;
        size++;
        offsets[size] = bytes;
        if (nullable) {
            nulls[size - 1] = false;
        }
    }

    @Override
    public void appendNull()
    {
        if (!nullable) {
            throw new IllegalArgumentException("Required nested value is missing");
        }
        ensurePositionCapacity(size + 1);
        offsets[size] = bytes;
        nulls[size] = true;
        size++;
        offsets[size] = bytes;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public Streams materialize(Allocator allocator, Allocator.Context context)
    {
        BinaryVector result = BinaryVector.allocate(allocator, context, size, bytes);
        System.arraycopy(offsets, 0, result.offsets(), 0, size + 1);
        System.arraycopy(data, 0, result.data(), 0, bytes);
        if (utf8) {
            result.addTrait(UTF8_STRING);
        }
        if (!nullable) {
            return Streams.ofValues(result);
        }
        BooleanVector resultNulls = allocator.allocate(context, BooleanVector.class, size, BooleanVector::new);
        System.arraycopy(nulls, 0, resultNulls.values(), 0, size);
        return Streams.ofValuesAndNulls(result, resultNulls);
    }

    private void ensurePositionCapacity(int required)
    {
        if (offsets.length >= required + 1) {
            return;
        }
        int capacity = Math.max(required, Math.max(16, offsets.length * 2));
        int[] replacementOffsets = arrayPool.borrowInts(capacity + 1);
        System.arraycopy(offsets, 0, replacementOffsets, 0, Math.min(offsets.length, size + 1));
        arrayPool.release(offsets);
        offsets = replacementOffsets;
        if (nullable) {
            boolean[] replacementNulls = arrayPool.borrowBooleans(capacity);
            System.arraycopy(nulls, 0, replacementNulls, 0, size);
            arrayPool.release(nulls);
            nulls = replacementNulls;
        }
    }

    private void ensureByteCapacity(int required)
    {
        if (data.length < required) {
            byte[] replacement = arrayPool.borrowBytes(Math.max(required, Math.max(64, data.length * 2)));
            System.arraycopy(data, 0, replacement, 0, bytes);
            arrayPool.release(data);
            data = replacement;
        }
    }

    @Override
    public void close()
    {
        if (arrayPool != null) {
            arrayPool.release(offsets);
            arrayPool.release(data);
            arrayPool.release(nulls);
            offsets = EMPTY_INTS;
            data = EMPTY_BYTES;
            nulls = EMPTY_BOOLEANS;
            arrayPool = null;
            size = 0;
            bytes = 0;
        }
    }
}
