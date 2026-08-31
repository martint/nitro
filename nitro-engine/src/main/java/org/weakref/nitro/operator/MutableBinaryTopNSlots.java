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
package org.weakref.nitro.operator;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/** Mutable winner slots for variable-width TopN keys, separated from compact vector layout. */
final class MutableBinaryTopNSlots
        implements AutoCloseable
{
    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final PrimitiveArrayPool arrays;
    private final byte[][] values;
    private final byte[] emptyValue;
    private final int[] lengths;
    private final boolean[] nulls;
    private Set<BinaryVector.Trait> commonTraits;
    private boolean exposesNulls;
    private long retainedBytes;
    private boolean closed;

    MutableBinaryTopNSlots(Allocator allocator, Allocator.Context allocationContext, int capacity)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.allocationContext = requireNonNull(allocationContext, "allocationContext is null");
        arrays = allocator.primitiveArrays();
        values = new byte[capacity][];
        emptyValue = arrays.borrowBytes(0);
        lengths = arrays.borrowInts(capacity);
        nulls = arrays.borrowBooleans(capacity);
        Arrays.fill(lengths, 0);
        Arrays.fill(nulls, false);
        updateRetainedBytes((long) capacity * (Long.BYTES + Integer.BYTES + 1));
    }

    void copy(
            VectorAccess.BinaryRegions regions,
            Set<BinaryVector.Trait> traits,
            boolean exposesNulls,
            boolean isNull,
            int position,
            int slot)
    {
        updateTraits(traits);
        this.exposesNulls |= exposesNulls;
        nulls[slot] = isNull;
        if (isNull) {
            replace(slot, null, 0, 0);
            return;
        }

        replace(slot, regions.data(position), regions.offset(position), regions.length(position));
    }

    int compare(VectorAccess.BinaryRegions candidate, int candidatePosition, int slot)
    {
        return Arrays.compareUnsigned(
                candidate.data(candidatePosition),
                candidate.offset(candidatePosition),
                candidate.offset(candidatePosition) + candidate.length(candidatePosition),
                value(slot),
                0,
                lengths[slot]);
    }

    int compare(int leftSlot, int rightSlot)
    {
        return Arrays.compareUnsigned(value(leftSlot), 0, lengths[leftSlot], value(rightSlot), 0, lengths[rightSlot]);
    }

    boolean isNull(int slot)
    {
        return nulls[slot];
    }

    Streams materialize(int[] orderedSlots, int count)
    {
        int totalBytes = 0;
        for (int index = 0; index < count; index++) {
            totalBytes = Math.addExact(totalBytes, lengths[orderedSlots[index]]);
        }
        BinaryVector result = BinaryVector.allocate(allocator, allocationContext, count, totalBytes);
        if (commonTraits != null) {
            result.addTraits(commonTraits);
        }
        BooleanVector resultNulls = exposesNulls
                ? allocator.allocate(allocationContext, BooleanVector.class, count, BooleanVector::new)
                : null;
        for (int index = 0; index < count; index++) {
            int slot = orderedSlots[index];
            if (resultNulls != null) {
                resultNulls.values()[index] = nulls[slot];
            }
            if (lengths[slot] == 0) {
                result.setNull(index);
            }
            else {
                result.setBytes(index, values[slot], 0, lengths[slot]);
            }
        }
        return resultNulls == null ? Streams.ofValues(result) : Streams.ofValuesAndNulls(result, resultNulls);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        for (byte[] value : values) {
            arrays.release(value);
        }
        arrays.release(emptyValue);
        arrays.release(lengths);
        arrays.release(nulls);
        updateRetainedBytes(0);
    }

    private void replace(int slot, byte[] source, int offset, int length)
    {
        byte[] previous = values[slot];
        if (length == 0) {
            values[slot] = null;
            lengths[slot] = 0;
            if (previous != null) {
                updateRetainedBytes(retainedBytes - previous.length);
                arrays.release(previous);
            }
            return;
        }
        if (previous == null || previous.length < length) {
            byte[] replacement = arrays.borrowBytesBetween(length, Allocator.growthCapacity(length));
            values[slot] = replacement;
            updateRetainedBytes(retainedBytes + replacement.length - (previous == null ? 0 : previous.length));
            arrays.release(previous);
        }
        if (length > 0) {
            System.arraycopy(source, offset, values[slot], 0, length);
        }
        lengths[slot] = length;
    }

    private byte[] value(int slot)
    {
        return values[slot] == null ? emptyValue : values[slot];
    }

    private void updateTraits(Set<BinaryVector.Trait> traits)
    {
        if (commonTraits == null) {
            commonTraits = Set.copyOf(traits);
            return;
        }
        if (commonTraits.equals(traits)) {
            return;
        }
        LinkedHashSet<BinaryVector.Trait> intersection = new LinkedHashSet<>(commonTraits);
        intersection.retainAll(traits);
        commonTraits = Set.copyOf(intersection);
    }

    private void updateRetainedBytes(long bytes)
    {
        retainedBytes = bytes;
        allocator.setRetainedBytes(allocationContext, this, bytes);
    }
}
