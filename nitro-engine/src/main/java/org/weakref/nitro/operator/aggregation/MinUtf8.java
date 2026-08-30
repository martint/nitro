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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MinUtf8StateVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class MinUtf8
        implements Accumulator
{
    private final int inputColumn;

    public MinUtf8(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        Allocator allocator = context.allocator();
        Allocator.Context allocationContext = context.allocationContext();
        return Streams.ofValues(allocator.allocate(allocationContext, MinUtf8StateVector.class, size, MinUtf8StateVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        MinUtf8StateVector values = (MinUtf8StateVector) state.values();
        if (values.length() >= size) {
            return state;
        }
        MinUtf8StateVector grown = MinUtf8StateVector.growOwned(allocator, allocationContext, values, size);
        return Streams.ofValues(grown);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        ((MinUtf8StateVector) state.values()).initialize(offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        MinUtf8StateVector values = (MinUtf8StateVector) state.values();
        Vector inputValues = streams.values(inputColumn);
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));
        for (int position : mask) {
            if (!inputNulls.value(position)) {
                update(values, group, inputValues, position);
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        MinUtf8StateVector values = (MinUtf8StateVector) state.values();
        org.weakref.nitro.data.I64Vector groupVector = (org.weakref.nitro.data.I64Vector) groups;
        Vector inputValues = streams.values(inputColumn);
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));
        for (int position : mask) {
            if (!inputNulls.value(position)) {
                update(values, toIntExact(groupVector.values()[position]), inputValues, position);
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        Mask allGroups = allocator.allocateAllMask(allocationContext, maxGroup + 1);
        try {
            return result(maxGroup, state, allGroups, output, allocator, allocationContext);
        }
        finally {
            allocator.release(allocationContext, allGroups);
        }
    }

    @Override
    public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        MinUtf8StateVector values = (MinUtf8StateVector) state.values();
        byte[] value = values.value(group);
        int byteCapacity = value == null ? 0 : value.length;

        BinaryVector outputValues = BinaryVector.allocateOrGrow(
                allocator,
                allocationContext,
                output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                size,
                byteCapacity);
        BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                size);

        if (value == null) {
            outputValues.setNull(outputPosition);
            outputNulls.values()[outputPosition] = true;
        }
        else {
            outputValues.setBytes(outputPosition, value);
            outputNulls.values()[outputPosition] = false;
        }
        return allocator.reuseValuesAndNulls(output, outputValues, outputNulls);
    }

    @Override
    public Streams result(int maxGroup, Streams state, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        MinUtf8StateVector values = (MinUtf8StateVector) state.values();
        int size = maxGroup + 1;
        int totalBytes = 0;
        if (mask.all()) {
            for (int group = 0; group < size; group++) {
                byte[] value = values.value(group);
                totalBytes += value == null ? 0 : value.length;
            }
        }
        else {
            for (int group : mask) {
                byte[] value = values.value(group);
                totalBytes += value == null ? 0 : value.length;
            }
        }

        BinaryVector outputValues = BinaryVector.allocateOrGrow(
                allocator,
                allocationContext,
                output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                size,
                totalBytes);
        Arrays.fill(outputValues.offsets(), 0);
        outputValues.clearTraits();
        outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);

        BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                size);
        Arrays.fill(outputNulls.values(), 0, size, false);

        if (mask.all()) {
            for (int group = 0; group < size; group++) {
                byte[] value = values.value(group);
                if (value == null) {
                    outputValues.setNull(group);
                    outputNulls.values()[group] = true;
                }
                else {
                    outputValues.setBytes(group, value);
                }
            }
        }
        else {
            int previousGroup = 0;
            for (int group : mask) {
                while (previousGroup < group) {
                    outputValues.setNull(previousGroup++);
                }
                byte[] value = values.value(group);
                if (value == null) {
                    outputValues.setNull(group);
                    outputNulls.values()[group] = true;
                }
                else {
                    outputValues.setBytes(group, value);
                }
                previousGroup = group + 1;
            }
            while (previousGroup < size) {
                outputValues.setNull(previousGroup++);
            }
        }
        return allocator.reuseValuesAndNulls(output, outputValues, outputNulls);
    }

    private static void update(MinUtf8StateVector state, int group, Vector inputValues, int position)
    {
        byte[] current = state.value(group);
        if (current == null || compare(inputValues, position, current) < 0) {
            state.setValue(group, copyBytes(inputValues, position));
        }
    }

    private static int compare(Vector inputValues, int position, byte[] bytes)
    {
        return switch (inputValues) {
            case BinaryVector values -> compare(values.data(), values.startOffset(position), values.length(position), bytes, 0, bytes.length);
            case DictionaryVector values -> compare(values.values(), values.ids()[position], bytes);
            case RleVector values -> compare(values.values(), values.runIndex(position), bytes);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + inputValues.getClass().getSimpleName());
        };
    }

    private static byte[] copyBytes(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.copyBytes(position);
            case DictionaryVector values -> copyBytes(values.values(), values.ids()[position]);
            case RleVector values -> copyBytes(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static int compare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        int minLength = Math.min(leftLength, rightLength);
        for (int index = 0; index < minLength; index++) {
            int comparison = Byte.toUnsignedInt(left[leftOffset + index]) - Byte.toUnsignedInt(right[rightOffset + index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }
}
