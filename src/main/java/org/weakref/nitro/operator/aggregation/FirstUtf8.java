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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Utf8StateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class FirstUtf8
        implements Accumulator
{
    private final int inputColumn;

    public FirstUtf8(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, Utf8StateVector.class, size, Utf8StateVector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        Utf8StateVector values = allocator.allocateOrGrow(allocationContext, (Utf8StateVector) state.values(), Utf8StateVector.class, size, Utf8StateVector::new);
        BooleanVector nulls = allocator.allocateOrGrow(allocationContext, (BooleanVector) state.get(Stream.NULLS), BooleanVector.class, size, BooleanVector::new);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        Arrays.fill(((BooleanVector) state.get(Stream.NULLS)).values(), offset, offset + length, true);
        Arrays.fill(((Utf8StateVector) state.values()).values(), offset, offset + length, null);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        Utf8StateVector stateValues = (Utf8StateVector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        Vector inputValues = streams.values(inputColumn);
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            if (stateNulls.values()[group] && !isNull(inputNulls, position)) {
                stateValues.values()[group] = value(inputValues, position);
                stateNulls.values()[group] = false;
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        Utf8StateVector stateValues = (Utf8StateVector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        I64Vector groupVector = (I64Vector) groups;
        Vector inputValues = streams.values(inputColumn);
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (stateNulls.values()[group] && !isNull(inputNulls, position)) {
                stateValues.values()[group] = value(inputValues, position);
                stateNulls.values()[group] = false;
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        Utf8StateVector stateValues = (Utf8StateVector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        int totalBytes = 0;
        boolean asciiOnly = true;
        for (byte[] value : stateValues.values()) {
            if (value == null) {
                continue;
            }
            totalBytes += value.length;
            asciiOnly &= isAscii(value);
        }

        BinaryVector values = output == null ? null : (BinaryVector) output.getOrNull(Stream.VALUES);
        values = allocator.allocateOrGrowBinary(allocationContext, values, stateValues.length(), totalBytes);
        values.addTrait(BinaryVector.Trait.UTF8_STRING);
        if (asciiOnly) {
            values.addTrait(BinaryVector.Trait.ASCII_ONLY);
        }
        for (int index = 0; index < stateValues.length(); index++) {
            byte[] value = stateValues.values()[index];
            if (value == null) {
                values.setNull(index);
            }
            else {
                values.setBytes(index, value);
            }
        }
        return Streams.ofValuesAndNulls(values, stateNulls);
    }

    private static byte[] value(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.copyBytes(position);
            case DictionaryVector values -> value(values.values(), values.ids()[position]);
            case RleVector values -> value(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static boolean isAscii(byte[] value)
    {
        for (byte b : value) {
            if ((b & 0x80) != 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean[] nulls(Vector vector)
    {
        return vector == null ? null : ((BooleanVector) vector).values();
    }

    private static boolean isNull(boolean[] nulls, int position)
    {
        return nulls != null && nulls[position];
    }
}
