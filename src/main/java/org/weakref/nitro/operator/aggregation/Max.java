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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class Max
        implements Accumulator
{
    private final int inputColumn;

    public Max(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                (I64Vector) allocator.allocate(allocationContext, size, I64Vector::new),
                (BooleanVector) allocator.allocate(allocationContext, size, BooleanVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        I64Vector values = (I64Vector) allocator.allocateOrGrow(allocationContext, state.values(), size, I64Vector::new);
        BooleanVector nulls = (BooleanVector) allocator.allocateOrGrow(allocationContext, state.get(Stream.NULLS), size, BooleanVector::new);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        Arrays.fill(((BooleanVector) state.get(Stream.NULLS)).values(), offset, offset + length, true);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        I64Vector stateValues = (I64Vector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        long[] inputValues = values(streams.values(inputColumn));
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            if (!isNull(inputNulls, position)) {
                if (stateNulls.values()[group]) {
                    stateValues.values()[group] = inputValues[position];
                    stateNulls.values()[group] = false;
                }
                else {
                    stateValues.values()[group] = Math.max(stateValues.values()[group], inputValues[position]);
                }
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        I64Vector stateValues = (I64Vector) state.values();
        BooleanVector stateNulls = (BooleanVector) state.get(Stream.NULLS);
        I64Vector groupVector = (I64Vector) groups;
        long[] inputValues = values(streams.values(inputColumn));
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (!isNull(inputNulls, position)) {
                if (stateNulls.values()[group]) {
                    stateValues.values()[group] = inputValues[position];
                    stateNulls.values()[group] = false;
                }
                else {
                    stateValues.values()[group] = Math.max(stateValues.values()[group], inputValues[position]);
                }
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return state;
    }

    private static long[] values(Vector v)
    {
        return switch (v) {
            case I64Vector iv -> iv.values();
            default -> throw new UnsupportedOperationException(v.getClass().getSimpleName());
        };
    }

    private static boolean[] nulls(Vector v)
    {
        return switch (v) {
            case null -> null;
            case BooleanVector vector -> vector.values();
            default -> null;
        };
    }

    private static boolean isNull(boolean[] nulls, int position)
    {
        return nulls != null && nulls[position];
    }
}
