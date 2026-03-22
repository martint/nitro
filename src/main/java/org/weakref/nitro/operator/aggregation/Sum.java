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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import static java.lang.Math.toIntExact;

public class Sum
        implements Accumulator
{
    private final int inputColumn;

    public Sum(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValues(
                allocator.allocate(allocationContext, SumStateVector.class, size, SumStateVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        SumStateVector values = (SumStateVector) state.values();
        if (values.length() >= size) {
            return state;
        }
        SumStateVector grown = allocator.adopt(allocationContext, SumStateVector.grow(values, size));
        allocator.discard(allocationContext, values);
        return Streams.ofValues(grown);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        ((SumStateVector) state.values()).initialize(offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        Vector inputValues = streams.values(inputColumn);
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        long sum = 0;
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                sum += isNull(inputNulls, position) ? 0 : value(inputValues, position);
            }
        }
        else {
            for (int position : mask) {
                sum += isNull(inputNulls, position) ? 0 : value(inputValues, position);
            }
        }

        stateVector.increment(group, sum);
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        I64Vector groupVector = (I64Vector) groups;
        Vector inputValues = streams.values(inputColumn);
        boolean[] inputNulls = nulls(streams.stream(inputColumn, Stream.NULLS));

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                int group = toIntExact(groupVector.values()[position]);
                stateVector.increment(group, isNull(inputNulls, position) ? 0 : value(inputValues, position));
            }
        }
        else {
            for (int position : mask) {
                int group = toIntExact(groupVector.values()[position]);
                stateVector.increment(group, isNull(inputNulls, position) ? 0 : value(inputValues, position));
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.values(),
                I64Vector.class,
                stateVector.length(),
                I64Vector::new);
        stateVector.copySumsTo(values);
        BooleanVector nulls = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (BooleanVector) output.getOrNull(Stream.NULLS),
                BooleanVector.class,
                stateVector.length(),
                BooleanVector::new);
        stateVector.copyNullsTo(nulls);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private static long value(Vector v, int position)
    {
        return switch (v) {
            case I64Vector values -> values.values()[position];
            case I32Vector values -> values.values()[position];
            case DictionaryVector values -> value(values.values(), values.ids()[position]);
            case RleVector values -> value(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but found " + v.getClass().getSimpleName());
        };
    }

    private static boolean[] nulls(Vector v)
    {
        return v == null ? null : ((BooleanVector) v).values();
    }

    private static boolean isNull(boolean[] nulls, int position)
    {
        return nulls != null && nulls[position];
    }
}
