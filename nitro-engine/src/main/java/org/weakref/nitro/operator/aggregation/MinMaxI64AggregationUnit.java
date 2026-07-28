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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

/**
 * A planner-selected physical aggregation unit that computes I64 minimum and maximum in one input
 * traversal. Result slot zero is the minimum and result slot one is the maximum.
 */
public final class MinMaxI64AggregationUnit
        implements PhysicalAggregationUnit
{
    private final int inputColumn;

    public MinMaxI64AggregationUnit(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    @Override
    public int outputCount()
    {
        return 2;
    }

    @Override
    public Object allocate(AggregationExecutionContext context, int size)
    {
        return new State(
                allocateResult(context.allocator(), context.allocationContext(), size),
                allocateResult(context.allocator(), context.allocationContext(), size));
    }

    private static Streams allocateResult(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public Object grow(Allocator allocator, Allocator.Context allocationContext, Object stateObject, int size)
    {
        State state = (State) stateObject;
        return new State(
                growResult(allocator, allocationContext, state.minimum(), size),
                growResult(allocator, allocationContext, state.maximum(), size));
    }

    private static Streams growResult(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                (I64Vector) state.values(),
                I64Vector.class,
                size,
                I64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                state.get(Stream.NULLS),
                size);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public void initialize(Object stateObject, int offset, int length)
    {
        State state = (State) stateObject;
        Arrays.fill(((BooleanVector) state.minimum().get(Stream.NULLS)).values(), offset, offset + length, true);
        Arrays.fill(((BooleanVector) state.maximum().get(Stream.NULLS)).values(), offset, offset + length, true);
    }

    @Override
    public void accumulate(Object stateObject, int group, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        Vector inputVector = streams.values(inputColumn);
        Vector inputNullVector = streams.stream(inputColumn, Stream.NULLS);
        if (VectorAccess.isAllFalseNulls(inputNullVector) && accumulateGlobalNullFree(state, group, mask, inputVector)) {
            return;
        }

        long[] minimums = ((I64Vector) state.minimum().values()).values();
        boolean[] minimumNulls = ((BooleanVector) state.minimum().get(Stream.NULLS)).values();
        long[] maximums = ((I64Vector) state.maximum().values()).values();
        boolean[] maximumNulls = ((BooleanVector) state.maximum().get(Stream.NULLS)).values();
        VectorAccess.LongValues values = VectorAccess.longValues(inputVector);
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(inputNullVector);

        for (int position : mask) {
            if (!nulls.value(position)) {
                update(group, values.value(position), minimums, minimumNulls, maximums, maximumNulls);
            }
        }
    }

    private static boolean accumulateGlobalNullFree(State state, int group, Mask mask, Vector inputVector)
    {
        if (mask.none()) {
            return true;
        }

        long[] minimums = ((I64Vector) state.minimum().values()).values();
        boolean[] minimumNulls = ((BooleanVector) state.minimum().get(Stream.NULLS)).values();
        long[] maximums = ((I64Vector) state.maximum().values()).values();
        boolean[] maximumNulls = ((BooleanVector) state.maximum().get(Stream.NULLS)).values();
        long minimum = minimumNulls[group] ? Long.MAX_VALUE : minimums[group];
        long maximum = maximumNulls[group] ? Long.MIN_VALUE : maximums[group];

        switch (inputVector) {
            case I64Vector vector -> {
                long[] values = vector.values();
                if (mask.all()) {
                    for (int position = 0; position <= mask.maxPosition(); position++) {
                        minimum = Math.min(minimum, values[position]);
                        maximum = Math.max(maximum, values[position]);
                    }
                }
                else {
                    for (int position : mask.selectedPositions()) {
                        minimum = Math.min(minimum, values[position]);
                        maximum = Math.max(maximum, values[position]);
                    }
                }
            }
            case I32Vector vector -> {
                int[] values = vector.values();
                if (mask.all()) {
                    for (int position = 0; position <= mask.maxPosition(); position++) {
                        minimum = Math.min(minimum, values[position]);
                        maximum = Math.max(maximum, values[position]);
                    }
                }
                else {
                    for (int position : mask.selectedPositions()) {
                        minimum = Math.min(minimum, values[position]);
                        maximum = Math.max(maximum, values[position]);
                    }
                }
            }
            default -> {
                return false;
            }
        }

        minimums[group] = minimum;
        maximums[group] = maximum;
        minimumNulls[group] = false;
        maximumNulls[group] = false;
        return true;
    }

    @Override
    public void accumulate(Object stateObject, Vector groups, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        long[] groupIds = ((I64Vector) groups).values();
        long[] minimums = ((I64Vector) state.minimum().values()).values();
        boolean[] minimumNulls = ((BooleanVector) state.minimum().get(Stream.NULLS)).values();
        long[] maximums = ((I64Vector) state.maximum().values()).values();
        boolean[] maximumNulls = ((BooleanVector) state.maximum().get(Stream.NULLS)).values();
        VectorAccess.LongValues values = VectorAccess.longValues(streams.values(inputColumn));
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            if (!nulls.value(position)) {
                update(
                        toIntExact(groupIds[position]),
                        values.value(position),
                        minimums,
                        minimumNulls,
                        maximums,
                        maximumNulls);
            }
        }
    }

    private static void update(
            int group,
            long value,
            long[] minimums,
            boolean[] minimumNulls,
            long[] maximums,
            boolean[] maximumNulls)
    {
        if (minimumNulls[group]) {
            minimums[group] = value;
            maximums[group] = value;
            minimumNulls[group] = false;
            maximumNulls[group] = false;
            return;
        }
        minimums[group] = Math.min(minimums[group], value);
        maximums[group] = Math.max(maximums[group], value);
    }

    @Override
    public Streams result(int output, int maxGroup, Object stateObject, Streams existing, Allocator allocator, Allocator.Context allocationContext)
    {
        State state = (State) stateObject;
        return switch (output) {
            case 0 -> state.minimum();
            case 1 -> state.maximum();
            default -> throw new IndexOutOfBoundsException("min/max result: " + output);
        };
    }

    private record State(Streams minimum, Streams maximum) {}
}
