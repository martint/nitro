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
import org.weakref.nitro.data.AvgStateVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.StddevSampStateVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import static java.lang.Math.toIntExact;

/**
 * A planner-selected physical aggregation unit that computes integral COUNT(column), AVG, and
 * STDDEV_SAMP in one input traversal. Result slots are count, average, and standard deviation.
 */
public final class CountAvgStddevI64AggregationUnit
        implements PhysicalAggregationUnit
{
    private final int inputColumn;
    private final CountColumn count;
    private final Avg average;
    private final StddevSamp stddev;

    public CountAvgStddevI64AggregationUnit(int inputColumn)
    {
        this.inputColumn = inputColumn;
        this.count = new CountColumn(inputColumn);
        this.average = new Avg(inputColumn);
        this.stddev = new StddevSamp(inputColumn);
    }

    @Override
    public int outputCount()
    {
        return 3;
    }

    @Override
    public Object allocate(AggregationExecutionContext context, int size)
    {
        return new State(
                count.allocate(context, size),
                average.allocate(context, size),
                stddev.allocate(context, size));
    }

    @Override
    public Object grow(Allocator allocator, Allocator.Context allocationContext, Object stateObject, int size)
    {
        State state = (State) stateObject;
        return new State(
                count.grow(allocator, allocationContext, state.count(), size),
                average.grow(allocator, allocationContext, state.average(), size),
                stddev.grow(allocator, allocationContext, state.stddev(), size));
    }

    @Override
    public void initialize(Object stateObject, int offset, int length)
    {
        State state = (State) stateObject;
        count.initialize(state.count(), offset, length);
        average.initialize(state.average(), offset, length);
        stddev.initialize(state.stddev(), offset, length);
    }

    @Override
    public void accumulate(Object stateObject, int group, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        Vector inputNullVector = streams.stream(inputColumn, Stream.NULLS);
        boolean nullFree = VectorAccess.isAllFalseNulls(inputNullVector);
        VectorAccess.BooleanValues inputNulls = nullFree ? null : VectorAccess.booleanValues(inputNullVector);
        VectorAccess.LongValues input = VectorAccess.longValues(streams.values(inputColumn));

        long[] counts = ((I64Vector) state.count().values()).values();
        AvgStateVector averages = (AvgStateVector) state.average().values();
        StddevSampStateVector deviations = (StddevSampStateVector) state.stddev().values();
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                if (nullFree || !inputNulls.value(position)) {
                    accumulate(counts, averages, deviations, group, input.value(position));
                }
            }
        }
        else {
            for (int position : mask) {
                if (nullFree || !inputNulls.value(position)) {
                    accumulate(counts, averages, deviations, group, input.value(position));
                }
            }
        }
    }

    @Override
    public void accumulate(Object stateObject, Vector groups, Mask mask, StreamAccessor streams)
    {
        State state = (State) stateObject;
        long[] groupIds = ((I64Vector) groups).values();
        Vector inputNullVector = streams.stream(inputColumn, Stream.NULLS);
        boolean nullFree = VectorAccess.isAllFalseNulls(inputNullVector);
        VectorAccess.BooleanValues inputNulls = nullFree ? null : VectorAccess.booleanValues(inputNullVector);
        VectorAccess.LongValues input = VectorAccess.longValues(streams.values(inputColumn));

        long[] counts = ((I64Vector) state.count().values()).values();
        AvgStateVector averages = (AvgStateVector) state.average().values();
        StddevSampStateVector deviations = (StddevSampStateVector) state.stddev().values();
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                if (nullFree || !inputNulls.value(position)) {
                    accumulate(counts, averages, deviations, toIntExact(groupIds[position]), input.value(position));
                }
            }
        }
        else {
            for (int position : mask) {
                if (nullFree || !inputNulls.value(position)) {
                    accumulate(counts, averages, deviations, toIntExact(groupIds[position]), input.value(position));
                }
            }
        }
    }

    private static void accumulate(
            long[] counts,
            AvgStateVector averages,
            StddevSampStateVector deviations,
            int group,
            long value)
    {
        counts[group]++;
        averages.increment(group, value, 1);
        deviations.addSample(group, value);
    }

    @Override
    public Streams result(
            int output,
            int maxGroup,
            Object stateObject,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        State state = (State) stateObject;
        return switch (output) {
            case 0 -> count.result(maxGroup, state.count(), existing, allocator, allocationContext);
            case 1 -> average.result(maxGroup, state.average(), existing, allocator, allocationContext);
            case 2 -> stddev.result(maxGroup, state.stddev(), existing, allocator, allocationContext);
            default -> throw new IndexOutOfBoundsException("count/average/stddev result: " + output);
        };
    }

    @Override
    public Streams copyResultPosition(
            int output,
            int group,
            int maxGroup,
            Object stateObject,
            Streams existing,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        State state = (State) stateObject;
        return switch (output) {
            case 0 -> count.copyResultPosition(group, maxGroup, state.count(), existing, outputPosition, size, allocator, allocationContext);
            case 1 -> average.copyResultPosition(group, maxGroup, state.average(), existing, outputPosition, size, allocator, allocationContext);
            case 2 -> stddev.copyResultPosition(group, maxGroup, state.stddev(), existing, outputPosition, size, allocator, allocationContext);
            default -> throw new IndexOutOfBoundsException("count/average/stddev result: " + output);
        };
    }

    private record State(Streams count, Streams average, Streams stddev) {}
}
