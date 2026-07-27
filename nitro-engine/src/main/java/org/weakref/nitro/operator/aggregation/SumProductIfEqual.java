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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import static java.lang.Math.toIntExact;

/**
 * Conditional SQL sum for pivot-style aggregations: {@code sum(if(discriminator = literal, left * right, 0))}.
 * Keeping the condition and product inside the accumulator avoids materializing a sparse value vector for every
 * pivot bucket. Null multiplication inputs are ignored when the condition matches, exactly like SQL {@code sum}.
 */
public final class SumProductIfEqual
        implements Accumulator
{
    private final int discriminatorColumn;
    private final long literal;
    private final int leftColumn;
    private final int rightColumn;

    public SumProductIfEqual(int discriminatorColumn, long literal, int leftColumn, int rightColumn)
    {
        this.discriminatorColumn = discriminatorColumn;
        this.literal = literal;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        Allocator allocator = context.allocator();
        Allocator.Context allocationContext = context.allocationContext();
        return Streams.ofValues(allocator.allocate(allocationContext, SumStateVector.class, size, SumStateVector::new));
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
        SumStateVector sums = (SumStateVector) state.values();
        Inputs inputs = inputs(streams);
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                accumulate(sums, group, inputs, position);
            }
        }
        else {
            for (int position : mask) {
                accumulate(sums, group, inputs, position);
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        SumStateVector sums = (SumStateVector) state.values();
        long[] groupIds = ((I64Vector) groups).values();
        Inputs inputs = inputs(streams);
        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                accumulate(sums, toIntExact(groupIds[position]), inputs, position);
            }
        }
        else {
            for (int position : mask) {
                accumulate(sums, toIntExact(groupIds[position]), inputs, position);
            }
        }
    }

    private Inputs inputs(StreamAccessor streams)
    {
        return new Inputs(
                VectorAccess.longValues(streams.values(discriminatorColumn)),
                VectorAccess.longValues(streams.values(leftColumn)),
                VectorAccess.booleanValues(streams.stream(leftColumn, Stream.NULLS)),
                VectorAccess.longValues(streams.values(rightColumn)),
                VectorAccess.booleanValues(streams.stream(rightColumn, Stream.NULLS)));
    }

    private void accumulate(SumStateVector sums, int group, Inputs inputs, int position)
    {
        if (inputs.discriminator().value(position) != literal) {
            sums.increment(group, 0);
            return;
        }
        if (inputs.leftNulls().value(position) || inputs.rightNulls().value(position)) {
            return;
        }
        sums.increment(group, inputs.left().value(position) * inputs.right().value(position));
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        SumStateVector stateVector = (SumStateVector) state.values();
        int visibleCount = Math.max(maxGroup + 1, 0);
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.values(),
                I64Vector.class,
                visibleCount,
                I64Vector::new);
        stateVector.copySumsTo(values, visibleCount);
        if (!stateVector.hasAnyNull()) {
            BooleanVector sentinel = allocator.allocate(allocationContext, BooleanVector.class, 1, BooleanVector::new);
            Vector nulls = allocator.allocateSingleRunRle(allocationContext, visibleCount, sentinel);
            return Streams.ofValues(values).with(Stream.NULLS, nulls);
        }
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                visibleCount);
        stateVector.copyNullsTo(nulls, visibleCount);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private record Inputs(
            VectorAccess.LongValues discriminator,
            VectorAccess.LongValues left,
            VectorAccess.BooleanValues leftNulls,
            VectorAccess.LongValues right,
            VectorAccess.BooleanValues rightNulls) {}
}
