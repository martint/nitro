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
package org.weakref.nitro.clickbench;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.AggregationExecutionContext;
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.aggregation.SumStateVector;

import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;

/** Test-harness sum with the per-input overflow semantics required by Trino {@code sum(bigint)}. */
final class ExactBigintSum
        implements Accumulator
{
    private final int inputColumn;
    private final Sum stateLayout;

    ExactBigintSum(int inputColumn)
    {
        this.inputColumn = inputColumn;
        this.stateLayout = new Sum(inputColumn);
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        return stateLayout.allocate(context, size);
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        return stateLayout.grow(allocator, allocationContext, state, size);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        stateLayout.initialize(state, offset, length);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        SumStateVector sums = (SumStateVector) state.values();
        Vector inputValues = streams.values(inputColumn);
        Vector inputNulls = streams.stream(inputColumn, Stream.NULLS);
        long initial = sums.sum(group);
        long sum = initial;
        boolean sawValue = false;

        if (mask.all() && VectorAccess.isAllFalseNulls(inputNulls)) {
            if (inputValues instanceof I64Vector values) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    sum = addExact(sum, values.values()[position]);
                }
                sawValue = mask.count() > 0;
            }
            else if (inputValues instanceof I32Vector values) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    sum = addExact(sum, values.values()[position]);
                }
                sawValue = mask.count() > 0;
            }
        }

        if (!sawValue) {
            VectorAccess.LongValues values = VectorAccess.longValues(inputValues);
            VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(inputNulls);
            for (int position : mask) {
                if (!nulls.value(position)) {
                    sum = addExact(sum, values.value(position));
                    sawValue = true;
                }
            }
        }
        if (sawValue) {
            sums.increment(group, sum - initial);
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        SumStateVector sums = (SumStateVector) state.values();
        VectorAccess.LongValues groupIds = VectorAccess.longValues(groups);
        VectorAccess.LongValues values = VectorAccess.longValues(streams.values(inputColumn));
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));
        for (int position : mask) {
            if (!nulls.value(position)) {
                int group = toIntExact(groupIds.value(position));
                long value = values.value(position);
                addExact(sums.sum(group), value);
                sums.increment(group, value);
            }
        }
    }

    @Override
    public Streams result(
            int maxGroup,
            Streams state,
            Streams output,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return stateLayout.result(maxGroup, state, output, allocator, allocationContext);
    }
}
