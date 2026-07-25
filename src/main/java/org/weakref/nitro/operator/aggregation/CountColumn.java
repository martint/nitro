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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class CountColumn
        implements GeneratedGroupedAccumulator
{
    private final int inputColumn;

    public CountColumn(int inputColumn)
    {
        this.inputColumn = inputColumn;
    }

    int inputColumn()
    {
        return inputColumn;
    }

    @Override
    public GeneratedGroupedAccumulatorUpdate generatedGroupedUpdate()
    {
        return GeneratedGroupedAccumulatorUpdate.constantWhenNotNull(1, inputColumn);
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        Allocator allocator = context.allocator();
        Allocator.Context allocationContext = context.allocationContext();
        return Streams.ofValuesAndNulls(
                allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new),
                allocator.allocate(allocationContext, BooleanVector.class, size, BooleanVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        I64Vector values = allocator.allocateOrGrow(allocationContext, (I64Vector) state.values(), I64Vector.class, size, I64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(allocator, allocationContext, state.get(Stream.NULLS), size);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        I64Vector stateVector = (I64Vector) state.values();
        BooleanVector nulls = (BooleanVector) state.get(Stream.NULLS);
        Arrays.fill(nulls.values(), offset, offset + length, false);
        Arrays.fill(stateVector.values(), offset, offset + length, 0L);
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        I64Vector stateVector = (I64Vector) state.values();
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            if (!inputNulls.value(position)) {
                stateVector.values()[group]++;
            }
        }
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        I64Vector stateVector = (I64Vector) state.values();
        I64Vector groupVector = (I64Vector) groups;
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

        for (int position : mask) {
            int group = toIntExact(groupVector.values()[position]);
            if (!inputNulls.value(position)) {
                stateVector.values()[group]++;
            }
        }
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return state;
    }
}
