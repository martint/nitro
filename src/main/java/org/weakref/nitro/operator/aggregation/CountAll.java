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
import org.weakref.nitro.data.CountStateVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class CountAll
        implements FusedAggregator
{
    @Override
    public FusedAccumulatorSpec fusedSpec()
    {
        return new FusedAccumulatorSpec(CountStateVector.class, -1);
    }

    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValues(
                allocator.allocate(allocationContext, CountStateVector.class, size, CountStateVector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        CountStateVector values = (CountStateVector) state.values();
        if (values.length() >= size) {
            return state;
        }
        CountStateVector grown = allocator.adopt(allocationContext, CountStateVector.grow(values, size));
        allocator.discard(allocationContext, values);
        return Streams.ofValues(grown);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        // CountAll state is append-only and newly allocated/grown ranges are already zeroed.
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        CountStateVector stateVector = (CountStateVector) state.values();
        accumulate(stateVector, group, mask.count());
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        CountStateVector stateVector = (CountStateVector) state.values();
        I64Vector groupVector = (I64Vector) groups;

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                int group = toIntExact(groupVector.values()[position]);
                stateVector.increment(group, 1);
            }
        }
        else {
            for (int position : mask) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, group, 1);
            }
        }
    }

    private static void accumulate(CountStateVector stateVector, int group, int count)
    {
        stateVector.increment(group, count);
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        CountStateVector stateVector = (CountStateVector) state.values();
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.values(),
                I64Vector.class,
                stateVector.length(),
                I64Vector::new);
        stateVector.copyTo(values);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                values.length());
        Arrays.fill(nulls.values(), 0, values.length(), false);
        return Streams.ofValuesAndNulls(values, nulls);
    }

    @Override
    public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
    {
        CountStateVector stateVector = (CountStateVector) state.values();
        I64Vector values = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (I64Vector) output.getOrNull(Stream.VALUES),
                I64Vector.class,
                size,
                I64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                allocationContext,
                output == null ? null : output.getOrNull(Stream.NULLS),
                size);
        values.values()[outputPosition] = stateVector.value(group);
        nulls.values()[outputPosition] = false;
        return Streams.ofValuesAndNulls(values, nulls);
    }
}
