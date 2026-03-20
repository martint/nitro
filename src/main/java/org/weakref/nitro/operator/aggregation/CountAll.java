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

public class CountAll
        implements Accumulator
{
    @Override
    public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        return Streams.ofValues(
                allocator.allocate(allocationContext, I64Vector.class, size, I64Vector::new));
    }

    @Override
    public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
    {
        I64Vector values = allocator.allocateOrGrow(allocationContext, (I64Vector) state.values(), I64Vector.class, size, I64Vector::new);
        return Streams.ofValues(values);
    }

    @Override
    public void initialize(Streams state, int offset, int length)
    {
        // CountAll state is append-only and newly allocated/grown ranges are already zeroed.
    }

    @Override
    public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
    {
        I64Vector stateVector = (I64Vector) state.values();
        accumulate(stateVector, group, mask.count());
    }

    @Override
    public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
    {
        I64Vector stateVector = (I64Vector) state.values();
        I64Vector groupVector = (I64Vector) groups;

        if (mask.all()) {
            for (int position = 0; position <= mask.maxPosition(); position++) {
                int group = toIntExact(groupVector.values()[position]);
                stateVector.values()[group] += 1;
            }
        }
        else {
            for (int position : mask) {
                int group = toIntExact(groupVector.values()[position]);
                accumulate(stateVector, group, 1);
            }
        }
    }

    private static void accumulate(I64Vector stateVector, int group, int count)
    {
        stateVector.values()[group] += count;
    }

    @Override
    public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        I64Vector values = (I64Vector) state.values();
        BooleanVector nulls = allocator.allocateOrGrow(
                allocationContext,
                output == null ? null : (BooleanVector) output.getOrNull(Stream.NULLS),
                BooleanVector.class,
                values.length(),
                BooleanVector::new);
        Arrays.fill(nulls.values(), 0, values.length(), false);
        return Streams.ofValuesAndNulls(values, nulls);
    }
}
