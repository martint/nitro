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

import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTarget;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.util.Arrays;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

public class CountAll
        implements GeneratedGroupedAccumulator
{
    private static final GroupedAggregationUpdateTarget GROUPED_UPDATE = groupedUpdate();

    @Override
    public GroupedAggregationUpdate generatedGroupedUpdate()
    {
        return GroupedAggregationUpdate.constant(
                1,
                GROUPED_UPDATE);
    }

    private static GroupedAggregationUpdateTarget groupedUpdate()
    {
        try {
            return new GroupedAggregationUpdateTarget(
                    lookup().findVirtual(CountStateVector.class, "update", methodType(void.class, int.class, long.class)),
                    Optional.of(lookup().findStatic(
                            CountAll.class,
                            "updateRepeated",
                            methodType(void.class, CountStateVector.class, int.class, long.class, int.class))));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void updateRepeated(CountStateVector state, int group, long contribution, int count)
    {
        state.increment(group, contribution * count);
    }

    @Override
    public Streams allocate(AggregationExecutionContext context, int size)
    {
        Allocator allocator = context.allocator();
        Allocator.Context allocationContext = context.allocationContext();
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
        CountStateVector grown = CountStateVector.growOwned(allocator, allocationContext, values, size);
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
        return allocator.reuseValuesAndNulls(output, values, nulls);
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
        return allocator.reuseValuesAndNulls(output, values, nulls);
    }

    @Override
    public Streams copyResultRange(int groupStart, int groupCount, int maxGroup, Streams state, Streams output, int outputStart, int size, Allocator allocator, Allocator.Context allocationContext)
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
        stateVector.copyRangeTo(values, groupStart, outputStart, groupCount);
        Arrays.fill(nulls.values(), outputStart, outputStart + groupCount, false);
        return allocator.reuseValuesAndNulls(output, values, nulls);
    }
}
