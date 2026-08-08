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

import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.MultiAggregationImplementation;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.SumStateVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.RegisteredMultiAggregationUnit;

import java.util.Collections;

import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static org.weakref.nitro.operator.aggregation.RegisteredMultiAggregationUnit.OutputMode.INTERMEDIATE;

/** SQL-shaped test implementation for planner-fused {@code sum(integer_base + integer_constant)} families. */
final class ExactAffineBigintSums
        implements MultiAggregationImplementation
{
    private final long[] offsets;

    ExactAffineBigintSums(int outputs)
    {
        if (outputs < 2) {
            throw new IllegalArgumentException("at least two outputs are required");
        }
        offsets = new long[outputs];
        for (int output = 0; output < outputs; output++) {
            offsets[output] = output;
        }
    }

    static RegisteredMultiAggregationUnit partial(int outputs)
    {
        return new RegisteredMultiAggregationUnit(
                new ExactAffineBigintSums(outputs),
                Collections.nCopies(outputs, INTERMEDIATE),
                new int[] {0});
    }

    @Override
    public int outputCount()
    {
        return offsets.length;
    }

    @Override
    public int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        return requiredGroups;
    }

    @Override
    public Object allocate(AggregationExecution execution, int groups)
    {
        SumStateVector[] states = new SumStateVector[offsets.length];
        for (int output = 0; output < states.length; output++) {
            states[output] = execution.allocator().allocate(
                    execution.allocationContext(),
                    SumStateVector.class,
                    groups,
                    SumStateVector::new);
        }
        return new State(states);
    }

    @Override
    public Object grow(Allocator allocator, Allocator.Context context, Object state, int groups)
    {
        State current = (State) state;
        for (int output = 0; output < current.states().length; output++) {
            SumStateVector values = current.states()[output];
            if (values.length() < groups) {
                SumStateVector grown = SumStateVector.grow(values, groups);
                allocator.discard(context, values);
                current.states()[output] = allocator.adopt(context, grown);
            }
        }
        return current;
    }

    @Override
    public void initialize(Object state, int offset, int length)
    {
        for (SumStateVector values : ((State) state).states()) {
            values.initialize(offset, length);
        }
    }

    @Override
    public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
    {
        State sums = (State) state;
        Vector inputValues = input.stream(0, Stream.VALUES);
        Vector inputNulls = input.stream(0, Stream.NULLS);
        DenseBatch batch = denseBatch(mask, inputValues, inputNulls);
        for (int output = 0; output < offsets.length; output++) {
            SumStateVector values = sums.states()[output];
            long previous = values.sum(group);
            Long reduced = batch == null ? null : reduce(previous, offsets[output], batch);
            long sum = reduced == null ? addInput(previous, offsets[output], mask, inputValues, inputNulls) : reduced;
            boolean sawValue = batch == null ? hasValue(mask, inputNulls) : batch.count() > 0;
            if (sawValue) {
                values.increment(group, sum - previous);
            }
        }
    }

    private static DenseBatch denseBatch(Mask mask, Vector values, Vector nulls)
    {
        if (!mask.all() || !VectorAccess.isAllFalseNulls(nulls) || mask.count() == 0) {
            return null;
        }
        int count = mask.count();
        long sum = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        try {
            if (values instanceof I64Vector longs) {
                for (int position = 0; position < count; position++) {
                    long value = longs.values()[position];
                    sum = addExact(sum, value);
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
            }
            else if (values instanceof I32Vector integers) {
                for (int position = 0; position < count; position++) {
                    long value = integers.values()[position];
                    sum += value;
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
            }
            else {
                VectorAccess.LongValues encoded = VectorAccess.longValues(values);
                for (int position = 0; position < count; position++) {
                    long value = encoded.value(position);
                    sum = addExact(sum, value);
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
            }
        }
        catch (ArithmeticException _) {
            return null;
        }
        return new DenseBatch(count, sum, min, max);
    }

    private static Long reduce(long previous, long offset, DenseBatch batch)
    {
        long minimum = affineValue(batch.min(), offset);
        long maximum = affineValue(batch.max(), offset);
        if (minimum < 0 && maximum > 0) {
            return null;
        }
        try {
            long offsetSum = multiplyExact(offset, batch.count());
            long batchSum = addExact(batch.sum(), offsetSum);
            return addExact(previous, batchSum);
        }
        catch (ArithmeticException _) {
            return null;
        }
    }

    private static long addInput(long sum, long offset, Mask mask, Vector inputValues, Vector inputNulls)
    {
        VectorAccess.LongValues inputLongs = VectorAccess.longValues(inputValues);
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(inputNulls);
        for (int position : mask) {
            if (!nulls.value(position)) {
                sum = addExact(sum, affineValue(inputLongs.value(position), offset));
            }
        }
        return sum;
    }

    private static boolean hasValue(Mask mask, Vector inputNulls)
    {
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(inputNulls);
        for (int position : mask) {
            if (!nulls.value(position)) {
                return true;
            }
        }
        return false;
    }

    private static long affineValue(long base, long offset)
    {
        return addExact(toIntExact(base), toIntExact(offset));
    }

    @Override
    public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
    {
        VectorAccess.LongValues groupIds = VectorAccess.longValues(groups);
        addInput((State) state, position -> toIntExact(groupIds.value(position)), mask, input);
    }

    private void addInput(State state, GroupLookup groups, Mask mask, AggregationInput input)
    {
        VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(input.stream(0, Stream.NULLS));
        for (int position : mask) {
            if (nulls.value(position)) {
                continue;
            }
            int group = groups.group(position);
            long base = values.value(position);
            for (int output = 0; output < offsets.length; output++) {
                SumStateVector sums = state.states()[output];
                long value = affineValue(base, offsets[output]);
                long previous = sums.sum(group);
                long sum = addExact(previous, value);
                sums.increment(group, sum - previous);
            }
        }
    }

    @Override
    public Streams intermediate(int output, int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context context)
    {
        int groups = Math.max(maxGroup + 1, 0);
        I64Vector values = allocator.allocateOrGrow(
                context,
                existing == null ? null : (I64Vector) existing.getOrNull(Stream.VALUES),
                I64Vector.class,
                groups,
                I64Vector::new);
        BooleanVector nulls = VectorAccess.writableBooleanVector(
                allocator,
                context,
                existing == null ? null : existing.getOrNull(Stream.NULLS),
                groups);
        ((State) state).states()[output].copySumsTo(values, groups);
        ((State) state).states()[output].copyNullsTo(nulls, groups);
        return allocator.reuseValuesAndNulls(existing, values, nulls);
    }

    @Override
    public Streams result(int output, int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context context)
    {
        return intermediate(output, maxGroup, state, existing, allocator, context);
    }

    @FunctionalInterface
    private interface GroupLookup
    {
        int group(int position);
    }

    private record State(SumStateVector[] states) {}

    private record DenseBatch(int count, long sum, long min, long max) {}
}
