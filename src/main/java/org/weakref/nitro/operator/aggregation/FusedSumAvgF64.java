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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import static java.lang.Math.toIntExact;

/**
 * Cooperating F64 SUM and AVG accumulators over one input column. The scanner updates both ordinary
 * accumulator states in one row pass; the follower retains its independent lifecycle and result contract.
 */
final class FusedSumAvgF64
{
    private FusedSumAvgF64() {}

    enum Kind
    {
        SUM,
        AVG
    }

    static final class SharedState
    {
        private Streams sumState;
        private Streams avgState;

        private void set(Kind kind, Streams state)
        {
            if (kind == Kind.SUM) {
                sumState = state;
            }
            else {
                avgState = state;
            }
        }
    }

    static Accumulator scanner(Accumulator delegate, int inputColumn, SharedState handle, Kind kind)
    {
        return new Scanner(delegate, inputColumn, handle, kind);
    }

    static Accumulator follower(Accumulator delegate, SharedState handle, Kind kind)
    {
        return new Follower(delegate, handle, kind);
    }

    private abstract static class Base
            implements Accumulator
    {
        final Accumulator delegate;
        final SharedState handle;
        final Kind kind;

        Base(Accumulator delegate, SharedState handle, Kind kind)
        {
            this.delegate = delegate;
            this.handle = handle;
            this.kind = kind;
        }

        @Override
        public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            Streams state = kind == Kind.AVG
                    ? Streams.ofValues(allocator.allocate(allocationContext, CountStateVector.class, size, CountStateVector::new))
                    : delegate.allocate(allocator, allocationContext, size);
            handle.set(kind, state);
            return state;
        }

        @Override
        public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
        {
            Streams grown;
            if (kind == Kind.AVG) {
                CountStateVector counts = (CountStateVector) state.values();
                if (counts.length() >= size) {
                    return state;
                }
                CountStateVector replacement = allocator.adopt(allocationContext, CountStateVector.grow(counts, size));
                allocator.discard(allocationContext, counts);
                grown = Streams.ofValues(replacement);
            }
            else {
                grown = delegate.grow(allocator, allocationContext, state, size);
            }
            handle.set(kind, grown);
            return grown;
        }

        @Override
        public void initialize(Streams state, int offset, int length)
        {
            if (kind == Kind.SUM) {
                delegate.initialize(state, offset, length);
            }
        }

        @Override
        public Streams result(int maxGroup, Streams state, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
        {
            return kind == Kind.AVG
                    ? averageResult(state, output, allocator, allocationContext)
                    : delegate.result(maxGroup, state, mask, output, allocator, allocationContext);
        }

        @Override
        public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
        {
            return kind == Kind.AVG
                    ? averageResult(state, output, allocator, allocationContext)
                    : delegate.result(maxGroup, state, output, allocator, allocationContext);
        }

        @Override
        public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
        {
            if (kind == Kind.SUM) {
                return delegate.copyResultPosition(group, maxGroup, state, output, outputPosition, size, allocator, allocationContext);
            }
            CountStateVector counts = (CountStateVector) state.values();
            F64Vector values = allocator.allocateOrGrow(
                    allocationContext,
                    output == null ? null : (F64Vector) output.getOrNull(Stream.VALUES),
                    F64Vector.class,
                    size,
                    F64Vector::new);
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    allocator,
                    allocationContext,
                    output == null ? null : output.getOrNull(Stream.NULLS),
                    size);
            long count = counts.value(group);
            nulls.values()[outputPosition] = count == 0;
            values.values()[outputPosition] = count == 0 ? 0 : ((F64Vector) handle.sumState.values()).values()[group] / count;
            return Streams.reuseValuesAndNulls(output, values, nulls);
        }

        private Streams averageResult(Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
        {
            CountStateVector counts = (CountStateVector) state.values();
            F64Vector values = allocator.allocateOrGrow(
                    allocationContext,
                    output == null ? null : (F64Vector) output.getOrNull(Stream.VALUES),
                    F64Vector.class,
                    counts.length(),
                    F64Vector::new);
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    allocator,
                    allocationContext,
                    output == null ? null : output.getOrNull(Stream.NULLS),
                    counts.length());
            double[] sums = ((F64Vector) handle.sumState.values()).values();
            for (int group = 0; group < counts.length(); group++) {
                long count = counts.value(group);
                nulls.values()[group] = count == 0;
                values.values()[group] = count == 0 ? 0 : sums[group] / count;
            }
            return Streams.reuseValuesAndNulls(output, values, nulls);
        }
    }

    private static final class Scanner
            extends Base
    {
        private final int inputColumn;

        Scanner(Accumulator delegate, int inputColumn, SharedState handle, Kind kind)
        {
            super(delegate, handle, kind);
            this.inputColumn = inputColumn;
        }

        @Override
        public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
        {
            Vector inputNullsVector = streams.stream(inputColumn, Stream.NULLS);
            VectorAccess.DoubleValues input = VectorAccess.doubleValues(streams.values(inputColumn));
            boolean nullFree = VectorAccess.isAllFalseNulls(inputNullsVector);
            VectorAccess.BooleanValues inputNulls = nullFree ? null : VectorAccess.booleanValues(inputNullsVector);

            double[] sum = ((F64Vector) handle.sumState.values()).values();
            boolean[] sumNulls = ((BooleanVector) handle.sumState.get(Stream.NULLS)).values();
            CountStateVector counts = (CountStateVector) handle.avgState.values();
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(sum, sumNulls, counts, group, input.value(position));
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(sum, sumNulls, counts, group, input.value(position));
                    }
                }
            }
        }

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
        {
            long[] groupIds = ((I64Vector) groups).values();
            Vector inputNullsVector = streams.stream(inputColumn, Stream.NULLS);
            VectorAccess.DoubleValues input = VectorAccess.doubleValues(streams.values(inputColumn));
            boolean nullFree = VectorAccess.isAllFalseNulls(inputNullsVector);
            VectorAccess.BooleanValues inputNulls = nullFree ? null : VectorAccess.booleanValues(inputNullsVector);

            double[] sum = ((F64Vector) handle.sumState.values()).values();
            boolean[] sumNulls = ((BooleanVector) handle.sumState.get(Stream.NULLS)).values();
            CountStateVector counts = (CountStateVector) handle.avgState.values();
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(sum, sumNulls, counts, toIntExact(groupIds[position]), input.value(position));
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(sum, sumNulls, counts, toIntExact(groupIds[position]), input.value(position));
                    }
                }
            }
        }

        private static void accumulate(double[] sum, boolean[] sumNulls, CountStateVector counts, int group, double value)
        {
            sum[group] += value;
            sumNulls[group] = false;
            counts.increment(group, 1);
        }
    }

    private static final class Follower
            extends Base
    {
        Follower(Accumulator delegate, SharedState handle, Kind kind)
        {
            super(delegate, handle, kind);
        }

        @Override
        public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams) {}

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams) {}
    }
}
