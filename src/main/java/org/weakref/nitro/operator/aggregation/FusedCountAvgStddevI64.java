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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import static java.lang.Math.toIntExact;

/**
 * Cooperating integral COUNT(column), AVG(column), and STDDEV_SAMP(column) accumulators. The first
 * accumulator in output order scans the shared input, null stream, and group-id stream once and
 * updates all three independent SQL states; the other two retain their ordinary lifecycle and
 * result contracts but do no repeated row scan.
 */
final class FusedCountAvgStddevI64
{
    enum Kind
    {
        COUNT,
        AVG,
        STDDEV
    }

    static final class SharedState
    {
        private Streams count;
        private Streams avg;
        private Streams stddev;

        private void set(Kind kind, Streams state)
        {
            switch (kind) {
                case COUNT -> count = state;
                case AVG -> avg = state;
                case STDDEV -> stddev = state;
            }
        }
    }

    private FusedCountAvgStddevI64() {}

    static Accumulator create(
            Accumulator delegate,
            int inputColumn,
            SharedState handle,
            Kind kind,
            boolean scanner)
    {
        return scanner
                ? new Scanner(delegate, inputColumn, handle, kind)
                : new Follower(delegate, handle, kind);
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
            Streams state = delegate.allocate(allocator, allocationContext, size);
            handle.set(kind, state);
            return state;
        }

        @Override
        public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
        {
            Streams grown = delegate.grow(allocator, allocationContext, state, size);
            handle.set(kind, grown);
            return grown;
        }

        @Override
        public void initialize(Streams state, int offset, int length)
        {
            delegate.initialize(state, offset, length);
        }

        @Override
        public Streams result(int maxGroup, Streams state, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
        {
            return delegate.result(maxGroup, state, mask, output, allocator, allocationContext);
        }

        @Override
        public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
        {
            return delegate.result(maxGroup, state, output, allocator, allocationContext);
        }

        @Override
        public Streams copyResultPosition(int group, int maxGroup, Streams state, Streams output, int outputPosition, int size, Allocator allocator, Allocator.Context allocationContext)
        {
            return delegate.copyResultPosition(group, maxGroup, state, output, outputPosition, size, allocator, allocationContext);
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
            Vector inputNullVector = streams.stream(inputColumn, Stream.NULLS);
            boolean nullFree = VectorAccess.isAllFalseNulls(inputNullVector);
            VectorAccess.BooleanValues inputNulls = nullFree ? null : VectorAccess.booleanValues(inputNullVector);
            VectorAccess.LongValues input = VectorAccess.longValues(streams.values(inputColumn));

            long[] counts = ((I64Vector) handle.count.values()).values();
            AvgStateVector averages = (AvgStateVector) handle.avg.values();
            StddevSampStateVector stddev = (StddevSampStateVector) handle.stddev.values();
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(counts, averages, stddev, group, input.value(position));
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(counts, averages, stddev, group, input.value(position));
                    }
                }
            }
        }

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
        {
            long[] groupIds = ((I64Vector) groups).values();
            Vector inputNullVector = streams.stream(inputColumn, Stream.NULLS);
            boolean nullFree = VectorAccess.isAllFalseNulls(inputNullVector);
            VectorAccess.BooleanValues inputNulls = nullFree ? null : VectorAccess.booleanValues(inputNullVector);
            VectorAccess.LongValues input = VectorAccess.longValues(streams.values(inputColumn));

            long[] counts = ((I64Vector) handle.count.values()).values();
            AvgStateVector averages = (AvgStateVector) handle.avg.values();
            StddevSampStateVector stddev = (StddevSampStateVector) handle.stddev.values();
            if (mask.all()) {
                for (int position = 0; position <= mask.maxPosition(); position++) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(counts, averages, stddev, toIntExact(groupIds[position]), input.value(position));
                    }
                }
            }
            else {
                for (int position : mask) {
                    if (nullFree || !inputNulls.value(position)) {
                        accumulate(counts, averages, stddev, toIntExact(groupIds[position]), input.value(position));
                    }
                }
            }
        }

        private static void accumulate(long[] counts, AvgStateVector averages, StddevSampStateVector stddev, int group, long value)
        {
            counts[group]++;
            averages.increment(group, value);
            stddev.addSample(group, value);
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
