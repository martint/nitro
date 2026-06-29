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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;

import static java.lang.Math.toIntExact;

/**
 * Two cooperating I64 min / max accumulators on the same input column that share a single input
 * scan per batch.
 *
 * <p>The paired accumulators expose the ordinary {@link Accumulator} contract so the operator can
 * drive them with no operation-specific knowledge. Internally, the first accumulator in iteration
 * order (the "scanner") walks the input once per batch and writes both the min and the max state
 * into its own state bundle plus a reference to the follower's state held on a shared handle.
 * The second accumulator (the "follower") contributes nothing during accumulate — its state has
 * already been updated by the scanner — and only participates during {@code allocate} /
 * {@code grow} / {@code initialize} / {@code result}.
 *
 * <p>The factory methods enforce the ordering. Callers that want to fuse a {@link Min} and a
 * {@link Max} instance on the same input column should use
 * {@link AccumulatorFusion#fuse(java.util.List)} rather than instantiating these directly; that
 * helper preserves the original list positions and paired ordering automatically.
 */
final class FusedMinMaxI64
{
    private FusedMinMaxI64() {}

    static final class SharedState
    {
        private Streams peerState;

        private Streams peerState()
        {
            return peerState;
        }

        private void setPeerState(Streams peerState)
        {
            this.peerState = peerState;
        }
    }

    /**
     * The scanner does both the min and the max update per batch in one pass.
     *
     * @param inputColumn the input column both min and max observe
     * @param handle      the shared handle that the follower will populate with its own
     *                    {@link Streams} during {@code allocate}
     * @param scannerKind whether this scanner's own state holds the min or the max result
     */
    static Accumulator scanner(int inputColumn, SharedState handle, Kind scannerKind)
    {
        return new Scanner(inputColumn, handle, scannerKind);
    }

    /**
     * The follower does no work during accumulate; the scanner already wrote to its state.
     */
    static Accumulator follower(int inputColumn, SharedState handle)
    {
        return new Follower(inputColumn, handle);
    }

    enum Kind
    {
        MIN,
        MAX
    }

    private abstract static class Base
            implements Accumulator
    {
        final int inputColumn;
        final SharedState handle;

        Base(int inputColumn, SharedState handle)
        {
            this.inputColumn = inputColumn;
            this.handle = handle;
        }

        @Override
        public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
        {
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
            Arrays.fill(((BooleanVector) state.get(Stream.NULLS)).values(), offset, offset + length, true);
        }

        @Override
        public Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext)
        {
            return state;
        }
    }

    private static final class Scanner
            extends Base
    {
        private final Kind kind;

        Scanner(int inputColumn, SharedState handle, Kind kind)
        {
            super(inputColumn, handle);
            this.kind = kind;
        }

        @Override
        public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
        {
            Vector inputVector = streams.values(inputColumn);
            Vector inputNullVector = streams.stream(inputColumn, Stream.NULLS);
            if (VectorAccess.isAllFalseNulls(inputNullVector)
                    && accumulateGlobalNullFree(state, group, mask, inputVector)) {
                return;
            }

            Streams peer = handle.peerState();
            Streams minState = kind == Kind.MIN ? state : peer;
            Streams maxState = kind == Kind.MAX ? state : peer;

            I64Vector minValues = (I64Vector) minState.values();
            BooleanVector minNulls = (BooleanVector) minState.get(Stream.NULLS);
            I64Vector maxValues = (I64Vector) maxState.values();
            BooleanVector maxNulls = (BooleanVector) maxState.get(Stream.NULLS);

            VectorAccess.LongValues inputValues = VectorAccess.longValues(inputVector);
            VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(inputNullVector);

            for (int position : mask) {
                if (inputNulls.value(position)) {
                    continue;
                }
                long value = inputValues.value(position);
                if (minNulls.values()[group]) {
                    minValues.values()[group] = value;
                    minNulls.values()[group] = false;
                    maxValues.values()[group] = value;
                    maxNulls.values()[group] = false;
                }
                else {
                    if (value < minValues.values()[group]) {
                        minValues.values()[group] = value;
                    }
                    if (value > maxValues.values()[group]) {
                        maxValues.values()[group] = value;
                    }
                }
            }
        }

        /**
         * Monomorphic fast path for a single (global) group over a null-free flat input: hoists the group's
         * running min/max into locals and scans the input array directly, with no per-position null lambda,
         * no boxed mask iterator, and no re-read of the state arrays. Returns false (falling back to the
         * general path) for input kinds it does not specialize. The caller guarantees the input is null-free.
         */
        private boolean accumulateGlobalNullFree(Streams state, int group, Mask mask, Vector inputVector)
        {
            if (mask.count() == 0) {
                return true;
            }

            Streams peer = handle.peerState();
            Streams minState = kind == Kind.MIN ? state : peer;
            Streams maxState = kind == Kind.MAX ? state : peer;
            long[] minValues = ((I64Vector) minState.values()).values();
            boolean[] minNulls = ((BooleanVector) minState.get(Stream.NULLS)).values();
            long[] maxValues = ((I64Vector) maxState.values()).values();
            boolean[] maxNulls = ((BooleanVector) maxState.get(Stream.NULLS)).values();

            long min;
            long max;
            if (minNulls[group]) {
                min = Long.MAX_VALUE;
                max = Long.MIN_VALUE;
            }
            else {
                min = minValues[group];
                max = maxValues[group];
            }

            switch (inputVector) {
                case I64Vector vector -> {
                    long[] input = vector.values();
                    if (mask.all()) {
                        int end = mask.maxPosition();
                        for (int position = 0; position <= end; position++) {
                            long value = input[position];
                            min = Math.min(min, value);
                            max = Math.max(max, value);
                        }
                    }
                    else {
                        for (int position : mask.selectedPositions()) {
                            long value = input[position];
                            min = Math.min(min, value);
                            max = Math.max(max, value);
                        }
                    }
                }
                case I32Vector vector -> {
                    int[] input = vector.values();
                    if (mask.all()) {
                        int end = mask.maxPosition();
                        for (int position = 0; position <= end; position++) {
                            long value = input[position];
                            min = Math.min(min, value);
                            max = Math.max(max, value);
                        }
                    }
                    else {
                        for (int position : mask.selectedPositions()) {
                            long value = input[position];
                            min = Math.min(min, value);
                            max = Math.max(max, value);
                        }
                    }
                }
                default -> {
                    return false;
                }
            }

            // count() > 0 and the input is null-free, so at least one value was folded in.
            minValues[group] = min;
            maxValues[group] = max;
            minNulls[group] = false;
            maxNulls[group] = false;
            return true;
        }

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
        {
            Streams peer = handle.peerState();
            Streams minState = kind == Kind.MIN ? state : peer;
            Streams maxState = kind == Kind.MAX ? state : peer;

            I64Vector minValues = (I64Vector) minState.values();
            BooleanVector minNulls = (BooleanVector) minState.get(Stream.NULLS);
            I64Vector maxValues = (I64Vector) maxState.values();
            BooleanVector maxNulls = (BooleanVector) maxState.get(Stream.NULLS);
            I64Vector groupVector = (I64Vector) groups;

            VectorAccess.LongValues inputValues = VectorAccess.longValues(streams.values(inputColumn));
            VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(streams.stream(inputColumn, Stream.NULLS));

            for (int position : mask) {
                if (inputNulls.value(position)) {
                    continue;
                }
                int group = toIntExact(groupVector.values()[position]);
                long value = inputValues.value(position);
                if (minNulls.values()[group]) {
                    minValues.values()[group] = value;
                    minNulls.values()[group] = false;
                    maxValues.values()[group] = value;
                    maxNulls.values()[group] = false;
                }
                else {
                    if (value < minValues.values()[group]) {
                        minValues.values()[group] = value;
                    }
                    if (value > maxValues.values()[group]) {
                        maxValues.values()[group] = value;
                    }
                }
            }
        }
    }

    private static final class Follower
            extends Base
    {
        Follower(int inputColumn, SharedState handle)
        {
            super(inputColumn, handle);
        }

        @Override
        public Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size)
        {
            Streams state = super.allocate(allocator, allocationContext, size);
            handle.setPeerState(state);
            return state;
        }

        @Override
        public Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size)
        {
            // Reallocating the follower's backing vectors may return a fresh Streams; refresh the
            // handle so the scanner writes into the new target on the next batch.
            Streams grown = super.grow(allocator, allocationContext, state, size);
            handle.setPeerState(grown);
            return grown;
        }

        @Override
        public void accumulate(Streams state, int group, Mask mask, StreamAccessor streams)
        {
            // Scanner already updated this state for the current batch.
        }

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
        {
            // Scanner already updated this state for the current batch.
        }
    }
}
