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
package org.weakref.nitro.operator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.OperatorAssertions;
import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.AggregationPositionAccumulator;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.data.Row.row;
import static org.weakref.nitro.operator.RegisteredAggregationWindowFunction.Frame.FULL_PARTITION;
import static org.weakref.nitro.operator.RegisteredAggregationWindowFunction.Frame.RUNNING_PEERS;
import static org.weakref.nitro.operator.RegisteredAggregationWindowFunction.Frame.RUNNING_ROWS;

class TestRegisteredAggregationWindowFunction
{
    @Test
    void evaluatesProviderAggregateForRunningAndFullPartitionFrames()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(allocator, 3, List.of(
                row(1L, 2L, 5L),
                row(1L, 1L, 3L),
                row(2L, 1L, (Object) null),
                row(1L, 3L, (Object) null),
                row(2L, 2L, 7L)));
        NullableSum running = new NullableSum(true);
        NullableSum partition = new NullableSum(false);

        try (Operator operator = new WindowOperator(
                allocator,
                source,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(
                        new RegisteredAggregationWindowFunction(
                                running,
                                source.outputSchema(),
                                new int[] {2},
                                RUNNING_ROWS),
                        new RegisteredAggregationWindowFunction(
                                partition,
                                source.outputSchema(),
                                new int[] {2},
                                FULL_PARTITION)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 1L, 3L, 3L, 8L),
                            row(1L, 2L, 5L, 8L, 8L),
                            row(1L, 3L, null, 8L, 8L),
                            row(2L, 1L, null, null, 7L),
                            row(2L, 2L, 7L, 7L, 7L));
        }
        assertThat(running.copyResultCalls).isEqualTo(5);
        assertThat(running.boundPositionCalls).isEqualTo(5);
        assertThat(partition.copyResultCalls).isZero();
    }

    @Test
    void evaluatesProviderAggregateForPeerRunningFrame()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(allocator, 3, List.of(
                row(1L, 2L, 5L),
                row(1L, 1L, 3L),
                row(1L, 1L, 7L),
                row(2L, 1L, 11L),
                row(1L, 3L, (Object) null),
                row(2L, 1L, 13L)));
        NullableSum peers = new NullableSum(false);

        try (Operator operator = new WindowOperator(
                allocator,
                source,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(new RegisteredAggregationWindowFunction(
                        peers,
                        source.outputSchema(),
                        new int[] {2},
                        RUNNING_PEERS,
                        new int[] {1})))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 1L, 3L, 10L),
                            row(1L, 1L, 7L, 10L),
                            row(1L, 2L, 5L, 15L),
                            row(1L, 3L, null, 15L),
                            row(2L, 1L, 11L, 24L),
                            row(2L, 1L, 13L, 24L));
        }
        assertThat(peers.copyResultCalls).isZero();
    }

    @Test
    void evaluatesProviderAggregateForBoundedFramesAcrossSourcePages()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        TableOperator source = new TableOperator(
                3,
                List.of(
                        TableOperator.Page.values(
                                3,
                                new Vector[] {
                                        new I64Vector(new long[] {1, 1, 2}),
                                        new I64Vector(new long[] {1, 3, 1}),
                                        new I64Vector(new long[] {2, 4, 10})},
                                Mask.all(3)),
                        TableOperator.Page.values(
                                2,
                                new Vector[] {
                                        new I64Vector(new long[] {1, 2}),
                                        new I64Vector(new long[] {2, 2}),
                                        new I64Vector(new long[] {3, 20})},
                                Mask.all(2))));
        NullableSum bounded = new NullableSum(false);
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(
                Math.max(0, outputPosition - 1),
                Math.min(partition.size(), outputPosition + 2));

        try (Operator operator = new WindowOperator(
                allocator,
                source,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(new RegisteredAggregationWindowFunction(
                        bounded,
                        source.outputSchema(),
                        new int[] {2},
                        frame)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 1L, 2L, 5L),
                            row(1L, 2L, 3L, 9L),
                            row(1L, 3L, 4L, 7L),
                            row(2L, 1L, 10L, 30L),
                            row(2L, 2L, 20L, 30L));
        }
        assertThat(bounded.copyResultCalls).isZero();
        assertThat(bounded.boundPositionCalls).isZero();
    }

    @Test
    void evaluatesEmptyBoundedFrameUsingAggregateIdentity()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(2L), row(3L)));
        WindowFrame frame = (_, outputPosition, bounds) -> {
            if (outputPosition == 1) {
                bounds.set(0, 1);
            }
        };

        try (Operator operator = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        new NullableSum(true),
                        source.outputSchema(),
                        new int[] {0},
                        frame)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(2L, null),
                            row(3L, 2L));
        }
    }

    @Test
    void resolvesPeerGroupsAcrossSourcePages()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        TableOperator source = new TableOperator(
                3,
                List.of(
                        TableOperator.Page.values(
                                2,
                                new Vector[] {
                                        new I64Vector(new long[] {1, 1}),
                                        new I64Vector(new long[] {1, 1}),
                                        new I64Vector(new long[] {2, 3})},
                                Mask.all(2)),
                        TableOperator.Page.values(
                                2,
                                new Vector[] {
                                        new I64Vector(new long[] {1, 1}),
                                        new I64Vector(new long[] {1, 2}),
                                        new I64Vector(new long[] {5, 7})},
                                Mask.all(2))));
        WindowFrame peers = (partition, outputPosition, bounds) -> bounds.set(
                partition.peerStart(outputPosition),
                partition.peerEnd(outputPosition));

        try (Operator operator = new WindowOperator(
                allocator,
                source,
                new int[] {0},
                new int[] {1},
                new boolean[] {false},
                List.of(new RegisteredAggregationWindowFunction(
                        new NullableSum(false),
                        source.outputSchema(),
                        new int[] {2},
                        peers)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 1L, 2L, 10L),
                            row(1L, 1L, 3L, 10L),
                            row(1L, 1L, 5L, 10L),
                            row(1L, 2L, 7L, 7L));
        }
    }

    private static final class NullableSum
            implements AggregationImplementation
    {
        private final boolean directCopy;
        private int copyResultCalls;
        private int boundPositionCalls;

        private NullableSum(boolean directCopy)
        {
            this.directCopy = directCopy;
        }

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            return new State();
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            return state;
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            State sum = (State) state;
            sum.value = 0;
            sum.hasValue = false;
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            State sum = (State) state;
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
            VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(input.stream(0, Stream.NULLS));
            for (int position : mask) {
                if (!nulls.value(position)) {
                    sum.value += values.value(position);
                    sum.hasValue = true;
                }
            }
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggregationPositionAccumulator bindRawInputPosition(Object state, int group, AggregationInput input)
        {
            if (!directCopy) {
                return null;
            }
            State sum = (State) state;
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
            VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(input.stream(0, Stream.NULLS));
            return position -> {
                boundPositionCalls++;
                if (!nulls.value(position)) {
                    sum.value += values.value(position);
                    sum.hasValue = true;
                }
            };
        }

        @Override
        public void addIntermediate(Object state, int group, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams intermediate(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams result(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            State sum = (State) state;
            I64Vector values = allocator.allocateOrGrow(
                    allocationContext,
                    (I64Vector) existing.getOrNull(Stream.VALUES),
                    I64Vector.class,
                    1,
                    I64Vector::new);
            BooleanVector nulls = allocator.allocateOrGrow(
                    allocationContext,
                    (BooleanVector) existing.getOrNull(Stream.NULLS),
                    BooleanVector.class,
                    1,
                    BooleanVector::new);
            values.values()[0] = sum.value;
            nulls.values()[0] = !sum.hasValue;
            return Streams.ofValuesAndNulls(values, nulls);
        }

        @Override
        public Streams copyResultPosition(
                int group,
                int maxGroup,
                Object state,
                Streams existing,
                int outputPosition,
                int size,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            if (!directCopy) {
                return null;
            }
            copyResultCalls++;
            State sum = (State) state;
            I64Vector values = allocator.allocateOrGrow(
                    allocationContext,
                    existing == null ? null : (I64Vector) existing.getOrNull(Stream.VALUES),
                    I64Vector.class,
                    size,
                    I64Vector::new);
            BooleanVector nulls = allocator.allocateOrGrow(
                    allocationContext,
                    existing == null ? null : (BooleanVector) existing.getOrNull(Stream.NULLS),
                    BooleanVector.class,
                    size,
                    BooleanVector::new);
            values.values()[outputPosition] = sum.value;
            nulls.values()[outputPosition] = !sum.hasValue;
            return Streams.ofValuesAndNulls(values, nulls);
        }

        private static final class State
        {
            private long value;
            private boolean hasValue;
        }
    }
}
