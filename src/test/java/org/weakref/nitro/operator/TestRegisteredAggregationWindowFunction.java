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
import org.weakref.nitro.core.function.aggregation.AggregationWindowFrameBounds;
import org.weakref.nitro.core.function.aggregation.AggregationWindowPartition;
import org.weakref.nitro.core.function.aggregation.PrimitiveAggregationInput;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeConsumer;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeContribution;
import org.weakref.nitro.core.function.aggregation.ReversibleAggregationPositionAccumulator;
import org.weakref.nitro.core.function.aggregation.ReversibleAggregationWindowKernel;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;
import static org.weakref.nitro.operator.RegisteredAggregationWindowFunction.Frame.FULL_PARTITION;
import static org.weakref.nitro.operator.RegisteredAggregationWindowFunction.Frame.RUNNING_PEERS;
import static org.weakref.nitro.operator.RegisteredAggregationWindowFunction.Frame.RUNNING_ROWS;
import static org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit.InputMode.RAW;
import static org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit.OutputMode.FINAL;

class TestRegisteredAggregationWindowFunction
{
    @Test
    void expandsRepeatedPrimitiveValuesByDefault()
    {
        long[] total = new long[1];
        int[] calls = new int[1];
        PrimitiveRangeConsumer consumer = new PrimitiveRangeConsumer()
        {
            @Override
            public void addLong(long value)
            {
                total[0] += value;
                calls[0]++;
            }
        };

        consumer.addRepeatedLong(7, 3);
        consumer.addRepeatedLong(99, 0);

        assertThat(total[0]).isEqualTo(21);
        assertThat(calls[0]).isEqualTo(3);
        assertThatThrownBy(() -> consumer.addRepeatedLong(1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("count is negative");
    }

    @Test
    void sendsPrimitiveWindowResultsToSumAndCardinalityProviders()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L), row(4L), row(5L)));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(
                Math.max(0, outputPosition - 1),
                Math.min(partition.size(), outputPosition + 2));
        PrimitiveInputAggregation sum = PrimitiveInputAggregation.sum();
        PrimitiveInputAggregation secondSum = PrimitiveInputAggregation.sum();
        PrimitiveInputAggregation count = PrimitiveInputAggregation.count();
        PhysicalAggregationProgram program = new PhysicalAggregationProgram(
                List.of(
                        new RegisteredAggregationUnit(sum, RAW, FINAL, new int[] {0}),
                        new RegisteredAggregationUnit(secondSum, RAW, FINAL, new int[] {1}),
                        new RegisteredAggregationUnit(count, RAW, FINAL, new int[0])),
                List.of(
                        new PhysicalAggregationProgram.Output(0, 0),
                        new PhysicalAggregationProgram.Output(1, 0),
                        new PhysicalAggregationProgram.Output(2, 0)));
        WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(
                        new RegisteredAggregationWindowFunction(
                                new TightCount(),
                                source.outputSchema(),
                                new int[0],
                                frame),
                        new RegisteredAggregationWindowFunction(
                                new TightCount(),
                                source.outputSchema(),
                                new int[0],
                                frame))).withOutputs(1, 2);
        AggregationSession aggregation = new AggregationSession(
                allocator,
                window.outputSchema(),
                program,
                EngineResources.from(allocator).operatorResources());

        try (Operator operator = new BatchAggregationOperator(window, aggregation, _ -> 0);
                Batch result = operator.next()) {
            assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values()).containsExactly(13L);
            assertThat(((I64Vector) result.output(1).borrow(Stream.VALUES)).values()).containsExactly(13L);
            assertThat(((I64Vector) result.output(2).borrow(Stream.VALUES)).values()).containsExactly(5L);
        }
        assertThat(sum.genericInputObserved).isFalse();
        assertThat(secondSum.genericInputObserved).isFalse();
        assertThat(count.genericInputObserved).isFalse();
    }

    @Test
    void propagatesPrimitiveConsumerCancellation()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L)));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(0, outputPosition + 1);
        AtomicBoolean called = new AtomicBoolean();

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        new TightCount(),
                        source.outputSchema(),
                        new int[0],
                        frame))).withOutputs(1)) {
            assertThatThrownBy(() -> window.drainPrimitiveTo(new PrimitiveRangeInputSink()
            {
                @Override
                public boolean supportsPrimitiveRangeInput(List<PrimitiveRangeContribution> outputs)
                {
                    return true;
                }

                @Override
                public PrimitiveRangeInput bindPrimitiveRangeInput(List<PrimitiveRangeContribution> outputs)
                {
                    return new PrimitiveRangeInput()
                    {
                        @Override
                        public PrimitiveRangeConsumer output(int output)
                        {
                            return new PrimitiveRangeConsumer()
                            {
                                @Override
                                public void addLong(long value)
                                {
                                    called.set(true);
                                    throw new IllegalStateException("cancelled");
                                }
                            };
                        }

                        @Override
                        public void addCardinality(long count) {}
                    };
                }
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("cancelled");
        }
        assertThat(called).isTrue();
    }

    @Test
    void primitiveMismatchFallsBackWithoutAdvancingWindow()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L)));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(0, outputPosition + 1);
        PrimitiveInputAggregation nullable = PrimitiveInputAggregation.nullableSum();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(nullable, RAW, FINAL, new int[] {0}));
        WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        new TightCount(),
                        source.outputSchema(),
                        new int[0],
                        frame))).withOutputs(1);
        AggregationSession aggregation = new AggregationSession(
                allocator,
                window.outputSchema(),
                program,
                EngineResources.from(allocator).operatorResources());

        try (Operator operator = new BatchAggregationOperator(window, aggregation, _ -> 0);
                Batch result = operator.next()) {
            assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values()).containsExactly(6L);
        }
        assertThat(nullable.genericInputObserved).isTrue();
    }

    @Test
    void nullablePrimitiveContributionPreservesNullSemantics()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PrimitiveInputAggregation nullable = PrimitiveInputAggregation.nullableSum();
        AggregationSession aggregation = new AggregationSession(
                allocator,
                Schema.unspecified(1),
                PhysicalAggregationProgram.singleUnit(new RegisteredAggregationUnit(nullable, RAW, FINAL, new int[] {0})),
                EngineResources.from(allocator).operatorResources());
        List<PrimitiveRangeContribution> outputs = List.of(
                PrimitiveRangeContribution.longValues(PrimitiveRangeContribution.NullConvention.NULLABLE));

        assertThat(aggregation.supportsPrimitiveRangeInput(outputs)).isTrue();
        PrimitiveRangeInput input = aggregation.bindPrimitiveRangeInput(outputs);
        input.output(0).addLong(2);
        input.output(0).addNull();
        input.output(0).addLong(3);
        input.addCardinality(3);
        try (aggregation; Batch result = aggregation.finish()) {
            assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values()).containsExactly(5L);
        }
        assertThat(nullable.genericInputObserved).isFalse();
    }

    @Test
    void drainsForwardResultsToDirectRangeSink()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L), row(4L)));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(
                Math.max(0, outputPosition - 1),
                Math.min(partition.size(), outputPosition + 2));
        long[] total = new long[1];
        int[] positions = new int[1];

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        new NullableSum(true),
                        source.outputSchema(),
                        new int[] {0},
                        frame))).withOutputs(1)) {
            assertThat(window.drainTo(new RangeInputSink()
            {
                @Override
                public boolean supportsRangeInput(Schema schema)
                {
                    return schema.size() == 1;
                }

                @Override
                public void addRange(int positionCount, org.weakref.nitro.operator.aggregation.StreamAccessor streams)
                {
                    positions[0] += positionCount;
                    long[] values = ((I64Vector) streams.stream(0, Stream.VALUES)).values();
                    for (int position = 0; position < positionCount; position++) {
                        total[0] += values[position];
                    }
                }
            })).isTrue();
            assertThat(window.hasNext()).isFalse();
        }

        assertThat(positions[0]).isEqualTo(4);
        assertThat(total[0]).isEqualTo(25);
    }

    @Test
    void declinesDirectRangeBeforeAdvancingAndRetainsBatchFallback()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L)));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(0, outputPosition + 1);

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        new NullableSum(true),
                        source.outputSchema(),
                        new int[] {0},
                        frame))).withOutputs(1)) {
            assertThat(window.drainTo(new RangeInputSink()
            {
                @Override
                public boolean supportsRangeInput(Schema schema)
                {
                    return false;
                }

                @Override
                public void addRange(int positionCount, org.weakref.nitro.operator.aggregation.StreamAccessor streams)
                {
                    throw new AssertionError("range must not be delivered");
                }
            })).isFalse();
            assertThat(OperatorAssertions.OperatorAssert.toRows(window))
                    .containsExactly(row(1L), row(3L), row(6L));
        }
    }

    @Test
    void releasesGeneratedRangeWhenSinkCancels()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(3L)));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(0, outputPosition + 1);
        AtomicBoolean called = new AtomicBoolean();

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        new NullableSum(true),
                        source.outputSchema(),
                        new int[] {0},
                        frame))).withOutputs(1)) {
            assertThatThrownBy(() -> window.drainTo(new RangeInputSink()
            {
                @Override
                public boolean supportsRangeInput(Schema schema)
                {
                    return true;
                }

                @Override
                public void addRange(int positionCount, org.weakref.nitro.operator.aggregation.StreamAccessor streams)
                {
                    called.set(true);
                    throw new IllegalStateException("cancelled");
                }
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("cancelled");
        }
        assertThat(called).isTrue();
    }

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
    void incrementallyUpdatesOverlappingBoundedFrames()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L), row(4L), row(5L)));
        NullableSum bounded = new NullableSum(true);
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(
                Math.max(0, outputPosition - 1),
                Math.min(partition.size(), outputPosition + 2));

        try (Operator operator = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        bounded,
                        source.outputSchema(),
                        new int[] {0},
                        frame)))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(operator))
                    .containsExactly(
                            row(1L, 3L),
                            row(2L, 6L),
                            row(3L, 9L),
                            row(4L, 12L),
                            row(5L, 9L));
        }
        assertThat(bounded.boundPositionCalls).isEqualTo(5);
        assertThat(bounded.removePositionCalls).isEqualTo(3);
    }

    @Test
    void materializesPositionalFramesIntoIndependentForwardBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        int rows = WindowOperatorPolicy.defaults().maxBatchRows() + 3;
        long[] values = new long[rows];
        for (int position = 0; position < rows; position++) {
            values[position] = position + 1L;
        }
        NullableSum bounded = new NullableSum(true);
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(
                Math.max(0, outputPosition - 1),
                Math.min(partition.size(), outputPosition + 2));
        Operator source = new TableOperator(1, List.of(TableOperator.Page.values(
                rows,
                new Vector[] {new I64Vector(values)},
                Mask.all(rows))));

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        bounded,
                        source.outputSchema(),
                        new int[] {0},
                        frame))).withOutputs(0, 1);
                Batch first = window.next();
                Batch second = window.next()) {
            assertThat(first.borrowMask().count()).isEqualTo(WindowOperatorPolicy.defaults().maxBatchRows());
            assertThat(second.borrowMask().count()).isEqualTo(3);
            assertThat(VectorAccess.longValues(first.output(1).borrow(Stream.VALUES)).value(rows - 4))
                    .isEqualTo(3L * (rows - 3));
            assertThat(VectorAccess.longValues(second.output(1).borrow(Stream.VALUES)).value(0))
                    .isEqualTo(3L * (rows - 2));
            assertThat(VectorAccess.longValues(first.output(0).borrow(Stream.VALUES)).value(0)).isEqualTo(1);
        }
        assertThat(bounded.maxDestinationSize).isEqualTo(WindowOperatorPolicy.defaults().maxBatchRows());
        assertThat(bounded.boundPositionCalls).isEqualTo(rows);
        assertThat(bounded.removePositionCalls).isEqualTo(rows - 2);
        assertThat(bounded.copyResultCalls).isEqualTo(rows);
    }

    @Test
    void runsProviderWindowKernelAndReportsExactWork()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L), row(4L), row(5L)));
        TightCount count = new TightCount();
        java.util.Map<String, Long> diagnostics = new java.util.HashMap<>();
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(
                Math.max(0, outputPosition - 1),
                Math.min(partition.size(), outputPosition + 2));

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        count,
                        source.outputSchema(),
                        new int[0],
                        frame))).withOutputs(1).withDiagnostics((event, value) -> diagnostics.merge(event, value, Long::sum))) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(window))
                    .containsExactly(row(2L), row(3L), row(3L), row(3L), row(2L));
        }

        assertThat(count.copyResultCalls).isZero();
        assertThat(diagnostics).containsEntry(RegisteredAggregationWindowFunction.KERNEL_POSITIONS, 5L)
                .containsEntry(RegisteredAggregationWindowFunction.KERNEL_ADDITIONS, 5L)
                .containsEntry(RegisteredAggregationWindowFunction.KERNEL_REMOVALS, 3L)
                .containsEntry(RegisteredAggregationWindowFunction.KERNEL_RESULTS, 5L);
    }

    @Test
    void resetsInsideBatchAndCrossesRetainedSourcePages()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        int firstRows = WindowOperatorPolicy.defaults().maxBatchRows() - 1;
        long[] firstPartitions = new long[firstRows];
        long[] firstValues = new long[firstRows];
        java.util.Arrays.fill(firstPartitions, 1);
        java.util.Arrays.fill(firstValues, 1);
        TableOperator source = TableOperator.retained(
                Schema.unspecified(2),
                List.of(
                        TableOperator.Page.values(firstRows, new Vector[] {
                                new I64Vector(firstPartitions),
                                new I64Vector(firstValues)}, Mask.all(firstRows)),
                        TableOperator.Page.values(4, new Vector[] {
                                new I64Vector(new long[] {1, 1, 2, 2}),
                                new I64Vector(new long[] {1, 1, 10, 20})}, Mask.all(4))));
        NullableSum bounded = new NullableSum(true);
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(
                Math.max(0, outputPosition - 1),
                outputPosition + 1);

        try (Operator window = new WindowOperator(
                allocator,
                source,
                new int[] {0},
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        bounded,
                        source.outputSchema(),
                        new int[] {1},
                        frame)),
                Schema.unspecified(1),
                EngineResources.from(allocator).operatorResources(),
                new WindowInputOrder(true, 0))) {
            List<org.weakref.nitro.data.Row> rows = OperatorAssertions.OperatorAssert.toRows(window);
            assertThat(rows.subList(rows.size() - 4, rows.size())).containsExactly(
                    row(1L, 1L, 2L),
                    row(1L, 1L, 2L),
                    row(2L, 10L, 10L),
                    row(2L, 20L, 30L));
        }
        assertThat(bounded.boundPositionCalls).isEqualTo(firstRows + 4);
    }

    @Test
    void keepsLegacyPlanesAndOmitsUnselectedCapableFunctions()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L)));
        NullableSum omitted = new NullableSum(true);
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(0, outputPosition + 1);

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(
                        new RegisteredAggregationWindowFunction(
                                omitted,
                                source.outputSchema(),
                                new int[] {0},
                                frame),
                        new RunningSumI64WindowFunction(0))).withOutputs(2)) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(window))
                    .containsExactly(row(1L), row(3L), row(6L));
        }
        assertThat(omitted.allocateCalls).isZero();
    }

    @Test
    void mixesCapableAndLegacyFunctionsWhileKeepingSourceOutputLazy()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        int[] sourceCopies = new int[1];
        I64Vector values = new I64Vector(new long[] {1, 2, 3})
        {
            @Override
            public Vector copyPositionsInto(
                    Allocator targetAllocator,
                    Allocator.Context context,
                    Vector existing,
                    int[] positions,
                    int count,
                    int outputStart,
                    int size)
            {
                sourceCopies[0]++;
                return super.copyPositionsInto(targetAllocator, context, existing, positions, count, outputStart, size);
            }
        };
        Operator source = TableOperator.retained(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(3, new Vector[] {values}, Mask.all(3))));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(0, outputPosition + 1);

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(
                        new RegisteredAggregationWindowFunction(
                                new NullableSum(true),
                                source.outputSchema(),
                                new int[] {0},
                                frame),
                        new RunningSumI64WindowFunction(0))).withOutputs(0, 1, 2);
                Batch batch = window.next()) {
            assertThat(sourceCopies).containsExactly(0);
            assertThat(VectorAccess.longValues(batch.output(1).borrow(Stream.VALUES)).value(2)).isEqualTo(6L);
            assertThat(VectorAccess.longValues(batch.output(2).borrow(Stream.VALUES)).value(2)).isEqualTo(6L);
            assertThat(sourceCopies).containsExactly(0);
            assertThat(VectorAccess.longValues(batch.output(0).borrow(Stream.VALUES)).value(2)).isEqualTo(3L);
            assertThat(sourceCopies).containsExactly(1);
        }
    }

    @Test
    void duplicatesCapableOutputWithIndependentOwnership()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ConstantTableOperator source = new ConstantTableOperator(
                allocator,
                1,
                List.of(row(1L), row(2L), row(3L)));
        WindowFrame frame = (partition, outputPosition, bounds) -> bounds.set(0, outputPosition + 1);

        try (WindowOperator window = new WindowOperator(
                allocator,
                source,
                new int[0],
                new int[0],
                new boolean[0],
                List.of(new RegisteredAggregationWindowFunction(
                        new NullableSum(true),
                        source.outputSchema(),
                        new int[] {0},
                        frame))).withOutputs(1, 1)) {
            assertThat(OperatorAssertions.OperatorAssert.toRows(window))
                    .containsExactly(row(1L, 1L), row(3L, 3L), row(6L, 6L));
        }
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
        WindowFrame peers = (partition, outputPosition, bounds) -> {
            assertThat(partition.compareNonNull(1, 0, 2, 2)).isNegative();
            bounds.set(partition.peerStart(outputPosition), partition.peerEnd(outputPosition));
        };

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
        private int removePositionCalls;
        private int allocateCalls;
        private int maxDestinationSize;

        private NullableSum(boolean directCopy)
        {
            this.directCopy = directCopy;
        }

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            allocateCalls++;
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
            sum.nonNullCount = 0;
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
                    sum.nonNullCount++;
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
                    sum.nonNullCount++;
                    sum.hasValue = true;
                }
            };
        }

        @Override
        public boolean supportsReversibleRawInputPosition()
        {
            return directCopy;
        }

        @Override
        public ReversibleAggregationPositionAccumulator bindReversibleRawInputPosition(Object state, int group, AggregationInput input)
        {
            if (!directCopy) {
                return null;
            }
            State sum = (State) state;
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
            VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(input.stream(0, Stream.NULLS));
            return new ReversibleAggregationPositionAccumulator()
            {
                @Override
                public void add(int position)
                {
                    boundPositionCalls++;
                    if (!nulls.value(position)) {
                        sum.value += values.value(position);
                        sum.nonNullCount++;
                        sum.hasValue = true;
                    }
                }

                @Override
                public void remove(int position)
                {
                    removePositionCalls++;
                    if (!nulls.value(position)) {
                        sum.value -= values.value(position);
                        sum.nonNullCount--;
                        sum.hasValue = sum.nonNullCount != 0;
                    }
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
            maxDestinationSize = Math.max(maxDestinationSize, size);
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
            private long nonNullCount;
            private boolean hasValue;
        }
    }

    private static final class TightCount
            implements AggregationImplementation
    {
        private int copyResultCalls;

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            return new long[groups];
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            return java.util.Arrays.copyOf((long[]) state, groups);
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            java.util.Arrays.fill((long[]) state, offset, offset + length, 0);
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            ((long[]) state)[group] += mask.count();
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReversibleAggregationWindowKernel bindReversibleWindowKernel(Object state, int group, int inputCount)
        {
            long[] count = (long[]) state;
            return new ReversibleAggregationWindowKernel()
            {
                private int start;
                private int end;

                @Override
                public void reset()
                {
                    start = 0;
                    end = 0;
                }

                @Override
                public Streams prepareOutput(Streams existing, int size, Allocator allocator, Allocator.Context context)
                {
                    I64Vector values = allocator.allocateOrGrow(
                            context,
                            existing == null ? null : (I64Vector) existing.values(),
                            I64Vector.class,
                            size,
                            I64Vector::new);
                    return Streams.ofValues(values);
                }

                @Override
                public PrimitiveRangeContribution primitiveResultContribution()
                {
                    return PrimitiveRangeContribution.longValues(PrimitiveRangeContribution.NullConvention.NEVER_NULL);
                }

                @Override
                public Work process(AggregationWindowPartition partition, AggregationWindowFrameBounds bounds, int rangeStart, int positions, Streams output, int outputStart)
                {
                    long additions = 0;
                    long removals = 0;
                    long[] values = ((I64Vector) output.values()).values();
                    for (int index = 0; index < positions; index++) {
                        int nextStart = bounds.start(rangeStart + index);
                        int nextEnd = bounds.end(rangeStart + index);
                        count[group] -= Math.max(0, Math.min(end, nextStart) - start);
                        removals += Math.max(0, Math.min(end, nextStart) - start);
                        count[group] += Math.max(0, nextEnd - Math.max(end, nextStart));
                        additions += Math.max(0, nextEnd - Math.max(end, nextStart));
                        start = nextStart;
                        end = nextEnd;
                        values[outputStart + index] = count[group];
                    }
                    return new Work(positions, additions, removals, positions);
                }

                @Override
                public Work process(AggregationWindowPartition partition, AggregationWindowFrameBounds bounds, int rangeStart, int positions, PrimitiveRangeConsumer consumer)
                {
                    long additions = 0;
                    long removals = 0;
                    for (int index = 0; index < positions; index++) {
                        int nextStart = bounds.start(rangeStart + index);
                        int nextEnd = bounds.end(rangeStart + index);
                        count[group] -= Math.max(0, Math.min(end, nextStart) - start);
                        removals += Math.max(0, Math.min(end, nextStart) - start);
                        count[group] += Math.max(0, nextEnd - Math.max(end, nextStart));
                        additions += Math.max(0, nextEnd - Math.max(end, nextStart));
                        start = nextStart;
                        end = nextEnd;
                        consumer.addLong(count[group]);
                    }
                    return new Work(positions, additions, removals, positions);
                }
            };
        }

        @Override
        public PrimitiveRangeContribution primitiveWindowResultContribution(int inputCount)
        {
            return PrimitiveRangeContribution.longValues(PrimitiveRangeContribution.NullConvention.NEVER_NULL);
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
        public Streams intermediate(int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context context)
        {
            return result(maxGroup, state, existing, allocator, context);
        }

        @Override
        public Streams result(int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context context)
        {
            return Streams.ofValues(new I64Vector(((long[]) state).clone()));
        }

        @Override
        public Streams copyResultPosition(int group, int maxGroup, Object state, Streams existing, int outputPosition, int size, Allocator allocator, Allocator.Context context)
        {
            copyResultCalls++;
            return null;
        }
    }

    private static final class PrimitiveInputAggregation
            implements AggregationImplementation
    {
        private final PrimitiveRangeContribution contribution;
        private final int input;
        private boolean genericInputObserved;

        private PrimitiveInputAggregation(PrimitiveRangeContribution contribution, int input)
        {
            this.contribution = contribution;
            this.input = input;
        }

        private static PrimitiveInputAggregation sum()
        {
            return new PrimitiveInputAggregation(
                    PrimitiveRangeContribution.longValues(PrimitiveRangeContribution.NullConvention.NEVER_NULL),
                    0);
        }

        private static PrimitiveInputAggregation nullableSum()
        {
            return new PrimitiveInputAggregation(
                    PrimitiveRangeContribution.longValues(PrimitiveRangeContribution.NullConvention.NULLABLE),
                    0);
        }

        private static PrimitiveInputAggregation count()
        {
            return new PrimitiveInputAggregation(PrimitiveRangeContribution.cardinality(), -1);
        }

        @Override
        public PrimitiveRangeContribution primitiveRangeInputContribution(int inputCount)
        {
            return contribution;
        }

        @Override
        public int primitiveRangeInputIndex(int inputCount)
        {
            return input;
        }

        @Override
        public PrimitiveAggregationInput bindPrimitiveRangeInput(Object state, int group, int inputCount)
        {
            long[] value = (long[]) state;
            return new PrimitiveAggregationInput(input, contribution, new PrimitiveRangeConsumer()
            {
                @Override
                public void addLong(long next)
                {
                    value[group] += next;
                }

                @Override
                public void addNull() {}

                @Override
                public void addCardinality(long count)
                {
                    value[group] += count;
                }
            });
        }

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            return new long[groups];
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            return java.util.Arrays.copyOf((long[]) state, groups);
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            java.util.Arrays.fill((long[]) state, offset, offset + length, 0);
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            genericInputObserved = true;
            long[] value = (long[]) state;
            if (this.input < 0) {
                value[group] += mask.count();
                return;
            }
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(this.input, Stream.VALUES));
            VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(input.stream(this.input, Stream.NULLS));
            for (int position : mask) {
                if (!nulls.value(position)) {
                    value[group] += values.value(position);
                }
            }
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
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
        public Streams intermediate(int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context context)
        {
            return result(maxGroup, state, existing, allocator, context);
        }

        @Override
        public Streams result(int maxGroup, Object state, Streams existing, Allocator allocator, Allocator.Context context)
        {
            return Streams.ofValues(new I64Vector(((long[]) state).clone()));
        }
    }
}
