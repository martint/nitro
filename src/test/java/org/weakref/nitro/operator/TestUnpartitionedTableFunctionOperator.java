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
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.execution.ExecutionPolicy;
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.core.function.table.TableFunctionArgument;
import org.weakref.nitro.core.function.table.TableFunctionInput;
import org.weakref.nitro.core.function.table.TableFunctionOutputBatch;
import org.weakref.nitro.core.function.table.TableFunctionOutputDemand;
import org.weakref.nitro.core.function.table.TableFunctionPassThroughColumn;
import org.weakref.nitro.core.function.table.TableFunctionProcessor;
import org.weakref.nitro.core.function.table.TableFunctionProgress;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MaskSelection;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorBatchScope;
import org.weakref.nitro.data.VectorColumnGeneration;
import org.weakref.nitro.data.VectorSourceBatch;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.source.SourceBatchOperatorIngress;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUnpartitionedTableFunctionOperator
{
    private static final Schema EMPTY_SCHEMA = new Schema(List.of());
    private static final TypeBinding BIGINT = bigintType();
    private static final Schema BIGINT_SCHEMA = new Schema(List.of(new Field("value", BIGINT, true)));

    @Test
    void testConsumesSuccessiveNativeInputBatchesWithoutCopying()
    {
        TestingSource source = new TestingSource(3, 2);
        TestingProcessor processor = new TestingProcessor();

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                UnpartitionedTableFunctionOperator operator = new UnpartitionedTableFunctionOperator(
                        EMPTY_SCHEMA,
                        processor,
                        source,
                        EMPTY_SCHEMA,
                        new int[0],
                        allocator,
                        new TestingIngress(),
                        new TestingExecutionContext())) {
            assertThat(operator.hasNext()).isTrue();
            try (Batch batch = operator.next()) {
                assertThat(batch.borrowMask().selectedCount()).isEqualTo(3);
            }
            assertThat(operator.hasNext()).isTrue();
            try (Batch batch = operator.next()) {
                assertThat(batch.borrowMask().selectedCount()).isEqualTo(2);
            }
            assertThat(operator.hasNext()).isFalse();
        }

        assertThat(source.pulls()).isEqualTo(2);
        assertThat(source.closedBatches()).isEqualTo(2);
        assertThat(processor.calls()).isEqualTo(3);
        assertThat(processor.closed()).isTrue();
    }

    @Test
    void testRejectsPassThroughUntilGatheringContractExists()
    {
        TestingSource source = new TestingSource(1);
        TableFunctionProcessor processor = new TableFunctionProcessor()
        {
            @Override
            public TableFunctionProgress process(
                    TableFunctionInput input,
                    TableFunctionOutputDemand outputDemand,
                    Allocator allocator,
                    Allocator.Context allocationContext,
                    ExecutionContext executionContext)
            {
                return new TableFunctionProgress.Produced(
                        referenceOutput(allocator, 0),
                        Set.of(0));
            }

            @Override
            public void close() {}
        };

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                UnpartitionedTableFunctionOperator operator = new UnpartitionedTableFunctionOperator(
                        EMPTY_SCHEMA,
                        processor,
                        source,
                        EMPTY_SCHEMA,
                        new int[0],
                        allocator,
                        new TestingIngress(),
                        new TestingExecutionContext())) {
            assertThatThrownBy(operator::hasNext)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("unpartitioned table function returned a pass-through reference");
        }
    }

    @Test
    void testConsumptionWithoutReadingInputStillAdvancesSource()
    {
        TestingSource source = new TestingSource(3, 2);
        AtomicInteger calls = new AtomicInteger();
        TableFunctionProcessor processor = new TableFunctionProcessor()
        {
            @Override
            public TableFunctionProgress process(
                    TableFunctionInput input,
                    TableFunctionOutputDemand outputDemand,
                    Allocator allocator,
                    Allocator.Context allocationContext,
                    ExecutionContext executionContext)
            {
                calls.incrementAndGet();
                if (input.arguments().getFirst() instanceof TableFunctionArgument.Finished) {
                    return TableFunctionProgress.Finished.FINISHED;
                }
                return new TableFunctionProgress.Consumed(Set.of(0));
            }

            @Override
            public void close() {}
        };

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                UnpartitionedTableFunctionOperator operator = new UnpartitionedTableFunctionOperator(
                        EMPTY_SCHEMA,
                        processor,
                        source,
                        EMPTY_SCHEMA,
                        new int[0],
                        allocator,
                        new TestingIngress(),
                        new TestingExecutionContext())) {
            assertThat(operator.hasNext()).isFalse();
        }

        assertThat(calls).hasValue(3);
        assertThat(source.pulls()).isEqualTo(2);
        assertThat(source.closedBatches()).isEqualTo(2);
    }

    @Test
    void testGathersPassThroughRowsFromBufferedInput()
    {
        TableOperator source = TableOperator.retained(
                BIGINT_SCHEMA,
                List.of(
                        TableOperator.Page.values(2, new I64Vector[] {new I64Vector(new long[] {10, 20})}, Mask.all(2)),
                        TableOperator.Page.values(1, new I64Vector[] {new I64Vector(new long[] {30})}, Mask.all(1))));
        AtomicInteger nextReference = new AtomicInteger(1);
        TableFunctionProcessor processor = new TableFunctionProcessor()
        {
            @Override
            public TableFunctionProgress process(
                    TableFunctionInput input,
                    TableFunctionOutputDemand outputDemand,
                    Allocator allocator,
                    Allocator.Context allocationContext,
                    ExecutionContext executionContext)
            {
                assertThat(outputDemand.passThroughArguments()).containsExactly(0);
                if (input.arguments().getFirst() instanceof TableFunctionArgument.Finished) {
                    return TableFunctionProgress.Finished.FINISHED;
                }
                return new TableFunctionProgress.Produced(
                        referenceOutput(allocator, nextReference.getAndIncrement()),
                        Set.of(0));
            }

            @Override
            public void close() {}
        };

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                UnpartitionedTableFunctionOperator operator = new UnpartitionedTableFunctionOperator(
                        BIGINT_SCHEMA,
                        0,
                        List.of(new TableFunctionPassThroughColumn(0, 0)),
                        processor,
                        source,
                        EMPTY_SCHEMA,
                        new int[0],
                        allocator,
                        new TestingIngress(),
                        new TestingExecutionContext(),
                        2)) {
            assertThat(operator.hasNext()).isTrue();
            try (Batch first = operator.next()) {
                assertThat(((I64Vector) first.output(0).borrow(Stream.VALUES)).values()).containsExactly(20);
            }
            assertThat(operator.hasNext()).isTrue();
            try (Batch second = operator.next()) {
                assertThat(((I64Vector) second.output(0).borrow(Stream.VALUES)).values()).containsExactly(30);
            }
            assertThat(operator.hasNext()).isFalse();
        }
    }

    @Test
    void testCreatesIsolatedProcessorForEachOrderedPartition()
    {
        TableOperator source = TableOperator.retained(
                BIGINT_SCHEMA,
                List.of(
                        TableOperator.Page.values(1, new I64Vector[] {new I64Vector(new long[] {1})}, Mask.all(1)),
                        TableOperator.Page.values(2, new I64Vector[] {new I64Vector(new long[] {1, 2})}, Mask.all(2)),
                        TableOperator.Page.values(1, new I64Vector[] {new I64Vector(new long[] {2})}, Mask.all(1))));
        AtomicInteger processors = new AtomicInteger();

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                PartitionedTableFunctionOperator operator = new PartitionedTableFunctionOperator(
                        allocator,
                        new TestingExecutionContext(),
                        source,
                        BIGINT_SCHEMA,
                        new int[] {0},
                        new int[] {0},
                        new WindowInputOrder(true, 0),
                        0,
                        BIGINT_SCHEMA,
                        0,
                        List.of(new TableFunctionPassThroughColumn(0, 0)),
                        () -> {
                            processors.incrementAndGet();
                            return new FirstRowProcessor();
                        },
                        false,
                        8,
                        resources.operatorResources())) {
            assertThat(operator.hasNext()).isTrue();
            try (Batch first = operator.next()) {
                assertThat(((I64Vector) first.output(0).borrow(Stream.VALUES)).values()).containsExactly(1);
            }
            assertThat(operator.hasNext()).isTrue();
            try (Batch second = operator.next()) {
                assertThat(((I64Vector) second.output(0).borrow(Stream.VALUES)).values()).containsExactly(2);
            }
            assertThat(operator.hasNext()).isFalse();
        }

        assertThat(processors).hasValue(2);
    }

    @Test
    void testEmptyPartitionPolicyControlsProcessorCreation()
    {
        AtomicInteger prunedProcessors = new AtomicInteger();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                PartitionedTableFunctionOperator operator = new PartitionedTableFunctionOperator(
                        allocator,
                        new TestingExecutionContext(),
                        TableOperator.retained(EMPTY_SCHEMA, List.of()),
                        EMPTY_SCHEMA,
                        new int[0],
                        new int[0],
                        new WindowInputOrder(true, 0),
                        0,
                        EMPTY_SCHEMA,
                        0,
                        List.of(),
                        () -> {
                            prunedProcessors.incrementAndGet();
                            return new FinishedProcessor();
                        },
                        false,
                        8,
                        resources.operatorResources())) {
            assertThat(operator.hasNext()).isFalse();
        }
        assertThat(prunedProcessors).hasValue(0);

        AtomicInteger retainedProcessors = new AtomicInteger();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                PartitionedTableFunctionOperator operator = new PartitionedTableFunctionOperator(
                        allocator,
                        new TestingExecutionContext(),
                        TableOperator.retained(EMPTY_SCHEMA, List.of()),
                        EMPTY_SCHEMA,
                        new int[0],
                        new int[0],
                        new WindowInputOrder(true, 0),
                        0,
                        EMPTY_SCHEMA,
                        0,
                        List.of(),
                        () -> {
                            retainedProcessors.incrementAndGet();
                            return new FinishedProcessor();
                        },
                        true,
                        8,
                        resources.operatorResources())) {
            assertThat(operator.hasNext()).isFalse();
        }
        assertThat(retainedProcessors).hasValue(1);
    }

    @Test
    void testExcludesMultiArgumentMarkerSuffixes()
    {
        Schema sourceSchema = new Schema(List.of(
                new Field("left", BIGINT, true),
                new Field("left_marker", BIGINT, true),
                new Field("right", BIGINT, true),
                new Field("right_marker", BIGINT, true)));
        TableOperator source = TableOperator.retained(
                sourceSchema,
                List.of(new TableOperator.Page(
                        3,
                        new Streams[] {
                                Streams.ofValues(new I64Vector(new long[] {10, 20, 0})),
                                Streams.of(
                                        new I64Vector(new long[] {1, 1, 0}),
                                        new BooleanVector(new boolean[] {false, false, true}),
                                        null),
                                Streams.ofValues(new I64Vector(new long[] {30, 0, 0})),
                                Streams.of(
                                        new I64Vector(new long[] {1, 0, 0}),
                                        new BooleanVector(new boolean[] {false, true, true}),
                                        null),
                        },
                        Mask.all(3))));
        TableFunctionProcessor processor = new TableFunctionProcessor()
        {
            private boolean produced;

            @Override
            public TableFunctionProgress process(
                    TableFunctionInput input,
                    TableFunctionOutputDemand outputDemand,
                    Allocator allocator,
                    Allocator.Context allocationContext,
                    ExecutionContext executionContext)
            {
                if (produced) {
                    return TableFunctionProgress.Finished.FINISHED;
                }
                produced = true;
                assertThat(((TableFunctionArgument.Rows) input.arguments().get(0)).batch().selection().count()).isEqualTo(2);
                assertThat(((TableFunctionArgument.Rows) input.arguments().get(1)).batch().selection().count()).isEqualTo(1);
                return new TableFunctionProgress.Produced(
                        referenceOutput(allocator, new long[] {1, 0}),
                        Set.of(0, 1));
            }

            @Override
            public void close() {}
        };

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                PartitionedTableFunctionOperator operator = new PartitionedTableFunctionOperator(
                        allocator,
                        new TestingExecutionContext(),
                        source,
                        List.of(
                                new TableFunctionArgumentLayout(BIGINT_SCHEMA, new int[] {0}, OptionalInt.of(1)),
                                new TableFunctionArgumentLayout(BIGINT_SCHEMA, new int[] {2}, OptionalInt.of(3))),
                        new int[0],
                        new WindowInputOrder(true, 0),
                        0,
                        new Schema(List.of(new Field("left", BIGINT, true), new Field("right", BIGINT, true))),
                        0,
                        List.of(new TableFunctionPassThroughColumn(0, 0), new TableFunctionPassThroughColumn(1, 2)),
                        () -> processor,
                        false,
                        8,
                        resources.operatorResources())) {
            assertThat(operator.hasNext()).isTrue();
            try (Batch output = operator.next()) {
                assertThat(((I64Vector) output.output(0).borrow(Stream.VALUES)).values()).containsExactly(20);
                assertThat(((I64Vector) output.output(1).borrow(Stream.VALUES)).values()).containsExactly(30);
            }
            assertThat(operator.hasNext()).isFalse();
        }
    }

    private static TableFunctionOutputBatch referenceOutput(Allocator allocator, long reference)
    {
        VectorBatchScope buffers = new VectorBatchScope(allocator, "reference-output");
        I64Vector references = allocator.allocate(buffers.context(), I64Vector.class, 1, I64Vector::new);
        references.values()[0] = reference;
        Mask mask = allocator.allocateRangeMask(buffers.context(), 0, 1);
        SourceBatch delegate = new VectorSourceBatch(
                BIGINT_SCHEMA,
                mask,
                new VectorColumnGeneration[] {
                        new VectorColumnGeneration(Set.of(Stream.VALUES), _ -> references, buffers),
                },
                buffers,
                _ -> {},
                () -> {});
        return new ReferenceOutputBatch(
                delegate,
                List.of(new TableFunctionOutputBatch.PassThroughReference(0, delegate.column(0))));
    }

    private static TableFunctionOutputBatch referenceOutput(Allocator allocator, long[] referenceValues)
    {
        VectorBatchScope buffers = new VectorBatchScope(allocator, "multi-reference-output");
        VectorColumnGeneration[] columns = new VectorColumnGeneration[referenceValues.length];
        for (int column = 0; column < columns.length; column++) {
            I64Vector references = allocator.allocate(buffers.context(), I64Vector.class, 1, I64Vector::new);
            references.values()[0] = referenceValues[column];
            columns[column] = new VectorColumnGeneration(Set.of(Stream.VALUES), _ -> references, buffers);
        }
        Mask mask = allocator.allocateRangeMask(buffers.context(), 0, 1);
        SourceBatch delegate = new VectorSourceBatch(
                Schema.unspecified(referenceValues.length),
                mask,
                columns,
                buffers,
                _ -> {},
                () -> {});
        return new MultiReferenceOutputBatch(delegate, java.util.stream.IntStream.range(0, referenceValues.length)
                .mapToObj(argument -> new TableFunctionOutputBatch.PassThroughReference(argument, delegate.column(argument)))
                .toList());
    }

    private static TypeBinding bigintType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:bigint");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(new TypeVectorFactory()
                {
                    @Override
                    public Vector constant(org.weakref.nitro.data.VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(org.weakref.nitro.data.VectorAllocator allocator, int length)
                    {
                        return allocator.allocate(I64Vector.class, length, I64Vector::new);
                    }
                });
            }
        };
    }

    private static final class TestingProcessor
            implements TableFunctionProcessor
    {
        private int calls;
        private boolean closed;

        @Override
        public TableFunctionProgress process(
                TableFunctionInput input,
                TableFunctionOutputDemand outputDemand,
                Allocator allocator,
                Allocator.Context allocationContext,
                ExecutionContext executionContext)
        {
            calls++;
            assertThat(input.arguments()).hasSize(1);
            if (input.arguments().getFirst() instanceof TableFunctionArgument.Finished) {
                return TableFunctionProgress.Finished.FINISHED;
            }
            SourceBatch batch = ((TableFunctionArgument.Rows) input.arguments().getFirst()).batch();
            return new TableFunctionProgress.Produced(
                    new TestingOutputBatch(batch.selection().count(), List.of()),
                    Set.of(0));
        }

        @Override
        public void close()
        {
            closed = true;
        }

        private int calls()
        {
            return calls;
        }

        private boolean closed()
        {
            return closed;
        }
    }

    private static final class FirstRowProcessor
            implements TableFunctionProcessor
    {
        private boolean produced;

        @Override
        public TableFunctionProgress process(
                TableFunctionInput input,
                TableFunctionOutputDemand outputDemand,
                Allocator allocator,
                Allocator.Context allocationContext,
                ExecutionContext executionContext)
        {
            if (produced || input.arguments().getFirst() instanceof TableFunctionArgument.Finished) {
                return TableFunctionProgress.Finished.FINISHED;
            }
            produced = true;
            assertThat(outputDemand.passThroughArguments()).containsExactly(0);
            return new TableFunctionProgress.Produced(referenceOutput(allocator, 0), Set.of(0));
        }

        @Override
        public void close() {}
    }

    private static final class FinishedProcessor
            implements TableFunctionProcessor
    {
        @Override
        public TableFunctionProgress process(
                TableFunctionInput input,
                TableFunctionOutputDemand outputDemand,
                Allocator allocator,
                Allocator.Context allocationContext,
                ExecutionContext executionContext)
        {
            assertThat(input.arguments().getFirst()).isEqualTo(TableFunctionArgument.Finished.FINISHED);
            return TableFunctionProgress.Finished.FINISHED;
        }

        @Override
        public void close() {}
    }

    private static final class TestingSource
            implements Operator
    {
        private final Queue<Integer> batches = new ArrayDeque<>();
        private final AtomicInteger closedBatches = new AtomicInteger();
        private int pulls;

        private TestingSource(Integer... batches)
        {
            this.batches.addAll(List.of(batches));
        }

        @Override
        public int outputCount()
        {
            return 0;
        }

        @Override
        public Schema outputSchema()
        {
            return EMPTY_SCHEMA;
        }

        @Override
        public boolean hasNext()
        {
            return !batches.isEmpty();
        }

        @Override
        public Batch next()
        {
            pulls++;
            return new Batch(
                    Mask.all(batches.remove()),
                    _ -> {},
                    identity(),
                    _ -> {},
                    closedBatches::incrementAndGet);
        }

        @Override
        public void constrain(Mask mask) {}

        @Override
        public void close() {}

        private int pulls()
        {
            return pulls;
        }

        private int closedBatches()
        {
            return closedBatches.get();
        }
    }

    private static final class TestingOutputBatch
            implements TableFunctionOutputBatch
    {
        private final List<PassThroughReference> passThroughReferences;
        private Selection selection;

        private TestingOutputBatch(int positions, List<PassThroughReference> passThroughReferences)
        {
            this.passThroughReferences = List.copyOf(passThroughReferences);
            selection = new MaskSelection(Mask.all(positions));
        }

        @Override
        public int properOutputCount()
        {
            return 0;
        }

        @Override
        public List<PassThroughReference> passThroughReferences()
        {
            return passThroughReferences;
        }

        @Override
        public Schema schema()
        {
            return EMPTY_SCHEMA;
        }

        @Override
        public Selection selection()
        {
            return selection;
        }

        @Override
        public ColumnView column(int index)
        {
            throw new IndexOutOfBoundsException(index);
        }

        @Override
        public void select(Selection selection)
        {
            this.selection = selection;
        }

        @Override
        public void close() {}
    }

    private record ReferenceOutputBatch(SourceBatch delegate, List<PassThroughReference> passThroughReferences)
            implements TableFunctionOutputBatch
    {
        @Override
        public int properOutputCount()
        {
            return 0;
        }

        @Override
        public Schema schema()
        {
            return EMPTY_SCHEMA;
        }

        @Override
        public Selection selection()
        {
            return delegate.selection();
        }

        @Override
        public ColumnView column(int index)
        {
            throw new IndexOutOfBoundsException(index);
        }

        @Override
        public void select(Selection selection)
        {
            delegate.select(selection);
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    private record MultiReferenceOutputBatch(SourceBatch delegate, List<PassThroughReference> passThroughReferences)
            implements TableFunctionOutputBatch
    {
        private MultiReferenceOutputBatch
        {
            passThroughReferences = List.copyOf(passThroughReferences);
        }

        @Override
        public int properOutputCount()
        {
            return 0;
        }

        @Override
        public Schema schema()
        {
            return EMPTY_SCHEMA;
        }

        @Override
        public Selection selection()
        {
            return delegate.selection();
        }

        @Override
        public ColumnView column(int index)
        {
            throw new IndexOutOfBoundsException(index);
        }

        @Override
        public void select(Selection selection)
        {
            delegate.select(selection);
        }

        @Override
        public <T> Optional<T> capability(org.weakref.nitro.core.batch.BatchCapability<T> capability)
        {
            return delegate.capability(capability);
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    private static final class TestingIngress
            implements SourceBatchOperatorIngress
    {
        @Override
        public Batch adapt(SourceBatch batch)
        {
            Mask mask = ((MaskSelection) batch.selection()).mask();
            return new Batch(mask, constrained -> batch.select(new MaskSelection(constrained)), identity(), _ -> {}, batch::close);
        }

        @Override
        public Selection selection(Mask mask)
        {
            return new MaskSelection(mask);
        }
    }

    private static final class TestingExecutionContext
            implements ExecutionContext
    {
        @Override
        public MemoryReservation memory()
        {
            return new MemoryReservation()
            {
                @Override
                public CompletionStage<Void> reserve(long bytes)
                {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public void release(long bytes) {}

                @Override
                public long reservedBytes()
                {
                    return 0;
                }
            };
        }

        @Override
        public ExecutionPolicy policy()
        {
            return new ExecutionPolicy() {};
        }

        @Override
        public ExecutionDiagnostics diagnostics()
        {
            return (_, _) -> {};
        }

        @Override
        public boolean isYieldRequested()
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public void requestMemoryRevocation() {}
    }
}
