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
import org.weakref.nitro.core.function.table.TableFunctionProcessor;
import org.weakref.nitro.core.function.table.TableFunctionProgress;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MaskSelection;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.source.SourceBatchOperatorIngress;

import java.util.ArrayDeque;
import java.util.List;
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
                        new TestingOutputBatch(1, List.of(new TableFunctionOutputBatch.PassThroughReference(0))),
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
