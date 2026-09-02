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
import org.weakref.nitro.core.batch.BatchCapability;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.execution.ExecutionPolicy;
import org.weakref.nitro.core.execution.ExecutionSuspension;
import org.weakref.nitro.core.execution.MemoryReservation;
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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.core.execution.ExecutionSuspension.Reason.BLOCKED;

final class TestLeafTableFunctionOperator
{
    private static final Schema EMPTY_SCHEMA = new Schema(List.of());

    @Test
    void testPullsMultipleBatchesAndConstrainStagedOutput()
    {
        TestOutputBatch first = new TestOutputBatch(3);
        TestOutputBatch second = new TestOutputBatch(2);
        TestingProcessor processor = new TestingProcessor(
                new TableFunctionProgress.Produced(first, Set.of()),
                new TableFunctionProgress.Produced(second, Set.of()),
                TableFunctionProgress.Finished.FINISHED);

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                LeafTableFunctionOperator operator = new LeafTableFunctionOperator(
                        EMPTY_SCHEMA,
                        processor,
                        allocator,
                        new TestingIngress(),
                        new TestingExecutionContext())) {
            assertThat(operator.hasNext()).isTrue();
            operator.constrain(Mask.sparse(new int[] {1}, 3));
            try (Batch batch = operator.next()) {
                assertThat(batch.borrowMask().selectedCount()).isOne();
                assertThat(batch.borrowMask().position(0)).isOne();
            }
            assertThat(first.closed()).isTrue();

            assertThat(operator.hasNext()).isTrue();
            try (Batch batch = operator.next()) {
                assertThat(batch.borrowMask().selectedCount()).isEqualTo(2);
            }
            assertThat(operator.hasNext()).isFalse();
        }
        assertThat(second.closed()).isTrue();
        assertThat(processor.closed()).isTrue();
    }

    @Test
    void testResumesBlockedProcessorWithoutLosingProgress()
    {
        CompletableFuture<Void> continuation = new CompletableFuture<>();
        TestingProcessor processor = new TestingProcessor(
                new TableFunctionProgress.Blocked(continuation),
                new TableFunctionProgress.Produced(new TestOutputBatch(1), Set.of()),
                TableFunctionProgress.Finished.FINISHED);

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                LeafTableFunctionOperator operator = new LeafTableFunctionOperator(
                        EMPTY_SCHEMA,
                        processor,
                        allocator,
                        new TestingIngress(),
                        new TestingExecutionContext())) {
            assertThatThrownBy(operator::hasNext)
                    .isInstanceOfSatisfying(ExecutionSuspension.class, suspension -> {
                        assertThat(suspension.reason()).isEqualTo(BLOCKED);
                        assertThat(suspension.continuation()).contains(continuation);
                    });

            continuation.complete(null);
            assertThat(operator.hasNext()).isTrue();
            try (Batch batch = operator.next()) {
                assertThat(batch.borrowMask().selectedCount()).isOne();
            }
            assertThat(operator.hasNext()).isFalse();
        }
        assertThat(processor.calls()).isEqualTo(3);
    }

    @Test
    void testRejectsPassThroughReference()
    {
        TestOutputBatch output = new TestOutputBatch(1, List.of(new TableFunctionOutputBatch.PassThroughReference(0)));
        TestingProcessor processor = new TestingProcessor(new TableFunctionProgress.Produced(output, Set.of()));

        try (Allocator allocator = new Allocator(EngineResources.createDefault());
                LeafTableFunctionOperator operator = new LeafTableFunctionOperator(
                        EMPTY_SCHEMA,
                        processor,
                        allocator,
                        new TestingIngress(),
                        new TestingExecutionContext())) {
            assertThatThrownBy(operator::hasNext)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("leaf table function returned a pass-through reference");
        }
        assertThat(output.closed()).isTrue();
    }

    private static final class TestingProcessor
            implements TableFunctionProcessor
    {
        private final Queue<TableFunctionProgress> progress = new ArrayDeque<>();
        private int calls;
        private boolean closed;

        private TestingProcessor(TableFunctionProgress... progress)
        {
            this.progress.addAll(List.of(progress));
        }

        @Override
        public TableFunctionProgress process(
                TableFunctionInput input,
                TableFunctionOutputDemand outputDemand,
                Allocator allocator,
                Allocator.Context allocationContext,
                ExecutionContext executionContext)
        {
            calls++;
            assertThat(input.arguments()).isEmpty();
            assertThat(outputDemand.passThroughArguments()).isEmpty();
            return progress.remove();
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

    private static final class TestOutputBatch
            implements TableFunctionOutputBatch
    {
        private final List<PassThroughReference> references;
        private Selection selection;
        private boolean closed;

        private TestOutputBatch(int positions)
        {
            this(positions, List.of());
        }

        private TestOutputBatch(int positions, List<PassThroughReference> references)
        {
            this.references = List.copyOf(references);
            this.selection = new MaskSelection(Mask.all(positions));
        }

        @Override
        public int properOutputCount()
        {
            return 0;
        }

        @Override
        public List<PassThroughReference> passThroughReferences()
        {
            return references;
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
        public <T> Optional<T> capability(BatchCapability<T> capability)
        {
            return Optional.empty();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        private boolean closed()
        {
            return closed;
        }
    }

    private static final class TestingIngress
            implements SourceBatchOperatorIngress
    {
        @Override
        public Batch adapt(SourceBatch sourceBatch)
        {
            Mask mask = ((MaskSelection) sourceBatch.selection()).mask();
            return new Batch(
                    mask,
                    constrained -> sourceBatch.select(new MaskSelection(constrained)),
                    identity(),
                    _ -> {},
                    sourceBatch::close);
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
