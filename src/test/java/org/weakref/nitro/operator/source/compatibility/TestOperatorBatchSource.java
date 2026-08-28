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
package org.weakref.nitro.operator.source.compatibility;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.execution.ExecutionPolicy;
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.LongDomain;
import org.weakref.nitro.core.source.LongDomainCapability;
import org.weakref.nitro.core.source.OrdinalSourceColumnHandle;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.MaskSelection;
import org.weakref.nitro.execution.DriverResult;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.execution.OperatorExecutionDriver;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.SourceOperatorIngress;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

class TestOperatorBatchSource
{
    @Test
    void testBlockedSourceSuspendsWithoutRepolling()
    {
        CompletableFuture<Void> inputReady = new CompletableFuture<>();
        AtomicInteger polls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        SourceBatch batch = emptySourceBatch();
        BatchSource batchSource = testingSource(polls, closed, inputReady, batch);
        TestingExecutionContext context = new TestingExecutionContext();
        Allocator allocator = new Allocator(EngineResources.createDefault());

        try (OperatorExecutionDriver driver = new OperatorExecutionDriver(
                new BatchSourceOperator(batchSource, emptyIngress(), context),
                allocator,
                context)) {
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.BLOCKED);
            assertThat(polls).hasValue(1);
            assertThat(driver.blocked()).contains(inputReady);

            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.BLOCKED);
            assertThat(polls).hasValue(1);

            inputReady.complete(null);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.OUTPUT);
            assertThat(polls).hasValue(2);
            assertThat(context.inputBatches).hasValue(1);
            assertThat(context.inputPositions).hasValue(0);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.FINISHED);
            assertThat(polls).hasValue(3);
            assertThat(closed).isTrue();
        }
    }

    @Test
    void testStackPreservingAwaitContinuesTheSamePull()
    {
        CompletableFuture<Void> inputReady = new CompletableFuture<>();
        AtomicInteger polls = new AtomicInteger();
        AtomicInteger awaits = new AtomicInteger();
        TestingExecutionContext context = new TestingExecutionContext()
        {
            @Override
            public void await(CompletionStage<Void> continuation)
            {
                assertThat(continuation).isSameAs(inputReady);
                inputReady.complete(null);
                awaits.incrementAndGet();
            }
        };
        BatchSource batchSource = testingSource(polls, new AtomicBoolean(), inputReady, emptySourceBatch());
        Operator source = new BatchSourceOperator(batchSource, emptyIngress(), context);

        assertThat(source.hasNext()).isTrue();
        assertThat(awaits).hasValue(1);
        assertThat(polls).hasValue(2);
        try (Batch _ = source.next()) {
            assertThat(polls).hasValue(2);
        }
        source.close();
    }

    @Test
    void testRuntimeFilterPublishesClassloaderNeutralLongDomain()
    {
        Schema schema = Schema.unspecified(List.of("probe_key"));
        DynamicFilter filter = DynamicFilter.fromRange(0, 10, 20);

        LongDomain domain = new NativeRuntimeFilterDomain(schema.field(0).type(), filter)
                .capability(LongDomainCapability.LONG_DOMAIN)
                .orElseThrow();

        assertThat(domain.size()).isEqualTo(11);
        assertThat(domain.isEmpty()).isFalse();
        assertThat(domain.rangeDensity()).isEqualTo(1);
        assertThat(domain.test(9)).isFalse();
        assertThat(domain.test(10)).isTrue();
        assertThat(domain.test(20)).isTrue();
        assertThat(domain.test(21)).isFalse();
    }

    @Test
    void testConstructedIngressOwnsArbitrarySourceBatchLifetime()
    {
        AtomicBoolean batchClosed = new AtomicBoolean();
        SourceBatch sourceBatch = new SourceBatch()
        {
            @Override
            public Schema schema()
            {
                return Schema.unspecified(0);
            }

            @Override
            public Selection selection()
            {
                return new MaskSelection(Mask.all(0));
            }

            @Override
            public ColumnView column(int index)
            {
                throw new IndexOutOfBoundsException(index);
            }

            @Override
            public void select(Selection selection) {}

            @Override
            public void close()
            {
                batchClosed.set(true);
            }
        };
        BatchSource batchSource = new BatchSource()
        {
            private boolean emitted;

            @Override
            public Schema schema()
            {
                return Schema.unspecified(0);
            }

            @Override
            public SourceColumnHandle column(int outputIndex)
            {
                throw new IndexOutOfBoundsException(outputIndex);
            }

            @Override
            public Set<SourceCapability> capabilities()
            {
                return Set.of();
            }

            @Override
            public SourcePoll poll()
            {
                if (emitted) {
                    return SourcePoll.Finished.FINISHED;
                }
                emitted = true;
                return new SourcePoll.Ready(sourceBatch);
            }

            @Override
            public void close() {}
        };
        SourceOperatorIngress ingress = batch -> new Batch(
                Mask.all(0),
                _ -> {},
                mask -> mask,
                _ -> {},
                batch::close,
                new Output[0]);
        Operator source = new BatchSourceOperator(batchSource, ingress);

        assertThat(source.hasNext()).isTrue();
        try (Batch _ = source.next()) {
            assertThat(batchClosed).isFalse();
        }
        assertThat(batchClosed).isTrue();
        assertThat(source.hasNext()).isFalse();
    }

    @Test
    void testConstructedIngressSeparatesConstrainedReborrowFromOpenBatchPolling()
    {
        BatchSource batchSource = new BatchSource()
        {
            @Override
            public Schema schema()
            {
                return Schema.unspecified(0);
            }

            @Override
            public SourceColumnHandle column(int outputIndex)
            {
                throw new IndexOutOfBoundsException(outputIndex);
            }

            @Override
            public Set<SourceCapability> capabilities()
            {
                return Set.of(SourceCapability.CONSTRAINED_REBORROW);
            }

            @Override
            public SourcePoll poll()
            {
                return SourcePoll.Finished.FINISHED;
            }

            @Override
            public void close() {}
        };
        SourceOperatorIngress ingress = batch -> {
            throw new AssertionError("no batch expected");
        };

        Operator source = new BatchSourceOperator(batchSource, ingress);

        assertThat(source.supportsConstrainedReborrow()).isTrue();
        assertThat(source.supportsOpenBatchHasNext()).isFalse();
    }

    @Test
    void testAvailabilityPollDoesNotAdvanceDecoder()
    {
        AtomicInteger pulls = new AtomicInteger();
        Operator decoder = new Operator()
        {
            private boolean emitted;

            @Override
            public int outputCount()
            {
                return 0;
            }

            @Override
            public boolean hasNext()
            {
                return !emitted;
            }

            @Override
            public Batch next()
            {
                emitted = true;
                pulls.incrementAndGet();
                return new Batch(Mask.all(0), new Output[0]);
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };
        Operator source = new BatchSourceOperator(new OperatorBatchSource(decoder), new NativeSourceOperatorIngress());

        assertThat(source.hasNext()).isTrue();
        assertThat(pulls).hasValue(0);
        try (Batch _ = source.next()) {
            assertThat(pulls).hasValue(1);
        }
        assertThat(source.hasNext()).isFalse();
    }

    @Test
    void testLazyNativeBatchDoesNotPullUntilTransfer()
    {
        AtomicInteger pulls = new AtomicInteger();
        NativeSourceBatch batch = new NativeSourceBatch(
                Schema.unspecified(0),
                false,
                false,
                () -> {
                    pulls.incrementAndGet();
                    return new Batch(Mask.all(0), new Output[0]);
                });

        NativeBatchAccess access = batch.capability(NativeBatchCapability.NATIVE_BATCH).orElseThrow();
        assertThat(pulls).hasValue(0);
        try (Batch _ = access.transfer()) {
            assertThat(pulls).hasValue(1);
        }
        batch.close();
        assertThat(pulls).hasValue(1);
    }

    @Test
    void testSchemaAndRuntimeFilterCrossSourceBoundary()
    {
        Schema schema = Schema.unspecified(List.of("probe_key", "payload"));
        CapturingSource decoder = new CapturingSource();
        OperatorBatchSource batchSource = new OperatorBatchSource(decoder, schema);
        Operator source = new BatchSourceOperator(batchSource, new NativeSourceOperatorIngress());

        assertThat(source.outputSchema()).isEqualTo(schema);
        assertThat(batchSource.column(0)).isSameAs(batchSource.column(0));
        assertThat(batchSource.supportsRuntimeFilter(batchSource.column(0))).isTrue();
        assertThat(batchSource.supportsRuntimeFilter(new OrdinalSourceColumnHandle(0, schema.field(0).type()))).isFalse();
        assertThat(source.exactOutputRows()).isEqualTo(123);
        assertThat(source.supportsDynamicFilterPushdown(0)).isTrue();
        assertThat(source.supportsDynamicFilterPushdown(1)).isFalse();
        assertThat(source.supportsDynamicFilterPushdown(2)).isFalse();
        assertThat(source.supportsConstrainedReborrow()).isFalse();
        assertThat(source.supportsOpenBatchHasNext()).isFalse();

        source.pushDynamicFilter(DynamicFilter.fromRange(0, 10, 20));

        assertThat(source.supportsConstrainedReborrow()).isTrue();
        assertThat(decoder.filter).isNotNull();
        assertThat(decoder.filter.column()).isZero();
        assertThat(decoder.filter.accepts(9)).isFalse();
        assertThat(decoder.filter.accepts(10)).isTrue();
        assertThat(decoder.filter.accepts(20)).isTrue();
        assertThat(decoder.filter.accepts(21)).isFalse();
    }

    private static final class CapturingSource
            implements Operator
    {
        private DynamicFilter filter;

        @Override
        public int outputCount()
        {
            return 2;
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Batch next()
        {
            throw new IllegalStateException("No rows");
        }

        @Override
        public void constrain(Mask mask) {}

        @Override
        public void pushDynamicFilter(DynamicFilter filter)
        {
            this.filter = filter;
        }

        @Override
        public boolean supportsDynamicFilterPushdown(int column)
        {
            return column == 0;
        }

        @Override
        public long exactOutputRows()
        {
            return 123;
        }

        @Override
        public boolean supportsConstrainedReborrow()
        {
            return filter != null;
        }

        @Override
        public void close() {}
    }

    private static BatchSource testingSource(
            AtomicInteger polls,
            AtomicBoolean closed,
            CompletableFuture<Void> inputReady,
            SourceBatch batch)
    {
        return new BatchSource()
        {
            @Override
            public Schema schema()
            {
                return Schema.unspecified(0);
            }

            @Override
            public SourceColumnHandle column(int outputIndex)
            {
                throw new IndexOutOfBoundsException(outputIndex);
            }

            @Override
            public Set<SourceCapability> capabilities()
            {
                return Set.of();
            }

            @Override
            public SourcePoll poll()
            {
                return switch (polls.getAndIncrement()) {
                    case 0 -> new SourcePoll.Blocked(inputReady);
                    case 1 -> new SourcePoll.Ready(batch);
                    default -> SourcePoll.Finished.FINISHED;
                };
            }

            @Override
            public void close()
            {
                closed.set(true);
            }
        };
    }

    private static SourceBatch emptySourceBatch()
    {
        return new SourceBatch()
        {
            @Override
            public Schema schema()
            {
                return Schema.unspecified(0);
            }

            @Override
            public Selection selection()
            {
                return new MaskSelection(Mask.all(0));
            }

            @Override
            public ColumnView column(int index)
            {
                throw new IndexOutOfBoundsException(index);
            }

            @Override
            public void select(Selection selection) {}

            @Override
            public void close() {}
        };
    }

    private static SourceOperatorIngress emptyIngress()
    {
        return batch -> new Batch(
                Mask.all(0),
                _ -> {},
                mask -> mask,
                _ -> {},
                batch::close,
                new Output[0]);
    }

    private static class TestingExecutionContext
            implements ExecutionContext
    {
        private final AtomicInteger inputBatches = new AtomicInteger();
        private final AtomicInteger inputPositions = new AtomicInteger();

        @Override
        public MemoryReservation memory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutionPolicy policy()
        {
            return new ExecutionPolicy() {};
        }

        @Override
        public ExecutionDiagnostics diagnostics()
        {
            return (event, value) -> {
                if (event.equals(BatchSourceOperator.INPUT_BATCHES)) {
                    inputBatches.addAndGet(toIntExact(value));
                }
                if (event.equals(BatchSourceOperator.INPUT_POSITIONS)) {
                    inputPositions.addAndGet(toIntExact(value));
                }
            };
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
