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
package org.weakref.nitro.operator.source;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.batch.ColumnView;
import org.weakref.nitro.core.batch.Selection;
import org.weakref.nitro.core.batch.SourceBatch;
import org.weakref.nitro.core.source.BatchSource;
import org.weakref.nitro.core.source.LongDomainCapability;
import org.weakref.nitro.core.source.RuntimeFilter;
import org.weakref.nitro.core.source.RuntimeFilterAcceptance;
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.source.SourceColumnHandle;
import org.weakref.nitro.core.source.SourceOutputDemand;
import org.weakref.nitro.core.source.SourceOutputDemandProtocol;
import org.weakref.nitro.core.source.SourcePoll;
import org.weakref.nitro.core.source.SourceProtocol;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.DynamicFilter;
import org.weakref.nitro.operator.GroupIdOperator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.StaticFilterEnforcement;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestBatchFeedOperator
{
    private static final Schema SCHEMA = Schema.unspecified(List.of("value"));

    @Test
    void testReusesIngressAcrossExternallyScheduledBatches()
    {
        AtomicInteger adaptations = new AtomicInteger();
        AtomicReference<Selection> selected = new AtomicReference<>();
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        SourceOperatorIngress ingress = new TestingIngress(adaptations, selected);

        try (BatchFeedOperator feed = new BatchFeedOperator(
                SCHEMA,
                ingress,
                Set.of(SourceCapability.STABLE_BATCH_BORROW, SourceCapability.CONSTRAINED_REBORROW))) {
            assertThat(feed.supportsStableBatchBorrow()).isTrue();
            assertThat(feed.supportsConstrainedReborrow()).isTrue();
            assertThat(feed.supportsOpenBatchHasNext()).isTrue();
            feed.addInput(new TestingSourceBatch(firstClosed));
            assertThat(feed.hasNext()).isTrue();
            try (Batch ignored = feed.next()) {
                assertThat(feed.hasNext()).isFalse();
            }
            assertThat(firstClosed).isTrue();
            feed.finishInput();

            feed.addInput(new TestingSourceBatch(secondClosed));
            feed.constrain(Mask.all(3));
            assertThat(selected.get().count()).isEqualTo(3);
            try (Batch ignored = feed.next()) {
                assertThat(feed.hasNext()).isFalse();
            }
            feed.finishInput();

            assertThat(secondClosed).isTrue();
            assertThat(adaptations).hasValue(2);
        }
    }

    @Test
    void testGroupIdPipelineResumesForNextExternallyScheduledBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        try (BatchFeedOperator feed = new BatchFeedOperator(SCHEMA, new TestingIngress(
                new AtomicInteger(),
                new AtomicReference<>()));
                GroupIdOperator groupId = new GroupIdOperator(
                        allocator,
                        feed,
                        new int[][] {{}},
                        EngineResources.from(allocator).operatorResources().groupIdPolicy())) {
            feed.addInput(new TestingSourceBatch(firstClosed));
            try (Batch ignored = groupId.next()) {
                assertThat(ignored.borrowMask().count()).isEqualTo(3);
            }
            assertThat(groupId.hasNext()).isFalse();
            feed.finishInput();

            feed.addInput(new TestingSourceBatch(secondClosed));
            assertThat(groupId.hasNext()).isTrue();
            try (Batch ignored = groupId.next()) {
                assertThat(ignored.borrowMask().count()).isEqualTo(3);
            }
            assertThat(groupId.hasNext()).isFalse();
            feed.finishInput();
        }

        assertThat(firstClosed).isTrue();
        assertThat(secondClosed).isTrue();
    }

    @Test
    void testFeedsNativeBatchWithoutIngressAdaptation()
    {
        AtomicInteger adaptations = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        Batch batch = new Batch(
                Mask.all(3),
                _ -> {},
                mask -> mask,
                _ -> {},
                () -> closed.set(true),
                new Output[0]);

        try (BatchFeedOperator feed = new BatchFeedOperator(SCHEMA, new TestingIngress(
                adaptations,
                new AtomicReference<>()))) {
            feed.addInput(batch);
            assertThat(feed.hasNext()).isTrue();
            try (Batch output = feed.next()) {
                assertThat(output).isSameAs(batch);
                assertThat(output.borrowMask().count()).isEqualTo(3);
            }
            feed.finishInput();
        }

        assertThat(adaptations).hasValue(0);
        assertThat(closed).isTrue();
    }

    @Test
    void testClosesUnconsumedNativeBatch()
    {
        AtomicBoolean closed = new AtomicBoolean();
        Batch batch = new Batch(
                Mask.all(3),
                _ -> {},
                mask -> mask,
                _ -> {},
                () -> closed.set(true),
                new Output[0]);

        try (BatchFeedOperator feed = new BatchFeedOperator(SCHEMA, new TestingIngress(
                new AtomicInteger(),
                new AtomicReference<>()))) {
            feed.addInput(batch);
        }

        assertThat(closed).isTrue();
    }

    @Test
    void testRejectsOverlappingOrUnconsumedInputs()
    {
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        SourceBatch first = new TestingSourceBatch(firstClosed);
        SourceBatch second = new TestingSourceBatch(secondClosed);

        try (BatchFeedOperator feed = new BatchFeedOperator(SCHEMA, new TestingIngress(
                new AtomicInteger(),
                new AtomicReference<>()))) {
            assertThat(feed.supportsStableBatchBorrow()).isFalse();
            assertThat(feed.supportsConstrainedReborrow()).isFalse();
            feed.addInput(first);
            assertThatThrownBy(() -> feed.addInput(second))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("operator already has input");
            assertThatThrownBy(feed::finishInput)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("operator has no consumed input");
        }

        assertThat(firstClosed).isTrue();
        assertThat(secondClosed).isFalse();
    }

    @Test
    void testAppliesRetainedDynamicFiltersThroughIngress()
    {
        AtomicReference<RuntimeFilter> applied = new AtomicReference<>();
        AtomicReference<Set<SourceColumnHandle>> retainedOutputs = new AtomicReference<>();
        SourceColumnHandle column = () -> SCHEMA.field(0).type();
        SourceOperatorIngress ingress = new SourceOperatorIngress()
        {
            @Override
            public Batch adapt(SourceBatch batch)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean supportsRuntimeFilter(BatchSource source, SourceColumnHandle sourceColumn)
            {
                return sourceColumn == column;
            }

            @Override
            public RuntimeFilter runtimeFilter(SourceColumnHandle sourceColumn, DynamicFilter filter)
            {
                return new RuntimeFilter(
                        sourceColumn,
                        new org.weakref.nitro.core.source.TypedDomain()
                        {
                            @Override
                            public TypeBinding type()
                            {
                                return sourceColumn.type();
                            }

                            @Override
                            public boolean includesNull()
                            {
                                return false;
                            }

                            @Override
                            public boolean isAll()
                            {
                                return false;
                            }

                            @Override
                            public boolean isNone()
                            {
                                return filter.isEmpty();
                            }

                            @Override
                            public <T> java.util.Optional<T> capability(org.weakref.nitro.core.source.DomainCapability<T> capability)
                            {
                                if (capability == LongDomainCapability.LONG_DOMAIN) {
                                    return java.util.Optional.of(capability.valueType().cast(filter));
                                }
                                return java.util.Optional.empty();
                            }
                        },
                        false);
            }
        };
        BatchSource source = new BatchSource()
        {
            @Override
            public Schema schema()
            {
                return SCHEMA;
            }

            @Override
            public SourceColumnHandle column(int outputIndex)
            {
                assertThat(outputIndex).isZero();
                return column;
            }

            @Override
            public Set<SourceCapability> capabilities()
            {
                return Set.of();
            }

            @Override
            public <T> Optional<T> protocol(SourceProtocol<T> protocol)
            {
                if (protocol == SourceOutputDemandProtocol.OUTPUT_DEMAND) {
                    SourceOutputDemand demand = retainedOutputs::set;
                    return Optional.of(protocol.valueType().cast(demand));
                }
                return Optional.empty();
            }

            @Override
            public boolean supportsRuntimeFilter(SourceColumnHandle sourceColumn)
            {
                return sourceColumn == column;
            }

            @Override
            public RuntimeFilterAcceptance addRuntimeFilter(RuntimeFilter filter)
            {
                applied.set(filter);
                return filter.residualRequired()
                        ? RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL
                        : RuntimeFilterAcceptance.ENFORCED;
            }

            @Override
            public SourcePoll poll()
            {
                return SourcePoll.Finished.FINISHED;
            }

            @Override
            public void close() {}
        };

        try (BatchFeedOperator feed = new BatchFeedOperator(SCHEMA, ingress)) {
            feed.pushDynamicFilter(DynamicFilter.fromRange(0, 10, 12));

            assertThat(feed.applyDynamicFilters(source))
                    .containsExactly(RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL);
            assertThat(applied.get()).isNotNull();
            var domain = applied.get().domain().capability(LongDomainCapability.LONG_DOMAIN).orElseThrow();
            assertThat(domain.test(9)).isFalse();
            assertThat(domain.test(10)).isTrue();
            assertThat(domain.test(12)).isTrue();
            assertThat(domain.test(13)).isFalse();

            StaticFilterEnforcement enforcement = feed.pushStaticFilter(DynamicFilter.fromRange(0, 20, 22));
            assertThat(enforcement.enforced()).isFalse();
            assertThat(feed.applyDynamicFilters(source)).containsExactly(
                    RuntimeFilterAcceptance.ACCEPTED_WITH_RESIDUAL,
                    RuntimeFilterAcceptance.ENFORCED);
            assertThat(applied.get().residualRequired()).isFalse();
            assertThat(enforcement.enforced()).isTrue();
        }

        try (BatchFeedOperator feed = new BatchFeedOperator(SCHEMA, ingress)) {
            feed.pushStaticFilter(DynamicFilter.fromRange(0, 20, 22));
            feed.applyDynamicFilters(source);
            feed.applySourceOutputDemand(source, feed.sourceOutputDemand(Set.of()));
            assertThat(retainedOutputs.get()).isEmpty();
        }
    }

    private record TestingIngress(AtomicInteger adaptations, AtomicReference<Selection> selected)
            implements SourceOperatorIngress
    {
        @Override
        public Batch adapt(SourceBatch batch)
        {
            adaptations.incrementAndGet();
            return new Batch(
                    Mask.all(3),
                    _ -> {},
                    mask -> mask,
                    _ -> {},
                    batch::close,
                    new Output[0]);
        }

        @Override
        public Selection selection(Mask mask)
        {
            Selection selection = new TestingSelection(mask);
            selected.set(selection);
            return selection;
        }
    }

    private record TestingSourceBatch(AtomicBoolean closed)
            implements SourceBatch
    {
        @Override
        public Schema schema()
        {
            return SCHEMA;
        }

        @Override
        public Selection selection()
        {
            return new TestingSelection(Mask.all(3));
        }

        @Override
        public ColumnView column(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void select(Selection selection) {}

        @Override
        public void close()
        {
            closed.set(true);
        }
    }

    private record TestingSelection(Mask mask)
            implements Selection
    {
        @Override
        public int positionCount()
        {
            return mask.size();
        }

        @Override
        public int count()
        {
            return mask.count();
        }

        @Override
        public int maxPosition()
        {
            return mask.count() == 0 ? -1 : mask.position(mask.count() - 1);
        }

        @Override
        public boolean isDense()
        {
            return mask.count() == mask.size();
        }

        @Override
        public int position(int index)
        {
            return mask.position(index);
        }
    }
}
