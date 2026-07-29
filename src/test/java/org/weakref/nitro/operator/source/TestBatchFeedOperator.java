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
import org.weakref.nitro.core.source.SourceCapability;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Output;

import java.util.List;
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
