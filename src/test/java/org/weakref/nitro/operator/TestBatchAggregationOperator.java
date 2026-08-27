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
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.ValueDemand;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.BatchAggregationSession.InputOwnership.CALLER;
import static org.weakref.nitro.operator.BatchAggregationSession.InputOwnership.SESSION;

final class TestBatchAggregationOperator
{
    private static final Schema INPUT_SCHEMA = Schema.unspecified(2);
    private static final Schema OUTPUT_SCHEMA = Schema.unspecified(1);

    @Test
    void testPullsInputsAndExposesIncrementalAndFinalOutput()
    {
        TestingOperator source = new TestingOperator(INPUT_SCHEMA, batches(2, 3));
        TestingAggregationSession aggregation = new TestingAggregationSession(OUTPUT_SCHEMA, true, false);
        try (BatchAggregationOperator operator = new BatchAggregationOperator(
                source,
                aggregation,
                batch -> batch.borrowMask().count() * 8L,
                Map.of(0, ValueDemand.FULL))) {
            assertThat(operator.hasNext()).isTrue();
            assertThat(aggregation.inputBytes).isEqualTo(16);
            try (Batch output = operator.next()) {
                assertThat(output.borrowMask().count()).isEqualTo(1);
            }

            assertThat(operator.hasNext()).isTrue();
            assertThat(aggregation.inputBytes).isEqualTo(40);
            try (Batch output = operator.next()) {
                assertThat(output.borrowMask().count()).isEqualTo(1);
            }
            assertThat(operator.hasNext()).isTrue();
            try (Batch output = operator.next()) {
                assertThat(output.borrowMask().count()).isEqualTo(1);
            }
            assertThat(operator.hasNext()).isFalse();

            assertThat(operator.sourceOutputDemand(Operator.fullOutputDemand(1)))
                    .contains(Map.of(0, ValueDemand.FULL));
        }
        assertThat(source.closed).isTrue();
        assertThat(aggregation.closed).isTrue();
        assertThat(source.inputs).allMatch(TrackingBatch::isClosed);
    }

    @Test
    void testSessionOwnsTransferredInput()
    {
        TestingOperator source = new TestingOperator(INPUT_SCHEMA, batches(4));
        TestingAggregationSession aggregation = new TestingAggregationSession(OUTPUT_SCHEMA, false, true);
        try (BatchAggregationOperator operator = new BatchAggregationOperator(source, aggregation, _ -> 0)) {
            assertThat(operator.hasNext()).isTrue();
            assertThat(source.inputs.getFirst().isClosed()).isFalse();
            try (Batch ignored = operator.next()) {
                assertThat(aggregation.finished).isTrue();
            }
        }
        assertThat(source.inputs.getFirst().isClosed()).isTrue();
    }

    @Test
    void testAllowsAggregationWithoutFinalOutput()
    {
        TestingOperator source = new TestingOperator(INPUT_SCHEMA, List.of());
        TestingAggregationSession aggregation = new TestingAggregationSession(OUTPUT_SCHEMA, false, false)
        {
            @Override
            public Optional<Batch> finishOutput()
            {
                finished = true;
                return Optional.empty();
            }
        };
        try (BatchAggregationOperator operator = new BatchAggregationOperator(source, aggregation, _ -> 0)) {
            assertThat(operator.hasNext()).isFalse();
        }
        assertThat(aggregation.finished).isTrue();
    }

    private static List<TrackingBatch> batches(int... positions)
    {
        return java.util.Arrays.stream(positions)
                .mapToObj(TrackingBatch::new)
                .toList();
    }

    private static final class TestingOperator
            implements Operator
    {
        private final Schema schema;
        private final List<TrackingBatch> inputs;
        private final Queue<TrackingBatch> batches;
        private boolean closed;

        private TestingOperator(Schema schema, List<TrackingBatch> batches)
        {
            this.schema = schema;
            this.inputs = batches;
            this.batches = new ArrayDeque<>(batches);
        }

        @Override
        public int outputCount()
        {
            return schema.size();
        }

        @Override
        public Schema outputSchema()
        {
            return schema;
        }

        @Override
        public boolean hasNext()
        {
            return !batches.isEmpty();
        }

        @Override
        public Batch next()
        {
            return batches.remove().batch();
        }

        @Override
        public void constrain(Mask mask)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
        {
            return Optional.of(Map.copyOf(demandedOutputs));
        }

        @Override
        public void close()
        {
            closed = true;
            batches.forEach(batch -> batch.batch().close());
            batches.clear();
        }
    }

    private static class TestingAggregationSession
            implements BatchAggregationSession
    {
        private final Schema schema;
        private final boolean incrementalOutput;
        private final boolean retainInput;
        private Batch output;
        private Batch retainedInput;
        private long inputBytes;
        protected boolean finished;
        private boolean closed;

        private TestingAggregationSession(Schema schema, boolean incrementalOutput, boolean retainInput)
        {
            this.schema = schema;
            this.incrementalOutput = incrementalOutput;
            this.retainInput = retainInput;
        }

        @Override
        public Schema outputSchema()
        {
            return schema;
        }

        @Override
        public void addInput(Batch batch)
        {
            addInputWithOwnership(batch, 0);
        }

        @Override
        public InputOwnership addInputWithOwnership(Batch batch, long inputBytes)
        {
            this.inputBytes += inputBytes;
            if (retainInput) {
                retainedInput = batch;
                return SESSION;
            }
            if (incrementalOutput && output == null) {
                output = new Batch(Mask.all(1));
            }
            return CALLER;
        }

        @Override
        public boolean hasOutput()
        {
            return output != null;
        }

        @Override
        public Batch getOutput()
        {
            Batch result = output;
            output = null;
            return result;
        }

        @Override
        public long retainedBytes()
        {
            return 0;
        }

        @Override
        public Batch finish()
        {
            finished = true;
            if (retainedInput != null) {
                Batch result = retainedInput;
                retainedInput = null;
                return result;
            }
            return new Batch(Mask.all(1));
        }

        @Override
        public void close()
        {
            closed = true;
            if (output != null) {
                output.close();
                output = null;
            }
            if (retainedInput != null) {
                retainedInput.close();
                retainedInput = null;
            }
        }
    }

    private record TrackingBatch(Batch batch, AtomicBoolean closed)
    {
        private TrackingBatch(int positions)
        {
            this(new AtomicBoolean(), positions);
        }

        private TrackingBatch(AtomicBoolean closed, int positions)
        {
            this(new Batch(Mask.all(positions), _ -> {}, java.util.function.Function.identity(), _ -> {}, () -> closed.set(true)), closed);
        }

        private boolean isClosed()
        {
            return closed.get();
        }
    }
}
