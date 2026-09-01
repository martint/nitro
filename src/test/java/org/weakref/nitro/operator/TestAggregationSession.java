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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.DistinctPhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.FilteredAccumulator;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestAggregationSession
{
    @Test
    void testOrdersRowsAcrossIndependentlyScheduledBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            RecordingAggregationSession delegate = new RecordingAggregationSession();
            try (OrderedAggregationSession session = new OrderedAggregationSession(
                    allocator,
                    Schema.unspecified(1),
                    new PhysicalOrdering(List.of(new PhysicalOrdering.Key(0, false, false))),
                    resources.operatorResources(),
                    delegate);
                    Batch first = values(9, 1);
                    Batch second = values(12, 5)) {
                allocator.beginExecution();
                assertThat(session.addInputWithOwnership(first, 20)).isEqualTo(BatchAggregationSession.InputOwnership.CALLER);
                assertThat(session.addInputWithOwnership(second, 30)).isEqualTo(BatchAggregationSession.InputOwnership.CALLER);

                try (Batch result = session.finish()) {
                    assertThat(result.borrowMask().count()).isOne();
                }
                assertThat(delegate.values).containsExactly(1L, 5L, 9L, 12L);
                assertThat(delegate.inputBytes).isEqualTo(50);
            }
        }
    }

    @Test
    void testAccumulatesAcrossIndependentlyScheduledBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                AggregationSession session = new AggregationSession(
                        allocator,
                        new Schema(List.of()),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch first = new Batch(Mask.all(2), new Output[0]);
                    Batch second = new Batch(Mask.all(3), new Output[0])) {
                session.addInput(first);
                session.addInput(second);
            }

            try (Batch result = session.finish()) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(5);
            }
            try (Batch late = new Batch(Mask.all(1), new Output[0])) {
                assertThatIllegalStateException()
                        .isThrownBy(() -> session.addInput(late))
                        .withMessage("aggregation session is finished");
            }
        }
    }

    @Test
    void testEmptyInputProducesDefaultAggregateResult()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                AggregationSession session = new AggregationSession(
                        allocator,
                        new Schema(List.of()),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch result = session.finish()) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(0);
            }
        }
    }

    @Test
    void testDistinctAggregationAcrossBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                AggregationSession session = new AggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        PhysicalAggregationProgram.singleUnit(
                                new DistinctPhysicalAggregationUnit(new CountAll(), new int[] {0})),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch first = values(11, 12, 11);
                    Batch second = values(12, 13, 13)) {
                session.addInput(first);
                session.addInput(second);
            }

            try (Batch result = session.finish()) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(3);
            }
        }
    }

    @Test
    void testDistinctAggregationsPartitionStateByFilter()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                AggregationSession session = new AggregationSession(
                        allocator,
                        Schema.unspecified(3),
                        new PhysicalAggregationProgram(
                                List.of(
                                        new DistinctPhysicalAggregationUnit(new FilteredAccumulator(new CountAll(), 1), new int[] {0}),
                                        new DistinctPhysicalAggregationUnit(new FilteredAccumulator(new CountAll(), 2), new int[] {0})),
                                List.of(
                                        new PhysicalAggregationProgram.Output(0, 0),
                                        new PhysicalAggregationProgram.Output(1, 0))),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch batch = new Batch(
                    Mask.all(4),
                    Output.of(Streams.ofValues(new I64Vector(new long[] {11, 22, 11, 22}))),
                    Output.of(Streams.ofValues(new BooleanVector(new boolean[] {true, true, false, false}))),
                    Output.of(Streams.ofValues(new BooleanVector(new boolean[] {false, false, true, true}))))) {
                session.addInput(batch);
            }

            try (Batch result = session.finish()) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values()).containsExactly(2);
                assertThat(((I64Vector) result.output(1).borrow(Stream.VALUES)).values()).containsExactly(2);
            }
        }
    }

    private static Batch values(long... values)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(values))));
    }

    private static final class RecordingAggregationSession
            implements BatchAggregationSession
    {
        private final java.util.ArrayList<Long> values = new java.util.ArrayList<>();
        private long inputBytes;

        @Override
        public Schema outputSchema()
        {
            return new Schema(List.of());
        }

        @Override
        public void addInput(Batch batch)
        {
            addInput(batch, 0);
        }

        @Override
        public void addInput(Batch batch, long inputBytes)
        {
            VectorAccess.LongValues vector = VectorAccess.longValues(batch.output(0).borrow(Stream.VALUES));
            for (int position : batch.borrowMask()) {
                values.add(vector.value(position));
            }
            this.inputBytes += inputBytes;
        }

        @Override
        public long retainedBytes()
        {
            return 0;
        }

        @Override
        public Batch finish()
        {
            return new Batch(Mask.all(1), new Output[0]);
        }

        @Override
        public Optional<Batch> finishOutput()
        {
            return Optional.of(finish());
        }

        @Override
        public void close() {}
    }
}
