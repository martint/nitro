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
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAllocator;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.FilteredAccumulator;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestGroupedAggregationSession
{
    @Test
    void testAdaptivePartialAggregationFlushAndPassthrough()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        control)) {
            allocator.beginExecution();
            try (Batch input = batch(1, 1, 2)) {
                session.addInput(input, 30);
            }
            session.flush();
            assertThat(session.hasOutput()).isTrue();
            try (Batch result = session.getOutput()) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(1, 2);
                assertThat(selectedLongValues(result, 1))
                        .containsExactly(2, 1);
            }
            assertThat(control.aggregatedFlushes).isEqualTo(1);
            assertThat(control.inputBytes).isEqualTo(30);
            assertThat(control.inputRows).isEqualTo(3);
            assertThat(control.outputRows).isEqualTo(2);

            control.enabled = false;
            try (Batch input = batch(3, 3);
                    Batch result = addAndGetOutput(session, input, 20)) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(3, 3);
                assertThat(selectedLongValues(result, 1))
                        .containsExactly(1, 1);
            }
            assertThat(control.passthroughFlushes).isEqualTo(1);
            assertThat(control.inputBytes).isEqualTo(50);
            assertThat(control.inputRows).isEqualTo(5);

            control.enabled = true;
            try (Batch input = batch(4, 4)) {
                session.addInput(input, 20);
            }
            try (Batch result = session.finish()) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(4);
                assertThat(selectedLongValues(result, 1))
                        .containsExactly(2);
            }
        }
    }

    @Test
    void testBuildsInitialAggregationRowsWithoutGrouping()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            InitialAggregationBatchBuilder builder = new InitialAggregationBatchBuilder(
                    allocator,
                    Schema.unspecified(3),
                    List.of(0),
                    PhysicalAggregationProgram.independent(List.of(
                            new CountAll(),
                            new FilteredAccumulator(new CountAll(), 2))),
                    resources.operatorResources());

            assertThat(builder.outputSchema().size()).isEqualTo(3);
            try (Batch input = new Batch(
                    Mask.sparse(new int[] {0, 2, 3}, 4),
                    Output.of(Streams.ofValues(new I64Vector(new long[] {11, 12, 13, 14}))),
                    Output.of(Streams.ofValues(new I64Vector(new long[] {101, 102, 103, 104}))),
                    Output.of(Streams.ofValues(new BooleanVector(new boolean[] {true, true, false, true}))));
                    Batch result = builder.build(input)) {
                assertThat(result.borrowMask().all()).isTrue();
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(11, 13, 14);
                assertThat(((I64Vector) result.output(1).borrow(Stream.VALUES)).values())
                        .containsExactly(1, 1, 1);
                assertThat(((I64Vector) result.output(2).borrow(Stream.VALUES)).values())
                        .containsExactly(1, 0, 1);
            }
        }
    }

    @Test
    void testAccumulatesGroupsAcrossIndependentlyScheduledBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch first = batch(1, 2, 1);
                    Batch second = batch(2, 3, 1, 3)) {
                session.addInput(first);
                session.addInput(second);
            }

            try (Batch result = session.finish()) {
                assertThat(((I64Vector) result.output(0).borrow(Stream.VALUES)).values())
                        .containsExactly(1, 2, 3);
                assertThat(Arrays.copyOf(
                        ((I64Vector) result.output(1).borrow(Stream.VALUES)).values(),
                        result.borrowMask().count()))
                        .containsExactly(3, 2, 2);
            }
            try (Batch late = batch(4)) {
                assertThatIllegalStateException()
                        .isThrownBy(() -> session.addInput(late))
                        .withMessage("grouped aggregation session is finished");
            }
        }
    }

    @Test
    void testEmptyInputProducesNoGroups()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch result = session.finish()) {
                assertThat(result.borrowMask().none()).isTrue();
            }
        }
    }

    @Test
    void testEmptyInputUsesGroupedKeyTypeVectorFactory()
    {
        TypeBinding binaryType = binaryType();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        new Schema(List.of(new Field(binaryType, true))),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch result = session.finish()) {
                assertThat(result.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
            }
        }
    }

    private static Batch batch(long... values)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(Streams.ofValues(new I64Vector(values))));
    }

    private static Batch addAndGetOutput(GroupedAggregationSession session, Batch input, long inputBytes)
    {
        session.addInput(input, inputBytes);
        assertThat(session.hasOutput()).isTrue();
        return session.getOutput();
    }

    private static long[] selectedLongValues(Batch batch, int output)
    {
        return Arrays.copyOf(
                ((I64Vector) batch.output(output).borrow(Stream.VALUES)).values(),
                batch.borrowMask().count());
    }

    private static final class TestingPartialAggregationControl
            implements PartialAggregationControl
    {
        private boolean enabled = true;
        private int aggregatedFlushes;
        private int passthroughFlushes;
        private long inputBytes;
        private long inputRows;
        private long outputRows;

        @Override
        public boolean aggregationEnabled()
        {
            return enabled;
        }

        @Override
        public void onAggregatedFlush(long inputBytes, long inputRows, long outputRows)
        {
            aggregatedFlushes++;
            this.inputBytes += inputBytes;
            this.inputRows += inputRows;
            this.outputRows += outputRows;
        }

        @Override
        public void onPassthroughFlush(long inputBytes, long inputRows)
        {
            passthroughFlushes++;
            this.inputBytes += inputBytes;
            this.inputRows += inputRows;
        }
    }

    private static TypeBinding binaryType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:binary");
            }

            @Override
            public Class<?> carrierType()
            {
                return String.class;
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
                    public Vector constant(VectorAllocator allocator, Object value, int length)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Vector nullValues(VectorAllocator allocator, int length)
                    {
                        return allocator.allocate(BinaryVector.class, length, size -> new BinaryVector(size, 0));
                    }
                });
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(BinaryVector.class);
            }
        };
    }
}
