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
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
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
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.aggregation.Sum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestGroupedAggregationSession
{
    @Test
    void testAggregationProgramDescribesInputValueReads()
    {
        assertThat(PhysicalAggregationProgram.independent(List.of(new CountAll())).readsInputValues()).isFalse();
        assertThat(PhysicalAggregationProgram.independent(List.of(new Sum(0))).readsInputValues()).isTrue();
        assertThat(PhysicalAggregationProgram.independent(List.of(new CountAll(), new Sum(0))).readsInputValues()).isTrue();
    }

    @Test
    void testStreamsFinalGroupsInBoundedBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        null,
                        2)) {
            allocator.beginExecution();
            try (Batch input = batch(10, 20, 30, 40, 50)) {
                session.addInput(input);
            }

            try (Batch first = session.finish()) {
                assertThat(selectedLongValues(first, 0)).containsExactly(10, 20);
            }
            assertThat(session.hasOutput()).isTrue();
            try (Batch second = session.getOutput()) {
                assertThat(selectedLongValues(second, 0)).containsExactly(30, 40);
            }
            assertThat(session.hasOutput()).isTrue();
            try (Batch third = session.getOutput()) {
                assertThat(selectedLongValues(third, 0)).containsExactly(50);
            }
            assertThat(session.hasOutput()).isFalse();
        }
    }

    @Test
    void testStreamsLargeBinaryGroupsAfterReleasingConsumedStorage()
    {
        int rows = 40_000;
        int outputRows = rows / 2;
        Schema schema = new Schema(List.of(new Field(binaryType(), false)));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        schema,
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        null,
                        outputRows)) {
            allocator.beginExecution();
            BinaryVector keys = new BinaryVector(rows, rows * 80);
            for (int position = 0; position < rows; position++) {
                keys.setBytes(position, binaryGroupKey(position).getBytes(UTF_8));
            }
            try (Batch input = new Batch(Mask.all(rows), Output.of(Streams.ofValues(keys)))) {
                session.addInput(input);
            }

            try (Batch first = session.finish()) {
                assertThat(selectedBinaryValues(first, 0).getFirst()).isEqualTo(binaryGroupKey(0));
                assertThat(selectedBinaryValues(first, 0).getLast()).isEqualTo(binaryGroupKey(outputRows - 1));
            }
            try (Batch second = session.getOutput()) {
                assertThat(selectedBinaryValues(second, 0).getFirst()).isEqualTo(binaryGroupKey(outputRows));
                assertThat(selectedBinaryValues(second, 0).getLast()).isEqualTo(binaryGroupKey(rows - 1));
            }
        }
    }

    private static String binaryGroupKey(int position)
    {
        return "key-%08d-abcdefghijklmnopqrstuvwxyz-ABCDEFGHIJKLMNOPQRSTUVWXYZ".formatted(position);
    }

    @Test
    void testStreamsRepeatedBinaryKeysWithIndependentDomainsInBoundedBatches()
    {
        int rows = 10_000;
        Schema schema = new Schema(List.of(
                new Field(binaryType(), true),
                new Field(binaryType(), true),
                Schema.unspecified(1).field(0)));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        schema,
                        List.of(0, 1, 2),
                        List.of(0, 1, 2),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        null,
                        rows / 2)) {
            allocator.beginExecution();
            BinaryVector keys = new BinaryVector(rows, rows);
            BinaryVector categories = new BinaryVector(rows, rows);
            boolean[] keyNulls = new boolean[rows];
            boolean[] categoryNulls = new boolean[rows];
            long[] groupIds = new long[rows];
            for (int position = 0; position < rows; position++) {
                keys.setBytes(position, (position % 2 == 0 ? "a" : "b").getBytes(UTF_8));
                categories.setBytes(position, "x".getBytes(UTF_8));
                groupIds[position] = position;
            }
            keyNulls[2] = true;
            keyNulls[rows / 2 + 2] = true;
            categoryNulls[3] = true;
            categoryNulls[rows / 2 + 3] = true;
            try (Batch input = new Batch(
                    Mask.all(rows),
                    Output.of(Streams.ofValuesAndNulls(keys, new BooleanVector(keyNulls))),
                    Output.of(Streams.ofValuesAndNulls(categories, new BooleanVector(categoryNulls))),
                    Output.of(Streams.ofValues(new I64Vector(groupIds))))) {
                session.addInput(input);
            }

            try (Batch first = session.finish()) {
                assertThat(first.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
                assertThat(selectedBinaryValues(first, 0).subList(0, 4)).containsExactly("a", "b", "", "b");
                assertThat(((BooleanVector) first.output(0).borrow(Stream.NULLS)).values()[2]).isTrue();
                assertThat(((BooleanVector) first.output(1).borrow(Stream.NULLS)).values()[3]).isTrue();
            }
            try (Batch second = session.getOutput()) {
                assertThat(second.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
                assertThat(selectedBinaryValues(second, 0).subList(0, 4)).containsExactly("a", "b", "", "b");
                assertThat(((BooleanVector) second.output(0).borrow(Stream.NULLS)).values()[2]).isTrue();
                assertThat(((BooleanVector) second.output(1).borrow(Stream.NULLS)).values()[3]).isTrue();
                long[] values = ((I64Vector) second.output(2).borrow(Stream.VALUES)).values();
                assertThat(values[0]).isEqualTo(rows / 2);
                assertThat(values[rows / 2 - 1]).isEqualTo(rows - 1);
            }
        }
    }

    @Test
    void testStreamsSharedDictionaryGroupsWithoutFlatBackingInBoundedBatches()
    {
        Schema schema = new Schema(List.of(
                new Field(binaryType(), false),
                new Field(binaryType(), false)));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        schema,
                        List.of(0, 1),
                        List.of(0, 1),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        null,
                        2)) {
            allocator.beginExecution();
            BinaryVector firstValues = new BinaryVector(2, 2);
            firstValues.setBytes(0, "a".getBytes(UTF_8));
            firstValues.setBytes(1, "b".getBytes(UTF_8));
            BinaryVector secondValues = new BinaryVector(2, 2);
            secondValues.setBytes(0, "x".getBytes(UTF_8));
            secondValues.setBytes(1, "y".getBytes(UTF_8));
            try (Batch input = new Batch(
                    Mask.all(4),
                    Output.of(Streams.ofValues(new DictionaryVector(new int[] {0, 0, 1, 1}, firstValues))),
                    Output.of(Streams.ofValues(new DictionaryVector(new int[] {0, 1, 0, 1}, secondValues))))) {
                session.addInput(input);
            }

            try (Batch first = session.finish()) {
                assertThat(selectedBinaryValues(first, 0)).containsExactly("a", "a");
                assertThat(selectedBinaryValues(first, 1)).containsExactly("x", "y");
            }
            try (Batch second = session.getOutput()) {
                assertThat(selectedBinaryValues(second, 0)).containsExactly("b", "b");
                assertThat(selectedBinaryValues(second, 1)).containsExactly("x", "y");
            }
        }
    }

    @Test
    void testMaterializesHighCardinalityBinaryRangesWithoutRetainingGlobalDictionary()
    {
        int rows = 1_000;
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        new Schema(List.of(new Field(binaryType(), false))),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        null,
                        100)) {
            allocator.beginExecution();
            BinaryVector keys = new BinaryVector(rows, rows * 8);
            for (int position = 0; position < rows; position++) {
                keys.setBytes(position, ("url-" + position).getBytes(UTF_8));
            }
            try (Batch input = new Batch(Mask.all(rows), Output.of(Streams.ofValues(keys)))) {
                session.addInput(input);
            }

            try (Batch first = session.finish()) {
                assertThat(first.output(0).borrow(Stream.VALUES)).isInstanceOf(BinaryVector.class);
            }
            assertThat(session.hasOutput()).isTrue();
        }
    }

    @Test
    void testBulkCopiesDenseBinaryGroupOutput()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        new Schema(List.of(new Field(binaryType(), false))),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        null,
                        4)) {
            allocator.beginExecution();
            BinaryVector keys = new BinaryVector(4, 20);
            keys.setBytes(0, "alpha".getBytes(UTF_8));
            keys.setBytes(1, "beta".getBytes(UTF_8));
            keys.setBytes(2, "gamma".getBytes(UTF_8));
            keys.setBytes(3, "delta".getBytes(UTF_8));
            try (Batch input = new Batch(Mask.all(4), Output.of(Streams.ofValues(keys)))) {
                session.addInput(input);
            }

            try (Batch result = session.finish()) {
                Streams copied = result.output(0).copyPositions(null, new int[] {3, 1}, 0, 2, 0, 2, true);
                BinaryVector values = (BinaryVector) copied.values();
                assertThat(new String(values.data(), values.startOffset(0), values.length(0), UTF_8)).isEqualTo("delta");
                assertThat(new String(values.data(), values.startOffset(1), values.length(1), UTF_8)).isEqualTo("beta");

                Streams single = result.output(0).copySinglePosition(null, 2, 0, 1);
                BinaryVector singleValue = (BinaryVector) single.values();
                assertThat(new String(singleValue.data(), singleValue.startOffset(0), singleValue.length(0), UTF_8)).isEqualTo("gamma");
            }
        }
    }

    @Test
    void testNarrowsBoundedFinalBatchBeforeMaterializingOutputs()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        null,
                        4)) {
            allocator.beginExecution();
            try (Batch input = batch(10, 20, 30, 40)) {
                session.addInput(input);
            }

            try (Batch result = session.finish()) {
                Mask selected = Mask.sparse(new int[] {1, 3}, 4);
                result.constrain(selected);
                assertThat(selectedLongValues(result, 0)).containsExactly(20, 40);
                assertThat(selectedLongValues(result, 1)).containsExactly(1, 1);
                assertThat(result.takeMask()).isSameAs(selected);
            }
        }
    }

    @Test
    void testFallsBackToFullAggregateResultWhenDensePositionCopyIsUnavailable()
    {
        AtomicInteger materializations = new AtomicInteger();
        CountAll aggregateWithoutPositionCopy = new CountAll()
        {
            @Override
            public Streams result(
                    int maxGroup,
                    Streams state,
                    Streams output,
                    Allocator allocator,
                    Allocator.Context allocationContext)
            {
                materializations.incrementAndGet();
                return super.result(maxGroup, state, output, allocator, allocationContext);
            }

            @Override
            public Streams copyResultPosition(
                    int group,
                    int maxGroup,
                    Streams state,
                    Streams output,
                    int outputPosition,
                    int size,
                    Allocator allocator,
                    Allocator.Context allocationContext)
            {
                return null;
            }

            @Override
            public Streams copyResultRange(
                    int groupStart,
                    int groupCount,
                    int maxGroup,
                    Streams state,
                    Streams output,
                    int outputStart,
                    int size,
                    Allocator allocator,
                    Allocator.Context allocationContext)
            {
                return null;
            }
        };
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(aggregateWithoutPositionCopy)),
                        resources.operatorResources(),
                        null,
                        2)) {
            allocator.beginExecution();
            try (Batch input = batch(10, 20, 10, 30, 40, 40, 40, 50)) {
                session.addInput(input);
            }

            try (Batch first = session.finish()) {
                first.constrain(Mask.sparse(new int[] {1}, 2));
                assertThat(selectedLongValues(first, 0)).containsExactly(20);
                assertThat(selectedLongValues(first, 1)).containsExactly(1);
            }
            try (Batch second = session.getOutput()) {
                second.constrain(Mask.sparse(new int[] {1}, 2));
                assertThat(selectedLongValues(second, 0)).containsExactly(40);
                assertThat(selectedLongValues(second, 1)).containsExactly(3);
            }
            try (Batch third = session.getOutput()) {
                assertThat(selectedLongValues(third, 0)).containsExactly(50);
                assertThat(selectedLongValues(third, 1)).containsExactly(1);
            }
            assertThat(materializations).hasValue(1);
        }
    }

    @Test
    void testRetainedBytesExcludeReleasedAllocatorPool()
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
            try (Batch input = batch(LongStream.range(0, 100_000).toArray())) {
                session.addInput(input);
            }
            long firstAggregationBytes = session.retainedBytes();
            assertThat(firstAggregationBytes).isPositive();
            session.flush();
            while (session.hasOutput()) {
                try (Batch ignored = session.getOutput()) {
                    // Closing every output permits the flushed aggregation to be released by the next input.
                }
            }

            try (Batch input = batch(1)) {
                session.addInput(input);
            }
            assertThat(session.retainedBytes()).isLessThan(firstAggregationBytes);

            // Output transfer can leave no incidental idle buffers behind. Seed the allocator pool explicitly so
            // this assertion tests the diagnostic contract rather than depending on a particular output path.
            Allocator.Context poolContext = new Allocator.Context("retained-bytes-pool");
            I64Vector pooled = allocator.allocate(poolContext, I64Vector.class, 1_024, I64Vector::new);
            allocator.release(poolContext, pooled);
            assertThat(allocator.residentBytes()).isGreaterThan(session.retainedBytes());
        }
    }

    @Test
    void testAdaptiveFlushBoundsAllocatorResidencyAcrossGenerations()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources, new TestingMemoryReservation());
                GroupedAggregationSession session = new GroupedAggregationSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        PhysicalAggregationProgram.independent(List.of(new CountAll())),
                        resources.operatorResources(),
                        control)) {
            allocator.beginExecution();
            long[] keys = LongStream.range(0, 20_000).toArray();
            long steadyStateResidentBytes = 0;
            for (int generation = 0; generation < 4; generation++) {
                try (Batch input = batch(keys)) {
                    session.addInput(input);
                }
                session.flush();
                while (session.hasOutput()) {
                    try (Batch ignored = session.getOutput()) {
                        selectedLongValues(ignored, 0);
                        selectedLongValues(ignored, 1);
                    }
                }
                if (generation == 1) {
                    steadyStateResidentBytes = allocator.residentBytes();
                }
                else if (generation > 1) {
                    assertThat(allocator.residentBytes())
                            .as(allocator.toString())
                            .isLessThanOrEqualTo(steadyStateResidentBytes);
                }
            }
        }
    }

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
    void testAdaptivePassthroughCanRetainDenseEncodedInput()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl();
        control.enabled = false;
        AtomicInteger inputCloses = new AtomicInteger();
        DictionaryVector keys = new DictionaryVector(
                new int[] {0, 1, 0, 1},
                new I64Vector(new long[] {11, 22}));
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
            Batch input = new Batch(
                    Mask.all(4),
                    _ -> {},
                    mask -> mask,
                    _ -> {},
                    inputCloses::incrementAndGet,
                    Output.of(Streams.ofValues(keys)));

            assertThat(session.addInputWithOwnership(input, 40))
                    .isEqualTo(BatchAggregationSession.InputOwnership.SESSION);
            assertThat(inputCloses).hasValue(0);
            try (Batch result = session.getOutput()) {
                assertThat(result.output(0).borrow(Stream.VALUES)).isSameAs(keys);
                assertThat(selectedLongValues(result, 1)).containsExactly(1, 1, 1, 1);
            }
            assertThat(inputCloses).hasValue(1);
            assertThatIllegalStateException().isThrownBy(input::borrowMask);
        }
    }

    @Test
    void testCardinalityObservationCanBypassBeforeGroupingStateIsBuilt()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl();
        control.sampleSize = 4;
        control.maximumDistinctKeysToAggregate = 3;
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
            try (Batch input = batch(10, 20, 30, 40);
                    Batch result = addAndGetOutput(session, input, 40)) {
                assertThat(selectedLongValues(result, 0)).containsExactly(10, 20, 30, 40);
                assertThat(selectedLongValues(result, 1)).containsExactly(1, 1, 1, 1);
            }
            assertThat(control.observedInput.sampledRows()).isEqualTo(4);
            assertThat(control.observedInput.distinctKeyHashes()).isEqualTo(4);
            assertThat(control.passthroughFlushes).isEqualTo(1);
            assertThat(session.retainedBytes()).isZero();
        }
    }

    @Test
    void testCardinalityObservationContinuesWhileAdmissionIsUndecided()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl();
        control.sampleSize = 2;
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
            try (Batch first = batch(10, 20);
                    Batch second = batch(30, 40)) {
                session.addInput(first, 20);
                session.addInput(second, 20);
            }
            assertThat(control.cardinalityObservations).isEqualTo(2);
            try (Batch result = session.finish()) {
                assertThat(selectedLongValues(result, 0)).containsExactly(10, 20, 30, 40);
            }
        }
    }

    @Test
    void testCardinalityObservationCanReenableAggregationAfterPassthrough()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl();
        control.enabled = false;
        control.sampleSize = 2;
        control.enableAfterCardinalityObservations = 2;
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
            try (Batch first = batch(10, 20);
                    Batch firstOutput = addAndGetOutput(session, first, 20)) {
                assertThat(selectedLongValues(firstOutput, 0)).containsExactly(10, 20);
            }
            assertThat(control.enabled).isFalse();

            try (Batch second = batch(30, 30)) {
                session.addInput(second, 20);
            }
            assertThat(control.enabled).isTrue();
            assertThat(control.cardinalityObservations).isEqualTo(2);
            try (Batch result = session.finish()) {
                assertThat(selectedLongValues(result, 0)).containsExactly(30);
                assertThat(selectedLongValues(result, 1)).containsExactly(2);
            }
        }
    }

    @Test
    void testCardinalityObservationHonorsMasksAndEncodedGroupingKeys()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            Batch batch = new Batch(
                    Mask.sparse(new int[] {4, 1, 3, 0}, 5),
                    Output.of(Streams.ofValues(new DictionaryVector(
                            new int[] {0, 1, 2, 1, 0},
                            new I64Vector(new long[] {11, 22, 33})))),
                    Output.of(Streams.ofValues(new I64Vector(new long[] {7, 8, 9, 8, 7}))));
            try (batch) {
                assertThat(GroupingCardinalitySampler.sample(
                        batch,
                        List.of(0, 1),
                        4,
                        allocator.primitiveArrays()))
                        .satisfies(statistics -> {
                            assertThat(statistics.sampledRows()).isEqualTo(4);
                            assertThat(statistics.distinctKeyHashes()).isEqualTo(2);
                            assertThat(statistics.sampledKeyBytes()).isEqualTo(68);
                            assertThat(statistics.variableWidthGroupingKeys()).isFalse();
                            assertThat(statistics.aggregationReadsInput()).isTrue();
                        });
            }
        }
    }

    @Test
    void testStreamsAdaptiveFlushInBoundedBatches()
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
                        control,
                        2)) {
            allocator.beginExecution();
            try (Batch input = batch(10, 20, 30, 40, 50)) {
                session.addInput(input, 50);
            }
            session.flush();

            try (Batch first = session.getOutput()) {
                assertThat(selectedLongValues(first, 0)).containsExactly(10, 20);
            }
            assertThat(session.hasOutput()).isTrue();
            try (Batch input = batch(60)) {
                assertThatIllegalStateException()
                        .isThrownBy(() -> session.addInput(input, 10))
                        .withMessage("grouped aggregation session has pending output");
            }
            try (Batch second = session.getOutput()) {
                assertThat(selectedLongValues(second, 0)).containsExactly(30, 40);
            }
            try (Batch third = session.getOutput()) {
                assertThat(selectedLongValues(third, 0)).containsExactly(50);
            }
            assertThat(session.hasOutput()).isFalse();
            assertThat(control.aggregatedFlushes).isEqualTo(1);
            assertThat(control.inputBytes).isEqualTo(50);
            assertThat(control.inputRows).isEqualTo(5);
            assertThat(control.outputRows).isEqualTo(5);

            try (Batch input = batch(60)) {
                session.addInput(input, 10);
            }
            try (Batch result = session.finish()) {
                assertThat(selectedLongValues(result, 0)).containsExactly(60);
            }
            assertThat(control.aggregatedFlushes).isEqualTo(2);
            assertThat(control.inputBytes).isEqualTo(60);
            assertThat(control.inputRows).isEqualTo(6);
            assertThat(control.outputRows).isEqualTo(6);
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
    void testUsesFunctionOwnedDirectInitialAggregationRows()
    {
        AtomicInteger directCalls = new AtomicInteger();
        CountAll directCount = new CountAll()
        {
            @Override
            public boolean supportsInitialInput()
            {
                return true;
            }

            @Override
            public Streams initialInput(
                    int output,
                    Mask mask,
                    StreamAccessor streams,
                    Allocator allocator,
                    Allocator.Context allocationContext)
            {
                directCalls.incrementAndGet();
                long[] values = new long[mask.count()];
                Arrays.fill(values, 1);
                return Streams.ofValues(allocator.adopt(allocationContext, new I64Vector(values)));
            }
        };
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            MutableAggregationPhaseMetrics phaseMetrics = new MutableAggregationPhaseMetrics();
            InitialAggregationBatchBuilder builder = new InitialAggregationBatchBuilder(
                    allocator,
                    Schema.unspecified(1),
                    List.of(0),
                    PhysicalAggregationProgram.independent(List.of(directCount)),
                    resources.operatorResources(),
                    phaseMetrics);

            try (Batch input = new Batch(
                    Mask.sparse(new int[] {3, 1}, 4),
                    Output.of(Streams.ofValues(new DictionaryVector(
                            new int[] {0, 1, 2, 3},
                            new I64Vector(new long[] {10, 20, 30, 40})))));
                    Batch result = builder.build(input)) {
                assertThat(selectedLongValues(result, 0)).containsExactly(40, 20);
                assertThat(selectedLongValues(result, 1)).containsExactly(1, 1);
                assertThat(directCalls).hasValue(1);
                AggregationPhaseMetrics metrics = phaseMetrics.snapshot();
                assertThat(metrics.initialKeyNanos()).isPositive();
                assertThat(metrics.initialEncodedKeyNanos()).isPositive();
                assertThat(metrics.initialFlatKeyNanos()).isZero();
                assertThat(metrics.initialAggregationNanos()).isPositive();
            }
        }
    }

    @Test
    void testRetainsSparseInputWhenInitialAggregationPreservesPositions()
    {
        CountAll positionPreservingCount = new CountAll()
        {
            @Override
            public boolean supportsInitialInput()
            {
                return true;
            }

            @Override
            public Streams initialInput(
                    int output,
                    Mask mask,
                    StreamAccessor streams,
                    Allocator allocator,
                    Allocator.Context allocationContext)
            {
                return Streams.ofValues(allocator.adopt(allocationContext, new I64Vector(new long[mask.count()])));
            }

            @Override
            public boolean supportsPositionPreservingInitialInput()
            {
                return true;
            }

            @Override
            public Streams positionPreservingInitialInput(
                    int output,
                    Mask mask,
                    StreamAccessor streams,
                    Allocator allocator,
                    Allocator.Context allocationContext)
            {
                long[] values = new long[mask.maxPosition() + 1];
                for (int position : mask) {
                    values[position] = 1;
                }
                return Streams.ofValues(allocator.adopt(allocationContext, new I64Vector(values)));
            }
        };
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            InitialAggregationBatchBuilder builder = new InitialAggregationBatchBuilder(
                    allocator,
                    Schema.unspecified(1),
                    List.of(0),
                    PhysicalAggregationProgram.independent(List.of(positionPreservingCount)),
                    resources.operatorResources());

            DictionaryVector keys = new DictionaryVector(
                    new int[] {0, 1, 2, 3},
                    new I64Vector(new long[] {10, 20, 30, 40}));
            Batch input = new Batch(
                    Mask.sparse(new int[] {1, 3}, 4),
                    Output.of(Streams.ofValues(keys)));
            try (Batch result = builder.buildRetaining(input)) {
                assertThat(result).isNotNull();
                assertThat(result.borrowMask().all()).isFalse();
                assertThat(result.output(0).borrow(Stream.VALUES)).isSameAs(keys);
                assertThat(OperatorVectorSupport.longValue(result.output(0).borrow(Stream.VALUES), 1)).isEqualTo(20);
                assertThat(OperatorVectorSupport.longValue(result.output(0).borrow(Stream.VALUES), 3)).isEqualTo(40);
                assertThat(selectedLongValues(result, 1)).containsExactly(1, 1);
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
        long[] values = ((I64Vector) batch.output(output).borrow(Stream.VALUES)).values();
        long[] selected = new long[batch.borrowMask().count()];
        int index = 0;
        for (int position : batch.borrowMask()) {
            selected[index++] = values[position];
        }
        return selected;
    }

    private static List<String> selectedBinaryValues(Batch batch, int output)
    {
        Vector values = batch.output(output).borrow(Stream.VALUES);
        List<String> selected = new ArrayList<>();
        for (int position : batch.borrowMask()) {
            selected.add(new String(OperatorKeySemantics.copyBinaryBytes(values, position), UTF_8));
        }
        return selected;
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
        private int sampleSize;
        private int maximumDistinctKeysToAggregate = Integer.MAX_VALUE;
        private int enableAfterCardinalityObservations = Integer.MAX_VALUE;
        private int cardinalityObservations;
        private PartialAggregationInputStatistics observedInput;

        @Override
        public boolean aggregationEnabled()
        {
            return enabled;
        }

        @Override
        public int inputCardinalitySampleSize()
        {
            return sampleSize;
        }

        @Override
        public boolean aggregationEnabled(PartialAggregationInputStatistics inputStatistics)
        {
            cardinalityObservations++;
            observedInput = inputStatistics;
            if (cardinalityObservations >= enableAfterCardinalityObservations) {
                enabled = true;
            }
            return enabled && inputStatistics.distinctKeyHashes() <= maximumDistinctKeysToAggregate;
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

    private static final class TestingMemoryReservation
            implements MemoryReservation
    {
        private long reservedBytes;

        @Override
        public CompletionStage<Void> reserve(long bytes)
        {
            reservedBytes += bytes;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void release(long bytes)
        {
            reservedBytes -= bytes;
        }

        @Override
        public long reservedBytes()
        {
            return reservedBytes;
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

            @Override
            public boolean supportsVector(Vector vector)
            {
                return vector instanceof BinaryVector ||
                        (vector instanceof DictionaryVector dictionary && dictionary.values() instanceof BinaryVector);
            }
        };
    }
}
