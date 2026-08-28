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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class TestKeyOnlyGroupingSession
{
    @Test
    void testStreamsDistinctKeysAcrossInputBatchesAndRetainsNull()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();

            try (Batch input = batch(new long[] {1, 2, 2, 0}, new boolean[] {false, false, false, true})) {
                session.addInput(input);
            }
            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(1L, 2L, 0L);
                assertThat(selectedNulls(output)).containsExactly(false, false, true);
            }

            try (Batch input = batch(new long[] {2, 3, 0}, new boolean[] {false, false, true})) {
                session.addInput(input);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(3L);
                assertThat(selectedNulls(output)).containsExactly(false);
            }

            try (Batch terminal = session.finish()) {
                assertThat(terminal.borrowMask().none()).isTrue();
                assertThat(terminal.output(0).borrow(Stream.VALUES).length()).isZero();
            }
        }
    }

    @Test
    void testProducesNoPendingOutputForDuplicateOnlyBatch()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();
            try (Batch input = batch(new long[] {7}, new boolean[] {false})) {
                session.addInput(input);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(7L);
            }
            try (Batch input = batch(new long[] {7, 7}, new boolean[] {false, false})) {
                session.addInput(input);
            }
            assertThat(session.hasOutput()).isFalse();
        }
    }

    @Test
    void testRetainsAllDistinctInputWhenOwnershipIsOffered()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();
            AtomicBoolean closed = new AtomicBoolean();
            Batch input = new Batch(
                    Mask.all(3),
                    _ -> {},
                    mask -> mask,
                    _ -> {},
                    () -> closed.set(true),
                    new Output[] {Output.of(Streams.ofValuesAndNulls(
                            new I64Vector(new long[] {1, 2, 3}),
                            new BooleanVector(new boolean[3])))});

            assertThat(session.addInputWithOwnership(input, 0)).isEqualTo(BatchAggregationSession.InputOwnership.SESSION);
            assertThat(closed).isFalse();
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(1L, 2L, 3L);
                assertThat(closed).isFalse();
            }
            assertThat(closed).isTrue();
        }
    }

    @Test
    void testRetainsSparseDistinctInputWhenOwnershipIsOffered()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();
            AtomicBoolean closed = new AtomicBoolean();
            I64Vector values = new I64Vector(new long[] {1, 1, 2, 3});
            Batch input = new Batch(
                    Mask.all(4),
                    _ -> {},
                    mask -> mask,
                    _ -> {},
                    () -> closed.set(true),
                    new Output[] {Output.of(Streams.ofValuesAndNulls(values, new BooleanVector(new boolean[4])))});

            assertThat(session.addInputWithOwnership(input, 0)).isEqualTo(BatchAggregationSession.InputOwnership.SESSION);
            assertThat(closed).isFalse();
            try (Batch output = session.getOutput()) {
                assertThat(output.borrowMask().all()).isFalse();
                assertThat(output.output(0).borrow(Stream.VALUES)).isSameAs(values);
                assertThat(selectedValues(output)).containsExactly(1L, 2L, 3L);
                assertThat(closed).isFalse();
            }
            assertThat(closed).isTrue();
        }
    }

    @Test
    void testCompactsPhysicallySparseDistinctInputWhenOwnershipIsOffered()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();
            AtomicBoolean closed = new AtomicBoolean();
            I64Vector values = new I64Vector(new long[] {1, 99, 99, 99, 2, 99, 99, 3});
            Batch input = new Batch(
                    Mask.sparse(new int[] {0, 4, 7}, values.length()),
                    _ -> {},
                    mask -> mask,
                    _ -> {},
                    () -> closed.set(true),
                    new Output[] {Output.of(Streams.ofValuesAndNulls(values, new BooleanVector(new boolean[values.length()])))});

            assertThat(session.addInputWithOwnership(input, 0)).isEqualTo(BatchAggregationSession.InputOwnership.CALLER);
            assertThat(closed).isFalse();
            try (Batch output = session.getOutput()) {
                assertThat(output.borrowMask().all()).isTrue();
                assertThat(output.output(0).borrow(Stream.VALUES)).isNotSameAs(values);
                assertThat(selectedValues(output)).containsExactly(1L, 2L, 3L);
            }
            input.close();
            assertThat(closed).isTrue();
        }
    }

    @Test
    void testDictionaryDomainGroupingPreservesNullAndValueRepresentatives()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources())) {
            allocator.beginExecution();
            int[] ids = new int[64];
            boolean[] nulls = new boolean[64];
            for (int position = 0; position < ids.length; position++) {
                ids[position] = position & 1;
            }
            // The same dictionary entry is a value in one row and SQL null in another. Both are groups.
            nulls[5] = true;
            try (Batch input = new Batch(
                    Mask.all(ids.length),
                    Output.of(Streams.ofValuesAndNulls(
                            DictionaryVector.wrap(ids, new I64Vector(new long[] {11, 22})),
                            new BooleanVector(nulls))))) {
                session.addInput(input);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(11L, 22L, 22L);
                assertThat(selectedNulls(output)).containsExactly(false, false, true);
            }
        }
    }

    @Test
    void testAdaptiveFlushStartsANewDistinctCohort()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl(true);
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources(),
                        control)) {
            allocator.beginExecution();
            try (Batch input = batch(new long[] {1, 1, 2}, new boolean[3])) {
                session.addInput(input, 30);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(1L, 2L);
            }

            session.flush();
            assertThat(control.aggregatedFlushes).isEqualTo(1);
            assertThat(control.aggregatedInputBytes).isEqualTo(30);
            assertThat(control.aggregatedInputRows).isEqualTo(3);
            assertThat(control.aggregatedOutputRows).isEqualTo(2);

            try (Batch input = batch(new long[] {1, 3}, new boolean[2])) {
                session.addInput(input, 20);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(1L, 3L);
            }
            try (Batch terminal = session.finish()) {
                assertThat(terminal.borrowMask().none()).isTrue();
            }
            assertThat(control.aggregatedFlushes).isEqualTo(2);
            assertThat(control.aggregatedInputRows).isEqualTo(5);
            assertThat(control.aggregatedOutputRows).isEqualTo(4);
        }
    }

    @Test
    void testAdaptiveBypassPreservesEveryInputPosition()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl(false);
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources(),
                        control)) {
            allocator.beginExecution();
            try (Batch input = batch(new long[] {7, 7, 8}, new boolean[3])) {
                session.addInput(input, 30);
            }
            try (Batch output = session.getOutput()) {
                assertThat(selectedValues(output)).containsExactly(7L, 7L, 8L);
            }
            assertThat(control.passthroughFlushes).isEqualTo(1);
            assertThat(control.passthroughInputRows).isEqualTo(3);
        }
    }

    @Test
    void testAdaptiveBypassCompactsPhysicallySparseInput()
    {
        TestingPartialAggregationControl control = new TestingPartialAggregationControl(false);
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                KeyOnlyGroupingSession session = new KeyOnlyGroupingSession(
                        allocator,
                        Schema.unspecified(1),
                        List.of(0),
                        List.of(0),
                        resources.operatorResources(),
                        control)) {
            allocator.beginExecution();
            I64Vector values = new I64Vector(new long[] {7, 99, 99, 99, 7, 99, 99, 8});
            try (Batch input = new Batch(
                    Mask.sparse(new int[] {0, 4, 7}, values.length()),
                    Output.of(Streams.ofValuesAndNulls(values, new BooleanVector(new boolean[values.length()]))))) {
                assertThat(session.addInputWithOwnership(input, 30)).isEqualTo(BatchAggregationSession.InputOwnership.CALLER);
                try (Batch output = session.getOutput()) {
                    assertThat(output.borrowMask().all()).isTrue();
                    assertThat(output.output(0).borrow(Stream.VALUES)).isNotSameAs(values);
                    assertThat(selectedValues(output)).containsExactly(7L, 7L, 8L);
                }
            }
        }
    }

    private static Batch batch(long[] values, boolean[] nulls)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(Streams.ofValuesAndNulls(new I64Vector(values), new BooleanVector(nulls))));
    }

    private static List<Long> selectedValues(Batch batch)
    {
        long[] values = ((I64Vector) batch.output(0).borrow(Stream.VALUES)).values();
        List<Long> selected = new ArrayList<>();
        for (int position : batch.borrowMask()) {
            selected.add(values[position]);
        }
        return selected;
    }

    private static List<Boolean> selectedNulls(Batch batch)
    {
        boolean[] nulls = ((BooleanVector) batch.output(0).borrow(Stream.NULLS)).values();
        List<Boolean> selected = new ArrayList<>();
        for (int position : batch.borrowMask()) {
            selected.add(nulls[position]);
        }
        return selected;
    }

    private static final class TestingPartialAggregationControl
            implements PartialAggregationControl
    {
        private final boolean aggregationEnabled;
        private long aggregatedFlushes;
        private long aggregatedInputBytes;
        private long aggregatedInputRows;
        private long aggregatedOutputRows;
        private long passthroughFlushes;
        private long passthroughInputRows;

        private TestingPartialAggregationControl(boolean aggregationEnabled)
        {
            this.aggregationEnabled = aggregationEnabled;
        }

        @Override
        public boolean aggregationEnabled()
        {
            return aggregationEnabled;
        }

        @Override
        public void onAggregatedFlush(long inputBytes, long inputRows, long outputRows)
        {
            aggregatedFlushes++;
            aggregatedInputBytes += inputBytes;
            aggregatedInputRows += inputRows;
            aggregatedOutputRows += outputRows;
        }

        @Override
        public void onPassthroughFlush(long inputBytes, long inputRows)
        {
            passthroughFlushes++;
            passthroughInputRows += inputRows;
        }
    }
}
