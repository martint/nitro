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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestNestedLoopJoinSession
{
    @Test
    void testPreservesBufferedInnerAcrossScheduledOuterBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                NestedLoopJoinSession session = new NestedLoopJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        table(10, 20))) {
            allocator.beginExecution();

            List<Long> outerValues = new ArrayList<>();
            List<Long> innerValues = new ArrayList<>();
            session.addInput(batch(1, 2));
            drain(session, outerValues, innerValues);
            session.addInput(batch(3));
            drain(session, outerValues, innerValues);
            session.finish();
            drain(session, outerValues, innerValues);

            assertThat(outerValues).containsExactly(1L, 2L, 1L, 2L, 3L, 3L);
            assertThat(innerValues).containsExactly(10L, 10L, 20L, 20L, 10L, 20L);
            assertThat(session.isFinished()).isTrue();
            try (Batch late = batch(4)) {
                assertThatIllegalStateException()
                        .isThrownBy(() -> session.addInput(late))
                        .withMessage("nested loop join session is finishing");
            }
        }
    }

    @Test
    void testAcceptsEmptyScheduledBatchBeforeNonEmptyBatch()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                NestedLoopJoinSession session = new NestedLoopJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        table(10))) {
            allocator.beginExecution();

            session.addInput(batch());
            assertThat(session.hasOutput()).isFalse();
            session.addInput(batch(2));
            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                assertThat(output.borrowMask().count()).isOne();
            }
        }
    }

    @Test
    void testComposesOutputPipelineOverNativeCrossJoinBatches()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                NestedLoopJoinSession session = new NestedLoopJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(1),
                        table(10, 20))
                        .withOutputPipeline(source -> new LimitOperator(allocator, 2, source))) {
            allocator.beginExecution();

            List<Long> outerValues = new ArrayList<>();
            List<Long> innerValues = new ArrayList<>();
            session.addInput(batch(1, 2));
            drain(session, outerValues, innerValues);
            session.finish();
            drain(session, outerValues, innerValues);

            assertThat(outerValues).containsExactly(1L, 2L);
            assertThat(innerValues).containsExactly(10L, 10L);
            assertThat(session.isFinished()).isTrue();
        }
    }

    @Test
    void testReordersOutputsBeforeComposingPipeline()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                NestedLoopJoinSession session = new NestedLoopJoinSession(
                        resources.operatorResources(),
                        allocator,
                        Schema.unspecified(2),
                        table(10, 20))
                        .withOutputs(1, 0, 2)
                        .withOutputPipeline(source -> new LimitOperator(allocator, 2, source))) {
            allocator.beginExecution();

            session.addInput(multiBatch(new long[] {1, 2}, new long[] {101, 102}));
            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                Mask mask = output.borrowMask();
                assertThat(values(output, 0, mask)).containsExactly(101L, 102L);
                assertThat(values(output, 1, mask)).containsExactly(1L, 2L);
                assertThat(values(output, 2, mask)).containsExactly(10L, 10L);
            }
        }
    }

    private static void drain(NestedLoopJoinSession session, List<Long> outerValues, List<Long> innerValues)
    {
        while (session.hasOutput()) {
            try (Batch output = session.getOutput()) {
                Mask mask = output.borrowMask();
                VectorAccess.LongValues outer = VectorAccess.longValues(output.output(0).borrow(Stream.VALUES));
                VectorAccess.LongValues inner = VectorAccess.longValues(output.output(1).borrow(Stream.VALUES));
                for (int position : mask) {
                    outerValues.add(outer.value(position));
                    innerValues.add(inner.value(position));
                }
            }
        }
    }

    private static Batch batch(long... values)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(values))));
    }

    private static Batch multiBatch(long[]... columns)
    {
        Output[] outputs = new Output[columns.length];
        for (int channel = 0; channel < columns.length; channel++) {
            outputs[channel] = Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(columns[channel])));
        }
        return new Batch(Mask.all(columns[0].length), outputs);
    }

    private static List<Long> values(Batch batch, int channel, Mask mask)
    {
        VectorAccess.LongValues values = VectorAccess.longValues(batch.output(channel).borrow(Stream.VALUES));
        List<Long> result = new ArrayList<>();
        for (int position : mask) {
            result.add(values.value(position));
        }
        return result;
    }

    private static Operator table(long... values)
    {
        return new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        values.length,
                        new org.weakref.nitro.data.Vector[] {new I64Vector(values)},
                        Mask.all(values.length))));
    }
}
