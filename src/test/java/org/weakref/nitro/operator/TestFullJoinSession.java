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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.data.Row.row;

class TestFullJoinSession
{
    @Test
    void testBuffersScheduledOuterInputAndEmitsUnmatchedRows()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            ConstantTableOperator outer = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L)));
            FullJoinSession session = new FullJoinSession(
                    resources.operatorResources(),
                    allocator,
                    outer.outputSchema(),
                    new int[] {0},
                    new ConstantTableOperator(allocator, 1, List.of(row(2L), row(3L))),
                    new int[] {0});
            session.addInput(outer.next());
            outer.close();
            session.finish();

            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                assertThat(output.borrowMask().selectedCount()).isEqualTo(3);
                assertThat(((I64Vector) output.output(0).borrow(Stream.VALUES)).values()).containsExactly(1, 2, 0);
                assertThat(((BooleanVector) output.output(0).borrow(Stream.NULLS)).values()).containsExactly(false, false, true);
                assertThat(((I64Vector) output.output(1).borrow(Stream.VALUES)).values()).containsExactly(0, 2, 3);
                assertThat(((BooleanVector) output.output(1).borrow(Stream.NULLS)).values()).containsExactly(true, false, false);
            }
            assertThat(session.isFinished()).isTrue();
            session.close();
        }
    }

    @Test
    void testUsesExplicitOrderedInputContract()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            ConstantTableOperator outer = new ConstantTableOperator(allocator, 1, List.of(row(1L), row(2L), row(2L)));
            FullJoinSession session = new FullJoinSession(
                    resources.operatorResources(),
                    allocator,
                    outer.outputSchema(),
                    new int[] {0},
                    new ConstantTableOperator(allocator, 1, List.of(row(2L), row(2L), row(3L))),
                    new int[] {0},
                    true);
            session.addInput(outer.next());
            outer.close();
            session.finish();

            assertThat(session.hasOutput()).isTrue();
            try (Batch output = session.getOutput()) {
                assertThat(output.borrowMask().selectedCount()).isEqualTo(6);
                assertThat(((I64Vector) output.output(0).borrow(Stream.VALUES)).values()).containsExactly(1, 2, 2, 2, 2, 0);
                assertThat(((BooleanVector) output.output(0).borrow(Stream.NULLS)).values()).containsExactly(false, false, false, false, false, true);
                assertThat(((I64Vector) output.output(1).borrow(Stream.VALUES)).values()).containsExactly(0, 2, 2, 2, 2, 3);
                assertThat(((BooleanVector) output.output(1).borrow(Stream.NULLS)).values()).containsExactly(true, false, false, false, false, false);
            }
            assertThat(session.isFinished()).isTrue();
            session.close();
        }
    }

    @Test
    void testSelectsOutputsBeforeComposedPipeline()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            allocator.beginExecution();
            ConstantTableOperator outer = new ConstantTableOperator(allocator, 2, List.of(row(1L, 10L), row(2L, 20L)));
            try (FullJoinSession session = new FullJoinSession(
                    resources.operatorResources(),
                    allocator,
                    outer.outputSchema(),
                    new int[] {0},
                    new ConstantTableOperator(allocator, 2, List.of(row(2L, 200L), row(3L, 300L))),
                    new int[] {0})
                    .withOutputs(3, 1)
                    .withOutputPipeline(source -> new ProjectOperator(
                            allocator,
                            new EvaluationPlan(List.of(), List.of(
                                    new Reference(new Input(1), Stream.VALUES),
                                    new Reference(new Input(0), Stream.VALUES))),
                            new PrimitiveRegistry(),
                            source))) {
                session.addInput(outer.next());
                outer.close();
                session.finish();

                assertThat(session.hasOutput()).isTrue();
                try (Batch output = session.getOutput()) {
                    assertThat(((I64Vector) output.output(0).borrow(Stream.VALUES)).values()).containsExactly(10, 20, 0);
                    assertThat(((I64Vector) output.output(1).borrow(Stream.VALUES)).values()).containsExactly(0, 200, 300);
                }
                assertThat(session.isFinished()).isTrue();
            }
        }
    }
}
