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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;

class TestProjectConstraintReuse
{
    @Test
    void testBatchConstraintRecyclesUnexposedIntermediateStorage()
    {
        assertBatchConstraintRecyclesUnexposedIntermediateStorage(false);
    }

    @Test
    void testForwardedLimitConstraintRecyclesUnexposedIntermediateStorage()
    {
        assertBatchConstraintRecyclesUnexposedIntermediateStorage(true);
    }

    private void assertBatchConstraintRecyclesUnexposedIntermediateStorage(boolean throughLimit)
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            PrimitiveRegistry registry = new PrimitiveRegistry();
            registry.register("increment", (inputs, mask, requested, _, context) -> {
                if (!requested.contains(Stream.VALUES)) {
                    return Streams.empty();
                }
                I64Vector result = context.allocator().allocate(
                        context.allocationContext("constraint-test"), I64Vector.class, mask.size(), I64Vector::new);
                VectorAccess.LongValues input = VectorAccess.longValues(inputs.getFirst().values());
                for (int position : mask) {
                    result.values()[position] = input.value(position) + 1;
                }
                return Streams.ofValues(result);
            });
            Variable intermediate = new Variable(0);
            Variable result = new Variable(1);
            EvaluationPlan plan = new EvaluationPlan(
                    List.of(
                            new Assignment(intermediate, new Call("increment", List.of(new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                            new Assignment(result, new Call("increment", List.of(new Reference(intermediate, Stream.VALUES))), AllMask.ALL)),
                    List.of(new Reference(result, Stream.VALUES)));
            Operator project = new ProjectOperator(allocator, plan, registry,
                    new ConstantTableOperator(allocator, 1, LongStream.range(0, 512).mapToObj(value -> row(value)).toList()));
            try (Operator operator = throughLimit ? new LimitOperator(allocator, 512, project) : project;
                    Batch batch = operator.next()) {
                long warmedAllocation = 0;
                for (int iteration = 0; iteration < 16; iteration++) {
                    Mask mask = iteration % 2 == 0 ? Mask.all(512) : Mask.sparse(new int[] {0, 255, 511}, 512);
                    batch.constrain(mask);
                    I64Vector output = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    for (int position : mask) {
                        assertThat(output.values()[position]).isEqualTo(position + 2);
                    }
                    long allocated = allocator.allocatedBytesByContext().get("constraint-test");
                    if (iteration == 3) {
                        warmedAllocation = allocated;
                    }
                    if (iteration > 3) {
                        assertThat(allocated).isEqualTo(warmedAllocation);
                    }
                }
                Allocator.Context consumer = new Allocator.Context("constraint-consumer");
                I64Vector taken = allocator.adopt(consumer, (I64Vector) batch.output(0).take(Stream.VALUES));
                assertThatThrownBy(() -> batch.constrain(Mask.all(512)))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("output stream was taken");
                assertThat(taken.values()[255]).isEqualTo(257);
                allocator.release(consumer, taken);
            }
        }
    }
}
