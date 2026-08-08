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
package org.weakref.nitro.clickbench;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.MultiAggregationImplementation;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestExactAffineBigintSums
{
    @Test
    void testDenseIntegerInputsAndOverflow()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Allocator.Context context = new Allocator.Context("exact-affine-integer-sums");
            MultiAggregationImplementation implementation = new ExactAffineBigintSums(2);
            Object state = implementation.allocate(new AggregationExecution(allocator, context, new Schema(List.of())), 1);
            implementation.initialize(state, 0, 1);
            implementation.addRawInput(state, 0, Mask.all(3), input(1, 2, 3));

            assertThat(((I64Vector) implementation.result(0, 0, state, null, allocator, context).values()).values())
                    .containsExactly(6);
            assertThat(((I64Vector) implementation.result(1, 0, state, null, allocator, context).values()).values())
                    .containsExactly(9);

            Object overflow = implementation.allocate(new AggregationExecution(allocator, context, new Schema(List.of())), 1);
            implementation.initialize(overflow, 0, 1);
            assertThatThrownBy(() -> implementation.addRawInput(overflow, 0, Mask.all(1), input(Integer.MAX_VALUE)))
                    .isInstanceOf(ArithmeticException.class);
            allocator.release(context);
        }
    }

    private static AggregationInput input(int... values)
    {
        Streams streams = Streams.of(new I32Vector(values), new BooleanVector(new boolean[values.length]), null);
        return (_, stream) -> streams.getOrNull(stream);
    }
}
