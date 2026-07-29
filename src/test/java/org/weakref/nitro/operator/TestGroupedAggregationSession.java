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
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestGroupedAggregationSession
{
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

    private static Batch batch(long... values)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(Streams.ofValues(new I64Vector(values))));
    }
}
