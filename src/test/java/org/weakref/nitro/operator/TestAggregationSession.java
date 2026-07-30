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
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.DistinctPhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class TestAggregationSession
{
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

    private static Batch values(long... values)
    {
        return new Batch(
                Mask.all(values.length),
                Output.of(org.weakref.nitro.data.Streams.ofValues(new I64Vector(values))));
    }
}
