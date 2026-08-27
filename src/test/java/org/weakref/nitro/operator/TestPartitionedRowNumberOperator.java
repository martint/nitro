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
import org.weakref.nitro.execution.EngineResources;

import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

class TestPartitionedRowNumberOperator
{
    @Test
    void testNumbersRowsAcrossBatchesForCompositePartitions()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator source = new MultiStageOperator(3, List.of(
                        List.of(
                                row(1L, "a", "first"),
                                row(2L, "a", "second"),
                                row(1L, "b", "third")),
                        List.of(
                                row(1L, "a", "fourth"),
                                row(1L, "b", "fifth"))),
                        rows -> new ConstantTableOperator(allocator, 3, rows));
                Operator rowNumber = new PartitionedRowNumberOperator(
                        allocator,
                        new int[] {0, 1},
                        source,
                        org.weakref.nitro.core.type.Schema.unspecified(1).field(0),
                        OptionalLong.empty(),
                        resources.operatorResources())) {
            assertThat(operator(rowNumber)).matchesExactly(List.of(
                    row(1L, "a", "first", 1L),
                    row(2L, "a", "second", 1L),
                    row(1L, "b", "third", 1L),
                    row(1L, "a", "fourth", 2L),
                    row(1L, "b", "fifth", 2L)));
        }
    }

    @Test
    void testAppliesPerPartitionLimit()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator rowNumber = new PartitionedRowNumberOperator(
                        allocator,
                        new int[] {0},
                        new ConstantTableOperator(allocator, 2, List.of(
                                row(1L, "first"),
                                row(1L, "discarded"),
                                row(2L, "second"),
                                row(2L, "also-discarded"))),
                        org.weakref.nitro.core.type.Schema.unspecified(1).field(0),
                        OptionalLong.of(1),
                        resources.operatorResources())) {
            assertThat(operator(rowNumber)).matchesExactly(List.of(
                    row(1L, "first", 1L),
                    row(2L, "second", 1L)));
        }
    }
}
