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
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

class TestAppendLongSequenceOperator
{
    @Test
    void testAppendsDisjointInjectedRanges()
    {
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            AtomicLong next = new AtomicLong(100);
            AppendLongSequenceOperator.RangeAllocator ranges = length -> next.getAndAdd(length);

            assertThat(operator(new AppendLongSequenceOperator(
                    allocator,
                    new ConstantTableOperator(allocator, 1, List.of(row(7L), row(8L))),
                    org.weakref.nitro.core.type.Schema.unspecified(1).field(0),
                    ranges)))
                    .matchesExactly(List.of(row(7L, 100L), row(8L, 101L)));

            assertThat(operator(new AppendLongSequenceOperator(
                    allocator,
                    new ConstantTableOperator(allocator, 1, List.of(row(9L))),
                    org.weakref.nitro.core.type.Schema.unspecified(1).field(0),
                    ranges)))
                    .matchesExactly(List.of(row(9L, 102L)));
        }
    }
}
