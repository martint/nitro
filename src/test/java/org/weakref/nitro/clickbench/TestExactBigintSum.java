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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.AggregationOperator;
import org.weakref.nitro.operator.ConstantTableOperator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.data.Row.row;

class TestExactBigintSum
{
    @Test
    void sumsNonNullInputs()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (var operator = new AggregationOperator(
                allocator,
                List.of(new ExactBigintSum(0)),
                new ConstantTableOperator(allocator, 1, List.of(row(1L), row((Object) null), row(2L))));
                var batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(3L);
        }
    }

    @Test
    void rejectsIntermediateOverflow()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (var operator = new AggregationOperator(
                allocator,
                List.of(new ExactBigintSum(0)),
                new ConstantTableOperator(allocator, 1, List.of(row(Long.MAX_VALUE), row(1L))));
                var batch = operator.next()) {
            assertThatThrownBy(() -> batch.output(0).borrow(Stream.VALUES))
                    .isInstanceOf(ArithmeticException.class);
        }
    }
}
