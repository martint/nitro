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
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestDynamicFilter
{
    @Test
    void testCollectedValuesPreserveExactMembershipAndDistinctSize()
    {
        long[] values = {0, -7, 12, -7, 12, 3};
        DynamicFilter filter = DynamicFilter.fromCollectedValues(2, values, values.length, -7, 12);

        assertThat(filter.column()).isEqualTo(2);
        assertThat(filter.size()).isEqualTo(4);
        assertThat(filter.accepts(-7)).isTrue();
        assertThat(filter.accepts(0)).isTrue();
        assertThat(filter.accepts(3)).isTrue();
        assertThat(filter.accepts(12)).isTrue();
        assertThat(filter.accepts(-8)).isFalse();
        assertThat(filter.accepts(1)).isFalse();
        assertThat(filter.accepts(13)).isFalse();
    }

    @Test
    void testCollectedValuesUseExactSparseFallback()
    {
        long[] values = {Long.MIN_VALUE, 5, Long.MAX_VALUE, 5};
        DynamicFilter filter = DynamicFilter.fromCollectedValues(
                0,
                values,
                values.length,
                Long.MIN_VALUE,
                Long.MAX_VALUE);

        assertThat(filter.size()).isEqualTo(3);
        assertThat(filter.accepts(Long.MIN_VALUE)).isTrue();
        assertThat(filter.accepts(5)).isTrue();
        assertThat(filter.accepts(Long.MAX_VALUE)).isTrue();
        assertThat(filter.accepts(6)).isFalse();
    }

    @Test
    void inclusiveRange()
    {
        DynamicFilter filter = DynamicFilter.fromRange(3, 10, 12);

        assertThat(filter.column()).isEqualTo(3);
        assertThat(filter.size()).isEqualTo(3);
        assertThat(filter.accepts(9)).isFalse();
        assertThat(filter.accepts(10)).isTrue();
        assertThat(filter.accepts(12)).isTrue();
        assertThat(filter.accepts(13)).isFalse();
    }

    @Test
    void emptyRange()
    {
        DynamicFilter filter = DynamicFilter.fromRange(1, 12, 10);

        assertThat(filter.isEmpty()).isTrue();
        assertThat(filter.accepts(11)).isFalse();
    }

    @Test
    void extractsStaticLongEqualityFromFilterPlan()
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(42L), AllMask.ALL),
                new Assignment(equals, new Call("eq", List.of(
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        ReferenceMask predicate = new ReferenceMask(new Reference(equals, Stream.VALUES));

        DynamicFilter filter = FilterOperator.staticLongEqualityFilter(plan, predicate).orElseThrow();
        assertThat(filter.column()).isEqualTo(3);
        assertThat(filter.accepts(41)).isFalse();
        assertThat(filter.accepts(42)).isTrue();
        assertThat(filter.accepts(43)).isFalse();
    }

    @Test
    void doesNotExtractNegatedLongEquality()
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(42L), AllMask.ALL),
                new Assignment(equals, new Call("eq", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());
        NotMask predicate = new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES)));

        assertThat(FilterOperator.staticLongEqualityFilter(plan, predicate)).isEmpty();
    }
}
