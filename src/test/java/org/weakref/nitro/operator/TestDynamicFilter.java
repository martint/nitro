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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.function.scalar.builtin.EqualI64;
import org.weakref.nitro.function.scalar.builtin.EqualI64Optimization;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64RangeOptimization;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualI64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualI64RangeOptimization;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestDynamicFilter
{
    @Test
    void representationPolicyPreservesExactSparseFallback()
    {
        it.unimi.dsi.fastutil.longs.LongSet values =
                new it.unimi.dsi.fastutil.longs.LongOpenHashSet(new long[] {10, 11, 12});
        DynamicFilter filter = DynamicFilter.fromValues(0, values, new DynamicFilterPolicy(0));

        assertThat(filter.size()).isEqualTo(3);
        assertThat(filter.accepts(9)).isFalse();
        assertThat(filter.accepts(10)).isTrue();
        assertThat(filter.accepts(12)).isTrue();
        assertThat(filter.accepts(13)).isFalse();
    }

    @Test
    void testCollectedValuesPreserveExactMembershipAndDistinctSize()
    {
        long[] values = {0, -7, 12, -7, 12, 3};
        DynamicFilter filter = DynamicFilter.fromCollectedValues(
                2,
                values,
                values.length,
                -7,
                12,
                DynamicFilterPolicy.defaults());

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
                Long.MAX_VALUE,
                DynamicFilterPolicy.defaults());

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
        assertThat(filter.mayOverlap(9, 9)).isFalse();
        assertThat(filter.mayOverlap(9, 10)).isTrue();
        assertThat(filter.mayOverlap(11, 11)).isTrue();
        assertThat(filter.mayOverlap(12, 13)).isTrue();
        assertThat(filter.mayOverlap(13, 20)).isFalse();
    }

    @Test
    void emptyRange()
    {
        DynamicFilter filter = DynamicFilter.fromRange(1, 12, 10);

        assertThat(filter.isEmpty()).isTrue();
        assertThat(filter.accepts(11)).isFalse();
        assertThat(filter.mayOverlap(Long.MIN_VALUE, Long.MAX_VALUE)).isFalse();
    }

    @Test
    void extractsStaticLongEqualityFromFilterPlan()
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        ReferenceMask predicate = new ReferenceMask(new Reference(equals, Stream.VALUES));

        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("aliased_equality", new EqualI64(), new EqualI64Optimization());
        EvaluationPlan aliasedPlan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(42L), AllMask.ALL),
                new Assignment(equals, new Call("aliased_equality", List.of(
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(literal, Stream.VALUES))), AllMask.ALL)), List.of());

        DynamicFilter filter = FilterOperator.staticLongEqualityFilter(aliasedPlan, predicate, registry).orElseThrow();
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

        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("eq", (inputs, mask, requestedStreams, output, context) -> output);
        assertThat(FilterOperator.staticLongEqualityFilter(plan, predicate, registry)).isEmpty();
    }

    @Test
    void extractsEachStaticLongEqualityConjunct()
    {
        Variable firstLiteral = new Variable(0);
        Variable secondLiteral = new Variable(1);
        Variable firstEquals = new Variable(2);
        Variable secondEquals = new Variable(3);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(firstLiteral, new Literal(42L), AllMask.ALL),
                new Assignment(secondLiteral, new Literal(7L), AllMask.ALL),
                new Assignment(firstEquals, new Call("eq", List.of(
                        new Reference(new Input(3), Stream.VALUES),
                        new Reference(firstLiteral, Stream.VALUES))), AllMask.ALL),
                new Assignment(secondEquals, new Call("eq", List.of(
                        new Reference(new Input(5), Stream.VALUES),
                        new Reference(secondLiteral, Stream.VALUES))), AllMask.ALL)), List.of());
        AndMask predicate = new AndMask(List.of(
                new ReferenceMask(new Reference(firstEquals, Stream.VALUES)),
                new ReferenceMask(new Reference(secondEquals, Stream.VALUES))));
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("eq", new EqualI64(), new EqualI64Optimization());

        List<DynamicFilter> filters = FilterOperator.staticLongEqualityFilters(plan, predicate, registry);

        assertThat(filters).hasSize(2);
        assertThat(filters.get(0).column()).isEqualTo(3);
        assertThat(filters.get(0).accepts(42)).isTrue();
        assertThat(filters.get(1).column()).isEqualTo(5);
        assertThat(filters.get(1).accepts(7)).isTrue();
    }

    @Test
    void extractsRegistryLoweredStaticLongRange()
    {
        Variable lowerLiteral = new Variable(0);
        Variable upperLiteral = new Variable(1);
        Variable lower = new Variable(2);
        Variable upper = new Variable(3);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(lowerLiteral, new Literal(9L), AllMask.ALL),
                new Assignment(upperLiteral, new Literal(13L), AllMask.ALL),
                new Assignment(lower, new Call("aliased_lt", List.of(
                        new Reference(lowerLiteral, Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                new Assignment(upper, new Call("aliased_lt", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(upperLiteral, Stream.VALUES))), AllMask.ALL)), List.of());
        AndMask predicate = new AndMask(List.of(
                new ReferenceMask(new Reference(lower, Stream.VALUES)),
                new ReferenceMask(new Reference(upper, Stream.VALUES))));
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("aliased_lt", new LessThanI64(), new LessThanI64RangeOptimization());

        List<DynamicFilter> filters = FilterOperator.staticLongRangeFilters(plan, predicate, registry);

        assertThat(filters).hasSize(1);
        DynamicFilter filter = filters.getFirst();
        assertThat(filter.column()).isEqualTo(2);
        assertThat(filter.accepts(9)).isFalse();
        assertThat(filter.accepts(10)).isTrue();
        assertThat(filter.accepts(12)).isTrue();
        assertThat(filter.accepts(13)).isFalse();
    }

    @Test
    void extractsMixedInclusiveAndExclusiveStaticLongRange()
    {
        Variable lowerLiteral = new Variable(0);
        Variable upperLiteral = new Variable(1);
        Variable lower = new Variable(2);
        Variable upper = new Variable(3);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(lowerLiteral, new Literal(10L), AllMask.ALL),
                new Assignment(upperLiteral, new Literal(13L), AllMask.ALL),
                new Assignment(lower, new Call("aliased_lte", List.of(
                        new Reference(lowerLiteral, Stream.VALUES),
                        new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                new Assignment(upper, new Call("aliased_lt", List.of(
                        new Reference(new Input(2), Stream.VALUES),
                        new Reference(upperLiteral, Stream.VALUES))), AllMask.ALL)), List.of());
        AndMask predicate = new AndMask(List.of(
                new ReferenceMask(new Reference(lower, Stream.VALUES)),
                new ReferenceMask(new Reference(upper, Stream.VALUES))));
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("aliased_lte", new LessThanOrEqualI64(), new LessThanOrEqualI64RangeOptimization());
        registry.register("aliased_lt", new LessThanI64(), new LessThanI64RangeOptimization());

        DynamicFilter filter = FilterOperator.staticLongRangeFilters(plan, predicate, registry).getFirst();

        assertThat(filter.column()).isEqualTo(2);
        assertThat(filter.accepts(9)).isFalse();
        assertThat(filter.accepts(10)).isTrue();
        assertThat(filter.accepts(12)).isTrue();
        assertThat(filter.accepts(13)).isFalse();
    }
}
