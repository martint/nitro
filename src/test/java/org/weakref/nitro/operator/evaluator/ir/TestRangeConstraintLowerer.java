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
package org.weakref.nitro.operator.evaluator.ir;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64RangeOptimization;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualI64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualI64RangeOptimization;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestRangeConstraintLowerer
{
    @Test
    void appliesRangeToDictionaryDomain()
    {
        LessThanI64RangeOptimization optimization = new LessThanI64RangeOptimization();
        Mask mask = Mask.all(5);

        assertThat(optimization.apply(
                Streams.ofValues(new DictionaryVector(
                        new int[] {0, 1, 2, 1, 0},
                        new I64Vector(new long[] {1, 5, 10}))),
                2L,
                8L,
                mask))
                .isTrue();
        assertThat(mask).containsExactly(1, 3);
    }

    @Test
    void lowersFromCapabilityWithoutDependingOnFunctionName()
    {
        PrimitiveRegistry registry = new PrimitiveRegistry();
        LessThanI64RangeOptimization optimization = new LessThanI64RangeOptimization();
        registry.register("aliased_comparison", new LessThanI64(), optimization);

        Variable lowerLiteral = new Variable(0);
        Variable upperLiteral = new Variable(1);
        Variable lowerComparison = new Variable(2);
        Variable upperComparison = new Variable(3);
        Reference input = new Reference(new Input(0), Stream.VALUES);
        Reference lowerPredicate = new Reference(lowerComparison, Stream.VALUES);
        Reference upperPredicate = new Reference(upperComparison, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(lowerLiteral, new Literal(3L), AllMask.ALL),
                new Assignment(upperLiteral, new Literal(11L), AllMask.ALL),
                new Assignment(lowerComparison, new Call("aliased_comparison", List.of(new Reference(lowerLiteral, Stream.VALUES), input)), AllMask.ALL),
                new Assignment(upperComparison, new Call("aliased_comparison", List.of(input, new Reference(upperLiteral, Stream.VALUES))), AllMask.ALL)),
                List.of());
        AndMask predicate = new AndMask(List.of(new ReferenceMask(lowerPredicate), new ReferenceMask(upperPredicate)));

        assertThat(RangeConstraintLowerer.lower(plan, registry, predicate, true))
                .isInstanceOfSatisfying(RangeConstrainedAndMask.class, range -> {
                    assertThat(range.input()).isEqualTo(input);
                    assertThat(range.lowerExclusive()).isEqualTo(3L);
                    assertThat(range.upperExclusive()).isEqualTo(11L);
                    assertThat(range.remainingTerms()).isEmpty();
                    assertThat(range.fallback()).isEqualTo(predicate);
                });
    }

    @Test
    void fusesInclusiveAndExclusiveIntegralBounds()
    {
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("lower", new LessThanOrEqualI64(), new LessThanOrEqualI64RangeOptimization());
        registry.register("upper", new LessThanI64(), new LessThanI64RangeOptimization());

        Variable lowerLiteral = new Variable(0);
        Variable upperLiteral = new Variable(1);
        Variable lowerComparison = new Variable(2);
        Variable upperComparison = new Variable(3);
        Reference input = new Reference(new Input(0), Stream.VALUES);
        Reference lowerPredicate = new Reference(lowerComparison, Stream.VALUES);
        Reference upperPredicate = new Reference(upperComparison, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(lowerLiteral, new Literal(3L), AllMask.ALL),
                new Assignment(upperLiteral, new Literal(11L), AllMask.ALL),
                new Assignment(lowerComparison, new Call("lower", List.of(new Reference(lowerLiteral, Stream.VALUES), input)), AllMask.ALL),
                new Assignment(upperComparison, new Call("upper", List.of(input, new Reference(upperLiteral, Stream.VALUES))), AllMask.ALL)),
                List.of());

        assertThat(RangeConstraintLowerer.lower(
                plan,
                registry,
                new AndMask(List.of(new ReferenceMask(lowerPredicate), new ReferenceMask(upperPredicate))),
                true))
                .isInstanceOf(RangeConstrainedAndMask.class);
    }

    @Test
    void functionNameDoesNotGrantRangeSemantics()
    {
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("lt", new LessThanI64());

        Variable literal = new Variable(0);
        Variable comparison = new Variable(1);
        Reference input = new Reference(new Input(0), Stream.VALUES);
        Reference predicate = new Reference(comparison, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(11L), AllMask.ALL),
                new Assignment(comparison, new Call("lt", List.of(input, new Reference(literal, Stream.VALUES))), AllMask.ALL)),
                List.of());

        assertThat(RangeConstraintLowerer.lower(plan, registry, new ReferenceMask(predicate), true))
                .isEqualTo(new ReferenceMask(predicate));
    }

    @Test
    void unpairedBoundRetainsTheOriginalMaskShape()
    {
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("aliased_comparison", new LessThanI64(), new LessThanI64RangeOptimization());

        Variable literal = new Variable(0);
        Variable comparison = new Variable(1);
        Reference input = new Reference(new Input(0), Stream.VALUES);
        Reference predicate = new Reference(comparison, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(List.of(
                new Assignment(literal, new Literal(11L), AllMask.ALL),
                new Assignment(comparison, new Call("aliased_comparison", List.of(input, new Reference(literal, Stream.VALUES))), AllMask.ALL)),
                List.of());

        assertThat(RangeConstraintLowerer.lower(plan, registry, new ReferenceMask(predicate), true))
                .isEqualTo(new ReferenceMask(predicate));
    }
}
