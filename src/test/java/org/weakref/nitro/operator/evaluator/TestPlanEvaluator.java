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
package org.weakref.nitro.operator.evaluator;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.MaterializationPolicy;
import org.weakref.nitro.operator.evaluator.ir.MemoizationPolicy;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Stream;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry;

public class TestPlanEvaluator
{
    @Test
    void testEvaluatesSimpleAddPlan()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(sum, new Call("add", List.of(
                        new Reference(new Input(0), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES),
                        new Reference(new Input(1), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(sum, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                Map.of(new Reference(sum, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30}))), new Allocator());

        I64Vector result = (I64Vector) evaluator.evaluate(new Reference(sum, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(result.values()).containsExactly(11L, 22L, 33L);
    }

    @Test
    void testEvaluatesNormalizedMerge()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                                new Reference(new Input(1), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES),
                                new Reference(new Input(2), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 2L);
    }

    @Test
    void testAddFunctionPreservesRleAcrossFullBatch()
    {
        PrimitiveFunction add = builtinPrimitiveRegistry().get("add");

        Streams result = add.apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {1, 5}))),
                        Streams.ofValues(new RleVector(new int[] {1, 3}, new I64Vector(new long[] {10, 20})))),
                Mask.all(4),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(result.values()).isInstanceOf(RleVector.class);
        RleVector vector = (RleVector) result.values();
        assertThat(vector.counts()).containsExactly(1, 1, 2);
        assertThat(((I64Vector) vector.values()).values()).containsExactly(11L, 21L, 25L);
    }

    @Test
    void testComparisonFunctionPreservesRleAcrossFullBatch()
    {
        PrimitiveFunction lessThan = builtinPrimitiveRegistry().get("lt");

        Streams result = lessThan.apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {1, 5}))),
                        Streams.ofValues(new RleVector(new int[] {1, 3}, new I64Vector(new long[] {10, 4})))),
                Mask.all(4),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(result.values()).isInstanceOf(RleVector.class);
        RleVector vector = (RleVector) result.values();
        assertThat(vector.counts()).containsExactly(1, 1, 2);
        assertThat(((BooleanVector) vector.values()).values()).containsExactly(true, true, false);
    }

    @Test
    void testExactFunctionsSupportRleInputs()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams addExact = primitiveRegistry.get("add_exact").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {1, 5}))),
                        Streams.ofValues(new RleVector(new int[] {1, 3}, new I64Vector(new long[] {10, 20})))),
                Mask.all(4),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(addExact.values()).isInstanceOf(RleVector.class);
        assertThat(((I64Vector) ((RleVector) addExact.values()).values()).values()).containsExactly(11L, 21L, 25L);
        assertThat(addExact.get(Stream.ERRORS)).isInstanceOf(RleVector.class);
        assertThat(((BooleanVector) ((RleVector) addExact.get(Stream.ERRORS)).values()).values()).containsExactly(false, false, false);

        Streams subtractExact = primitiveRegistry.get("subtract_exact").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {10, 30}))),
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4}))),
                Mask.all(4),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(subtractExact.values()).isInstanceOf(I64Vector.class);
        assertThat(((I64Vector) subtractExact.values()).values()).containsExactly(9L, 8L, 27L, 26L);
        assertThat(((BooleanVector) subtractExact.get(Stream.ERRORS)).values()).containsExactly(false, false, false, false);
    }

    @Test
    void testExactFunctionsReportOverflowViaErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams addExact = primitiveRegistry.get("add_exact").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {Long.MAX_VALUE, 1})),
                        Streams.ofValues(new I64Vector(new long[] {1, 2}))),
                Mask.all(2),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) addExact.values()).values()).containsExactly(Long.MIN_VALUE, 3L);
        assertThat(((BooleanVector) addExact.get(Stream.ERRORS)).values()).containsExactly(true, false);

        Streams subtractExact = primitiveRegistry.get("subtract_exact").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {Long.MIN_VALUE, 10})),
                        Streams.ofValues(new I64Vector(new long[] {1, 3}))),
                Mask.all(2),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) subtractExact.values()).values()).containsExactly(Long.MAX_VALUE, 7L);
        assertThat(((BooleanVector) subtractExact.get(Stream.ERRORS)).values()).containsExactly(true, false);
    }

    @Test
    void testDivideAndModuloReportErrorsViaErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams divide = primitiveRegistry.get("divide").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {20, 21, 22})),
                        Streams.ofValues(new I64Vector(new long[] {5, 0, 2}))),
                Mask.all(3),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) divide.values()).values()).containsExactly(4L, 0L, 11L);
        assertThat(((BooleanVector) divide.get(Stream.ERRORS)).values()).containsExactly(false, true, false);

        Streams modulo = primitiveRegistry.get("modulo").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 1}, new I64Vector(new long[] {20, 22}))),
                        Streams.ofValues(new I64Vector(new long[] {6, 0, 5}))),
                Mask.all(3),
                null,
                new PrimitiveExecutionContext(new Allocator()));

        assertThat(((I64Vector) modulo.values()).values()).containsExactly(2L, 0L, 2L);
        assertThat(((BooleanVector) modulo.get(Stream.ERRORS)).values()).containsExactly(false, true, false);
    }

    @Test
    void testMergeCopiesRleInputsIntoMaskedOutput()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                                new Reference(new Input(1), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES),
                                new Reference(new Input(2), org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new RleVector(new int[] {2, 2}, new I64Vector(new long[] {10, 20})),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, org.weakref.nitro.operator.evaluator.ir.Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(10L, 1L, 20L, 1L);
    }

    @Test
    void testEvaluatesCompositeMaskExpressions()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        Reference left = new Reference(new Input(0), Stream.VALUES);
        Reference right = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new OrMask(
                                        new AndMask(new ReferenceMask(left), new NotMask(new ReferenceMask(right))),
                                        new AndMask(new NotMask(new ReferenceMask(left)), new ReferenceMask(right))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, false, false}),
                new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {false, true, true, false}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator());

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 2L);
    }

    @Test
    void testEvaluatesErrorsStreamDirectly()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable quotient = new Variable(0);
        Reference errors = new Reference(quotient, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        quotient,
                        new Call("divide", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(errors),
                Map.of(errors, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {20, 21, 22}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {5, 0, 2}))), new Allocator());

        BooleanVector errorsVector = (BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS);
        assertThat(errorsVector.values()).containsExactly(false, true, false);
    }

    @Test
    void testMemoizesSiblingValueAndErrorStreamsTogether()
    {
        AtomicInteger evaluations = new AtomicInteger();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("counting", (inputs, mask, output, context) -> {
            evaluations.incrementAndGet();
            return Streams.ofValues(new I64Vector(new long[] {1, 2, 3}))
                    .with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
        });

        Variable result = new Variable(0);
        Reference values = new Reference(result, Stream.VALUES);
        Reference errors = new Reference(result, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("counting", List.of()), AllMask.ALL)),
                List.of(values, errors),
                Map.of(
                        values, StreamPlan.MATERIALIZED,
                        errors, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = new PlanEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator());

        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(((I64Vector) evaluator.evaluate(values, Mask.all(3)).get(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(evaluations).hasValue(1);
    }

    private static PlanEvaluator.InputResolver inputResolver(Map<Reference, org.weakref.nitro.data.Vector> inputs)
    {
        return (reference, mask) -> {
            org.weakref.nitro.data.Vector vector = inputs.get(reference);
            if (vector == null) {
                throw new IllegalArgumentException("Unexpected input " + reference);
            }
            return vector;
        };
    }

    private static PrimitiveRegistry builtinPrimitiveRegistry()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(scalarRegistry.register(AddI64.class));
        primitiveRegistry.register(scalarRegistry.register(LessThanI64.class));
        return primitiveRegistry;
    }
}
