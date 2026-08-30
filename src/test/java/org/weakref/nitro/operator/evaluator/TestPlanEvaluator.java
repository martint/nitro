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
import org.weakref.nitro.core.function.EncodedDomainReuse;
import org.weakref.nitro.core.source.LongDomain;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorConstructor;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.AllocationResources;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.ErrorValue;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.AnnotatedScalarLoader;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.ScalarRegistry;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.CoalesceI64;
import org.weakref.nitro.function.scalar.builtin.CoalesceI64Policy;
import org.weakref.nitro.function.scalar.builtin.DivideScaleRoundI64;
import org.weakref.nitro.function.scalar.builtin.EqualI64;
import org.weakref.nitro.function.scalar.builtin.InUtf8;
import org.weakref.nitro.function.scalar.builtin.InUtf8SourceMaskOptimization;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualI64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualUtf8;
import org.weakref.nitro.function.scalar.builtin.LessThanUtf8;
import org.weakref.nitro.function.scalar.builtin.ScaledRelativeDifferenceGtI64;
import org.weakref.nitro.function.scalar.builtin.SubstringUtf8;
import org.weakref.nitro.function.scalar.builtin.SubstringUtf8BinarySliceProjection;
import org.weakref.nitro.jit.ProjectionMaskCompiler;
import org.weakref.nitro.operator.EvaluationOperatorPolicy;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.AndMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.Coalesce;
import org.weakref.nitro.operator.evaluator.ir.Conditional;
import org.weakref.nitro.operator.evaluator.ir.Construct;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.IrNormalizer;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.LongDomainMask;
import org.weakref.nitro.operator.evaluator.ir.MaterializationPolicy;
import org.weakref.nitro.operator.evaluator.ir.MemoizationPolicy;
import org.weakref.nitro.operator.evaluator.ir.NotMask;
import org.weakref.nitro.operator.evaluator.ir.OrMask;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Sequence;
import org.weakref.nitro.operator.evaluator.ir.StreamPlan;
import org.weakref.nitro.operator.evaluator.ir.StructField;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.TestPrimitiveFunctions.primitiveRegistry;

public class TestPlanEvaluator
{
    @Test
    void testTypedLiteralUsesProviderVectorFactory()
    {
        Variable literal = new Variable(0);
        Reference values = new Reference(literal, Stream.VALUES);
        TypeBinding type = testingIntegerType();
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(literal, new Literal(37, type), AllMask.ALL)),
                List.of(values));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of()),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(values, Mask.all(4));

        assertThat(result.get(Stream.VALUES)).isInstanceOf(RleVector.class);
        RleVector vector = (RleVector) result.get(Stream.VALUES);
        assertThat(vector.counts()).containsExactly(4);
        assertThat(((I32Vector) vector.values()).values()).containsExactly(37);
    }

    @Test
    void testTypedNullUsesProviderPlaceholderAndSeparateNullStream()
    {
        Variable literal = new Variable(0);
        Reference values = new Reference(literal, Stream.VALUES);
        Reference nulls = new Reference(literal, Stream.NULLS);
        TypeBinding type = testingIntegerType();
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(literal, new Literal(null, type), AllMask.ALL)),
                List.of(values, nulls));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of()),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(values, Mask.all(3));

        assertThat(result.get(Stream.VALUES)).isInstanceOf(RleVector.class);
        RleVector vector = (RleVector) result.get(Stream.VALUES);
        assertThat(vector.counts()).containsExactly(3);
        assertThat(((I32Vector) vector.values()).values()).containsExactly(0);
        assertThat(readBooleans(result.get(Stream.NULLS))).containsExactly(true, true, true);
    }

    @Test
    void testStructuralConstructionUsesProviderAndPropagatesChildErrors()
    {
        Variable constructed = new Variable(0);
        Reference values = new Reference(constructed, Stream.VALUES);
        Reference errors = new Reference(constructed, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        constructed,
                        new Construct(
                                testingStructType(),
                                List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(values, errors));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {false, true, false}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30}),
                        new Reference(new Input(1), Stream.ERRORS), new BooleanVector(new boolean[] {false, false, true}))),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(values, Mask.all(3));

        StructVector struct = (StructVector) result.values();
        assertThat(((I64Vector) struct.field(0).values()).values()).containsExactly(1, 2, 3);
        assertThat(((I64Vector) struct.field(1).values()).values()).containsExactly(10, 20, 30);
        assertThat(readBooleans(result.get(Stream.ERRORS))).containsExactly(false, true, true);
    }

    @Test
    void testStructuralConstructionPreservesSharedRunDomain()
    {
        Variable constructed = new Variable(0);
        Reference values = new Reference(constructed, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        constructed,
                        new Construct(
                                testingStructType(),
                                List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(values));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new RleVector(new int[] {4}, new I64Vector(new long[] {7})),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(4),
                        new Reference(new Input(1), Stream.VALUES), new RleVector(new int[] {4}, new I64Vector(new long[] {11})))),
                new Allocator(EngineResources.createDefault()));

        RleVector result = (RleVector) evaluator.evaluate(values, Mask.all(4)).values();

        assertThat(result.counts()).containsExactly(4);
        StructVector physical = (StructVector) result.values();
        assertThat(physical.length()).isEqualTo(1);
        assertThat(((I64Vector) physical.field(0).values()).values()).containsExactly(7);
        assertThat(((I64Vector) physical.field(1).values()).values()).containsExactly(11);
    }

    @Test
    void testStructuralConstructionPreservesRichChildError()
    {
        ErrorValue diagnostic = new ErrorValue("test", 17, "BAD_ARGUMENT", "USER_ERROR", "bad argument");
        ErrorVector richErrors = new ErrorVector(3);
        richErrors.setError(1, diagnostic);

        Variable constructed = new Variable(0);
        Reference values = new Reference(constructed, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        constructed,
                        new Construct(
                                testingStructType(),
                                List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(values));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}),
                        new Reference(new Input(0), Stream.ERRORS), richErrors,
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30}),
                        new Reference(new Input(1), Stream.ERRORS), new BooleanVector(new boolean[] {false, false, true}))),
                new Allocator(EngineResources.createDefault()));

        ErrorVector errors = (ErrorVector) evaluator.evaluate(values, Mask.all(3)).get(Stream.ERRORS);

        assertThat(errors.values()).containsExactly(false, true, true);
        assertThat(errors.error(1)).isEqualTo(diagnostic);
        assertThat(errors.error(2)).isNull();
    }

    @Test
    void testInputErrorMergeTreatsUnevaluatedTailAsAbsent()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("combine", (inputs, _, _, _, _) -> Streams.ofValues(inputs.getFirst().values()));

        Variable combined = new Variable(0);
        Reference values = new Reference(combined, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        combined,
                        new Call("combine", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(values));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3, 4, 5}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {false, true}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30, 40, 50}),
                        new Reference(new Input(1), Stream.ERRORS), new BooleanVector(new boolean[] {false, false, false, false, true}))),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(values, Mask.sparse(new int[] {0, 1, 4}, 5));

        assertThat(readBooleans(result.get(Stream.ERRORS))).containsExactly(false, true, false, false, true);
    }

    @Test
    void testFlatBooleanReferenceCompactsOwnedMaskInPlaceWithoutTemporaryMask()
    {
        try (AllocationResources resources = AllocationResources.createDefault();
                Allocator allocator = new Allocator(resources)) {
            Reference valuesReference = new Reference(new Input(0), Stream.VALUES);
            PlanEvaluator evaluator = planEvaluator(
                    new EvaluationPlan(List.of(), List.of()),
                    primitiveRegistry(),
                    inputResolver(Map.of(valuesReference, new BooleanVector(new boolean[] {false, true, true, false, true, false}))),
                    allocator,
                    new ProjectionMaskCompiler());
            Mask owned = Mask.sparse(new int[] {0, 1, 2, 4, 5}, 6);

            Mask result = evaluator.evaluateInPlace(new ReferenceMask(valuesReference), owned);

            assertThat(result).isSameAs(owned);
            assertThat(result.selectedCount()).isEqualTo(3);
            assertThat(result.position(0)).isEqualTo(1);
            assertThat(result.position(1)).isEqualTo(2);
            assertThat(result.position(2)).isEqualTo(4);
            assertThat(allocator.totalBytes(new Allocator.Context("PlanEvaluator"))).isZero();
        }
    }

    @Test
    void testEvaluatesSimpleAddPlan()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable sum = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(sum, new Call("add", List.of(
                        new Reference(new Input(0), org.weakref.nitro.data.Stream.VALUES),
                        new Reference(new Input(1), org.weakref.nitro.data.Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(sum, org.weakref.nitro.data.Stream.VALUES)),
                Map.of(new Reference(sum, org.weakref.nitro.data.Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30}))), new Allocator(EngineResources.createDefault()));

        I64Vector result = (I64Vector) evaluator.evaluate(new Reference(sum, org.weakref.nitro.data.Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(result.values()).containsExactly(11L, 22L, 33L);
    }

    @Test
    void testSequenceStrictlyEvaluatesFirstAndPropagatesItsErrors()
    {
        PrimitiveRegistry registry = new PrimitiveRegistry();
        AtomicInteger evaluations = new AtomicInteger();
        registry.register("strict_first", (_, mask, _, _, _) -> {
            evaluations.incrementAndGet();
            assertThat(mask).containsExactly(1, 3);
            return Streams.ofValues(new I64Vector(new long[4]))
                    .with(Stream.ERRORS, new BooleanVector(new boolean[] {false, false, false, true}));
        });

        Variable first = new Variable(0);
        Variable body = new Variable(1);
        Variable sequence = new Variable(2);
        Reference output = new Reference(sequence, Stream.VALUES);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(
                        new Assignment(first, new Call("strict_first", List.of()), AllMask.ALL),
                        new Assignment(body, new Literal(41L), AllMask.ALL),
                        new Assignment(sequence, new Sequence(
                                new Reference(first, Stream.VALUES),
                                new Reference(body, Stream.VALUES)), AllMask.ALL)),
                List.of(output)));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                registry,
                inputResolver(Map.of()),
                new Allocator(EngineResources.createDefault()),
                new Object(),
                true);

        Streams result = evaluator.evaluate(output, Mask.sparse(new int[] {1, 3}, 4));

        assertThat(evaluations).hasValue(1);
        assertThat(readBooleans(result.get(Stream.ERRORS))).containsExactly(false, false, false, true);
        assertThat(((RleVector) result.get(Stream.VALUES)).counts()).containsExactly(4);
    }

    @Test
    void testValuesProjectionPropagatesNullsThroughIntermediateArithmetic()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable firstPair = new Variable(0);
        Variable total = new Variable(1);
        Reference totalValues = new Reference(total, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(firstPair, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(total, new Call("add", List.of(
                                new Reference(firstPair, Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                List.of(totalValues));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 0, 3}),
                new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {100, 200, 300}))), new Allocator(EngineResources.createDefault()), new Object(), true);

        Streams result = evaluator.evaluate(totalValues, Mask.all(3));
        assertThat(((I64Vector) result.get(Stream.VALUES)).values()).containsExactly(111L, 220L, 333L);
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, false);
    }

    @Test
    void testTwoIndependentAddColumnsDoNotAliasBuffersAcrossReset()
    {
        // Two independent `add` results computed by the same primitive (AddI64) share AddI64's static
        // allocation context and therefore its vector pool. ProjectOperator reads outputs lazily: it
        // materializes one column, then re-evaluates the plan after a constrain()/reset() before
        // reading the other. If the still-referenced buffer of the first result is returned to the
        // pool by reset() and re-borrowed for the second result, both columns alias and report the
        // same values. This reproduces that lifecycle without TPC-DS data.
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable first = new Variable(0);
        Variable second = new Variable(1);
        Reference firstValues = new Reference(first, Stream.VALUES);
        Reference secondValues = new Reference(second, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(first, new Call("add", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(second, new Call("add", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES))), AllMask.ALL)),
                List.of(firstValues, secondValues),
                Map.of(
                        firstValues, new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE),
                        secondValues, new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3, 4}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {10, 20, 30, 40}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {100, 200, 300, 400}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {1000, 2000, 3000, 4000}))), new Allocator(EngineResources.createDefault()));

        // Materialize the first column and hold its result, as a downstream consumer would after the
        // ProjectOperator hands out the column's vector.
        I64Vector firstResult = (I64Vector) evaluator.evaluate(firstValues, Mask.all(4)).get(Stream.VALUES);
        assertThat(firstResult.values()).containsExactly(11L, 22L, 33L, 44L);

        // A constrain()/reset() cycle, as ProjectOperator performs when a downstream operator pushes
        // a narrower mask. reset() returns the AddI64 buffers to the pool, including firstResult's
        // buffer, even though the downstream consumer still holds firstResult.
        evaluator.reset();

        // The second column is re-evaluated after the reset. Its add re-borrows from the AddI64 pool;
        // if it re-borrows firstResult's buffer it overwrites the value the consumer still holds.
        I64Vector secondResult = (I64Vector) evaluator.evaluate(secondValues, Mask.all(4)).get(Stream.VALUES);
        assertThat(secondResult.values()).containsExactly(1100L, 2200L, 3300L, 4400L);

        // firstResult must still hold its own values, not the second column's.
        assertThat(firstResult.values()).containsExactly(11L, 22L, 33L, 44L);
    }

    @Test
    void testEvaluatesNullI64Function()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable nullValue = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(nullValue, new Call("null_i64", List.of()), AllMask.ALL)),
                List.of(new Reference(nullValue, Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(nullValue, Stream.VALUES), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(true, true, true);
        assertThat(((I64Vector) result.get(Stream.VALUES)).values()).containsExactly(0L, 0L, 0L);
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
                                new ReferenceMask(new Reference(new Input(0), org.weakref.nitro.data.Stream.VALUES)),
                                new Reference(new Input(1), org.weakref.nitro.data.Stream.VALUES),
                                new Reference(new Input(2), org.weakref.nitro.data.Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, org.weakref.nitro.data.Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, org.weakref.nitro.data.Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 2L);
    }

    @Test
    void testConditionalMergesRepeatedValuesInOutputOrder()
    {
        ArrayVector emptyArrays = new ArrayVector(4);
        emptyArrays.setElements(Streams.ofValues(new I64Vector(new long[0])));

        ArrayVector populatedArrays = new ArrayVector(4);
        System.arraycopy(new int[] {0, 1, 2, 3, 4}, 0, populatedArrays.offsets(), 0, 5);
        populatedArrays.setElements(Streams.ofValues(new I64Vector(new long[] {10, 20, 30, 40})));

        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                new PrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                        new Reference(new Input(1), Stream.VALUES), emptyArrays,
                        new Reference(new Input(2), Stream.VALUES), populatedArrays)),
                new Allocator(EngineResources.createDefault()));

        ArrayVector values = (ArrayVector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).values();
        assertThat(values.offsets()).containsExactly(0, 0, 1, 1, 2);
        assertThat(((I64Vector) values.elementValues()).values()).startsWith(20, 40);
    }

    @Test
    void testConditionalPropagatesConditionErrorsAndTreatsNullAsFalse()
    {
        ErrorValue diagnostic = new ErrorValue("test", 23, "CONDITION_ERROR", "USER_ERROR", "condition failed");
        ErrorVector conditionErrors = new ErrorVector(3);
        conditionErrors.setError(2, diagnostic);

        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                new PrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, false}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false}),
                        new Reference(new Input(0), Stream.ERRORS), conditionErrors,
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {11, 12, 13}),
                        new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {21, 22, 23}))),
                new Allocator(EngineResources.createDefault()));

        Streams output = evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(3));
        assertThat(((I64Vector) output.values()).values()).startsWith(11, 22);
        ErrorVector errors = (ErrorVector) output.get(Stream.ERRORS);
        assertThat(errors.values()).containsExactly(false, false, true);
        assertThat(errors.error(2)).isEqualTo(diagnostic);
    }

    @Test
    void testConditionalEvaluatesNestedBranchesUnderBranchMasks()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(new ScalarDescriptor("true_branch", true, maskedConstant(10, Set.of(0, 2))));
        primitiveRegistry.register(new ScalarDescriptor("false_branch", true, maskedConstant(20, Set.of(1, 3))));

        Variable trueBranch = new Variable(0);
        Variable falseBranch = new Variable(1);
        Variable result = new Variable(2);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(
                        new Assignment(trueBranch, new Call("true_branch", List.of()), AllMask.ALL),
                        new Assignment(falseBranch, new Call("false_branch", List.of()), AllMask.ALL),
                        new Assignment(
                                result,
                                new Conditional(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(trueBranch, Stream.VALUES),
                                        new Reference(falseBranch, Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}))),
                new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(10L, 20L, 10L, 20L);
    }

    @Test
    void testConditionalCallsEvaluateOnlySelectedDictionaryDomains()
    {
        AtomicReference<List<Integer>> trueDomains = new AtomicReference<>();
        AtomicReference<List<Integer>> falseDomains = new AtomicReference<>();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("true_domain", recordSelectedDomains(trueDomains, 10));
        primitiveRegistry.register("false_domain", recordSelectedDomains(falseDomains, 100));

        Reference input = new Reference(new Input(1), Stream.VALUES);
        Variable trueBranch = new Variable(0);
        Variable falseBranch = new Variable(1);
        Variable result = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(trueBranch, new Call("true_domain", List.of(input)), AllMask.ALL),
                        new Assignment(falseBranch, new Call("false_domain", List.of(input)), AllMask.ALL),
                        new Assignment(
                                result,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new Reference(trueBranch, Stream.VALUES),
                                        new Reference(falseBranch, Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        int[] ids = {0, 1, 2, 0, 2, 1};
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES),
                        DictionaryVector.wrap(ids, new BooleanVector(new boolean[] {true, false, true})),
                        input,
                        DictionaryVector.wrap(ids, new I64Vector(new long[] {1, 8, 3})))),
                new Allocator(EngineResources.createDefault()));

        DictionaryVector encoded = (DictionaryVector) evaluator.evaluate(
                        new Reference(result, Stream.VALUES),
                        Mask.all(ids.length))
                .values();
        assertThat(readLongs(encoded))
                .containsExactly(10, 800, 30, 10, 30, 800);
        assertThat(encoded.hasDomainFrequencies()).isTrue();
        assertThat(encoded.domainFrequency(0)).isEqualTo(2);
        assertThat(encoded.domainFrequency(1)).isEqualTo(2);
        assertThat(encoded.domainFrequency(2)).isEqualTo(2);
        assertThat(trueDomains.get()).containsExactly(0, 2);
        assertThat(falseDomains.get()).containsExactly(1);
    }

    @Test
    void testConditionalMergesCompactAndWideIntegerRepresentations()
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                new PrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                        new Reference(new Input(1), Stream.VALUES), new I32Vector(new int[] {1, 2, 3, 4}),
                        new Reference(new Input(2), Stream.VALUES), new RleVector(new int[] {4}, new I64Vector(new long[] {0})))),
                new Allocator(EngineResources.createDefault()));

        I64Vector values = (I64Vector) evaluator.evaluate(
                        new Reference(result, Stream.VALUES),
                        Mask.all(4))
                .values();
        assertThat(values.values()).containsExactly(1, 0, 3, 0);
    }

    @Test
    void testConditionalDoesNotEvaluateUnselectedNestedBranch()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(new ScalarDescriptor("selected_branch", true, maskedConstant(10, Set.of(0, 1, 2))));
        primitiveRegistry.register(new ScalarDescriptor("unselected_branch", true, (_, _, _, _, _) -> {
            throw new AssertionError("unselected branch was evaluated");
        }));

        Variable selectedBranch = new Variable(0);
        Variable unselectedBranch = new Variable(1);
        Variable result = new Variable(2);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(
                        new Assignment(selectedBranch, new Call("selected_branch", List.of()), AllMask.ALL),
                        new Assignment(unselectedBranch, new Call("unselected_branch", List.of()), AllMask.ALL),
                        new Assignment(
                                result,
                                new Conditional(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(selectedBranch, Stream.VALUES),
                                        new Reference(unselectedBranch, Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true}))),
                new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(10L, 10L, 10L);
    }

    @Test
    void testCoalesceDoesNotEvaluateUnselectedNestedFallback()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(new ScalarDescriptor("selected_branch", true, maskedConstant(10, Set.of(0, 1, 2))));
        primitiveRegistry.register(new ScalarDescriptor("unselected_branch", true, (_, _, _, _, _) -> {
            throw new AssertionError("unselected fallback was evaluated");
        }));

        Variable selectedBranch = new Variable(0);
        Variable unselectedBranch = new Variable(1);
        Variable result = new Variable(2);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(
                        new Assignment(selectedBranch, new Call("selected_branch", List.of()), AllMask.ALL),
                        new Assignment(unselectedBranch, new Call("unselected_branch", List.of()), AllMask.ALL),
                        new Assignment(
                                result,
                                new Coalesce(
                                        new Reference(selectedBranch, Stream.VALUES),
                                        new Reference(unselectedBranch, Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of()),
                new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(10L, 10L, 10L);
    }

    @Test
    void testReferenceMaskTreatsMissingOptionalInputStreamsAsAbsent()
    {
        EvaluationPlan plan = new EvaluationPlan(
                List.of(),
                List.of(new Reference(new Input(0), Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(new Input(0), Stream.VALUES)), Mask.all(3));
        assertThat(result.count()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testLongDomainMaskEvaluatesDictionaryEntriesOnce()
    {
        AtomicInteger evaluations = new AtomicInteger();
        LongDomain domain = new LongDomain()
        {
            @Override
            public boolean test(long value)
            {
                evaluations.incrementAndGet();
                return value == 7 || value == 42;
            }

            @Override
            public long contentGeneration()
            {
                return 0;
            }

            @Override
            public int size()
            {
                return 2;
            }

            @Override
            public boolean isEmpty()
            {
                return false;
            }

            @Override
            public double rangeDensity()
            {
                return 2.0 / 36;
            }
        };
        Reference input = new Reference(new Input(0), Stream.VALUES);
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry(),
                inputResolver(Map.of(
                        input, DictionaryVector.wrap(
                                new int[] {0, 1, 2, 0, 2},
                                new I64Vector(new long[] {7, 8, 42})))),
                new Allocator(EngineResources.createDefault()));

        assertThat(evaluator.evaluate(new LongDomainMask(input, domain), Mask.all(5)))
                .containsExactly(0, 2, 3, 4);
        assertThat(evaluations).hasValue(3);
    }

    @Test
    void testReferenceMaskUsesInputMaskResolverWithoutBorrowingValues()
    {
        org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("nitro.expression.inputMaskResolver", "true")));

        Reference reference = new Reference(new Input(0), Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(List.of(), List.of(reference));
        AtomicInteger vectorResolveCount = new AtomicInteger();
        AtomicInteger maskResolveCount = new AtomicInteger();
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                new PlanEvaluator.InputResolver()
                {
                    @Override
                    public org.weakref.nitro.data.Vector resolve(Reference reference, Mask mask)
                    {
                        vectorResolveCount.incrementAndGet();
                        return new BooleanVector(new boolean[] {false, true, false, true, false});
                    }

                    @Override
                    public Mask resolveMask(Reference requestedReference, Mask mask, boolean selectTrue, Allocator resultAllocator, Allocator.Context resultAllocationContext)
                    {
                        assertThat(requestedReference).isEqualTo(reference);
                        maskResolveCount.incrementAndGet();
                        if (selectTrue) {
                            return resultAllocator.allocateSparseMask(resultAllocationContext, new int[] {1, 3}, 2, mask.size());
                        }
                        return resultAllocator.allocateSparseMask(resultAllocationContext, new int[] {0, 2, 4}, 3, mask.size());
                    }
                },
                allocator);

        Mask trueResult = evaluator.evaluate(new ReferenceMask(reference), Mask.all(5));
        assertThat(trueResult.selectedCount()).isEqualTo(2);
        assertThat(trueResult.position(0)).isEqualTo(1);
        assertThat(trueResult.position(1)).isEqualTo(3);

        Mask falseResult = evaluator.evaluate(new NotMask(new ReferenceMask(reference)), Mask.all(5));
        assertThat(falseResult.selectedCount()).isEqualTo(3);
        assertThat(falseResult.position(0)).isEqualTo(0);
        assertThat(falseResult.position(1)).isEqualTo(2);
        assertThat(falseResult.position(2)).isEqualTo(4);

        assertThat(vectorResolveCount).hasValue(0);
        assertThat(maskResolveCount).hasValue(2);
    }

    @Test
    void testReferenceMaskClassifiesErrorsThenNullsThenTrueValues()
    {
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false, true}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false, false}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {false, false, true, false, false}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(new Input(0), Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(4);
    }

    @Test
    void testReferenceMaskOptimizesSimpleLongComparisonsViaPrimitiveMaskEvaluation()
    {
        Variable sixtyTwo = new Variable(0);
        Variable refreshZero = new Variable(1);
        Variable julyStart = new Variable(2);
        Variable augustStart = new Variable(3);
        Variable counterEquals = new Variable(4);
        Variable refreshEquals = new Variable(5);
        Variable afterStart = new Variable(6);
        Variable beforeEnd = new Variable(7);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(sixtyTwo, new Literal(62L), AllMask.ALL),
                        new Assignment(refreshZero, new Literal(0L), AllMask.ALL),
                        new Assignment(julyStart, new Literal(1_372_636_800L), AllMask.ALL),
                        new Assignment(augustStart, new Literal(1_375_315_200L), AllMask.ALL),
                        new Assignment(counterEquals, new Call("eq", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(sixtyTwo, Stream.VALUES))), AllMask.ALL),
                        new Assignment(refreshEquals, new Call("eq", List.of(
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(refreshZero, Stream.VALUES))), AllMask.ALL),
                        new Assignment(afterStart, new Call("lt", List.of(
                                new Reference(julyStart, Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))), AllMask.ALL),
                        new Assignment(beforeEnd, new Call("lt", List.of(
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(augustStart, Stream.VALUES))), AllMask.ALL)),
                List.of(),
                Map.of(),
                Map.of());

        PlanEvaluator evaluator = planEvaluator(
                plan,
                builtinPrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {62, 62, 62, 62}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {1_372_636_800L, 1_373_000_000L, 1_373_000_000L, 1_375_315_200L}),
                        new Reference(new Input(1), Stream.NULLS), new BooleanVector(new boolean[] {false, false, true, false}),
                        new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {0, 0, 0, 1}),
                        new Reference(new Input(2), Stream.NULLS), new BooleanVector(new boolean[] {false, false, false, true}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(
                new AndMask(List.of(
                        new ReferenceMask(new Reference(counterEquals, Stream.VALUES)),
                        new ReferenceMask(new Reference(refreshEquals, Stream.VALUES)),
                        new ReferenceMask(new Reference(afterStart, Stream.VALUES)),
                        new ReferenceMask(new Reference(beforeEnd, Stream.VALUES)))),
                Mask.all(4));

        assertThat(result.selectedCount()).isEqualTo(1);
        assertThat(result.position(0)).isEqualTo(1);
    }

    @Test
    void testReferenceMaskOptimizesUtf8RangeComparisonsViaPrimitiveMaskEvaluation()
    {
        Variable lower = new Variable(0);
        Variable upper = new Variable(1);
        Variable afterLower = new Variable(2);
        Variable beforeUpper = new Variable(3);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(lower, new Literal("b"), AllMask.ALL),
                        new Assignment(upper, new Literal("d"), AllMask.ALL),
                        new Assignment(afterLower, new Call("lte_utf8", List.of(
                                new Reference(lower, Stream.VALUES),
                                new Reference(new Input(0), Stream.VALUES))), AllMask.ALL),
                        new Assignment(beforeUpper, new Call("lt_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(upper, Stream.VALUES))), AllMask.ALL)),
                List.of());

        int[] ids = {0, 1, 2, 3, 2};
        PlanEvaluator evaluator = planEvaluator(
                plan,
                builtinPrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES),
                        DictionaryVector.wrap(ids, utf8Vector("a", "b", "c", "d")))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(
                new AndMask(List.of(
                        new ReferenceMask(new Reference(afterLower, Stream.VALUES)),
                        new ReferenceMask(new Reference(beforeUpper, Stream.VALUES)))),
                Mask.all(ids.length));

        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(1);
        assertThat(result.position(1)).isEqualTo(2);
        assertThat(result.position(2)).isEqualTo(4);
    }

    @Test
    void testProviderAuthoredLongMaskSupportsAllocatingAndInPlaceModes()
    {
        Variable one = new Variable(0);
        Variable equal = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(equal, new Call("eq", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(one, Stream.VALUES))), AllMask.ALL)),
                List.of());
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 1, 2, 2}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {false, false, false, true}))),
                new Allocator(EngineResources.createDefault()));
        ReferenceMask predicate = new ReferenceMask(new Reference(equal, Stream.VALUES));

        assertThat(evaluator.evaluate(predicate, Mask.all(4))).containsExactly(0);
        assertThat(evaluator.evaluate(new NotMask(predicate), Mask.all(4))).containsExactly(2);

        Mask inPlace = Mask.all(4);
        assertThat(evaluator.evaluateInPlace(predicate, inPlace)).isSameAs(inPlace);
        assertThat(inPlace).containsExactly(0);
    }

    @Test
    void testProviderAuthoredDoubleComparisonMaskPreservesNullSemantics()
    {
        Variable two = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(two, new Literal(2.0), AllMask.ALL),
                        new Assignment(lessThan, new Call("lt_f64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(two, Stream.VALUES))), AllMask.ALL)),
                List.of());
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new F64Vector(new double[] {1.0, 1.0, 3.0, 3.0}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, true}))),
                new Allocator(EngineResources.createDefault()));
        ReferenceMask predicate = new ReferenceMask(new Reference(lessThan, Stream.VALUES));

        Mask trueResult = evaluator.evaluate(predicate, Mask.all(4));
        Mask falseResult = evaluator.evaluate(new NotMask(predicate), Mask.all(4));

        assertThat(trueResult).containsExactly(0);
        assertThat(falseResult).containsExactly(2);
    }

    @Test
    void testUtf8LiteralUsesRleAndContainsSupportsIt()
    {
        Variable needle = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(needle, new Literal("go"), AllMask.ALL),
                        new Assignment(
                                contains,
                                new Call("contains_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(needle, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(
                        new Reference(needle, Stream.VALUES),
                        new Reference(contains, Stream.VALUES)),
                Map.of(
                        new Reference(needle, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE),
                        new Reference(contains, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        BinaryVector input = new BinaryVector(3, 16);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        input.setBytes(0, "google".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(1, "bing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(2, "golang".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), input)),
                new Allocator(EngineResources.createDefault()));

        Streams literal = evaluator.evaluate(new Reference(needle, Stream.VALUES), Mask.all(3));
        assertThat(literal.values()).isInstanceOf(RleVector.class);

        Streams result = evaluator.evaluate(new Reference(contains, Stream.VALUES), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.VALUES)).values()).containsExactly(true, false, true);
    }

    @Test
    void testContainsProbesOnlySelectedDictionaryIdsForSparseMask()
    {
        Variable needle = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(needle, new Literal("google"), AllMask.ALL),
                        new Assignment(
                                contains,
                                new Call("contains_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(needle, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(contains, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(3, 64);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "google.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "example.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "maps.google.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new DictionaryVector(new int[] {0, 1, 2, 0, 2}, dictionary))),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(
                new Reference(contains, Stream.VALUES),
                Mask.sparse(new int[] {1, 4}, 5));
        assertThat(result.get(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
        // Position 2 is outside the requested mask but shares physical dictionary id 2 with selected position 4.
        assertThat(readBooleans(result.get(Stream.VALUES))).containsExactly(false, false, true, false, true);
    }

    @Test
    void testProjectedInputValuesCanStillLoadCompanionInputStreams()
    {
        Variable length = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        length,
                        new Call("length_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(length, Stream.VALUES),
                        new Reference(new Input(0), Stream.VALUES)));

        BinaryVector inputValues = new BinaryVector(2, 16);
        inputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        inputValues.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        inputValues.setNull(1);
        BooleanVector inputNulls = new BooleanVector(new boolean[] {false, true});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), inputValues,
                        new Reference(new Input(0), Stream.NULLS), inputNulls)),
                new Allocator(EngineResources.createDefault()));

        Streams lengths = evaluator.evaluate(new Reference(length, Stream.VALUES), Mask.all(2));
        assertThat(((I64Vector) lengths.get(Stream.VALUES)).values()).containsExactly(5L, 0L);
        assertThat(((BooleanVector) lengths.get(Stream.NULLS)).values()).containsExactly(false, true);

        Streams projectedInput = evaluator.evaluate(new Reference(new Input(0), Stream.VALUES), Mask.all(2));
        assertThat(utf8((BinaryVector) projectedInput.get(Stream.VALUES), 0)).isEqualTo("alpha");
        assertThat(((BooleanVector) projectedInput.get(Stream.NULLS)).values()).containsExactly(false, true);
    }

    @Test
    void testMultiplyPreservesNullsForProjectedValues()
    {
        Variable product = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        product,
                        new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(product, Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {7, 11}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {3, 5}))),
                new Allocator(EngineResources.createDefault()));

        Streams productStreams = evaluator.evaluate(new Reference(product, Stream.VALUES), Mask.all(2));
        assertThat(((I64Vector) productStreams.get(Stream.VALUES)).values()[0]).isEqualTo(21L);
        assertThat(((BooleanVector) productStreams.get(Stream.NULLS)).values()).containsExactly(false, true);
    }

    @Test
    void testContainsUtf8HandlesVectorCandidatesAcrossBoundaries()
    {
        Variable needle = new Variable(0);
        Variable contains = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(needle, new Literal("aaab"), AllMask.ALL),
                        new Assignment(
                                contains,
                                new Call("contains_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(needle, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(contains, Stream.VALUES)),
                Map.of(new Reference(contains, Stream.VALUES), new StreamPlan(MaterializationPolicy.MATERIALIZE, MemoizationPolicy.MEMOIZE)));

        BinaryVector input = new BinaryVector(6, 512);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        input.setBytes(0, ("x".repeat(31) + "aaab" + "tail").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(1, ("x".repeat(63) + "aaab").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(2, ("a".repeat(96) + "b").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(3, ("a".repeat(95) + "c").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // A candidate formed only by concatenating adjacent rows must not count as a match.
        input.setBytes(4, "aaa".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        input.setBytes(5, "b".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), input)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(contains, Stream.VALUES), Mask.all(6));
        assertThat(((BooleanVector) result.get(Stream.VALUES)).values()).containsExactly(true, true, true, false, false, false);
    }

    @Test
    void testExtractHostUtf8PreservesDictionaryEncoding()
    {
        Variable host = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        host,
                        new Call("extract_host_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(host, Stream.VALUES),
                        new Reference(host, Stream.NULLS)));

        BinaryVector dictionaryValues = new BinaryVector(6, 256);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionaryValues.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(2, "https://www.google.com/maps".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(3, "https://www.example.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(4, "ftp://www.example.com/path".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(5, "http://www./path".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector input = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 4, 5}, dictionaryValues);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, false, false, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), input,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), Mask.all(6));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector hosts = (DictionaryVector) result.values();
        assertThat(hosts.ids()).containsExactly(0, 1, 2, 3, 4, 5);
        BinaryVector extractedValues = (BinaryVector) hosts.values();
        assertThat(utf8(extractedValues, 0)).isEqualTo("google.com");
        assertThat(utf8(extractedValues, 1)).isEqualTo("news.ycombinator.com");
        assertThat(utf8(extractedValues, 2)).isEqualTo("google.com");
        assertThat(utf8(extractedValues, 3)).isEqualTo("https://www.example.com");
        assertThat(utf8(extractedValues, 4)).isEqualTo("ftp://www.example.com/path");
        assertThat(utf8(extractedValues, 5)).isEqualTo("www.");
        assertThat(extractedValues.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID)).isTrue();
        assertThat(extractedValues.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)).isTrue();
    }

    @Test
    void testReusesDeterministicImmutableDictionaryDomainAcrossBatches()
    {
        AtomicInteger invocations = new AtomicInteger();
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("counted_domain", new PrimitiveFunction()
        {
            @Override
            public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
            {
                return requestedOutputStreams.contains(Stream.VALUES) ? Set.of(Stream.VALUES) : Set.of();
            }

            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                invocations.incrementAndGet();
                VectorAccess.BinaryRegions values = VectorAccess.binaryRegions(inputs.getFirst().values());
                long[] result = new long[mask.size()];
                for (int position : mask) {
                    result[position] = values.length(position) * 10L;
                }
                return Streams.ofValues(new I64Vector(result));
            }
        }, EncodedDomainReuse.ENABLED);

        Variable result = new Variable(0);
        Reference input = new Reference(new Input(0), Stream.VALUES);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("counted_domain", List.of(input)), AllMask.ALL)),
                List.of(output));
        BinaryVector domain = utf8Vector("cat", "giraffe");
        domain.freezeContent();
        AtomicReference<DictionaryVector> current = new AtomicReference<>(DictionaryVector.wrap(new int[] {0, 1, 0}, domain));
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PlanEvaluator evaluator = planEvaluator(plan, registry, (reference, _) -> reference.equals(input) ? current.get() : null, allocator);

        DictionaryVector first = (DictionaryVector) evaluator.evaluate(output, Mask.all(3)).values();
        assertThat(readLongs(first)).containsExactly(30, 70, 30);
        evaluator.resetForReuse();

        BinaryVector equalDomain = utf8Vector("cat", "giraffe");
        equalDomain.freezeContent();
        current.set(DictionaryVector.wrap(new int[] {1, 1, 0, 1}, equalDomain));
        DictionaryVector second = (DictionaryVector) evaluator.evaluate(output, Mask.all(4)).values();
        assertThat(readLongs(second)).containsExactly(70, 70, 30, 70);
        assertThat(invocations).hasValue(1);

        evaluator.resetForReuse();
        BinaryVector replacementDomain = utf8Vector("horse", "hippopotamus");
        replacementDomain.freezeContent();
        current.set(DictionaryVector.wrap(new int[] {0, 1}, replacementDomain));
        DictionaryVector third = (DictionaryVector) evaluator.evaluate(output, Mask.all(2)).values();
        assertThat(readLongs(third)).containsExactly(50, 120);
        assertThat(invocations).hasValue(2);

        evaluator.resetForReuse();
        BinaryVector recurringDomain = utf8Vector("cat", "giraffe");
        recurringDomain.freezeContent();
        current.set(DictionaryVector.wrap(new int[] {1, 0}, recurringDomain));
        DictionaryVector fourth = (DictionaryVector) evaluator.evaluate(output, Mask.all(2)).values();
        assertThat(readLongs(fourth)).containsExactly(70, 30);
        assertThat(invocations).hasValue(2);

        evaluator.resetForReuse();
        BinaryVector collisionDomain = utf8Vector("a".repeat(100));
        collisionDomain.freezeContent();
        current.set(DictionaryVector.wrap(new int[] {0}, collisionDomain));
        evaluator.evaluate(output, Mask.all(1));
        assertThat(invocations).hasValue(3);

        evaluator.resetForReuse();
        BinaryVector distinctCollisionDomain = utf8Vector("a".repeat(50) + "b" + "a".repeat(49));
        distinctCollisionDomain.freezeContent();
        assertThat(distinctCollisionDomain.contentFingerprint()).isEqualTo(collisionDomain.contentFingerprint());
        current.set(DictionaryVector.wrap(new int[] {0}, distinctCollisionDomain));
        evaluator.evaluate(output, Mask.all(1));
        assertThat(invocations).hasValue(4);
        evaluator.close();
    }

    @Test
    void testPeelsDictionaryValuesAcrossConstantEncodedCompanionMapping()
    {
        AtomicInteger physicalPositions = new AtomicInteger();
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("counted_nullable_domain", new PrimitiveFunction()
        {
            @Override
            public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
            {
                return ALL_INPUT_STREAMS;
            }

            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                physicalPositions.addAndGet(mask.count());
                return Streams.ofValues(new I64Vector(mask.size()));
            }
        });

        Variable result = new Variable(0);
        Reference values = new Reference(new Input(0), Stream.VALUES);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("counted_nullable_domain", List.of(values)), AllMask.ALL)),
                List.of(output));
        int[] ids = {0, 1, 0, 1, 1, 0};
        PlanEvaluator evaluator = planEvaluator(
                plan,
                registry,
                inputResolver(Map.of(
                        values, DictionaryVector.wrap(ids, new I64Vector(new long[] {11, 22})),
                        new Reference(new Input(0), Stream.NULLS), DictionaryVector.wrap(new int[] {0, 0, 0, 0, 0, 0}, new BooleanVector(new boolean[] {false})))),
                new Allocator(EngineResources.createDefault()));

        assertThat(evaluator.evaluate(output, Mask.all(ids.length)).values()).isInstanceOf(DictionaryVector.class);
        assertThat(physicalPositions).hasValue(2);
    }

    @Test
    void testRegexpReplaceUtf8SparseMaskKeepsOffsetsAligned()
    {
        // A sparse mask must not shear the output: the BinaryVector offsets are a cumulative chain over ALL
        // positions, so skipped positions need their offsets filled forward (regression: masked regexp output
        // read back rotated bytes of neighboring values).
        Variable pattern = new Variable(0);
        Variable replacement = new Variable(1);
        Variable host = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(pattern, new Literal("^https?://(?:www\\.)?([^/]+)/.*$"), AllMask.ALL),
                        new Assignment(replacement, new Literal("\\1"), AllMask.ALL),
                        new Assignment(
                                host,
                                new Call("regexp_replace_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(pattern, Stream.VALUES),
                                        new Reference(replacement, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES)));

        BinaryVector values = new BinaryVector(5, 200);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        values.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "https://example.com/page".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(3, "http://tambov.irr.ru/0/c1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(4, "https://www.wildberries.ru/catalog".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Mask mask = Mask.sparse(new int[] {1, 3, 4}, 5);
        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), mask);
        BinaryVector rewritten = (BinaryVector) result.values();
        assertThat(utf8(rewritten, 1)).isEqualTo("news.ycombinator.com");
        assertThat(utf8(rewritten, 3)).isEqualTo("tambov.irr.ru");
        assertThat(utf8(rewritten, 4)).isEqualTo("wildberries.ru");
    }

    @Test
    void testIfUtf8SparseMaskKeepsOffsetsAligned()
    {
        // Same sparse-mask offsets-chain regression as regexp_replace: skipped positions must be filled forward.
        Variable selected = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(selected, new Call("if_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES)));

        BinaryVector trueValues = new BinaryVector(4, 64);
        trueValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        trueValues.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(1, "bravo".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(2, "charlie".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        BinaryVector falseValues = new BinaryVector(4, 64);
        falseValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        falseValues.setBytes(0, "w".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        falseValues.setBytes(1, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        falseValues.setBytes(2, "y".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        falseValues.setBytes(3, "z".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        org.weakref.nitro.data.BooleanVector conditions = new org.weakref.nitro.data.BooleanVector(new boolean[] {true, false, true, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), conditions,
                        new Reference(new Input(1), Stream.VALUES), trueValues,
                        new Reference(new Input(2), Stream.VALUES), falseValues)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(selected, Stream.VALUES), Mask.sparse(new int[] {1, 3}, 4));
        BinaryVector values = (BinaryVector) result.values();
        assertThat(utf8(values, 1)).isEqualTo("x");
        assertThat(utf8(values, 3)).isEqualTo("z");
    }

    @Test
    void testIfUtf8DoesNotSizeOutputFromNullBranchPayload()
    {
        Variable selected = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(selected, new Call("if_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(selected, Stream.VALUES)));

        BinaryVector trueValues = new BinaryVector(
                2,
                new int[] {0, 5, 5},
                "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        trueValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        // Bytes under a null position are semantically undefined. A lazy producer is therefore allowed to leave
        // offsets that cannot be used as a value; IF must inspect the selected null stream before sizing its output.
        BinaryVector falseValues = new BinaryVector(
                2,
                new int[] {0, 5, 0},
                new byte[5]);
        falseValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(2),
                        new Reference(new Input(1), Stream.VALUES), trueValues,
                        new Reference(new Input(1), Stream.NULLS), new BooleanVector(2),
                        new Reference(new Input(2), Stream.VALUES), falseValues,
                        new Reference(new Input(2), Stream.NULLS), new BooleanVector(new boolean[] {false, true}))),
                new Allocator(EngineResources.createDefault()));

        BinaryVector values = (BinaryVector) evaluator.evaluate(new Reference(selected, Stream.VALUES), Mask.all(2)).values();
        assertThat(utf8(values, 0)).isEqualTo("alpha");
        assertThat(values.length(1)).isZero();
    }

    @Test
    void testExtractHostUtf8SparseMaskKeepsOffsetsAligned()
    {
        Variable host = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(host, new Call("extract_host_utf8", List.of(new Reference(new Input(0), Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES)));

        BinaryVector values = new BinaryVector(4, 160);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        values.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "https://example.com/page".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(3, "http://tambov.irr.ru/0/c1".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), Mask.sparse(new int[] {1, 3}, 4));
        BinaryVector hosts = (BinaryVector) result.values();
        assertThat(utf8(hosts, 1)).isEqualTo("news.ycombinator.com");
        assertThat(utf8(hosts, 3)).isEqualTo("tambov.irr.ru");
    }

    @Test
    void testRegexpReplaceUtf8SupportsCaptureGroupReplacement()
    {
        Variable pattern = new Variable(0);
        Variable replacement = new Variable(1);
        Variable host = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(pattern, new Literal("^https?://(?:www\\.)?([^/]+)/.*$"), AllMask.ALL),
                        new Assignment(replacement, new Literal("\\1"), AllMask.ALL),
                        new Assignment(
                                host,
                                new Call("regexp_replace_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(pattern, Stream.VALUES),
                                        new Reference(replacement, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES)));

        BinaryVector dictionaryValues = new BinaryVector(4, 160);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionaryValues.setBytes(0, "https://www.google.com/search".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(1, "http://news.ycombinator.com/item".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(2, "https://example.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionaryValues.setBytes(3, "mailto:test@example.com".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector input = DictionaryVector.wrap(new int[] {0, 1, 2, 3}, dictionaryValues);
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), input)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(host, Stream.VALUES), Mask.all(4));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector hosts = (DictionaryVector) result.values();
        BinaryVector rewrittenValues = (BinaryVector) hosts.values();
        assertThat(utf8(rewrittenValues, hosts.ids()[0])).isEqualTo("google.com");
        assertThat(utf8(rewrittenValues, hosts.ids()[1])).isEqualTo("news.ycombinator.com");
        assertThat(utf8(rewrittenValues, hosts.ids()[2])).isEqualTo("https://example.com");
        assertThat(utf8(rewrittenValues, hosts.ids()[3])).isEqualTo("mailto:test@example.com");
    }

    @Test
    void testSpecializedHostExtractionMatchesJoniOnRegexEdgeCases()
    {
        String patternText = "^https?://(?:www\\.)?([^/]+)/.*$";
        String replacementText = "\\1";
        Variable pattern = new Variable(0);
        Variable replacement = new Variable(1);
        Variable host = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(pattern, new Literal(patternText), AllMask.ALL),
                        new Assignment(replacement, new Literal(replacementText), AllMask.ALL),
                        new Assignment(
                                host,
                                new Call("regexp_replace_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(pattern, Stream.VALUES),
                                        new Reference(replacement, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(host, Stream.VALUES)));

        String[] inputs = {
                "https://www.example.com/path",
                "http:///path",
                "http://svpressa.ru/path\nmore",
                "http://svpressa.ru/path\n",
                "http://svpressa.ru/path\n\n",
                "http://svpressa.ru/pa\rth",
                "http://svpressa.ru/path\r\n",
                "http://svpressa.ru\n/path",
                "http://www./path",
        };
        BinaryVector dictionaryValues = new BinaryVector(inputs.length, 512);
        dictionaryValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        for (int position = 0; position < inputs.length; position++) {
            dictionaryValues.setBytes(position, inputs[position].getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        DictionaryVector input = DictionaryVector.wrap(java.util.stream.IntStream.range(0, inputs.length).toArray(), dictionaryValues);
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), input)),
                new Allocator(EngineResources.createDefault()));

        DictionaryVector actual = (DictionaryVector) evaluator.evaluate(new Reference(host, Stream.VALUES), Mask.all(inputs.length)).values();
        BinaryVector actualValues = (BinaryVector) actual.values();
        io.airlift.joni.Regex patternRegex = org.weakref.nitro.function.scalar.builtin.JoniRegexpSupport.compile(
                io.airlift.slice.Slices.utf8Slice(patternText));
        io.airlift.slice.Slice replacementSlice = org.weakref.nitro.function.scalar.builtin.RegexpReplaceUtf8.translateReplacement(
                io.airlift.slice.Slices.utf8Slice(replacementText));
        for (int position = 0; position < inputs.length; position++) {
            String expected = org.weakref.nitro.function.scalar.builtin.JoniRegexpSupport.replace(
                    io.airlift.slice.Slices.utf8Slice(inputs[position]),
                    patternRegex,
                    replacementSlice,
                    org.weakref.nitro.function.scalar.builtin.JoniRegexpPolicy.defaults()).toStringUtf8();
            assertThat(utf8(actualValues, actual.ids()[position]))
                    .as("input at position %d", position)
                    .isEqualTo(expected);
        }
    }

    @Test
    void testUpperUtf8ProjectsUppercaseBytes()
    {
        Variable upper = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        upper,
                        new Call("upper_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(upper, Stream.VALUES),
                        new Reference(upper, Stream.NULLS)));

        BinaryVector input = new BinaryVector(3, 32);
        input.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        input.setBytes(0, "mixed".getBytes(UTF_8));
        input.setBytes(1, "mañana".getBytes(UTF_8));
        input.setNull(2);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, true});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), input,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Streams valuesResult = evaluator.evaluate(new Reference(upper, Stream.VALUES), Mask.all(3));
        Streams nullsResult = evaluator.evaluate(new Reference(upper, Stream.NULLS), Mask.all(3));

        BinaryVector values = (BinaryVector) valuesResult.get(Stream.VALUES);
        BooleanVector resultNulls = (BooleanVector) nullsResult.get(Stream.NULLS);
        assertThat(utf8(values, 0)).isEqualTo("MIXED");
        assertThat(utf8(values, 1)).isEqualTo("MAÑANA");
        assertThat(resultNulls.values()).containsExactly(false, false, true);
    }

    @Test
    void testCastUtf8ToI64ParsesSignedDigits()
    {
        Variable cast = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        cast,
                        new Call("cast_utf8_to_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(cast, Stream.VALUES),
                        new Reference(cast, Stream.NULLS)));

        BinaryVector input = new BinaryVector(3, 16);
        input.setBytes(0, "12345".getBytes(UTF_8));
        input.setBytes(1, "-7".getBytes(UTF_8));
        input.setNull(2);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, true});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), input,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Streams valuesResult = evaluator.evaluate(new Reference(cast, Stream.VALUES), Mask.all(3));
        Streams nullsResult = evaluator.evaluate(new Reference(cast, Stream.NULLS), Mask.all(3));

        I64Vector values = (I64Vector) valuesResult.get(Stream.VALUES);
        BooleanVector resultNulls = (BooleanVector) nullsResult.get(Stream.NULLS);
        assertThat(values.values()).containsExactly(12345L, -7L, 0L);
        assertThat(resultNulls.values()).containsExactly(false, false, true);
    }

    @Test
    void testStructFieldCombinesParentAndChildNulls()
    {
        Variable name = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        name,
                        new StructField(new Reference(new Input(0), Stream.VALUES), 0),
                        AllMask.ALL)),
                List.of(
                        new Reference(name, Stream.VALUES),
                        new Reference(name, Stream.NULLS)));

        StructVector person = new StructVector(4);
        BinaryVector names = new BinaryVector(4, 16);
        names.setBytes(0, "alice".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        names.setNull(1);
        names.setNull(2);
        names.setBytes(3, "carol".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        BooleanVector childNulls = new BooleanVector(new boolean[] {false, true, false, false});
        BooleanVector parentNulls = new BooleanVector(new boolean[] {false, false, true, false});
        person.setField("name", Streams.ofValues(names).with(Stream.NULLS, childNulls));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), person,
                        new Reference(new Input(0), Stream.NULLS), parentNulls)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(name, Stream.VALUES), Mask.all(4));
        BinaryVector values = (BinaryVector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(utf8(values, 0)).isEqualTo("alice");
        assertThat(utf8(values, 3)).isEqualTo("carol");
        assertThat(nulls.values()).containsExactly(false, true, true, false);
    }

    @Test
    void testMapLookupCombinesMapKeyAndEntryNulls()
    {
        Variable lookup = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        lookup,
                        new Call("element_at_i64_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(lookup, Stream.VALUES),
                        new Reference(lookup, Stream.NULLS)));

        MapVector maps = new MapVector(4);
        maps.offsets()[0] = 0;
        maps.offsets()[1] = 2;
        maps.offsets()[2] = 2;
        maps.offsets()[3] = 3;
        maps.offsets()[4] = 4;

        BinaryVector mapKeys = new BinaryVector(4, 19);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        mapKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(3, "zeta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        I64Vector mapValues = new I64Vector(new long[] {10, 0, 30, 0});
        BooleanVector mapValueNulls = new BooleanVector(new boolean[] {false, true, false, true});
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(mapValues).with(Stream.NULLS, mapValueNulls));

        BinaryVector lookupKeys = new BinaryVector(4, 20);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        lookupKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(1, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(2, "missing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setNull(3);

        BooleanVector mapNulls = new BooleanVector(new boolean[] {false, true, false, false});
        BooleanVector lookupNulls = new BooleanVector(new boolean[] {false, false, false, true});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), maps,
                        new Reference(new Input(0), Stream.NULLS), mapNulls,
                        new Reference(new Input(1), Stream.VALUES), lookupKeys,
                        new Reference(new Input(1), Stream.NULLS), lookupNulls)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(lookup, Stream.VALUES), Mask.all(4));
        I64Vector values = (I64Vector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(values.values()).containsExactly(10L, 0L, 0L, 0L);
        assertThat(nulls.values()).containsExactly(false, true, true, true);
    }

    @Test
    void testMapLookupPropagatesInputErrors()
    {
        Variable lookup = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        lookup,
                        new Call("element_at_i64_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(lookup, Stream.ERRORS)));

        MapVector maps = new MapVector(3);
        BinaryVector mapKeys = new BinaryVector(0, 0);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(new I64Vector(new long[0])));

        BinaryVector lookupKeys = new BinaryVector(3, 3);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        lookupKeys.setBytes(0, "a".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(1, "b".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(2, "c".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BooleanVector mapErrors = new BooleanVector(new boolean[] {false, true, false});
        BooleanVector lookupErrors = new BooleanVector(new boolean[] {true, false, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), maps,
                        new Reference(new Input(0), Stream.ERRORS), mapErrors,
                        new Reference(new Input(1), Stream.VALUES), lookupKeys,
                        new Reference(new Input(1), Stream.ERRORS), lookupErrors)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(lookup, Stream.ERRORS), Mask.all(3));
        BooleanVector errors = (BooleanVector) result.get(Stream.ERRORS);
        assertThat(errors.values()).containsExactly(true, true, false);
    }

    @Test
    void testMapContainsKeyCombinesNullsAndErrors()
    {
        Variable contains = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        contains,
                        new Call("map_contains_key_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(contains, Stream.VALUES),
                        new Reference(contains, Stream.NULLS),
                        new Reference(contains, Stream.ERRORS)));

        MapVector maps = new MapVector(4);
        maps.offsets()[0] = 0;
        maps.offsets()[1] = 2;
        maps.offsets()[2] = 2;
        maps.offsets()[3] = 3;
        maps.offsets()[4] = 4;

        BinaryVector mapKeys = new BinaryVector(4, 19);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        mapKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        mapKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        mapKeys.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        maps.setEntries(Streams.ofValues(mapKeys), Streams.ofValues(new I64Vector(new long[] {10, 20, 30, 40})));

        BinaryVector lookupKeys = new BinaryVector(4, 22);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        lookupKeys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        lookupKeys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(1, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(2, "missing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        lookupKeys.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BooleanVector mapNulls = new BooleanVector(new boolean[] {false, true, false, false});
        BooleanVector keyNulls = new BooleanVector(new boolean[] {false, false, true, false});
        BooleanVector mapErrors = new BooleanVector(new boolean[] {false, false, false, true});
        BooleanVector keyErrors = new BooleanVector(new boolean[] {false, false, false, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), maps,
                        new Reference(new Input(0), Stream.NULLS), mapNulls,
                        new Reference(new Input(0), Stream.ERRORS), mapErrors,
                        new Reference(new Input(1), Stream.VALUES), lookupKeys,
                        new Reference(new Input(1), Stream.NULLS), keyNulls,
                        new Reference(new Input(1), Stream.ERRORS), keyErrors)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(contains, Stream.VALUES), Mask.all(4));
        BooleanVector values = (BooleanVector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);
        BooleanVector errors = (BooleanVector) result.get(Stream.ERRORS);

        assertThat(values.values()).containsExactly(true, false, false, true);
        assertThat(nulls.values()).containsExactly(false, true, true, false);
        assertThat(errors.values()).containsExactly(false, false, false, true);
    }

    @Test
    void testArrayElementCombinesParentIndexAndElementSemantics()
    {
        Variable element = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        element,
                        new Call("array_element_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(element, Stream.VALUES),
                        new Reference(element, Stream.NULLS),
                        new Reference(element, Stream.ERRORS)));

        ArrayVector arrays = new ArrayVector(6);
        arrays.offsets()[0] = 0;
        arrays.offsets()[1] = 3;
        arrays.offsets()[2] = 3;
        arrays.offsets()[3] = 4;
        arrays.offsets()[4] = 6;
        arrays.offsets()[5] = 7;
        arrays.offsets()[6] = 8;

        I64Vector elementValues = new I64Vector(new long[] {10, 0, 30, 40, 50, 60, 0, 70});
        BooleanVector elementNulls = new BooleanVector(new boolean[] {false, true, false, false, false, false, false, false});
        BooleanVector elementErrors = new BooleanVector(new boolean[] {false, false, false, false, false, true, false, false});
        arrays.setElements(Streams.ofValues(elementValues)
                .with(Stream.NULLS, elementNulls)
                .with(Stream.ERRORS, elementErrors));

        I64Vector indices = new I64Vector(new long[] {0, 0, 0, 5, 1, 0});
        BooleanVector arrayNulls = new BooleanVector(new boolean[] {false, false, true, false, false, false});
        BooleanVector indexNulls = new BooleanVector(new boolean[] {false, false, false, false, true, false});
        BooleanVector arrayErrors = new BooleanVector(new boolean[] {false, false, false, false, false, true});
        BooleanVector indexErrors = new BooleanVector(new boolean[] {false, false, false, false, false, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), arrays,
                        new Reference(new Input(0), Stream.NULLS), arrayNulls,
                        new Reference(new Input(0), Stream.ERRORS), arrayErrors,
                        new Reference(new Input(1), Stream.VALUES), indices,
                        new Reference(new Input(1), Stream.NULLS), indexNulls,
                        new Reference(new Input(1), Stream.ERRORS), indexErrors)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(element, Stream.VALUES), Mask.all(6));
        I64Vector values = (I64Vector) result.get(Stream.VALUES);
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);
        BooleanVector errors = (BooleanVector) result.get(Stream.ERRORS);

        assertThat(values.values()).containsExactly(10L, 0L, 0L, 0L, 0L, 70L);
        assertThat(nulls.values()).containsExactly(false, true, true, true, true, false);
        assertThat(errors.values()).containsExactly(false, false, false, false, false, true);
    }

    @Test
    void testMapKeysDoesNotReuseNestedOutputForSparseMasks()
    {
        PrimitiveFunction mapKeys = primitiveRegistry().get("map_keys");
        MapVector maps = createUtf8I64MapVector();
        BooleanVector inputNulls = new BooleanVector(new boolean[] {false, true, false, false});

        ArrayVector reusableValues = new ArrayVector(4);
        reusableValues.offsets()[0] = 99;
        reusableValues.offsets()[1] = 99;
        reusableValues.offsets()[2] = 99;
        reusableValues.offsets()[3] = 99;
        reusableValues.offsets()[4] = 99;
        reusableValues.setElements(Streams.ofValues(new BinaryVector(1, 8)));
        BooleanVector reusableNulls = new BooleanVector(new boolean[] {true, true, true, true});

        Streams output = Streams.of(Stream.VALUES, reusableValues)
                .with(Stream.NULLS, reusableNulls);

        Streams result = mapKeys.apply(
                List.of(Streams.ofValuesAndNulls(maps, inputNulls)),
                Mask.sparse(new int[] {1, 3}, maps.length()),
                Set.of(Stream.VALUES, Stream.NULLS),
                output,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(result.values()).isNotSameAs(reusableValues);
        assertThat(result.get(Stream.NULLS)).isNotSameAs(reusableNulls);
        assertThat(reusableValues.offsets()).containsExactly(99, 99, 99, 99, 99);
        assertThat(reusableNulls.values()).containsExactly(true, true, true, true);

        ArrayVector arrays = (ArrayVector) result.values();
        BinaryVector keys = (BinaryVector) arrays.elementValues();
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(arrays.offsets()).containsExactly(0, 0, 1, 1, 3);
        assertThat(utf8(keys, 0)).isEqualTo("gamma");
        assertThat(utf8(keys, 1)).isEqualTo("delta");
        assertThat(utf8(keys, 2)).isEqualTo("epsilon");
        assertThat(nulls.values()).containsExactly(false, true, false, false);
    }

    @Test
    void testMapValuesDoesNotReuseNestedOutputForSparseMasks()
    {
        PrimitiveFunction mapValues = primitiveRegistry().get("map_values");
        MapVector maps = createUtf8I64MapVector();
        BooleanVector inputNulls = new BooleanVector(new boolean[] {false, true, false, false});

        ArrayVector reusableValues = new ArrayVector(4);
        reusableValues.offsets()[0] = 77;
        reusableValues.offsets()[1] = 77;
        reusableValues.offsets()[2] = 77;
        reusableValues.offsets()[3] = 77;
        reusableValues.offsets()[4] = 77;
        reusableValues.setElements(Streams.ofValues(new I64Vector(new long[] {999})));
        BooleanVector reusableNulls = new BooleanVector(new boolean[] {true, true, true, true});

        Streams output = Streams.of(Stream.VALUES, reusableValues)
                .with(Stream.NULLS, reusableNulls);

        Streams result = mapValues.apply(
                List.of(Streams.ofValuesAndNulls(maps, inputNulls)),
                Mask.sparse(new int[] {0, 2}, maps.length()),
                Set.of(Stream.VALUES, Stream.NULLS),
                output,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(result.values()).isNotSameAs(reusableValues);
        assertThat(result.get(Stream.NULLS)).isNotSameAs(reusableNulls);
        assertThat(reusableValues.offsets()).containsExactly(77, 77, 77, 77, 77);
        assertThat(reusableNulls.values()).containsExactly(true, true, true, true);

        ArrayVector arrays = (ArrayVector) result.values();
        I64Vector values = (I64Vector) arrays.elementValues();
        BooleanVector elementNulls = arrays.elementNulls();
        BooleanVector nulls = (BooleanVector) result.get(Stream.NULLS);

        assertThat(arrays.offsets()).containsExactly(0, 2, 2, 2, 2);
        assertThat(values.values()).containsExactly(10L, 20L);
        assertThat(elementNulls.values()).containsExactly(false, false);
        assertThat(nulls.values()).containsExactly(false, false, false, false);
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
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

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
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(result.values()).isInstanceOf(RleVector.class);
        RleVector vector = (RleVector) result.values();
        assertThat(vector.counts()).containsExactly(1, 1, 2);
        assertThat(((BooleanVector) vector.values()).values()).containsExactly(true, true, false);
    }

    @Test
    void testIntegralLessThanOrEqualSupportsNullsAndRleInputs()
    {
        PrimitiveFunction lessThanOrEqual = new LessThanOrEqualI64();
        Streams result = lessThanOrEqual.apply(
                List.of(
                        Streams.ofValuesAndNulls(
                                new RleVector(new int[] {2, 2}, new I64Vector(new long[] {1, 5})),
                                new BooleanVector(new boolean[] {false, true, false, false})),
                        Streams.ofValues(new RleVector(new int[] {1, 3}, new I64Vector(new long[] {1, 4})))),
                Mask.all(4),
                Set.of(Stream.VALUES, Stream.NULLS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(readBooleans(result.values())).containsExactly(true, true, false, false);
        assertThat(readBooleans(result.get(Stream.NULLS))).containsExactly(false, true, false, false);
    }

    @Test
    void testDivideScaleRoundFunctionRoundsScaledDivision()
    {
        PrimitiveFunction divideScaleRound = builtinPrimitiveRegistry().get("divide_scale_round_i64");

        Streams result = divideScaleRound.apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 5})),
                        Streams.ofValues(new I64Vector(new long[] {6, 3, 2})),
                        Streams.ofValues(new I64Vector(new long[] {100, 100, 10}))),
                Mask.all(3),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(((I64Vector) result.values()).values()).containsExactly(17L, 67L, 25L);
    }

    @Test
    void testAddFunctionSupportsDictionaryInputs()
    {
        PrimitiveFunction add = builtinPrimitiveRegistry().get("add");

        Streams flatDictionary = add.apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4})),
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        Streams dictionaryFlat = add.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}))),
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4}))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        Streams dictionaryDictionary = add.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}))),
                        Streams.ofValues(new DictionaryVector(new int[] {1, 1, 0, 0}, new I64Vector(new long[] {1, 2})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(((I64Vector) flatDictionary.values()).values()).containsExactly(31L, 12L, 23L, 34L);
        assertThat(((I64Vector) dictionaryFlat.values()).values()).containsExactly(31L, 12L, 23L, 34L);
        assertThat(((I64Vector) dictionaryDictionary.values()).values()).containsExactly(32L, 12L, 21L, 31L);
    }

    @Test
    void testBooleanFunctionSupportsDictionaryInputs()
    {
        PrimitiveFunction or = primitiveRegistry().get("or");

        Streams dictionaryFlat = or.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new BooleanVector(new boolean[] {false, true, false}))),
                        Streams.ofValues(new BooleanVector(new boolean[] {false, false, true, false}))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        Streams dictionaryDictionary = or.apply(
                List.of(
                        Streams.ofValues(new DictionaryVector(new int[] {2, 0, 1, 2}, new BooleanVector(new boolean[] {false, true, false}))),
                        Streams.ofValues(new DictionaryVector(new int[] {1, 1, 0, 0}, new BooleanVector(new boolean[] {false, true})))),
                Mask.all(4),
                Set.of(Stream.VALUES),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(((BooleanVector) dictionaryFlat.values()).values()).containsExactly(false, false, true, false);
        assertThat(((BooleanVector) dictionaryDictionary.values()).values()).containsExactly(true, true, true, false);
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
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(addExact.values()).isInstanceOf(RleVector.class);
        assertThat(((I64Vector) ((RleVector) addExact.values()).values()).values()).containsExactly(11L, 21L, 25L);
        assertThat(addExact.get(Stream.ERRORS)).isInstanceOf(RleVector.class);
        assertThat(((BooleanVector) ((RleVector) addExact.get(Stream.ERRORS)).values()).values()).containsExactly(false, false, false);

        Streams subtractExact = primitiveRegistry.get("subtract_exact").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 2}, new I64Vector(new long[] {10, 30}))),
                        Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4}))),
                Mask.all(4),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

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
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(((I64Vector) addExact.values()).values()).containsExactly(Long.MIN_VALUE, 3L);
        assertThat(((BooleanVector) addExact.get(Stream.ERRORS)).values()).containsExactly(true, false);

        Streams subtractExact = primitiveRegistry.get("subtract_exact").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {Long.MIN_VALUE, 10})),
                        Streams.ofValues(new I64Vector(new long[] {1, 3}))),
                Mask.all(2),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

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
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(((I64Vector) divide.values()).values()).containsExactly(4L, 0L, 11L);
        assertThat(((BooleanVector) divide.get(Stream.ERRORS)).values()).containsExactly(false, true, false);

        Streams modulo = primitiveRegistry.get("modulo").apply(
                List.of(
                        Streams.ofValues(new RleVector(new int[] {2, 1}, new I64Vector(new long[] {20, 22}))),
                        Streams.ofValues(new I64Vector(new long[] {6, 0, 5}))),
                Mask.all(3),
                Set.of(Stream.VALUES, Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(((I64Vector) modulo.values()).values()).containsExactly(2L, 0L, 2L);
        assertThat(((BooleanVector) modulo.get(Stream.ERRORS)).values()).containsExactly(false, true, false);
    }

    @Test
    void testDivideCanProduceOnlyErrorsStream()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();

        Streams divide = primitiveRegistry.get("divide").apply(
                List.of(
                        Streams.ofValues(new I64Vector(new long[] {20, 21, 22})),
                        Streams.ofValues(new I64Vector(new long[] {5, 0, 2}))),
                Mask.all(3),
                Set.of(Stream.ERRORS),
                null,
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(divide.has(Stream.VALUES)).isFalse();
        assertThat(((BooleanVector) divide.get(Stream.ERRORS)).values()).containsExactly(false, true, false);
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
                                new ReferenceMask(new Reference(new Input(0), org.weakref.nitro.data.Stream.VALUES)),
                                new Reference(new Input(1), org.weakref.nitro.data.Stream.VALUES),
                                new Reference(new Input(2), org.weakref.nitro.data.Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, org.weakref.nitro.data.Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new RleVector(new int[] {2, 2}, new I64Vector(new long[] {10, 20})),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}))), new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, org.weakref.nitro.data.Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(10L, 1L, 20L, 1L);
    }

    @Test
    void testCopyCanForwardDictionaryInput()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        DictionaryVector dictionary = new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30}));
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Copy(new Reference(new Input(0), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), dictionary)), new Allocator(EngineResources.createDefault()));

        assertThat(evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES)).isSameAs(dictionary);
    }

    @Test
    void testMergeCopiesDictionaryInputsIntoMaskedOutput()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true, false}),
                new Reference(new Input(1), Stream.VALUES), new DictionaryVector(new int[] {2, 0, 1, 2}, new I64Vector(new long[] {10, 20, 30})),
                new Reference(new Input(2), Stream.VALUES), new DictionaryVector(new int[] {1, 1, 0, 0}, new I64Vector(new long[] {1, 2})))), new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(30L, 2L, 20L, 1L);
    }

    @Test
    void testMergePreservesSharedDictionaryDomain()
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        int[] ids = {0, 1, 0, 2, 1, 2};
        DictionaryVector condition = DictionaryVector.wrap(ids, new BooleanVector(new boolean[] {true, false, true}));
        DictionaryVector trueValues = DictionaryVector.wrap(ids, new I64Vector(new long[] {10, 20, 30}));
        RleVector falseValues = new RleVector(new int[] {ids.length}, new I64Vector(new long[] {99}));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                new PrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), condition,
                        new Reference(new Input(1), Stream.VALUES), trueValues,
                        new Reference(new Input(2), Stream.VALUES), falseValues)),
                new Allocator(EngineResources.createDefault()));

        var resultVector = evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(ids.length)).values();
        assertThat(resultVector).isInstanceOf(DictionaryVector.class);
        DictionaryVector dictionary = (DictionaryVector) resultVector;
        assertThat(dictionary.ids()).isSameAs(ids);
        assertThat(dictionary.hasDomainFrequencies()).isTrue();
        assertThat(dictionary.domainFrequency(0)).isEqualTo(2);
        assertThat(dictionary.domainFrequency(1)).isEqualTo(2);
        assertThat(dictionary.domainFrequency(2)).isEqualTo(2);
        assertThat(readLongs(dictionary)).containsExactly(10, 99, 10, 30, 99, 30);

        DictionaryVector transferable = (DictionaryVector) evaluator.prepareResultForTransfer(dictionary);
        assertThat(transferable.ids()).isNotSameAs(ids);
        assertThat(transferable.hasDomainFrequencies()).isTrue();
        assertThat(transferable.domainFrequency(0)).isEqualTo(2);
        assertThat(transferable.domainFrequency(1)).isEqualTo(2);
        assertThat(transferable.domainFrequency(2)).isEqualTo(2);
    }

    @Test
    void testMergeForwardsUniformEncodedBranch()
    {
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                new Reference(new Input(1), Stream.VALUES),
                                new Reference(new Input(2), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        int[] ids = {0, 1, 0, 1};
        RleVector selected = new RleVector(new int[] {ids.length}, new I64Vector(new long[] {11}));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                new PrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES),
                        DictionaryVector.wrap(ids, new BooleanVector(new boolean[] {true, true})),
                        new Reference(new Input(1), Stream.VALUES), selected)),
                new Allocator(EngineResources.createDefault()));

        assertThat(evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(ids.length)).values())
                .isSameAs(selected);
    }

    @Test
    void testNestedDictionaryMergeTreatsNullStreamAsConditionValues()
    {
        Variable nullable = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                nullable,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(new Input(2), Stream.VALUES)),
                                AllMask.ALL),
                        new Assignment(
                                result,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new NotMask(new ReferenceMask(new Reference(nullable, Stream.NULLS))),
                                        new Reference(nullable, Stream.VALUES),
                                        new Reference(new Input(3), Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        int[] ids = {0, 1, 0, 2, 3, 1};
        PlanEvaluator evaluator = planEvaluator(
                plan,
                new PrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES),
                        DictionaryVector.wrap(ids, new BooleanVector(new boolean[] {true, false, false, false})),
                        new Reference(new Input(1), Stream.VALUES),
                        new RleVector(new int[] {ids.length}, new I64Vector(new long[] {0})),
                        new Reference(new Input(1), Stream.NULLS),
                        new RleVector(new int[] {ids.length}, new BooleanVector(new boolean[] {true})),
                        new Reference(new Input(2), Stream.VALUES),
                        DictionaryVector.wrap(ids, new I64Vector(new long[] {0, 1, 2, 3})),
                        new Reference(new Input(3), Stream.VALUES),
                        DictionaryVector.wrap(ids, new I64Vector(new long[] {0, 5, 6, 7})))),
                new Allocator(EngineResources.createDefault()));

        var resultVector = evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(ids.length)).values();
        assertThat(resultVector).isInstanceOf(DictionaryVector.class);
        DictionaryVector dictionary = (DictionaryVector) resultVector;
        assertThat(dictionary.ids()).isSameAs(ids);
        assertThat(readLongs(dictionary)).containsExactly(0, 1, 0, 2, 3, 1);
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
                                new OrMask(List.of(
                                        new AndMask(List.of(new ReferenceMask(left), new NotMask(new ReferenceMask(right)))),
                                        new AndMask(List.of(new NotMask(new ReferenceMask(left)), new ReferenceMask(right))))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, false, false}),
                new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {false, true, true, false}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 2L);
    }

    @Test
    void testConstantRleBooleanMaskDoesNotScanOrAllocate()
    {
        Reference input = new Reference(new Input(0), Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(List.of(), List.of());
        RleVector values = new RleVector(new int[] {10_000}, new BooleanVector(new boolean[] {true}));
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PlanEvaluator evaluator = planEvaluator(
                plan,
                new PrimitiveRegistry(),
                inputResolver(Map.of(input, values)),
                allocator);
        Mask mask = Mask.sparse(new int[] {1, 10, 100, 1_000, 9_999}, 10_000);

        long allocatedBytes = allocator.allocatedBytes();
        Mask result = evaluator.evaluate(new ReferenceMask(input), mask);

        assertThat(result).isSameAs(mask);
        assertThat(allocator.allocatedBytes()).isEqualTo(allocatedBytes);
    }

    @Test
    void testEvaluatesMergeConditionThroughBooleanReferenceMask()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable leftOnly = new Variable(0);
        Variable rightOnly = new Variable(1);
        Variable predicate = new Variable(2);
        Variable result = new Variable(3);
        Reference left = new Reference(new Input(0), Stream.VALUES);
        Reference right = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(leftOnly, new Call("lt", List.of(left, new Reference(new Input(2), Stream.VALUES))), AllMask.ALL),
                        new Assignment(rightOnly, new Call("lt", List.of(new Reference(new Input(3), Stream.VALUES), right)), AllMask.ALL),
                        new Assignment(predicate, new Call("or", List.of(
                                new Reference(leftOnly, Stream.VALUES),
                                new Reference(rightOnly, Stream.VALUES))), AllMask.ALL),
                        new Assignment(result, new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new ReferenceMask(new Reference(predicate, Stream.VALUES)),
                                new Reference(new Input(4), Stream.VALUES),
                                new Reference(new Input(5), Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)),
                Map.of(),
                Map.of(
                        new Reference(predicate, Stream.VALUES),
                        new OrMask(List.of(
                                new ReferenceMask(new Reference(leftOnly, Stream.VALUES)),
                                new ReferenceMask(new Reference(rightOnly, Stream.VALUES))))));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                left, new I64Vector(new long[] {1, 5, 7, 3}),
                right, new I64Vector(new long[] {9, 2, 6, 1}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {4, 4, 4, 4}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {4, 4, 4, 4}),
                new Reference(new Input(4), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(5), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 2L, 1L, 1L);
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

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {20, 21, 22}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {5, 0, 2}))), new Allocator(EngineResources.createDefault()));

        BooleanVector errorsVector = (BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS);
        assertThat(errorsVector.values()).containsExactly(false, true, false);
    }

    @Test
    void testSynthesizesAbsentInputNullsAndErrors()
    {
        Reference inputNulls = new Reference(new Input(0), Stream.NULLS);
        Reference inputErrors = new Reference(new Input(0), Stream.ERRORS);
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(List.of(), List.of(inputNulls, inputErrors)),
                new PrimitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}))),
                new Allocator(EngineResources.createDefault()));

        assertThat(readBooleans(evaluator.evaluate(inputNulls, Mask.all(3)).get(Stream.NULLS))).containsExactly(false, false, false);
        assertThat(readBooleans(evaluator.evaluate(inputErrors, Mask.all(3)).get(Stream.ERRORS))).containsExactly(false, false, false);
    }

    @Test
    void testForwardsKnownEmptyErrorsWithoutMerging()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("no_local_errors", new PrimitiveFunction()
        {
            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                return Streams.empty();
            }

            @Override
            public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
            {
                return ALL_INPUT_STREAMS;
            }
        });
        Allocator allocator = new Allocator(EngineResources.createDefault());
        var noErrors = allocator.borrowAllFalseBoolean(new Allocator.Context("test"), 3);
        Variable result = new Variable(0);
        Reference leftValues = new Reference(new Input(0), Stream.VALUES);
        Reference rightValues = new Reference(new Input(1), Stream.VALUES);
        Reference resultErrors = new Reference(result, Stream.ERRORS);
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(
                        List.of(new Assignment(result, new Call("no_local_errors", List.of(leftValues, rightValues)), AllMask.ALL)),
                        List.of(resultErrors)),
                primitiveRegistry,
                inputResolver(Map.of(
                        leftValues, new I64Vector(new long[] {1, 2, 3}),
                        rightValues, new I64Vector(new long[] {4, 5, 6}),
                        new Reference(new Input(0), Stream.ERRORS), noErrors,
                        new Reference(new Input(1), Stream.ERRORS), noErrors)),
                allocator);

        assertThat(evaluator.evaluate(resultErrors, Mask.all(3)).get(Stream.ERRORS)).isSameAs(noErrors);
    }

    @Test
    void testForwardsInputErrorsPastDictionaryEncodedEmptyErrors()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("dictionary_empty_errors", new PrimitiveFunction()
        {
            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                return Streams.of(
                        Stream.ERRORS,
                        DictionaryVector.wrap(
                                new int[] {0, 0, 0},
                                new BooleanVector(new boolean[] {false})));
            }

            @Override
            public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
            {
                return ALL_INPUT_STREAMS;
            }
        });
        BooleanVector inputErrors = new BooleanVector(new boolean[] {false, true, false});
        Variable result = new Variable(0);
        Reference inputValues = new Reference(new Input(0), Stream.VALUES);
        Reference resultErrors = new Reference(result, Stream.ERRORS);
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(
                        List.of(new Assignment(result, new Call("dictionary_empty_errors", List.of(inputValues)), AllMask.ALL)),
                        List.of(resultErrors)),
                primitiveRegistry,
                inputResolver(Map.of(
                        inputValues, new I64Vector(new long[] {1, 2, 3}),
                        new Reference(new Input(0), Stream.ERRORS), inputErrors)),
                new Allocator(EngineResources.createDefault()));

        assertThat(evaluator.evaluate(resultErrors, Mask.all(3)).get(Stream.ERRORS)).isSameAs(inputErrors);
    }

    @Test
    void testCopyOfErrorsRequestsOnlyErrors()
    {
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("source", (inputs, mask, requested, output, context) -> {
            requestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
        });

        Variable source = new Variable(0);
        Variable copied = new Variable(1);
        Reference copiedErrors = new Reference(copied, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(source, new Call("source", List.of()), AllMask.ALL),
                        new Assignment(copied, new org.weakref.nitro.operator.evaluator.ir.Copy(new Reference(source, Stream.ERRORS)), AllMask.ALL)),
                List.of(copiedErrors));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(copiedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(requestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testCopyOfValuesCanProjectSiblingErrors()
    {
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("source", (inputs, mask, requested, output, context) -> {
            requestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable source = new Variable(0);
        Variable copied = new Variable(1);
        Reference copiedErrors = new Reference(copied, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(source, new Call("source", List.of()), AllMask.ALL),
                        new Assignment(copied, new org.weakref.nitro.operator.evaluator.ir.Copy(new Reference(source, Stream.VALUES)), AllMask.ALL)),
                List.of(copiedErrors));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(copiedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(requestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testMergeOfErrorsRequestsOnlyErrors()
    {
        AtomicReference<Set<Stream>> trueRequestedStreams = new AtomicReference<>(Set.of());
        AtomicReference<Set<Stream>> falseRequestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("when_true", (inputs, mask, requested, output, context) -> {
            trueRequestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {true, true, false}));
        });
        primitiveRegistry.register("when_false", (inputs, mask, requested, output, context) -> {
            falseRequestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {false, false, true}));
        });

        Variable whenTrue = new Variable(0);
        Variable whenFalse = new Variable(1);
        Variable merged = new Variable(2);
        Reference mergedErrors = new Reference(merged, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(whenTrue, new Call("when_true", List.of()), AllMask.ALL),
                        new Assignment(whenFalse, new Call("when_false", List.of()), AllMask.ALL),
                        new Assignment(
                                merged,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new Reference(whenTrue, Stream.ERRORS),
                                        new Reference(whenFalse, Stream.ERRORS)),
                                AllMask.ALL)),
                List.of(mergedErrors));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true}))), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(mergedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(true, false, false);
        assertThat(trueRequestedStreams.get()).containsExactly(Stream.ERRORS);
        assertThat(falseRequestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testMergePreservesSelectedRichErrorDiagnostics()
    {
        ErrorValue diagnostic = new ErrorValue("test", 23, "TRUE_BRANCH", "USER_ERROR", "true branch");
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("when_true", (_, _, _, _, _) -> {
            ErrorVector errors = new ErrorVector(3);
            errors.setError(0, diagnostic);
            errors.setError(1, new ErrorValue("test", 24, "UNSELECTED", "USER_ERROR", "unselected"));
            return Streams.of(Stream.ERRORS, errors);
        });
        primitiveRegistry.register("when_false", (_, _, _, _, _) ->
                Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false})));

        Variable whenTrue = new Variable(0);
        Variable whenFalse = new Variable(1);
        Variable merged = new Variable(2);
        Reference mergedErrors = new Reference(merged, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(whenTrue, new Call("when_true", List.of()), AllMask.ALL),
                        new Assignment(whenFalse, new Call("when_false", List.of()), AllMask.ALL),
                        new Assignment(
                                merged,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new Reference(whenTrue, Stream.ERRORS),
                                        new Reference(whenFalse, Stream.ERRORS)),
                                AllMask.ALL)),
                List.of(mergedErrors));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new BooleanVector(new boolean[] {true, false, true}))),
                new Allocator(EngineResources.createDefault()));

        ErrorVector errors = (ErrorVector) evaluator.evaluate(mergedErrors, Mask.all(3)).get(Stream.ERRORS);

        assertThat(errors.values()).containsExactly(true, true, false);
        assertThat(errors.error(0)).isEqualTo(diagnostic);
        assertThat(errors.error(1)).isNull();
    }

    @Test
    void testMergeOfValuesCanProjectSiblingErrors()
    {
        AtomicReference<Set<Stream>> trueRequestedStreams = new AtomicReference<>(Set.of());
        AtomicReference<Set<Stream>> falseRequestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("when_true", (inputs, mask, requested, output, context) -> {
            trueRequestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 1, 1}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {true, true, false}));
            }
            return result;
        });
        primitiveRegistry.register("when_false", (inputs, mask, requested, output, context) -> {
            falseRequestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {2, 2, 2}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, false, true}));
            }
            return result;
        });

        Variable whenTrue = new Variable(0);
        Variable whenFalse = new Variable(1);
        Variable merged = new Variable(2);
        Reference mergedErrors = new Reference(merged, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(whenTrue, new Call("when_true", List.of()), AllMask.ALL),
                        new Assignment(whenFalse, new Call("when_false", List.of()), AllMask.ALL),
                        new Assignment(
                                merged,
                                new org.weakref.nitro.operator.evaluator.ir.Merge(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new Reference(whenTrue, Stream.VALUES),
                                        new Reference(whenFalse, Stream.VALUES)),
                                AllMask.ALL)),
                List.of(mergedErrors));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, false, true}))), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(mergedErrors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(true, false, false);
        assertThat(trueRequestedStreams.get()).containsExactly(Stream.ERRORS);
        assertThat(falseRequestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testRequestsOnlyErrorsWhenOnlyErrorsAreProjected()
    {
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("counting", (inputs, mask, requested, output, context) -> {
            requestedStreams.set(Set.copyOf(requested));
            return Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
        });

        Variable result = new Variable(0);
        Reference errors = new Reference(result, Stream.ERRORS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("counting", List.of()), AllMask.ALL)),
                List.of(errors),
                Map.of(errors, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(requestedStreams.get()).containsExactly(Stream.ERRORS);
    }

    @Test
    void testMemoizesSiblingValueAndErrorStreamsTogether()
    {
        AtomicInteger evaluations = new AtomicInteger();
        AtomicReference<Set<Stream>> requestedStreams = new AtomicReference<>(Set.of());
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("counting", (inputs, mask, requested, output, context) -> {
            evaluations.incrementAndGet();
            requestedStreams.set(Set.copyOf(requested));
            Streams result = Streams.empty();
            if (requested.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requested.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
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

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(((I64Vector) evaluator.evaluate(values, Mask.all(3)).get(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(evaluations).hasValue(1);
        assertThat(requestedStreams.get()).containsExactlyInAnyOrder(Stream.VALUES, Stream.ERRORS);
    }

    @Test
    void testEvaluatesNullsStreamDirectly()
    {
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable", (inputs, mask, requestedStreams, output, context) -> {
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requestedStreams.contains(Stream.NULLS)) {
                result = result.with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference nulls = new Reference(result, Stream.NULLS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("nullable", List.of()), AllMask.ALL)),
                List.of(nulls),
                Map.of(nulls, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        BooleanVector nullsVector = (BooleanVector) evaluator.evaluate(nulls, Mask.all(3)).get(Stream.NULLS);
        assertThat(nullsVector.values()).containsExactly(false, true, false);
    }

    @Test
    void testMemoizesSiblingValueAndNullStreamsTogether()
    {
        AtomicInteger evaluations = new AtomicInteger();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("nullable", (inputs, mask, requestedStreams, output, context) -> {
            evaluations.incrementAndGet();
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new I64Vector(new long[] {1, 2, 3}));
            }
            if (requestedStreams.contains(Stream.NULLS)) {
                result = result.with(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference values = new Reference(result, Stream.VALUES);
        Reference nulls = new Reference(result, Stream.NULLS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("nullable", List.of()), AllMask.ALL)),
                List.of(values, nulls),
                Map.of(
                        values, StreamPlan.MATERIALIZED,
                        nulls, StreamPlan.MATERIALIZED));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(nulls, Mask.all(3)).get(Stream.NULLS)).values()).containsExactly(false, true, false);
        assertThat(((I64Vector) evaluator.evaluate(values, Mask.all(3)).get(Stream.VALUES)).values()).containsExactly(1L, 2L, 3L);
        assertThat(evaluations).hasValue(1);
    }

    @Test
    void testDoesNotMemoizeScratchOnlyMaskBundle()
    {
        AtomicInteger evaluations = new AtomicInteger();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("predicate", (inputs, mask, requestedStreams, output, context) -> {
            evaluations.incrementAndGet();
            Streams result = Streams.empty();
            if (requestedStreams.contains(Stream.VALUES)) {
                result = result.with(Stream.VALUES, new BooleanVector(new boolean[] {true, false, true}));
            }
            if (requestedStreams.contains(Stream.ERRORS)) {
                result = result.with(Stream.ERRORS, new BooleanVector(new boolean[] {false, true, false}));
            }
            return result;
        });

        Variable result = new Variable(0);
        Reference values = new Reference(result, Stream.VALUES);
        Reference errors = new Reference(result, Stream.ERRORS);
        EvaluationPlan normalizedPlan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(new Assignment(result, new Call("predicate", List.of()), AllMask.ALL)),
                List.of(),
                Map.of(
                        values, StreamPlan.MATERIALIZED,
                        errors, StreamPlan.MATERIALIZED),
                Map.of(values, new ReferenceMask(values))));

        PlanEvaluator evaluator = planEvaluator(normalizedPlan, primitiveRegistry, inputResolver(Map.of()), new Allocator(EngineResources.createDefault()));

        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(((BooleanVector) evaluator.evaluate(errors, Mask.all(3)).get(Stream.ERRORS)).values()).containsExactly(false, true, false);
        assertThat(evaluations).hasValue(2);
    }

    @Test
    void testAdaptiveAndReorderingUsesMoreSelectiveTermFirstOnLaterRuns()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        Reference first = new Reference(new Input(0), Stream.VALUES);
        Reference second = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new AndMask(List.of(new ReferenceMask(first), new ReferenceMask(second))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        AtomicReference<List<Integer>> maskSizes = new AtomicReference<>(List.of());
        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, (reference, mask) -> {
            if (reference.equals(first) || reference.equals(second)) {
                maskSizes.updateAndGet(existing -> {
                    var updated = new java.util.ArrayList<>(existing);
                    updated.add(mask.selectedCount());
                    return List.copyOf(updated);
                });
            }

            if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {true, true, true, true, true, false, false, false});
            }
            if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {false, false, false, false, true, false, false, false});
            }
            if (reference.equals(new Reference(new Input(2), Stream.VALUES))) {
                return new I64Vector(new long[] {1, 1, 1, 1, 1, 1, 1, 1});
            }
            if (reference.equals(new Reference(new Input(3), Stream.VALUES))) {
                return new I64Vector(new long[] {2, 2, 2, 2, 2, 2, 2, 2});
            }
            return null;
        }, new Allocator(EngineResources.createDefault()));

        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 5);

        maskSizes.set(List.of());
        evaluator.reset();
        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 1);
    }

    @Test
    void testAdaptiveOrReorderingUsesMoreSelectiveTermFirstOnLaterRuns()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        Reference first = new Reference(new Input(0), Stream.VALUES);
        Reference second = new Reference(new Input(1), Stream.VALUES);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new OrMask(List.of(new ReferenceMask(first), new ReferenceMask(second))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        AtomicReference<List<Integer>> maskSizes = new AtomicReference<>(List.of());
        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, (reference, mask) -> {
            if (reference.equals(first) || reference.equals(second)) {
                maskSizes.updateAndGet(existing -> {
                    var updated = new java.util.ArrayList<>(existing);
                    updated.add(mask.selectedCount());
                    return List.copyOf(updated);
                });
            }

            if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {true, false, false, false, false, false, false, false});
            }
            if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                return new BooleanVector(new boolean[] {true, true, true, true, true, false, false, false});
            }
            if (reference.equals(new Reference(new Input(2), Stream.VALUES))) {
                return new I64Vector(new long[] {1, 1, 1, 1, 1, 1, 1, 1});
            }
            if (reference.equals(new Reference(new Input(3), Stream.VALUES))) {
                return new I64Vector(new long[] {2, 2, 2, 2, 2, 2, 2, 2});
            }
            return null;
        }, new Allocator(EngineResources.createDefault()));

        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 7);

        maskSizes.set(List.of());
        evaluator.reset();
        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(8));
        assertThat(maskSizes.get()).containsExactly(8, 3);
    }

    @Test
    void testResetReleasesPrimitiveScratchContexts()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new Call(
                                "add",
                                List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {1, 2, 3}),
                new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {4, 5, 6}))), allocator);

        evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(3));
        assertThat(allocator.currentBytes(new Allocator.Context("AddI64"))).isPositive();

        evaluator.reset();

        assertThat(allocator.currentBytes(new Allocator.Context("AddI64"))).isZero();
        assertThat(allocator.currentBytes(new Allocator.Context("PlanEvaluator"))).isZero();
    }

    @Test
    void testAndMaskKeepsNullAndErrorRowsOutOfFinalTrueResult()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new AndMask(List.of(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}),
                new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, true}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator(EngineResources.createDefault()));

        Streams output = evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4));
        I64Vector resultVector = (I64Vector) output.get(Stream.VALUES);
        // The value in an error row is undefined; only the error stream is observable there.
        assertThat(resultVector.values()).endsWith(2L, 1L, 2L);
        assertThat(((BooleanVector) output.get(Stream.ERRORS)).values()).containsExactly(true, false, false, false);
    }

    @Test
    void testPredicateValueAndCompanionStreamsAreEvaluatedTogether()
    {
        AtomicInteger invocations = new AtomicInteger();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register("counted_predicate", new PrimitiveFunction()
        {
            @Override
            public Streams apply(
                    List<Streams> inputs,
                    Mask mask,
                    Set<Stream> requestedStreams,
                    Streams output,
                    PrimitiveExecutionContext context)
            {
                invocations.incrementAndGet();
                return Streams.of(
                        new BooleanVector(new boolean[] {true, true, true, true}),
                        new BooleanVector(new boolean[] {false, true, false, false}),
                        new BooleanVector(new boolean[] {false, false, false, true}));
            }
        });

        Variable result = new Variable(0);
        Reference resultValues = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("counted_predicate", List.of()), AllMask.ALL)),
                List.of(resultValues));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of()),
                new Allocator(EngineResources.createDefault()));

        Mask resultMask = evaluator.evaluate(new ReferenceMask(resultValues), Mask.all(4));
        assertThat(resultMask.selectedCount()).isEqualTo(2);
        assertThat(resultMask.position(0)).isEqualTo(0);
        assertThat(resultMask.position(1)).isEqualTo(2);
        assertThat(invocations).hasValue(1);
    }

    @Test
    void testDirectAndMaskEvaluationDropsRowsThatAreNotUltimatelyTrue()
    {
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                        new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, true}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(
                new AndMask(List.of(
                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                Mask.all(4));

        assertThat(result.selectedCount()).isEqualTo(1);
        assertThat(result.position(0)).isEqualTo(2);
    }

    @Test
    void testDirectNotMaskEvaluationKeepsOnlyDefiniteFalseRows()
    {
        Variable predicate = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        predicate,
                        new Call("contains_utf8", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of());

        BinaryVector haystack = new BinaryVector(5, 64);
        haystack.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        haystack.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        haystack.setBytes(0, "google".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(1, "bing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(2, "maps.google".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(3, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        haystack.setBytes(4, "ask".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BooleanVector haystackNulls = new BooleanVector(new boolean[] {false, false, false, true, false});
        BinaryVector needle = new BinaryVector(1, 16);
        needle.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        needle.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        needle.setBytes(0, "google".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), haystack,
                        new Reference(new Input(0), Stream.NULLS), haystackNulls,
                        new Reference(new Input(1), Stream.VALUES), new RleVector(new int[] {5}, needle))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(
                new NotMask(new ReferenceMask(new Reference(predicate, Stream.VALUES))),
                Mask.all(5));

        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(1);
        assertThat(result.position(1)).isEqualTo(4);
    }

    @Test
    void testDirectNotMaskEvaluationOptimizesSimpleLongEquality()
    {
        Variable zero = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                equals,
                                new Call("eq", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {0, 1, 0, 5, 7}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, false, true, false, false}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(
                new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))),
                Mask.all(5));

        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(1);
        assertThat(result.position(1)).isEqualTo(3);
        assertThat(result.position(2)).isEqualTo(4);
    }

    @Test
    void testCoalesceI64ReplacesNullWithFallbackValue()
    {
        Variable zero = new Variable(0);
        Variable value = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                value,
                                new Call("coalesce_i64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(
                        new Reference(value, Stream.VALUES),
                        new Reference(value, Stream.NULLS)));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {7L, 0L, 9L}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false}))),
                new Allocator(EngineResources.createDefault()));

        Streams valuesResult = evaluator.evaluate(new Reference(value, Stream.VALUES), Mask.all(3));
        assertThat(((I64Vector) valuesResult.get(Stream.VALUES)).values()).containsExactly(7L, 0L, 9L);
        assertThat(((BooleanVector) valuesResult.get(Stream.NULLS)).values()).containsExactly(false, false, false);
    }

    @Test
    void testCoalesceI64UsesExplicitNullElisionPolicy()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            List<Streams> inputs = List.of(
                    Streams.ofValues(new I64Vector(new long[] {7L, 8L})),
                    Streams.ofValues(new I64Vector(new long[] {0L, 0L})));
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);

            Streams retainedNulls = new CoalesceI64(new CoalesceI64Policy(false, false))
                    .apply(inputs, Mask.all(2), Set.of(Stream.NULLS), null, context);
            Streams omittedNulls = new CoalesceI64(new CoalesceI64Policy(true, false))
                    .apply(inputs, Mask.all(2), Set.of(Stream.NULLS), null, context);

            assertThat(retainedNulls.has(Stream.NULLS)).isTrue();
            assertThat(((BooleanVector) retainedNulls.get(Stream.NULLS)).values())
                    .containsExactly(false, false);
            assertThat(omittedNulls.has(Stream.NULLS)).isFalse();
        }
    }

    @Test
    void testMultiplyNullAsZeroI64CoalescesEitherNullInput()
    {
        Variable value = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        value,
                        new Call("multiply_null_as_zero_i64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(value, Stream.VALUES),
                        new Reference(value, Stream.NULLS)));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {10, 20, 30, 40}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, true}),
                        new Reference(new Input(1), Stream.VALUES), new I32Vector(new int[] {2, 3, 4, 5}),
                        new Reference(new Input(1), Stream.NULLS), new BooleanVector(new boolean[] {false, false, true, true}))),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(value, Stream.VALUES), Mask.all(4));
        assertThat(((I64Vector) result.get(Stream.VALUES)).values()).containsExactly(20L, 0L, 0L, 0L);
        assertThat(readBooleans(result.get(Stream.NULLS))).containsExactly(false, false, false, false);
    }

    @Test
    void testCastI64ToI32ProjectsDictionaryEncodedValues()
    {
        Variable castValue = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        castValue,
                        new Call("cast_i64_to_i32", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(castValue, Stream.VALUES)));

        int[] ids = {2, 0, 1, 2};
        DictionaryVector values = DictionaryVector.wrap(ids, new I64Vector(new long[] {7L, 11L, 13L}));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(castValue, Stream.VALUES), Mask.all(4));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector encoded = (DictionaryVector) result.values();
        assertThat(encoded.ids()).isSameAs(ids);
        assertThat(encoded.ids()).containsExactly(2, 0, 1, 2);
        assertThat(((I32Vector) encoded.values()).values()).containsExactly(7, 11, 13);

        DictionaryVector transferable = (DictionaryVector) evaluator.prepareResultForTransfer(encoded);
        assertThat(transferable.ids()).isNotSameAs(ids);
        ids[0] = 0;
        assertThat(transferable.ids()).containsExactly(2, 0, 1, 2);
    }

    @Test
    void testCastI64ToI64ProjectsDictionaryEncodedValues()
    {
        Variable castValue = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        castValue,
                        new Call("cast_i64_to_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(castValue, Stream.VALUES)));

        int[] ids = {2, 0, 1, 2};
        DictionaryVector values = DictionaryVector.wrap(ids, new I64Vector(new long[] {7L, 11L, 13L}));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(castValue, Stream.VALUES), Mask.all(4));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector encoded = (DictionaryVector) result.values();
        assertThat(encoded.ids()).isSameAs(ids);
        assertThat(encoded.ids()).containsExactly(2, 0, 1, 2);
        assertThat(((I64Vector) encoded.values()).values()).containsExactly(7L, 11L, 13L);
    }

    @Test
    void testCastI64ToI32DoesNotRequestInputNullsWhenOnlyValuesAreNeeded()
    {
        Variable castValue = new Variable(0);
        Variable zero = new Variable(1);
        Variable sum = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                castValue,
                                new Call("cast_i64_to_i32", List.of(new Reference(new Input(0), Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(zero, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                sum,
                                new Call("add", List.of(
                                        new Reference(castValue, Stream.VALUES),
                                        new Reference(zero, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(sum, Stream.VALUES)));

        AtomicBoolean requestedNulls = new AtomicBoolean();
        DictionaryVector values = DictionaryVector.wrap(new int[] {1, 0, 1}, new I64Vector(new long[] {17L, 29L}));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        requestedNulls.set(true);
                        throw new AssertionError("cast_i64_to_i32 should not request input nulls for values-only output");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return values;
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(sum, Stream.VALUES), Mask.all(3));
        assertThat(result.get(Stream.VALUES)).isInstanceOf(DictionaryVector.class);
        DictionaryVector encoded = (DictionaryVector) result.get(Stream.VALUES);
        assertThat(encoded.ids()).containsExactly(1, 0, 1);
        assertThat(((I64Vector) encoded.values()).values()).containsExactly(17L, 29L);
        assertThat(requestedNulls).isFalse();
    }

    @Test
    void testIsNullI64RequestsOnlyInputNullStream()
    {
        Variable isNull = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        isNull,
                        new Call("is_null_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(isNull, Stream.VALUES)));

        AtomicBoolean requestedValues = new AtomicBoolean();
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        requestedValues.set(true);
                        throw new AssertionError("is_null_i64 should not request the VALUES stream");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, true, false});
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(isNull, Stream.VALUES), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.VALUES)).values()).containsExactly(false, true, false);
        assertThat(requestedValues).isFalse();
    }

    @Test
    void testIsNullI64ReadsDenseDictionaryNullStream()
    {
        Variable isNull = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        isNull,
                        new Call("is_null_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(isNull, Stream.VALUES)));
        DictionaryVector nulls = DictionaryVector.wrap(
                new int[] {1, 0, 1, 1, 0},
                new BooleanVector(new boolean[] {false, true}));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> reference.equals(new Reference(new Input(0), Stream.NULLS)) ? nulls : null,
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(isNull, Stream.VALUES), Mask.all(5));

        assertThat(((BooleanVector) result.get(Stream.VALUES)).values())
                .containsExactly(true, false, true, true, false);
    }

    @Test
    void testIsNullI32ReadsDenseDictionaryNullStream()
    {
        Variable isNull = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        isNull,
                        new Call("is_null_i32", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(isNull, Stream.VALUES)));
        DictionaryVector nulls = DictionaryVector.wrap(
                new int[] {0, 1, 1, 0},
                new BooleanVector(new boolean[] {false, true}));
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> reference.equals(new Reference(new Input(0), Stream.NULLS)) ? nulls : null,
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(isNull, Stream.VALUES), Mask.all(4));

        assertThat(((BooleanVector) result.get(Stream.VALUES)).values())
                .containsExactly(false, true, true, false);
    }

    @Test
    void testIsNullI64CompactsTrueAndFalseMasksInPlaceFromFlatNulls()
    {
        Variable isNull = new Variable(0);
        Reference resultReference = new Reference(isNull, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        isNull,
                        new Call("is_null_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(resultReference));
        AtomicBoolean requestedValues = new AtomicBoolean();
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        requestedValues.set(true);
                        return new I64Vector(6);
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, true, false, true, true, false});
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Mask nullRows = Mask.sparse(new int[] {0, 1, 2, 4, 5}, 6);
        assertThat(evaluator.evaluateInPlace(new ReferenceMask(resultReference), nullRows)).isSameAs(nullRows);
        assertThat(nullRows.selectedCount()).isEqualTo(2);
        assertThat(nullRows.position(0)).isEqualTo(1);
        assertThat(nullRows.position(1)).isEqualTo(4);

        Mask nonNullRows = Mask.all(6);
        assertThat(evaluator.evaluateInPlace(new NotMask(new ReferenceMask(resultReference)), nonNullRows)).isSameAs(nonNullRows);
        assertThat(nonNullRows.selectedCount()).isEqualTo(3);
        assertThat(nonNullRows.position(0)).isEqualTo(0);
        assertThat(nonNullRows.position(1)).isEqualTo(2);
        assertThat(nonNullRows.position(2)).isEqualTo(5);
        assertThat(requestedValues).isFalse();
    }

    @Test
    void testIsNullI64DelegatesInPlaceMaskToPhysicalInputNullStream()
    {
        org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("nitro.expression.inputMaskResolver", "true")));
        org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("nitro.expression.directPrimitiveInputMask", "true")));

        Variable isNull = new Variable(0);
        Reference resultReference = new Reference(isNull, Stream.VALUES);
        Reference inputNulls = new Reference(new Input(0), Stream.NULLS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        isNull,
                        new Call("is_null_i64", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(resultReference));
        AtomicInteger vectorResolveCount = new AtomicInteger();
        AtomicInteger maskResolveCount = new AtomicInteger();
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                new PlanEvaluator.InputResolver()
                {
                    @Override
                    public org.weakref.nitro.data.Vector resolve(Reference reference, Mask mask)
                    {
                        vectorResolveCount.incrementAndGet();
                        throw new AssertionError("direct IS NULL mask evaluation must not resolve a vector");
                    }

                    @Override
                    public Mask resolveMask(Reference reference, Mask mask, boolean selectTrue, Allocator allocator, Allocator.Context context)
                    {
                        assertThat(reference).isEqualTo(inputNulls);
                        maskResolveCount.incrementAndGet();
                        return selectTrue
                                ? allocator.allocateSparseMask(context, new int[] {1, 4}, 2, mask.size())
                                : allocator.allocateSparseMask(context, new int[] {0, 2, 5}, 3, mask.size());
                    }
                },
                new Allocator(EngineResources.createDefault()));

        Mask nullRows = Mask.sparse(new int[] {0, 1, 2, 4, 5}, 6);
        assertThat(evaluator.evaluateInPlace(new ReferenceMask(resultReference), nullRows)).isSameAs(nullRows);
        assertThat(nullRows).containsExactly(1, 4);

        Mask nonNullRows = Mask.all(6);
        assertThat(evaluator.evaluateInPlace(new NotMask(new ReferenceMask(resultReference)), nonNullRows)).isSameAs(nonNullRows);
        assertThat(nonNullRows).containsExactly(0, 2, 5);
        assertThat(vectorResolveCount).hasValue(0);
        assertThat(maskResolveCount).hasValue(2);
    }

    @Test
    void testDivideI64ToF64ProjectsFloatingPointAverage()
    {
        Variable average = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        average,
                        new Call("divide_i64_to_f64", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(
                        new Reference(average, Stream.VALUES),
                        new Reference(average, Stream.NULLS)));

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {9L, 5L}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {2L, 0L}))),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(average, Stream.VALUES), Mask.all(2));
        assertThat(((F64Vector) result.get(Stream.VALUES)).values()).containsExactly(4.5, 0.0);
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true);
    }

    @Test
    void testSubtractExactErrorPathDoesNotRequestInputNulls()
    {
        Variable difference = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        difference,
                        new Call("subtract_exact", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(difference, Stream.ERRORS)));

        AtomicBoolean requestedNulls = new AtomicBoolean();
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS)) || reference.equals(new Reference(new Input(1), Stream.NULLS))) {
                        requestedNulls.set(true);
                        throw new AssertionError("subtract_exact error-only path should not request input nulls");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return new I64Vector(new long[] {Long.MIN_VALUE, 10L});
                    }
                    if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                        return new I64Vector(new long[] {1L, 3L});
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(difference, Stream.ERRORS), Mask.all(2));
        assertThat(((BooleanVector) result.get(Stream.ERRORS)).values()).containsExactly(true, false);
        assertThat(requestedNulls).isFalse();
    }

    @Test
    void testLengthUtf8DoesNotRequestInputNullsWhenOnlyValuesAreNeeded()
    {
        Variable length = new Variable(0);
        Variable one = new Variable(1);
        Variable shifted = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(
                                length,
                                new Call("length_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(one, new Literal(1L), AllMask.ALL),
                        new Assignment(
                                shifted,
                                new Call("add", List.of(
                                        new Reference(length, Stream.VALUES),
                                        new Reference(one, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(shifted, Stream.VALUES)));

        AtomicBoolean requestedNulls = new AtomicBoolean();
        BinaryVector values = utf8Vector("go", "nitro", "x");
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        requestedNulls.set(true);
                        throw new AssertionError("length_utf8 values-only path should not request input nulls");
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return values;
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(shifted, Stream.VALUES), Mask.all(3));
        assertThat(((I64Vector) result.get(Stream.VALUES)).values()).containsExactly(3L, 6L, 2L);
        assertThat(requestedNulls).isFalse();
    }

    @Test
    void testLengthUtf8AcceptsNestedDictionary()
    {
        Variable length = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        length,
                        new Call("length_utf8", List.of(new Reference(new Input(0), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(length, Stream.VALUES)));
        DictionaryVector inner = DictionaryVector.wrap(
                new int[] {0, 1, 0},
                utf8Vector("a", "nitro"));
        DictionaryVector outer = DictionaryVector.wrapNested(
                new int[] {0, 1, 2, 0, 2, 1},
                6,
                inner);
        Allocator allocator = new Allocator(EngineResources.createDefault());
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), outer)),
                allocator);

        Vector result = evaluator.evaluate(new Reference(length, Stream.VALUES), Mask.all(6)).values();
        assertThat(readLongs(result)).containsExactly(1, 5, 1, 1, 1, 5);
        assertThat(result).isInstanceOf(DictionaryVector.class);
        DictionaryVector outerResult = (DictionaryVector) result;
        assertThat(outerResult.ids()).isSameAs(outer.ids());
        assertThat(outerResult.values()).isInstanceOf(DictionaryVector.class);
        assertThat(((DictionaryVector) outerResult.values()).ids()).isSameAs(inner.ids());
        assertThat(allocator.allocatedVectorBytesByType()).doesNotContainKey("DictionaryVector");
    }

    @Test
    void testEqualMaskEvaluationRequestsInputNulls()
    {
        Variable seven = new Variable(0);
        Variable equalsSeven = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(seven, new Literal(7L), AllMask.ALL),
                        new Assignment(
                                equalsSeven,
                                new Call("eq", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(seven, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        AtomicBoolean requestedNulls = new AtomicBoolean();
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        return new I64Vector(new long[] {7L, 7L, 8L});
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        requestedNulls.set(true);
                        return new BooleanVector(new boolean[] {false, true, false});
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(equalsSeven, Stream.VALUES)), Mask.all(3));
        assertThat(result.selectedCount()).isEqualTo(1);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(requestedNulls).isTrue();
    }

    @Test
    void testLessThanNullEvaluationStillRequestsInputValues()
    {
        Variable threshold = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(threshold, new Literal(10L), AllMask.ALL),
                        new Assignment(
                                lessThan,
                                new Call("lt", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(threshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(lessThan, Stream.NULLS)));

        AtomicBoolean requestedValues = new AtomicBoolean();
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        requestedValues.set(true);
                        return new I64Vector(new long[] {1L, 2L, 3L});
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, true, false});
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(lessThan, Stream.NULLS), Mask.all(3));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, false);
        assertThat(requestedValues).isTrue();
    }

    @Test
    void testScaledRelativeDifferenceGtI64OptimizesQuarterlyDeviationMask()
    {
        Variable ten = new Variable(0);
        Variable deviationLarge = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(ten, new Literal(10L), AllMask.ALL),
                        new Assignment(
                                deviationLarge,
                                new Call("scaled_relative_difference_gt_i64", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(ten, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        PlanEvaluator evaluator = planEvaluator(
                plan,
                builtinPrimitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new I64Vector(new long[] {100L, 105L, 80L, 100L}),
                        new Reference(new Input(1), Stream.VALUES), new I64Vector(new long[] {90L, 100L, 100L, 0L}),
                        new Reference(new Input(1), Stream.NULLS), new BooleanVector(new boolean[] {false, false, false, false}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(deviationLarge, Stream.VALUES)), Mask.all(4));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testMultiplyNullEvaluationStillRequestsInputValues()
    {
        Variable product = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        product,
                        new Call("multiply", List.of(
                                new Reference(new Input(0), Stream.VALUES),
                                new Reference(new Input(1), Stream.VALUES))),
                        AllMask.ALL)),
                List.of(new Reference(product, Stream.NULLS)));

        AtomicBoolean requestedLeftValues = new AtomicBoolean();
        AtomicBoolean requestedRightValues = new AtomicBoolean();
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                (reference, mask) -> {
                    if (reference.equals(new Reference(new Input(0), Stream.VALUES))) {
                        requestedLeftValues.set(true);
                        return new I64Vector(new long[] {3L, 4L});
                    }
                    if (reference.equals(new Reference(new Input(1), Stream.VALUES))) {
                        requestedRightValues.set(true);
                        return new I64Vector(new long[] {5L, 6L});
                    }
                    if (reference.equals(new Reference(new Input(0), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, true});
                    }
                    if (reference.equals(new Reference(new Input(1), Stream.NULLS))) {
                        return new BooleanVector(new boolean[] {false, false});
                    }
                    return null;
                },
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(product, Stream.NULLS), Mask.all(2));
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true);
        assertThat(requestedLeftValues).isTrue();
        assertThat(requestedRightValues).isTrue();
    }

    @Test
    void testOrMaskAllowsLaterTrueToSuppressNullAndError()
    {
        PrimitiveRegistry primitiveRegistry = builtinPrimitiveRegistry();
        Variable result = new Variable(0);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(
                        result,
                        new org.weakref.nitro.operator.evaluator.ir.Merge(
                                new OrMask(List.of(
                                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                                new Reference(new Input(2), Stream.VALUES),
                                new Reference(new Input(3), Stream.VALUES)),
                        AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        PlanEvaluator evaluator = planEvaluator(plan, primitiveRegistry, inputResolver(Map.of(
                new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, false, false}),
                new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}),
                new Reference(new Input(2), Stream.VALUES), new I64Vector(new long[] {1, 1, 1, 1}),
                new Reference(new Input(3), Stream.VALUES), new I64Vector(new long[] {2, 2, 2, 2}))), new Allocator(EngineResources.createDefault()));

        I64Vector resultVector = (I64Vector) evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4)).get(Stream.VALUES);
        assertThat(resultVector.values()).containsExactly(1L, 1L, 1L, 2L);
    }

    @Test
    void testDirectOrMaskEvaluationKeepsRowsThatBecomeTrueLater()
    {
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(List.of(), List.of()),
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), new BooleanVector(new boolean[] {true, true, false, false}),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, true, false, false}),
                        new Reference(new Input(0), Stream.ERRORS), new BooleanVector(new boolean[] {true, false, false, false}),
                        new Reference(new Input(1), Stream.VALUES), new BooleanVector(new boolean[] {true, true, true, false}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(
                new OrMask(List.of(
                        new ReferenceMask(new Reference(new Input(0), Stream.VALUES)),
                        new ReferenceMask(new Reference(new Input(1), Stream.VALUES)))),
                Mask.all(4));

        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(1);
        assertThat(result.position(2)).isEqualTo(2);
    }

    @Test
    void testUtf8DictionaryLiteralReferenceMaskUsesPrimitiveTrueMask()
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal(""), AllMask.ALL),
                        new Assignment(
                                equals,
                                new Call("eq_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1}, dictionary);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(equals, Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testUtf8DictionaryLiteralNotMaskUsesPrimitiveFalseMask()
    {
        Variable literal = new Variable(0);
        Variable equals = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal(""), AllMask.ALL),
                        new Assignment(
                                equals,
                                new Call("eq_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1}, dictionary);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(1);
        assertThat(result.position(1)).isEqualTo(4);
    }

    @Test
    void testDirectUtf8MaskCompletesDerivedValueArguments()
    {
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable leftPrefix = new Variable(2);
        Variable rightPrefix = new Variable(3);
        Variable equals = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(5L), AllMask.ALL),
                        new Assignment(
                                leftPrefix,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(
                                rightPrefix,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(1), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(
                                equals,
                                new Call("eq_utf8", List.of(
                                        new Reference(leftPrefix, Stream.VALUES),
                                        new Reference(rightPrefix, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), utf8Vector("12345-a", "54321-a", "99999-a"),
                        new Reference(new Input(1), Stream.VALUES), utf8Vector("12345-b", "00000-b", "99999-b"))),
                new Allocator(EngineResources.createDefault()));

        // Seed a narrower cached materialization, as happens when a preceding join/filter branch requests one
        // derived argument before the enclosing predicate is evaluated over the complete surviving mask.
        evaluator.evaluate(new Reference(leftPrefix, Stream.VALUES), Mask.sparse(new int[] {1}, 3));

        Mask result = evaluator.evaluate(
                new NotMask(new ReferenceMask(new Reference(equals, Stream.VALUES))),
                Mask.all(3));
        assertThat(result).containsExactly(1);
    }

    @Test
    void testDictionaryLiteralCallIsPeeledAndRewrappedByEvaluator()
    {
        Variable literal = new Variable(0);
        Variable lessThan = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal("m"), AllMask.ALL),
                        new Assignment(
                                lessThan,
                                new Call("lt_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(lessThan, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "android".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "iphone".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1}, dictionary);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(lessThan, Stream.VALUES), Mask.all(5));
        assertThat(result.values()).isInstanceOf(DictionaryVector.class);

        DictionaryVector encoded = (DictionaryVector) result.values();
        assertThat(encoded.ids()).containsExactly(0, 1, 0, 2, 1);
        assertThat(encoded.hasDomainFrequencies()).isFalse();
        BooleanVector dictionaryValues = (BooleanVector) encoded.values();
        assertThat(dictionaryValues.values()).containsExactly(true, true, false);
    }

    @Test
    void testDictionaryPeelingUsesCachedBooleanStreamClassification()
    {
        AtomicInteger allFalseClassifications = new AtomicInteger();
        BooleanVector knownAllFalse = new BooleanVector(new boolean[6])
        {
            @Override
            public boolean isAllFalse()
            {
                allFalseClassifications.incrementAndGet();
                return true;
            }

            @Override
            public boolean isAllTrue()
            {
                throw new AssertionError("known all-false stream must not be classified again");
            }
        };
        AtomicInteger evaluatedPositions = new AtomicInteger();
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("dictionary_identity", new PrimitiveFunction()
        {
            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                evaluatedPositions.set(mask.count());
                return inputs.getFirst();
            }

            @Override
            public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
            {
                return ALL_INPUT_STREAMS;
            }
        });

        Variable result = new Variable(0);
        Reference inputValues = new Reference(new Input(0), Stream.VALUES);
        Reference resultValues = new Reference(result, Stream.VALUES);
        int[] ids = {0, 1, 0, 1, 0, 1};
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(
                        List.of(new Assignment(result, new Call("dictionary_identity", List.of(inputValues)), AllMask.ALL)),
                        List.of(resultValues)),
                registry,
                inputResolver(Map.of(
                        inputValues, DictionaryVector.wrap(ids, new I64Vector(new long[] {11, 29})),
                        new Reference(new Input(0), Stream.NULLS), knownAllFalse)),
                new Allocator(EngineResources.createDefault()));

        Streams streams = evaluator.evaluate(resultValues, Mask.all(ids.length));

        assertThat(evaluatedPositions).hasValue(2);
        assertThat(allFalseClassifications).hasValue(1);
        assertThat(streams.values()).isInstanceOf(DictionaryVector.class);
        assertThat(((DictionaryVector) streams.values()).ids()).isSameAs(ids);
    }

    @Test
    void testDictionaryPeelingReusesAndClearsWideDomainSelectionScratch()
    {
        AtomicReference<List<Integer>> selectedDomains = new AtomicReference<>();
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("record_domains", recordSelectedDomains(selectedDomains, 1));

        Variable result = new Variable(0);
        Reference input = new Reference(new Input(0), Stream.VALUES);
        Reference output = new Reference(result, Stream.VALUES);
        int[] ids = new int[256];
        long[] domain = new long[128];
        for (int position = 0; position < ids.length; position++) {
            ids[position] = position % domain.length;
        }
        for (int position = 0; position < domain.length; position++) {
            domain[position] = position;
        }

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            PlanEvaluator evaluator = planEvaluator(
                    new EvaluationPlan(
                            List.of(new Assignment(result, new Call("record_domains", List.of(input)), AllMask.ALL)),
                            List.of(output)),
                    registry,
                    inputResolver(Map.of(input, DictionaryVector.wrap(ids, new I64Vector(domain)))),
                    allocator);

            evaluator.evaluate(output, Mask.sparse(new int[] {2, 70, 130, 255}, ids.length));
            assertThat(selectedDomains.get()).containsExactly(2, 70, 127);

            evaluator.resetForReuse();
            evaluator.evaluate(output, Mask.sparse(new int[] {1, 80, 129}, ids.length));
            assertThat(selectedDomains.get()).containsExactly(1, 80);
            evaluator.close();
        }
    }

    @Test
    void testDictionaryPeelingPreservesExactSharedMapping()
    {
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("dictionary_identity", new PrimitiveFunction()
        {
            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                return inputs.getFirst();
            }

            @Override
            public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
            {
                return ALL_INPUT_STREAMS;
            }
        });

        Variable result = new Variable(0);
        Reference inputValues = new Reference(new Input(0), Stream.VALUES);
        Reference inputNulls = new Reference(new Input(0), Stream.NULLS);
        Reference resultValues = new Reference(result, Stream.VALUES);
        int[] ids = {0, 1, 0, 2, 1, 2};
        DictionaryVector values = DictionaryVector.wrapWithDomainFrequencies(
                ids,
                ids.length,
                new I64Vector(new long[] {11, 29, 0}),
                new int[] {2, 2, 2});
        DictionaryVector nulls = values.sharedMappingWithValues(new BooleanVector(new boolean[] {false, false, true}));
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(
                        List.of(new Assignment(result, new Call("dictionary_identity", List.of(inputValues)), AllMask.ALL)),
                        List.of(resultValues)),
                registry,
                inputResolver(Map.of(inputValues, values, inputNulls, nulls)),
                new Allocator(EngineResources.createDefault()));

        Streams streams = evaluator.evaluate(resultValues, Mask.all(ids.length));

        assertThat(streams.values()).isInstanceOf(DictionaryVector.class);
        assertThat(streams.get(Stream.NULLS)).isInstanceOf(DictionaryVector.class);
        DictionaryVector resultDictionary = (DictionaryVector) streams.values();
        DictionaryVector resultNulls = (DictionaryVector) streams.get(Stream.NULLS);
        assertThat(resultDictionary.hasSameRowMapping(resultNulls)).isTrue();
        assertThat(resultDictionary.hasDomainFrequencies()).isTrue();
        assertThat(resultDictionary.domainFrequency(0)).isEqualTo(2);
        assertThat(resultDictionary.domainFrequency(1)).isEqualTo(2);
        assertThat(resultDictionary.domainFrequency(2)).isEqualTo(2);
    }

    @Test
    void testDictionaryPeelingAndIndependentDomainsRetainExactSemantics()
    {
        AtomicInteger evaluatedPositions = new AtomicInteger();
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("first_dictionary", (inputs, mask, requestedStreams, output, context) -> {
            evaluatedPositions.set(mask.count());
            return inputs.getFirst();
        });

        Variable result = new Variable(0);
        Reference left = new Reference(new Input(0), Stream.VALUES);
        Reference right = new Reference(new Input(1), Stream.VALUES);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("first_dictionary", List.of(left, right)), AllMask.ALL)),
                List.of(output));

        int[] leftIds = new int[128];
        int[] equivalentIds = new int[128];
        int[] differentIds = new int[128];
        for (int position = 0; position < leftIds.length; position++) {
            leftIds[position] = position % 2;
            equivalentIds[position] = position % 2;
            differentIds[position] = (position + 1) % 3;
        }

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            PlanEvaluator equivalent = planEvaluator(
                    plan,
                    registry,
                    inputResolver(Map.of(
                            left, DictionaryVector.wrap(leftIds, new I64Vector(new long[] {11, 29})),
                            right, DictionaryVector.wrap(equivalentIds, new I64Vector(new long[] {3, 5})))),
                    allocator);

            equivalent.evaluate(output, Mask.all(leftIds.length));
            assertThat(evaluatedPositions).hasValue(2);
            equivalent.close();

            PlanEvaluator different = planEvaluator(
                    plan,
                    registry,
                    inputResolver(Map.of(
                            left, DictionaryVector.wrap(leftIds, new I64Vector(new long[] {11, 29})),
                            right, DictionaryVector.wrap(differentIds, new I64Vector(new long[] {3, 5, 7})))),
                    allocator);

            Streams differentResult = different.evaluate(output, Mask.all(leftIds.length));
            assertThat(evaluatedPositions).hasValue(6);
            assertThat(differentResult.values()).isInstanceOf(DictionaryVector.class);
            assertThat(readLongs(differentResult.values())).containsExactly(
                    java.util.stream.IntStream.range(0, leftIds.length)
                            .mapToLong(position -> position % 2 == 0 ? 11 : 29)
                            .toArray());
            DictionaryVector encoded = (DictionaryVector) differentResult.values();
            assertThat(encoded.hasDomainFrequencies()).isTrue();
            int frequency = 0;
            for (int domain = 0; domain < encoded.values().length(); domain++) {
                frequency += encoded.domainFrequency(domain);
            }
            assertThat(frequency).isEqualTo(leftIds.length);
            different.close();
        }
    }

    @Test
    void testIndependentDictionaryDomainsShareAllocatorOwnedOutputMapping()
    {
        AtomicInteger evaluatedPositions = new AtomicInteger();
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("three_input_dictionary", new PrimitiveFunction()
        {
            @Override
            public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
            {
                evaluatedPositions.set(mask.count());
                return Streams.ofValuesAndNulls(new I64Vector(mask.size()), new BooleanVector(mask.size()));
            }

            @Override
            public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
            {
                return ALL_INPUT_STREAMS;
            }
        });

        Variable result = new Variable(0);
        List<Reference> arguments = java.util.stream.IntStream.range(0, 3)
                .mapToObj(input -> new Reference(new Input(input), Stream.VALUES))
                .toList();
        Reference outputValues = new Reference(result, Stream.VALUES);
        Reference outputNulls = new Reference(result, Stream.NULLS);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call("three_input_dictionary", arguments), AllMask.ALL)),
                List.of(outputValues, outputNulls),
                Map.of(outputValues, StreamPlan.MATERIALIZED, outputNulls, StreamPlan.MATERIALIZED));

        int positions = 256;
        Map<Reference, org.weakref.nitro.data.Vector> inputVectors = new HashMap<>();
        for (int input = 0; input < arguments.size(); input++) {
            int[] ids = new int[positions];
            for (int position = 0; position < positions; position++) {
                ids[position] = (position >>> input) & 1;
            }
            Input producer = (Input) arguments.get(input).producer();
            inputVectors.put(arguments.get(input), DictionaryVector.wrap(ids, new I64Vector(new long[] {input, input + 10L})));
            inputVectors.put(new Reference(producer, Stream.NULLS), new BooleanVector(positions));
            inputVectors.put(new Reference(producer, Stream.ERRORS), new BooleanVector(positions));
        }

        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            PlanEvaluator evaluator = planEvaluator(plan, registry, inputResolver(inputVectors), allocator);
            Streams evaluated = evaluator.evaluate(outputValues, Mask.all(positions));

            assertThat(evaluatedPositions).hasValue(8);
            assertThat(evaluated.values()).isInstanceOf(DictionaryVector.class);
            assertThat(evaluated.get(Stream.NULLS)).isInstanceOf(DictionaryVector.class);
            DictionaryVector values = (DictionaryVector) evaluated.values();
            DictionaryVector nulls = (DictionaryVector) evaluated.get(Stream.NULLS);
            assertThat(values.hasSameRowMapping(nulls)).isTrue();
            assertThat(values.hasOwnedMapping()).isTrue();
            assertThat(nulls.hasOwnedMapping()).isFalse();
            assertThat(evaluator.prepareResultForTransfer(values)).isSameAs(values);
            evaluator.close();
        }
    }

    @Test
    void testDictionaryPeelingDoesNotAttachFrequenciesToChangedOutputDomain()
    {
        PrimitiveRegistry registry = new PrimitiveRegistry();
        registry.register("padded_domain", (inputs, mask, requestedStreams, output, context) ->
                Streams.ofValues(new I64Vector(mask.size() + 1)));

        Variable result = new Variable(0);
        Reference input = new Reference(new Input(0), Stream.VALUES);
        Reference resultValues = new Reference(result, Stream.VALUES);
        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(
                        List.of(new Assignment(result, new Call("padded_domain", List.of(input)), AllMask.ALL)),
                        List.of(resultValues)),
                registry,
                inputResolver(Map.of(
                        input,
                        DictionaryVector.wrap(
                                new int[] {0, 1, 0, 1, 0},
                                new I64Vector(new long[] {11, 29})))),
                new Allocator(EngineResources.createDefault()));

        DictionaryVector encoded = (DictionaryVector) evaluator.evaluate(resultValues, Mask.all(5)).values();
        assertThat(encoded.values().length()).isEqualTo(3);
        assertThat(encoded.hasDomainFrequencies()).isFalse();
    }

    @Test
    void testDictionaryLiteralLessThanOrEqualIncludesEqualValues()
    {
        Variable literal = new Variable(0);
        Variable lessThanOrEqual = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(literal, new Literal("z"), AllMask.ALL),
                        new Assignment(
                                lessThanOrEqual,
                                new Call("lte_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(lessThanOrEqual, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "iphone".getBytes(UTF_8));
        dictionary.setBytes(1, "z".getBytes(UTF_8));
        dictionary.setBytes(2, "é".getBytes(UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 2}, dictionary);
        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Streams result = evaluator.evaluate(new Reference(lessThanOrEqual, Stream.VALUES), Mask.all(3));
        DictionaryVector encoded = (DictionaryVector) result.values();
        BooleanVector dictionaryValues = (BooleanVector) encoded.values();
        assertThat(dictionaryValues.values()).containsExactly(true, true, false);
    }

    @Test
    void testInUtf8OversizedDictionaryLiteralReferenceMaskFallsBackToActiveRows()
    {
        Variable firstLiteral = new Variable(0);
        Variable secondLiteral = new Variable(1);
        Variable matches = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(firstLiteral, new Literal("apple"), AllMask.ALL),
                        new Assignment(secondLiteral, new Literal("samsung"), AllMask.ALL),
                        new Assignment(
                                matches,
                                new Call("in_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(firstLiteral, Stream.VALUES),
                                        new Reference(secondLiteral, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(8, 48);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "apple".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(1, "pixel".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(2, "samsung".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dictionary.setBytes(3, "nokia".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 2}, dictionary);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluateInPlace(new ReferenceMask(new Reference(matches, Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
        assertThat(result.position(2)).isEqualTo(4);
    }

    @Test
    void testInUtf8FlatSameWidthShortValuesUseExactMembership()
    {
        List<Assignment> assignments = new java.util.ArrayList<>();
        List<Reference> arguments = new java.util.ArrayList<>();
        arguments.add(new Reference(new Input(0), Stream.VALUES));
        for (int index = 0; index < 7; index++) {
            Variable literal = new Variable(index);
            assignments.add(new Assignment(literal, new Literal(Integer.toString(10 + index)), AllMask.ALL));
            arguments.add(new Reference(literal, Stream.VALUES));
        }
        Variable matches = new Variable(7);
        assignments.add(new Assignment(matches, new Call("in_utf8", arguments), AllMask.ALL));
        EvaluationPlan plan = new EvaluationPlan(assignments, List.of(new Reference(matches, Stream.VALUES)));

        BinaryVector values = new BinaryVector(7, 14);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        values.setBytes(0, "10".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(1, "16".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(2, "17".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(3, "01".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(4, "12".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(5, "99".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        values.setBytes(6, "15".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, false, true, false, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        BooleanVector result = (BooleanVector) evaluator.evaluate(new Reference(matches, Stream.VALUES), Mask.all(7)).values();
        assertThat(result.values()).containsExactly(true, true, false, false, false, false, true);
    }

    @Test
    void testSourceMaskOptimizationUsesCapabilitiesInsteadOfFunctionNames()
    {
        PrimitiveRegistry registry = primitiveRegistry();
        registry.register(
                "test_slice_alias",
                new SubstringUtf8(),
                new SubstringUtf8BinarySliceProjection());
        registry.register(
                "test_membership_alias",
                new InUtf8(),
                new InUtf8SourceMaskOptimization());

        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        Variable firstLiteral = new Variable(3);
        Variable secondLiteral = new Variable(4);
        Variable matches = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(5L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("test_slice_alias", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(firstLiteral, new Literal("80348"), AllMask.ALL),
                        new Assignment(secondLiteral, new Literal("81792"), AllMask.ALL),
                        new Assignment(
                                matches,
                                new Call("test_membership_alias", List.of(
                                        new Reference(substring, Stream.VALUES),
                                        new Reference(firstLiteral, Stream.VALUES),
                                        new Reference(secondLiteral, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(4, 48);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "80348-1234".getBytes(UTF_8));
        dictionary.setBytes(1, "99999-1234".getBytes(UTF_8));
        dictionary.setBytes(2, "81792-1234".getBytes(UTF_8));
        dictionary.setBytes(3, "80348-9999".getBytes(UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 2}, dictionary);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true, false});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                registry,
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(matches, Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(3);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
        assertThat(result.position(2)).isEqualTo(4);

        Mask falseResult = evaluator.evaluate(new NotMask(new ReferenceMask(new Reference(matches, Stream.VALUES))), Mask.all(5));
        assertThat(falseResult.selectedCount()).isEqualTo(1);
        assertThat(falseResult.position(0)).isEqualTo(1);
    }

    @Test
    void testSourceMaskOptimizationIndexesLargeLiteralSets()
    {
        PrimitiveRegistry registry = primitiveRegistry();
        registry.register(
                "test_slice_alias",
                new SubstringUtf8(),
                new SubstringUtf8BinarySliceProjection());
        registry.register(
                "test_membership_alias",
                new InUtf8(),
                new InUtf8SourceMaskOptimization());

        List<Assignment> assignments = new ArrayList<>();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        assignments.add(new Assignment(start, new Literal(1L), AllMask.ALL));
        assignments.add(new Assignment(length, new Literal(5L), AllMask.ALL));
        assignments.add(new Assignment(
                substring,
                new Call("test_slice_alias", List.of(
                        new Reference(new Input(0), Stream.VALUES),
                        new Reference(start, Stream.VALUES),
                        new Reference(length, Stream.VALUES))),
                AllMask.ALL));

        List<Reference> arguments = new ArrayList<>();
        arguments.add(new Reference(substring, Stream.VALUES));
        for (int index = 0; index < 32; index++) {
            Variable literal = new Variable(assignments.size());
            assignments.add(new Assignment(literal, new Literal("%05d".formatted(index)), AllMask.ALL));
            arguments.add(new Reference(literal, Stream.VALUES));
        }
        Variable matches = new Variable(assignments.size());
        assignments.add(new Assignment(matches, new Call("test_membership_alias", arguments), AllMask.ALL));

        PlanEvaluator evaluator = planEvaluator(
                new EvaluationPlan(assignments, List.of()),
                registry,
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), utf8Vector("00000-x", "00017-x", "00031-x", "99999-x", "00001-x"),
                        new Reference(new Input(0), Stream.NULLS), new BooleanVector(new boolean[] {false, false, false, false, true}))),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(matches, Stream.VALUES)), Mask.all(5));
        assertThat(result).containsExactly(0, 1, 2);

        Mask falseResult = evaluator.evaluate(new NotMask(new ReferenceMask(new Reference(matches, Stream.VALUES))), Mask.all(5));
        assertThat(falseResult).containsExactly(3);
    }

    @Test
    void testConditionalSourceMaskOptimizationAvoidsFlatProjectionMaterialization()
    {
        PrimitiveRegistry registry = primitiveRegistry();
        registry.register(
                "test_slice_alias",
                (_, _, _, _, _) -> {
                    throw new AssertionError("source projection was materialized");
                },
                new SubstringUtf8BinarySliceProjection());
        registry.register(
                "test_membership_alias",
                new InUtf8(),
                new InUtf8SourceMaskOptimization());

        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        Variable firstLiteral = new Variable(3);
        Variable secondLiteral = new Variable(4);
        Variable matches = new Variable(5);
        Variable whenTrue = new Variable(6);
        Variable whenFalse = new Variable(7);
        Variable result = new Variable(8);
        EvaluationPlan plan = IrNormalizer.standard().normalizePlan(new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(5L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("test_slice_alias", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(firstLiteral, new Literal("80348"), AllMask.ALL),
                        new Assignment(secondLiteral, new Literal("81792"), AllMask.ALL),
                        new Assignment(
                                matches,
                                new Call("test_membership_alias", List.of(
                                        new Reference(substring, Stream.VALUES),
                                        new Reference(firstLiteral, Stream.VALUES),
                                        new Reference(secondLiteral, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(whenTrue, new Literal(1L), AllMask.ALL),
                        new Assignment(whenFalse, new Literal(0L), AllMask.ALL),
                        new Assignment(
                                result,
                                new Conditional(
                                        new Reference(matches, Stream.VALUES),
                                        new Reference(whenTrue, Stream.VALUES),
                                        new Reference(whenFalse, Stream.VALUES)),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES))));

        BinaryVector values = utf8Vector("80348-1234", "99999-1234", "81792-1234", "80348-9999");
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, true});
        PlanEvaluator evaluator = planEvaluator(
                plan,
                registry,
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Streams output = evaluator.evaluate(new Reference(result, Stream.VALUES), Mask.all(4));
        VectorAccess.LongValues resultValues = VectorAccess.longValues(output.values());
        assertThat(resultValues.value(0)).isEqualTo(1);
        assertThat(resultValues.value(1)).isZero();
        assertThat(resultValues.value(2)).isEqualTo(1);
        assertThat(resultValues.value(3)).isZero();
    }

    @Test
    void testInUtf8SubstringNestedDictionaryLiteralReferenceMaskUsesPrimitiveTrueMaskInPlace()
    {
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        Variable firstLiteral = new Variable(3);
        Variable secondLiteral = new Variable(4);
        Variable matches = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(5L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(firstLiteral, new Literal("80348"), AllMask.ALL),
                        new Assignment(secondLiteral, new Literal("81792"), AllMask.ALL),
                        new Assignment(
                                matches,
                                new Call("in_utf8", List.of(
                                        new Reference(substring, Stream.VALUES),
                                        new Reference(firstLiteral, Stream.VALUES),
                                        new Reference(secondLiteral, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(3, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "80348-1234".getBytes(UTF_8));
        dictionary.setBytes(1, "99999-1234".getBytes(UTF_8));
        dictionary.setBytes(2, "81792-1234".getBytes(UTF_8));

        DictionaryVector nested = DictionaryVector.wrap(new int[] {0, 1, 2, 1}, dictionary);
        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 0}, nested);
        BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, false, true});

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(
                        new Reference(new Input(0), Stream.VALUES), values,
                        new Reference(new Input(0), Stream.NULLS), nulls)),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluateInPlace(new ReferenceMask(new Reference(matches, Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testInUtf8SubstringDoubleNestedDictionaryLiteralReferenceMaskUsesPrimitiveTrueMaskInPlace()
    {
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        Variable firstLiteral = new Variable(3);
        Variable secondLiteral = new Variable(4);
        Variable matches = new Variable(5);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(5L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(firstLiteral, new Literal("11111"), AllMask.ALL),
                        new Assignment(secondLiteral, new Literal("33333"), AllMask.ALL),
                        new Assignment(
                                matches,
                                new Call("in_utf8", List.of(
                                        new Reference(substring, Stream.VALUES),
                                        new Reference(firstLiteral, Stream.VALUES),
                                        new Reference(secondLiteral, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector base = new BinaryVector(3, 48);
        base.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        base.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        base.setBytes(0, "11111-0000".getBytes(UTF_8));
        base.setBytes(1, "22222-0000".getBytes(UTF_8));
        base.setBytes(2, "33333-0000".getBytes(UTF_8));

        DictionaryVector inner = DictionaryVector.wrap(new int[] {2, 0, 1}, base);
        DictionaryVector middle = DictionaryVector.wrapNested(new int[] {1, 2, 0, 1}, 4, inner);
        DictionaryVector values = DictionaryVector.wrapNested(new int[] {3, 0, 1, 2, 3}, 5, middle);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluateInPlace(new ReferenceMask(new Reference(matches, Stream.VALUES)), Mask.all(5));
        assertThat(result.selectedCount()).isEqualTo(4);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(1);
        assertThat(result.position(2)).isEqualTo(3);
        assertThat(result.position(3)).isEqualTo(4);
    }

    @Test
    void testInUtf8SubstringDictionaryMaskFallsBackForMultibyteUtf8()
    {
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        Variable literal = new Variable(3);
        Variable matches = new Variable(4);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(2L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL),
                        new Assignment(literal, new Literal("éc"), AllMask.ALL),
                        new Assignment(
                                matches,
                                new Call("in_utf8", List.of(
                                        new Reference(substring, Stream.VALUES),
                                        new Reference(literal, Stream.VALUES))),
                                AllMask.ALL)),
                List.of());

        BinaryVector dictionary = new BinaryVector(2, 16);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.setBytes(0, "éclair".getBytes(UTF_8));
        dictionary.setBytes(1, "ecole".getBytes(UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0}, dictionary);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry(),
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        Mask result = evaluator.evaluate(new ReferenceMask(new Reference(matches, Stream.VALUES)), Mask.all(3));
        assertThat(result.selectedCount()).isEqualTo(2);
        assertThat(result.position(0)).isEqualTo(0);
        assertThat(result.position(1)).isEqualTo(2);
    }

    @Test
    void testSubstringUtf8KeepsLiteralScalarPositionsSeparateFromDictionaryValuePositions()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(2L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(substring, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(4, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "70000".getBytes(UTF_8));
        dictionary.setBytes(1, "81111".getBytes(UTF_8));
        dictionary.setBytes(2, "92222".getBytes(UTF_8));
        dictionary.setBytes(3, "10333".getBytes(UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {3, 1, 0}, dictionary);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        org.weakref.nitro.data.Vector result = evaluator.evaluate(new Reference(substring, Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(result).isInstanceOf(DictionaryVector.class);
        assertThat(decodeUtf8(result, 0)).isEqualTo("10");
        assertThat(decodeUtf8(result, 1)).isEqualTo("81");
        assertThat(decodeUtf8(result, 2)).isEqualTo("70");
    }

    @Test
    void testSubstringUtf8KeepsDictionaryEncodingForSparseMask()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(2L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(substring, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(4, 32);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        dictionary.setBytes(0, "70000".getBytes(UTF_8));
        dictionary.setBytes(1, "81111".getBytes(UTF_8));
        dictionary.setBytes(2, "92222".getBytes(UTF_8));
        dictionary.setBytes(3, "10333".getBytes(UTF_8));

        DictionaryVector values = DictionaryVector.wrap(new int[] {3, 1, 0}, dictionary);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        org.weakref.nitro.data.Vector result = evaluator.evaluate(new Reference(substring, Stream.VALUES), Mask.sparse(new int[] {0, 2}, 3)).get(Stream.VALUES);
        assertThat(result).isInstanceOf(DictionaryVector.class);
        assertThat(decodeUtf8(result, 0)).isEqualTo("10");
        assertThat(decodeUtf8(result, 2)).isEqualTo("70");
    }

    @Test
    void testSubstringUtf8CompactsSparseDictionaryBackingEvenWithAllMask()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(2L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(substring, Stream.VALUES)));

        BinaryVector dictionary = new BinaryVector(50, 512);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        dictionary.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        for (int position = 0; position < 50; position++) {
            dictionary.setBytes(position, ("9" + position + "000").getBytes(UTF_8));
        }

        DictionaryVector values = DictionaryVector.wrap(new int[] {49, 1, 0}, dictionary);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        org.weakref.nitro.data.Vector result = evaluator.evaluate(new Reference(substring, Stream.VALUES), Mask.all(3)).get(Stream.VALUES);
        assertThat(result).isInstanceOf(DictionaryVector.class);
        assertThat(((DictionaryVector) result).values()).isInstanceOf(BinaryVector.class);
        assertThat(((BinaryVector) ((DictionaryVector) result).values()).length()).isEqualTo(3);
        assertThat(decodeUtf8(result, 0)).isEqualTo("94");
        assertThat(decodeUtf8(result, 1)).isEqualTo("91");
        assertThat(decodeUtf8(result, 2)).isEqualTo("90");
    }

    @Test
    void testSubstringUtf8KeepsRleEncodingForConstantSubstring()
    {
        PrimitiveRegistry primitiveRegistry = primitiveRegistry();
        Variable start = new Variable(0);
        Variable length = new Variable(1);
        Variable substring = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(start, new Literal(1L), AllMask.ALL),
                        new Assignment(length, new Literal(2L), AllMask.ALL),
                        new Assignment(
                                substring,
                                new Call("substring_utf8", List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(start, Stream.VALUES),
                                        new Reference(length, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(substring, Stream.VALUES)));

        BinaryVector runValues = new BinaryVector(2, 16);
        runValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        runValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        runValues.setBytes(0, "70000".getBytes(UTF_8));
        runValues.setBytes(1, "81111".getBytes(UTF_8));

        RleVector values = new RleVector(new int[] {2, 3}, runValues);

        PlanEvaluator evaluator = planEvaluator(
                plan,
                primitiveRegistry,
                inputResolver(Map.of(new Reference(new Input(0), Stream.VALUES), values)),
                new Allocator(EngineResources.createDefault()));

        org.weakref.nitro.data.Vector result = evaluator.evaluate(new Reference(substring, Stream.VALUES), Mask.all(5)).get(Stream.VALUES);
        assertThat(result).isInstanceOf(RleVector.class);
        assertThat(((RleVector) result).counts()).containsExactly(2, 3);
        assertThat(decodeUtf8(result, 0)).isEqualTo("70");
        assertThat(decodeUtf8(result, 1)).isEqualTo("70");
        assertThat(decodeUtf8(result, 2)).isEqualTo("81");
        assertThat(decodeUtf8(result, 4)).isEqualTo("81");
    }

    @Test
    void testSubstringUtf8NullsOnlyDoesNotRequireValues()
    {
        BooleanVector valueNulls = new BooleanVector(new boolean[] {false, true, false});
        BooleanVector startNulls = new BooleanVector(new boolean[] {false, false, true});
        BooleanVector lengthNulls = new BooleanVector(new boolean[] {false, false, false});

        Streams result = new SubstringUtf8().apply(
                List.of(
                        Streams.of(Stream.NULLS, valueNulls),
                        Streams.of(Stream.NULLS, startNulls),
                        Streams.of(Stream.NULLS, lengthNulls)),
                Mask.all(3),
                Set.of(Stream.NULLS),
                Streams.empty(),
                new PrimitiveExecutionContext(new Allocator(EngineResources.createDefault())));

        assertThat(result.has(Stream.VALUES)).isFalse();
        assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, true);
    }

    private static PlanEvaluator planEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            PlanEvaluator.InputResolver input,
            Allocator allocator)
    {
        return new PlanEvaluator(
                plan,
                primitiveRegistry,
                input,
                allocator,
                EngineResources.from(allocator).operatorCodeGeneration().projectionMask(),
                EvaluationOperatorPolicy.defaults());
    }

    private static PlanEvaluator planEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            PlanEvaluator.InputResolver input,
            Allocator allocator,
            ProjectionMaskCompiler projectionMaskCompiler)
    {
        return new PlanEvaluator(plan, primitiveRegistry, input, allocator, projectionMaskCompiler, EvaluationOperatorPolicy.defaults());
    }

    private static PlanEvaluator planEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            PlanEvaluator.InputResolver input,
            Allocator allocator,
            Object poolGroup)
    {
        return planEvaluator(plan, primitiveRegistry, input, allocator, poolGroup, false);
    }

    private static PlanEvaluator planEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            PlanEvaluator.InputResolver input,
            Allocator allocator,
            Object poolGroup,
            boolean requireProjectedCompanionStreams)
    {
        return new PlanEvaluator(
                plan,
                primitiveRegistry,
                input,
                allocator,
                EngineResources.from(allocator).operatorCodeGeneration().projectionMask(),
                EvaluationOperatorPolicy.defaults(),
                poolGroup,
                requireProjectedCompanionStreams);
    }

    private static PlanEvaluator planEvaluator(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            PlanEvaluator.InputResolver input,
            Allocator allocator,
            ProjectionMaskCompiler projectionMaskCompiler,
            Object poolGroup,
            boolean requireProjectedCompanionStreams)
    {
        return new PlanEvaluator(
                plan,
                primitiveRegistry,
                input,
                allocator,
                projectionMaskCompiler,
                EvaluationOperatorPolicy.defaults(),
                poolGroup,
                requireProjectedCompanionStreams);
    }

    private static PlanEvaluator.InputResolver inputResolver(Map<Reference, org.weakref.nitro.data.Vector> inputs)
    {
        return (reference, mask) -> inputs.get(reference);
    }

    private static long[] readLongs(org.weakref.nitro.data.Vector vector)
    {
        VectorAccess.LongValues values = VectorAccess.longValues(vector);
        long[] result = new long[vector.length()];
        for (int position = 0; position < result.length; position++) {
            result[position] = values.value(position);
        }
        return result;
    }

    private static PrimitiveFunction maskedConstant(long value, Set<Integer> expectedPositions)
    {
        return (_, mask, _, _, _) -> {
            assertThat(mask).containsExactlyInAnyOrderElementsOf(expectedPositions);
            long[] values = new long[mask.maxPosition() + 1];
            for (int position : mask) {
                values[position] = value;
            }
            return Streams.ofValues(new I64Vector(values));
        };
    }

    private static PrimitiveFunction recordSelectedDomains(AtomicReference<List<Integer>> selectedDomains, long multiplier)
    {
        return (inputs, mask, _, _, _) -> {
            List<Integer> selected = new ArrayList<>();
            for (int domain : mask) {
                selected.add(domain);
            }
            selectedDomains.set(List.copyOf(selected));
            VectorAccess.LongValues input = VectorAccess.longValues(inputs.getFirst().values());
            long[] values = new long[mask.size()];
            for (int domain : mask) {
                values[domain] = input.value(domain) * multiplier;
            }
            return Streams.ofValues(new I64Vector(values));
        };
    }

    private static TypeBinding testingIntegerType()
    {
        TypeVectorFactory vectorFactory = new TypeVectorFactory()
        {
            @Override
            public org.weakref.nitro.data.Vector constant(
                    org.weakref.nitro.data.VectorAllocator allocator,
                    Object value,
                    int length)
            {
                I32Vector scalar = allocator.allocate(I32Vector.class, 1, I32Vector::new);
                scalar.values()[0] = (Integer) value;
                return allocator.runLength(new int[] {length}, scalar);
            }

            @Override
            public org.weakref.nitro.data.Vector nullValues(
                    org.weakref.nitro.data.VectorAllocator allocator,
                    int length)
            {
                I32Vector scalar = allocator.allocate(I32Vector.class, 1, I32Vector::new);
                return allocator.runLength(new int[] {length}, scalar);
            }
        };
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("test:integer");
            }

            @Override
            public Class<?> carrierType()
            {
                return int.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorFactory> vectorFactory()
            {
                return Optional.of(vectorFactory);
            }

            @Override
            public Set<Class<? extends org.weakref.nitro.data.Vector>> supportedVectorTypes()
            {
                return Set.of(I32Vector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding testingStructType()
    {
        TypeVectorConstructor constructor = (allocator, arguments, length) -> {
            StructVector struct = allocator.allocate(StructVector.class, length, StructVector::new);
            for (int field = 0; field < arguments.size(); field++) {
                struct.setField(Integer.toString(field), arguments.get(field));
            }
            return struct;
        };
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("test:struct");
            }

            @Override
            public Class<?> carrierType()
            {
                return Object.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<TypeVectorConstructor> vectorConstructor()
            {
                return Optional.of(constructor);
            }

            @Override
            public Set<Class<? extends org.weakref.nitro.data.Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class, RleVector.class);
            }
        };
    }

    private static boolean[] readBooleans(org.weakref.nitro.data.Vector vector)
    {
        VectorAccess.BooleanValues values = VectorAccess.booleanValues(vector);
        boolean[] result = new boolean[vector.length()];
        for (int position = 0; position < result.length; position++) {
            result[position] = values.value(position);
        }
        return result;
    }

    private static String decodeUtf8(org.weakref.nitro.data.Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector binaryVector -> new String(binaryVector.copyBytes(position), UTF_8);
            case DictionaryVector dictionaryVector -> decodeUtf8(dictionaryVector.values(), dictionaryVector.ids()[position]);
            case RleVector rleVector -> decodeUtf8(rleVector.values(), rleVector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported utf8 vector type: " + vector.getClass().getSimpleName());
        };
    }

    private static PrimitiveRegistry builtinPrimitiveRegistry()
    {
        ScalarRegistry scalarRegistry = new ScalarRegistry();
        AnnotatedScalarLoader scalarLoader = new AnnotatedScalarLoader();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(AddI64.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(DivideScaleRoundI64.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(EqualI64.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(InUtf8.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(LessThanI64.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(LessThanOrEqualI64.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(LessThanUtf8.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(LessThanOrEqualUtf8.class)));
        primitiveRegistry.register(scalarRegistry.register(scalarLoader.load(ScaledRelativeDifferenceGtI64.class)));
        return primitiveRegistry;
    }

    private static MapVector createUtf8I64MapVector()
    {
        MapVector maps = new MapVector(4);
        maps.offsets()[0] = 0;
        maps.offsets()[1] = 2;
        maps.offsets()[2] = 3;
        maps.offsets()[3] = 3;
        maps.offsets()[4] = 5;

        BinaryVector keys = new BinaryVector(5, 26);
        keys.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        keys.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        keys.setBytes(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(2, "gamma".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(3, "delta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        keys.setBytes(4, "epsilon".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        I64Vector values = new I64Vector(new long[] {10, 20, 30, 40, 50});
        BooleanVector valueNulls = new BooleanVector(new boolean[] {false, false, true, false, false});
        maps.setEntries(Streams.ofValues(keys), Streams.ofValues(values).with(Stream.NULLS, valueNulls));
        return maps;
    }

    private static BinaryVector utf8Vector(String... values)
    {
        int totalBytes = 0;
        byte[][] encoded = new byte[values.length][];
        for (int index = 0; index < values.length; index++) {
            encoded[index] = values[index].getBytes(UTF_8);
            totalBytes += encoded[index].length;
        }

        BinaryVector vector = new BinaryVector(values.length, totalBytes);
        vector.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
        vector.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, encoded[index]);
        }
        return vector;
    }

    private static String utf8(BinaryVector vector, int position)
    {
        return new String(vector.copyBytes(position), UTF_8);
    }
}
