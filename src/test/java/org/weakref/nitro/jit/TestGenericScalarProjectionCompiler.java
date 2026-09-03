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
package org.weakref.nitro.jit;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.FunctionIdentity;
import org.weakref.nitro.core.function.FunctionSemantics;
import org.weakref.nitro.core.function.ResolvedCall;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarAdapterGenerator;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.ScalarMethodTarget;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.EvaluationOperatorPolicy;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.TableOperator;
import org.weakref.nitro.operator.evaluator.PlanEvaluator;
import org.weakref.nitro.operator.evaluator.PrimitiveInvocationBinding;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.ReferenceMask;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.CALLED_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;
import static org.weakref.nitro.execution.EngineResources.createDefault;

final class TestGenericScalarProjectionCompiler
{
    private static final TypeBinding LONG = new TestingTypeBinding(new TypeIdentity("test-long"), long.class);
    private static final TypeBinding DOUBLE = new TestingTypeBinding(new TypeIdentity("test-double"), double.class);
    private static final TypeBinding BOOLEAN = new TestingTypeBinding(new TypeIdentity("test-boolean"), boolean.class);
    private static final PrimitiveFunction UNUSED_VECTOR_IMPLEMENTATION = (_, _, _, _, _) -> {
        throw new AssertionError("generic fused projection used the vector fallback");
    };

    @Test
    void testComposesResolvedScalarCallsWithoutProjectionProvider()
            throws Throwable
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ResolvedCall multiply = resolvedCall(
                "dynamic_multiply",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "multiply", MethodType.methodType(long.class, long.class, long.class)));
        ResolvedCall add = resolvedCall(
                "dynamic_add",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "add", MethodType.methodType(long.class, long.class, long.class)));
        PrimitiveRegistry registry = new PrimitiveRegistry();

        Variable product = new Variable(0);
        Variable result = new Variable(1);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(product, new Call(multiply, List.of(input(0), input(1))), AllMask.ALL),
                        new Assignment(result, new Call(add, List.of(new Reference(product, Stream.VALUES), input(2))), AllMask.ALL)),
                List.of(output));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler();
                Allocator allocator = new Allocator(createDefault())) {
            FusedProjectionCompiler.CompiledMultiProjection compiled = compiler.tryCompile(plan, registry, List.of(output)).orElseThrow();
            assertThat(compiled.inputs()).containsExactly(input(0), input(1), input(2));
            assertThat(compiled.compilationKind()).isEqualTo(FusedProjectionCompiler.CompilationKind.SCALAR_TARGET);

            Streams[] results = compiled.kernel().apply(
                    List.of(
                            Streams.ofValues(new I64Vector(new long[] {2, 3, 4, 5})),
                            Streams.ofValuesAndNulls(
                                    new I64Vector(new long[] {10, 20, 30, 40}),
                                    new BooleanVector(new boolean[] {false, true, false, false})),
                            Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4}))),
                    Mask.sparse(new int[] {0, 1, 3}, 4),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((I64Vector) results[0].values()).values()).containsExactly(21, 0, 0, 204);
            assertThat(((BooleanVector) results[0].get(Stream.NULLS)).values()).containsExactly(false, true, false, false);
        }
    }

    @Test
    void testNondeterministicScalarPredicateNarrowsMaskInOneGeneratedLoop()
            throws Throwable
    {
        AlternatingDouble target = new AlternatingDouble();
        FunctionSemantics nondeterministic = new FunctionSemantics(false, List.of(), false, NEVER_FAILS);
        ScalarDescriptor descriptor = new ScalarAdapterGenerator().adapt(
                "dynamic_value",
                new BoundSignature(DOUBLE, List.of()),
                nondeterministic,
                new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                AlternatingDouble.class,
                                "nextValue",
                                MethodType.methodType(double.class))
                        .bindTo(target)));
        ResolvedCall value = new ResolvedCall(
                new FunctionIdentity("dynamic_value"),
                new BoundSignature(DOUBLE, List.of()),
                nondeterministic,
                List.of(),
                new PrimitiveInvocationBinding(
                        descriptor.implementation(),
                        descriptor.capabilities()));
        ResolvedCall lessThan = resolvedCall(
                "dynamic_less_than",
                BOOLEAN,
                List.of(DOUBLE, DOUBLE),
                MethodHandles.lookup().findStatic(
                        TestGenericScalarProjectionCompiler.class,
                        "lessThan",
                        MethodType.methodType(boolean.class, double.class, double.class)));

        Variable generated = new Variable(0);
        Variable limit = new Variable(1);
        Variable predicate = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(generated, new Call(value, List.of()), AllMask.ALL),
                        new Assignment(limit, new Literal(0.5, DOUBLE), AllMask.ALL),
                        new Assignment(predicate, new Call(lessThan, List.of(
                                new Reference(generated, Stream.VALUES),
                                new Reference(limit, Stream.VALUES))), AllMask.ALL)),
                List.of());

        try (Allocator allocator = new Allocator(createDefault());
                ProjectionMaskCompiler maskCompiler = new ProjectionMaskCompiler()) {
            PlanEvaluator evaluator = new PlanEvaluator(
                    plan,
                    new PrimitiveRegistry(),
                    (_, _) -> {
                        throw new AssertionError("predicate has no source inputs");
                    },
                    allocator,
                    maskCompiler,
                    EvaluationOperatorPolicy.defaults());
            Mask mask = Mask.sparse(new int[] {1, 2, 4}, 6);
            assertThat(evaluator.evaluateInPlace(
                    new ReferenceMask(new Reference(predicate, Stream.VALUES)), mask)).isSameAs(mask);
            assertThat(mask).containsExactly(1, 4);
            assertThat(target.invocations).isEqualTo(3);
            assertThat(evaluator.maskExecutionDiagnostics().compiledMaskSuccesses()).isEqualTo(1);
            assertThat(evaluator.maskExecutionDiagnostics().materializedMaskFallbacks()).isZero();
        }
    }

    @Test
    void testCanonicalizesRepeatedLeafInput()
            throws Throwable
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ResolvedCall multiply = resolvedCall(
                "dynamic_square",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "multiply", MethodType.methodType(long.class, long.class, long.class)));
        ResolvedCall add = resolvedCall(
                "dynamic_offset",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "add", MethodType.methodType(long.class, long.class, long.class)));
        PrimitiveRegistry registry = new PrimitiveRegistry();

        Variable square = new Variable(0);
        Variable result = new Variable(1);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(square, new Call(multiply, List.of(input(0), input(0))), AllMask.ALL),
                        new Assignment(result, new Call(add, List.of(new Reference(square, Stream.VALUES), input(1))), AllMask.ALL)),
                List.of(output));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            FusedProjectionCompiler.CompiledMultiProjection compiled = compiler.tryCompile(plan, registry, List.of(output)).orElseThrow();
            assertThat(compiled.inputs()).containsExactly(input(0), input(1));
        }
    }

    @Test
    void testSingleCallIsAvailableOnlyForDictionaryDomainDemand()
            throws Throwable
    {
        ResolvedCall call = resolvedCall(
                "dynamic_single",
                LONG,
                List.of(LONG),
                MethodHandles.lookup().findStatic(
                        TestGenericScalarProjectionCompiler.class,
                        "negate",
                        MethodType.methodType(long.class, long.class)));
        Variable result = new Variable(0);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(new Assignment(result, new Call(call, List.of(input(0))), AllMask.ALL)),
                List.of(output));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            FusedProjectionCompiler.CompiledMultiProjection compiled = compiler
                    .tryCompile(plan, new PrimitiveRegistry(), List.of(output))
                    .orElseThrow();

            assertThat(compiled.compilationKind()).isEqualTo(FusedProjectionCompiler.CompilationKind.SCALAR_TARGET);
            assertThat(compiled.dictionaryDomainOnly()).isTrue();
        }
    }

    @Test
    void testDoesNotDuplicateSharedComputedExpression()
            throws Throwable
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ResolvedCall multiply = resolvedCall(
                "shared_multiply",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "multiply", MethodType.methodType(long.class, long.class, long.class)));
        ResolvedCall add = resolvedCall(
                "shared_add",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "add", MethodType.methodType(long.class, long.class, long.class)));

        Variable product = new Variable(0);
        Variable result = new Variable(1);
        Reference productValue = new Reference(product, Stream.VALUES);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(product, new Call(multiply, List.of(input(0), input(1))), AllMask.ALL),
                        new Assignment(result, new Call(add, List.of(productValue, productValue)), AllMask.ALL)),
                List.of(output));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler()) {
            assertThat(compiler.tryCompile(plan, new PrimitiveRegistry(), List.of(output))).isEmpty();
        }
    }

    @Test
    void testProjectOperatorExecutesGenericFusedExpression()
            throws Throwable
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ResolvedCall multiply = resolvedCall(
                "project_multiply",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "multiply", MethodType.methodType(long.class, long.class, long.class)));
        ResolvedCall add = resolvedCall(
                "project_add",
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "add", MethodType.methodType(long.class, long.class, long.class)));
        PrimitiveRegistry registry = new PrimitiveRegistry();

        Variable product = new Variable(0);
        Variable result = new Variable(1);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(product, new Call(multiply, List.of(input(0), input(1))), AllMask.ALL),
                        new Assignment(result, new Call(add, List.of(new Reference(product, Stream.VALUES), input(2))), AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        try (Allocator allocator = new Allocator(createDefault());
                ProjectOperator operator = new ProjectOperator(
                        allocator,
                        plan,
                        registry,
                        new ConstantTableOperator(
                                allocator,
                                3,
                                List.of(Row.row(2L, 10L, 1L), Row.row(3L, 20L, 2L), Row.row(4L, 30L, 3L))));
                Batch batch = operator.next()) {
            assertThat(((I64Vector) batch.output(0).borrow(Stream.VALUES)).values()).containsExactly(21, 62, 123);
        }
    }

    @Test
    void testComposesMixedCarrierCalls()
            throws Throwable
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ResolvedCall cast = resolvedCall(
                "dynamic_cast",
                DOUBLE,
                List.of(LONG),
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "cast", MethodType.methodType(double.class, long.class)));
        ResolvedCall add = resolvedCall(
                "dynamic_double_add",
                DOUBLE,
                List.of(DOUBLE, DOUBLE),
                lookup.findStatic(TestGenericScalarProjectionCompiler.class, "addDouble", MethodType.methodType(double.class, double.class, double.class)));

        Variable castValue = new Variable(0);
        Variable result = new Variable(1);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(castValue, new Call(cast, List.of(input(0))), AllMask.ALL),
                        new Assignment(result, new Call(add, List.of(new Reference(castValue, Stream.VALUES), input(1))), AllMask.ALL)),
                List.of(output));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler();
                Allocator allocator = new Allocator(createDefault())) {
            FusedProjectionCompiler.CompiledMultiProjection compiled = compiler.tryCompile(plan, new PrimitiveRegistry(), List.of(output)).orElseThrow();
            Streams[] results = compiled.kernel().apply(
                    List.of(
                            Streams.ofValues(new I64Vector(new long[] {1, 2, 3})),
                            Streams.ofValues(new F64Vector(new double[] {0.5, 1.5, 2.5}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((F64Vector) results[0].values()).values()).containsExactly(1.5, 3.5, 5.5);
        }
    }

    @Test
    void testComposesNullableVariadicCallThroughExplicitNullPropagatingTarget()
            throws Throwable
    {
        ResolvedCall cast = resolvedCall(
                "dynamic_cast",
                DOUBLE,
                List.of(LONG),
                MethodHandles.lookup().findStatic(TestGenericScalarProjectionCompiler.class, "cast", MethodType.methodType(double.class, long.class)));
        ResolvedCall greatest = nullableGreatestCall(3);

        Variable castValue = new Variable(0);
        Variable result = new Variable(1);
        Reference output = new Reference(result, Stream.VALUES);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(castValue, new Call(cast, List.of(input(0))), AllMask.ALL),
                        new Assignment(result, new Call(greatest, List.of(new Reference(castValue, Stream.VALUES), input(1), input(2))), AllMask.ALL)),
                List.of(output));

        try (FusedProjectionCompiler compiler = new FusedProjectionCompiler();
                Allocator allocator = new Allocator(createDefault())) {
            FusedProjectionCompiler.CompiledMultiProjection compiled = compiler.tryCompile(plan, new PrimitiveRegistry(), List.of(output)).orElseThrow();
            assertThat(compiled.compilationKind()).isEqualTo(FusedProjectionCompiler.CompilationKind.SCALAR_TARGET);

            Streams[] results = compiled.kernel().apply(
                    List.of(
                            Streams.ofValues(new I64Vector(new long[] {1, 7, 4, 9})),
                            Streams.ofValuesAndNulls(
                                    new F64Vector(new double[] {2, 8, 3, 1}),
                                    new BooleanVector(new boolean[] {false, false, true, false})),
                            Streams.ofValues(new F64Vector(new double[] {0, 6, 5, Double.NaN}))),
                    Mask.all(4),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((F64Vector) results[0].values()).values()).containsExactly(2, 8, 0, Double.NaN);
            assertThat(((BooleanVector) results[0].get(Stream.NULLS)).values()).containsExactly(false, false, true, false);
        }
    }

    @Test
    void testNullableScalarProjectionPreservesDictionaryDomainCountsAcrossStreams()
            throws Throwable
    {
        ResolvedCall cast = resolvedCall(
                "dynamic_cast",
                DOUBLE,
                List.of(LONG),
                MethodHandles.lookup().findStatic(TestGenericScalarProjectionCompiler.class, "cast", MethodType.methodType(double.class, long.class)));
        ResolvedCall greatest = nullableGreatestCall(2);
        Variable castValue = new Variable(0);
        Variable literal = new Variable(1);
        Variable result = new Variable(2);
        EvaluationPlan plan = new EvaluationPlan(
                List.of(
                        new Assignment(castValue, new Call(cast, List.of(input(0))), AllMask.ALL),
                        new Assignment(literal, new Literal(8.0), AllMask.ALL),
                        new Assignment(result, new Call(greatest, List.of(
                                new Reference(castValue, Stream.VALUES),
                                new Reference(literal, Stream.VALUES))), AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));
        DictionaryVector input = DictionaryVector.wrapWithDomainFrequencies(
                new int[] {0, 1, 0, 2, 0, 1, 0, 2, 0, 1, 0, 2},
                12,
                new I64Vector(new long[] {0, 8, 15}),
                new int[] {6, 3, 3});
        TableOperator source = new TableOperator(1, List.of(new TableOperator.Page(
                12,
                new Streams[] {Streams.ofValues(input)},
                Mask.all(12))))
        {
            @Override
            public Optional<Map<Integer, ValueDemand>> sourceOutputDemand(Map<Integer, ValueDemand> demandedOutputs)
            {
                return Optional.of(Map.copyOf(demandedOutputs));
            }
        };

        try (Allocator allocator = new Allocator(createDefault());
                ProjectOperator project = new ProjectOperator(
                        allocator,
                        plan,
                        new PrimitiveRegistry(),
                        source)) {
            assertThat(project.sourceOutputDemand(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS)).orElseThrow())
                    .containsExactlyEntriesOf(Map.of(0, ValueDemand.FULL_WITH_DOMAIN_COUNTS));
            try (Batch batch = project.next()) {
                DictionaryVector values = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
                DictionaryVector nulls = (DictionaryVector) batch.output(0).borrow(Stream.NULLS);
                assertThat(values.hasSameRowMapping(nulls)).isTrue();
                assertThat(values.hasDomainFrequencies()).isTrue();
                assertThat(values.domainFrequency(0)).isEqualTo(6);
                assertThat(((F64Vector) values.values()).values()).containsExactly(8, 8, 15);
                assertThat(((BooleanVector) nulls.values()).values()).containsExactly(false, false, false);
            }
        }
    }

    private static ResolvedCall resolvedCall(String name, MethodHandle target)
    {
        return resolvedCall(name, LONG, List.of(LONG, LONG), target);
    }

    private static ResolvedCall nullableGreatestCall(int arity)
            throws Throwable
    {
        if (arity < 1) {
            throw new IllegalArgumentException("arity must be positive");
        }
        List<TypeBinding> arguments = nCopies(arity, DOUBLE);
        BoundSignature signature = new BoundSignature(DOUBLE, arguments);
        FunctionSemantics semantics = new FunctionSemantics(true, nCopies(arity, CALLED_ON_NULL), true, NEVER_FAILS);
        MethodHandle maximum = MethodHandles.lookup().findStatic(
                TestGenericScalarProjectionCompiler.class,
                "maximum",
                MethodType.methodType(double.class, double.class, double.class));
        MethodHandle target = arity == 1 ? MethodHandles.identity(double.class) : maximum;
        for (int argument = 2; argument < arity; argument++) {
            target = MethodHandles.collectArguments(maximum, 0, target);
        }
        ScalarDescriptor descriptor = new ScalarAdapterGenerator().adaptNullPropagating(
                "dynamic_greatest",
                signature,
                semantics,
                new ScalarMethodTarget(target));
        return new ResolvedCall(
                new FunctionIdentity("dynamic_greatest"),
                signature,
                semantics,
                List.of(),
                new PrimitiveInvocationBinding(UNUSED_VECTOR_IMPLEMENTATION, descriptor.capabilities()));
    }

    private static ResolvedCall resolvedCall(String name, TypeBinding resultType, List<TypeBinding> argumentTypes, MethodHandle target)
    {
        FunctionSemantics semantics = new FunctionSemantics(
                true,
                argumentTypes.stream().map(_ -> RETURN_NULL_ON_NULL).toList(),
                false,
                NEVER_FAILS);
        return new ResolvedCall(
                new FunctionIdentity(name),
                new BoundSignature(resultType, argumentTypes),
                semantics,
                List.of(),
                new PrimitiveInvocationBinding(UNUSED_VECTOR_IMPLEMENTATION, List.of(new ScalarMethodTarget(target))));
    }

    private static Reference input(int index)
    {
        return new Reference(new Input(index), Stream.VALUES);
    }

    private static long multiply(long left, long right)
    {
        return left * right;
    }

    private static long add(long left, long right)
    {
        return left + right;
    }

    private static double cast(long value)
    {
        return value;
    }

    private static double maximum(double left, double right)
    {
        return Math.max(left, right);
    }

    private static double addDouble(double left, double right)
    {
        return left + right;
    }

    private static boolean lessThan(double left, double right)
    {
        return left < right;
    }

    private static long negate(long value)
    {
        return -value;
    }

    private static final class AlternatingDouble
    {
        private int invocations;

        private double nextValue()
        {
            invocations++;
            return (invocations & 1) != 0 ? 0.25 : 0.75;
        }
    }

    private record TestingTypeBinding(TypeIdentity identity, Class<?> carrierType)
            implements TypeBinding
    {
        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    }
}
