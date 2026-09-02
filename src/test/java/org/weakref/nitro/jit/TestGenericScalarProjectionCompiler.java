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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarMethodTarget;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.ConstantTableOperator;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.evaluator.PrimitiveInvocationBinding;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;
import static org.weakref.nitro.execution.EngineResources.createDefault;

final class TestGenericScalarProjectionCompiler
{
    private static final TypeBinding LONG = new TestingTypeBinding(new TypeIdentity("test-long"), long.class);
    private static final TypeBinding DOUBLE = new TestingTypeBinding(new TypeIdentity("test-double"), double.class);
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

    private static ResolvedCall resolvedCall(String name, MethodHandle target)
    {
        return resolvedCall(name, LONG, List.of(LONG, LONG), target);
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

    private static double addDouble(double left, double right)
    {
        return left + right;
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
