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
package org.weakref.nitro.function.scalar;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.FunctionSemantics;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.CALLED_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;
import static org.weakref.nitro.execution.EngineResources.createDefault;

final class TestScalarAdapterGenerator
{
    private static final TypeBinding LONG = new TestingTypeBinding(new TypeIdentity("test-long"), long.class);
    private static final TypeBinding DOUBLE = new TestingTypeBinding(new TypeIdentity("test-double"), double.class);
    private static final TypeBinding BOOLEAN = new TestingTypeBinding(new TypeIdentity("test-boolean"), boolean.class);

    @Test
    void testArbitraryCarrierTupleAndArityWithEncodedInputs()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "select_arithmetic",
                new BoundSignature(DOUBLE, List.of(LONG, DOUBLE, BOOLEAN)),
                strictSemantics(3),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "selectArithmetic",
                        MethodType.methodType(double.class, long.class, double.class, boolean.class))))
                .implementation();

        List<Streams> inputs = List.of(
                Streams.ofValuesAndNulls(
                        new DictionaryVector(new int[] {0, 1, 2, 1}, new I64Vector(new long[] {5, 7, 9})),
                        new BooleanVector(new boolean[] {false, false, true, false})),
                Streams.ofValues(new RleVector(new int[] {2, 2}, new F64Vector(new double[] {1.5, 3.5}))),
                Streams.ofValues(new BooleanVector(new boolean[] {true, false, true, false})));
        F64Vector proposedValues = new F64Vector(new double[] {99, 99, 99, 99});
        BooleanVector proposedNulls = new BooleanVector(new boolean[] {true, true, true, true});

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    inputs,
                    Mask.sparse(new int[] {0, 2, 3}, 4),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.ofValuesAndNulls(proposedValues, proposedNulls),
                    new PrimitiveExecutionContext(allocator));

            assertThat(result.values()).isSameAs(proposedValues);
            assertThat(((F64Vector) result.values()).values()).containsExactly(6.5, 99, 99, 3.5);
            assertThat(result.get(Stream.NULLS)).isSameAs(proposedNulls);
            assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, true, false);
        }
    }

    @Test
    void testDenseUnaryAdapterUsesExactPrimitiveDescriptor()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "half",
                new BoundSignature(DOUBLE, List.of(LONG)),
                strictSemantics(1),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "half",
                        MethodType.methodType(double.class, long.class))))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {2, 5, -4}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((F64Vector) result.values()).values()).containsExactly(1, 2.5, -2);
            assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, false, false);
        }
    }

    @Test
    void testNullOnlyDemandDoesNotReadValuesOrInvokeTarget()
            throws Throwable
    {
        CountingTarget target = new CountingTarget();
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "counting",
                new BoundSignature(LONG, List.of(LONG)),
                strictSemantics(1),
                new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                CountingTarget.class,
                                "apply",
                                MethodType.methodType(long.class, long.class))
                        .bindTo(target)))
                .implementation();

        assertThat(function.requiredInputStreams(0, EnumSet.of(Stream.NULLS))).containsExactly(Stream.NULLS);
        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(Streams.of(Stream.NULLS, new BooleanVector(new boolean[] {false, true, false}))),
                    Mask.all(3),
                    EnumSet.of(Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, false);
            assertThat(target.invocations).isZero();
        }
    }

    @Test
    void testUnsupportedSemanticFormsFailAtBinding()
            throws Throwable
    {
        ScalarAdapterGenerator generator = new ScalarAdapterGenerator();
        ScalarMethodTarget target = new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                TestScalarAdapterGenerator.class,
                "half",
                MethodType.methodType(double.class, long.class)));

        assertThatThrownBy(() -> generator.adapt(
                "called_on_null",
                new BoundSignature(DOUBLE, List.of(LONG)),
                new FunctionSemantics(true, List.of(CALLED_ON_NULL), false, NEVER_FAILS),
                target))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strict arguments");
    }

    private static FunctionSemantics strictSemantics(int arity)
    {
        return new FunctionSemantics(true, Collections.nCopies(arity, RETURN_NULL_ON_NULL), false, NEVER_FAILS);
    }

    private static double selectArithmetic(long left, double right, boolean add)
    {
        return add ? left + right : left - right;
    }

    private static double half(long value)
    {
        return value / 2.0;
    }

    private static final class CountingTarget
    {
        private int invocations;

        public long apply(long value)
        {
            invocations++;
            return value;
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
