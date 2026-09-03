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
import org.weakref.nitro.core.function.NullPropagatingScalarInvocationProvider;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

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
    private static final TypeBinding STRING = new ReferenceTypeBinding(
            new TypeIdentity("test-string"),
            String.class,
            valueReader("readAscii", String.class));

    @Test
    void testRegistryOwnedReferenceCarrierAcrossPhysicalEncodings()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "string_length",
                new BoundSignature(LONG, List.of(STRING)),
                strictSemantics(1),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "stringLength",
                        MethodType.methodType(long.class, String.class))))
                .implementation();

        BinaryVector flat = binary("one", "twelve", "xx");
        try (Allocator allocator = new Allocator(createDefault())) {
            Streams dense = function.apply(
                    List.of(Streams.ofValues(flat)),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(((I64Vector) dense.values()).values()).containsExactly(3, 6, 2);

            Streams dictionary = function.apply(
                    List.of(Streams.ofValues(new DictionaryVector(new int[] {1, 0, 1, 2}, flat))),
                    Mask.sparse(new int[] {0, 2, 3}, 4),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(((I64Vector) dictionary.values()).values()).containsExactly(6, 0, 6, 2);

            Streams nullable = function.apply(
                    List.of(Streams.ofValuesAndNulls(
                            new DictionaryVector(new int[] {1, 0, 2}, flat),
                            new BooleanVector(new boolean[] {false, true, false}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(((I64Vector) nullable.values()).values()).containsExactly(6, 0, 2);
            assertThat(((BooleanVector) nullable.get(Stream.NULLS)).values()).containsExactly(false, true, false);

            Streams rle = function.apply(
                    List.of(Streams.ofValues(new RleVector(new int[] {2, 3}, binary("one", "twelve")))),
                    Mask.all(5),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(rle.values()).isInstanceOf(RleVector.class);
            RleVector encoded = (RleVector) rle.values();
            assertThat(encoded.counts()).containsExactly(2, 3);
            assertThat(((I64Vector) encoded.values()).values()).containsExactly(3, 6);
        }
    }

    @Test
    void testReferenceCarrierRequiresExactRegistryReader()
            throws Throwable
    {
        TypeBinding missingReader = new TestingTypeBinding(new TypeIdentity("missing-reader"), String.class);
        ScalarMethodTarget target = new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                TestScalarAdapterGenerator.class,
                "stringLength",
                MethodType.methodType(long.class, String.class)));

        assertThatThrownBy(() -> new ScalarAdapterGenerator().adapt(
                "string_length",
                new BoundSignature(LONG, List.of(missingReader)),
                strictSemantics(1),
                target))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not provide a value reader");
    }

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
    void testComposedMethodHandleRunsAsSingleGeneratedLoop()
            throws Throwable
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "multiply_add",
                new BoundSignature(LONG, List.of(LONG, LONG, LONG)),
                strictSemantics(3),
                new ScalarMethodTarget(MethodHandles.collectArguments(
                        lookup.findStatic(TestScalarAdapterGenerator.class, "add", MethodType.methodType(long.class, long.class, long.class)),
                        0,
                        lookup.findStatic(TestScalarAdapterGenerator.class, "multiply", MethodType.methodType(long.class, long.class, long.class)))))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(
                            Streams.ofValues(new I64Vector(new long[] {2, 3, 4})),
                            Streams.ofValues(new I64Vector(new long[] {5, 6, 7})),
                            Streams.ofValues(new I64Vector(new long[] {8, 9, 10}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((I64Vector) result.values()).values()).containsExactly(18, 27, 38);
        }
    }

    @Test
    void testArbitraryArityPreservesRleDomains()
            throws Throwable
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "multiply_add",
                new BoundSignature(LONG, List.of(LONG, LONG, LONG)),
                strictSemantics(3),
                new ScalarMethodTarget(MethodHandles.collectArguments(
                        lookup.findStatic(TestScalarAdapterGenerator.class, "add", MethodType.methodType(long.class, long.class, long.class)),
                        0,
                        lookup.findStatic(TestScalarAdapterGenerator.class, "multiply", MethodType.methodType(long.class, long.class, long.class)))))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(
                            Streams.ofValues(new RleVector(new int[] {2, 4}, new I64Vector(new long[] {2, 3}))),
                            Streams.ofValues(new RleVector(new int[] {1, 2, 3}, new I64Vector(new long[] {5, 6, 7}))),
                            Streams.ofValues(new RleVector(new int[] {3, 3}, new I64Vector(new long[] {8, 9})))),
                    Mask.all(6),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(result.values()).isInstanceOf(RleVector.class);
            RleVector values = (RleVector) result.values();
            assertThat(values.counts()).containsExactly(1, 1, 1, 3);
            assertThat(((I64Vector) values.values()).values()).containsExactly(18, 20, 26, 30);
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

            Streams values = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {3, 5}))),
                    Mask.all(2),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(((I64Vector) values.values()).values()).containsExactly(3, 5);
            assertThat(target.invocations).isEqualTo(2);
        }
    }

    @Test
    void testExplicitNullPropagatingConvention()
            throws Throwable
    {
        CountingTarget target = new CountingTarget();
        BoundSignature signature = new BoundSignature(LONG, List.of(LONG, LONG));
        ScalarDescriptor descriptor = new ScalarAdapterGenerator().adaptNullPropagating(
                "nullable_maximum",
                signature,
                new FunctionSemantics(true, List.of(CALLED_ON_NULL, CALLED_ON_NULL), true, NEVER_FAILS),
                new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                CountingTarget.class,
                                "maximum",
                                MethodType.methodType(long.class, long.class, long.class))
                        .bindTo(target)));

        assertThat(descriptor.capabilities()).singleElement().isInstanceOf(NullPropagatingScalarInvocationProvider.class);
        NullPropagatingScalarInvocationProvider provider = (NullPropagatingScalarInvocationProvider) descriptor.capabilities().getFirst();
        assertThat(provider.target(signature)).isPresent();
        assertThat(provider.target(new BoundSignature(LONG, List.of(LONG)))).isEmpty();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = descriptor.implementation().apply(
                    List.of(
                            Streams.ofValues(new I64Vector(new long[] {3, 8, 5})),
                            Streams.ofValuesAndNulls(
                                    new I64Vector(new long[] {7, 2, 9}),
                                    new BooleanVector(new boolean[] {false, true, false}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((I64Vector) result.values()).values()).containsExactly(7, 0, 9);
            assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, false);
            assertThat(target.invocations).isEqualTo(2);
        }
    }

    @Test
    void testNullPropagatingDoubleTargetPreservesFloatingPointSemantics()
            throws Throwable
    {
        BoundSignature signature = new BoundSignature(DOUBLE, List.of(DOUBLE, DOUBLE, DOUBLE));
        ScalarDescriptor descriptor = new ScalarAdapterGenerator().adaptNullPropagating(
                "nullable_maximum",
                signature,
                new FunctionSemantics(true, List.of(CALLED_ON_NULL, CALLED_ON_NULL, CALLED_ON_NULL), true, NEVER_FAILS),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "maximum",
                        MethodType.methodType(double.class, double.class, double.class, double.class))));

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = descriptor.implementation().apply(
                    List.of(
                            Streams.ofValues(new F64Vector(new double[] {-0.0, 7.0, Double.NaN})),
                            Streams.ofValuesAndNulls(
                                    new F64Vector(new double[] {0.0, 3.0, 10.0}),
                                    new BooleanVector(new boolean[] {false, true, false})),
                            Streams.ofValues(new F64Vector(new double[] {-1.0, 6.0, 9.0}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            double[] values = ((F64Vector) result.values()).values();
            assertThat(Double.doubleToRawLongBits(values[0])).isEqualTo(Double.doubleToRawLongBits(0.0));
            assertThat(values[2]).isNaN();
            assertThat(((BooleanVector) result.get(Stream.NULLS)).values()).containsExactly(false, true, false);
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

    private static long add(long left, long right)
    {
        return left + right;
    }

    private static long multiply(long left, long right)
    {
        return left * right;
    }

    private static double maximum(double first, double second, double third)
    {
        return Math.max(first, Math.max(second, third));
    }

    private static long stringLength(String value)
    {
        return value.length();
    }

    private static String readAscii(Vector vector, int position)
    {
        VectorAccess.BinaryRegions values = VectorAccess.binaryRegions(vector);
        return new String(values.data(position), values.offset(position), values.length(position), java.nio.charset.StandardCharsets.US_ASCII);
    }

    private static BinaryVector binary(String... values)
    {
        int[] offsets = new int[values.length + 1];
        int bytes = 0;
        for (int index = 0; index < values.length; index++) {
            bytes += values[index].length();
            offsets[index + 1] = bytes;
        }
        byte[] data = new byte[bytes];
        int offset = 0;
        for (String value : values) {
            byte[] encoded = value.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            System.arraycopy(encoded, 0, data, offset, encoded.length);
            offset += encoded.length;
        }
        return new BinaryVector(values.length, offsets, data);
    }

    private static java.lang.invoke.MethodHandle valueReader(String name, Class<?> result)
    {
        try {
            return MethodHandles.lookup().findStatic(
                    TestScalarAdapterGenerator.class,
                    name,
                    MethodType.methodType(result, Vector.class, int.class));
        }
        catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private static final class CountingTarget
    {
        private int invocations;

        public long apply(long value)
        {
            invocations++;
            return value;
        }

        public long maximum(long left, long right)
        {
            invocations++;
            return Math.max(left, right);
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

    private record ReferenceTypeBinding(TypeIdentity identity, Class<?> carrierType, java.lang.invoke.MethodHandle reader)
            implements TypeBinding
    {
        @Override
        public Optional<java.lang.invoke.MethodHandle> scalarValueReader()
        {
            return Optional.of(reader);
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    }
}
