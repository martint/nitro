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
import org.weakref.nitro.core.function.ScalarFailureMapper;
import org.weakref.nitro.core.function.ScalarResultWriter;
import org.weakref.nitro.core.function.ScalarResultWriterFactory;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.ErrorValue;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.data.VectorAllocator;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.CALLED_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.MAY_FAIL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;
import static org.weakref.nitro.execution.EngineResources.createDefault;

final class TestScalarAdapterGenerator
{
    private static final ErrorValue NEGATIVE_VALUE = new ErrorValue("test", 1, "NEGATIVE_VALUE", "USER_ERROR", "negative value");
    private static final TypeBinding LONG = new TestingTypeBinding(new TypeIdentity("test-long"), long.class);
    private static final TypeBinding DOUBLE = new TestingTypeBinding(new TypeIdentity("test-double"), double.class);
    private static final TypeBinding BOOLEAN = new TestingTypeBinding(new TypeIdentity("test-boolean"), boolean.class);
    private static final TypeBinding STRING = new ReferenceTypeBinding(
            new TypeIdentity("test-string"),
            String.class,
            valueReader("readAscii", String.class));
    private static final TypeBinding STRING_RESULT = new ResultTypeBinding(
            new TypeIdentity("test-string-result"),
            String.class,
            resultWriter());

    @Test
    void testNondeterministicZeroArgumentTargetRunsOncePerSelectedLogicalRow()
            throws Throwable
    {
        CountingTarget target = new CountingTarget();
        FunctionSemantics semantics = new FunctionSemantics(false, List.of(), false, NEVER_FAILS);
        ScalarDescriptor descriptor = new ScalarAdapterGenerator().adapt(
                "next_value",
                new BoundSignature(DOUBLE, List.of()),
                semantics,
                new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                CountingTarget.class,
                                "nextValue",
                                MethodType.methodType(double.class))
                        .bindTo(target)));

        assertThat(descriptor.deterministic()).isFalse();
        assertThat(descriptor.implementation().deterministic()).isFalse();
        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = descriptor.implementation().apply(
                    List.of(),
                    Mask.sparse(new int[] {1, 3}, 5),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((F64Vector) result.values()).values()).containsExactly(0, 1, 0, 2, 0);
            assertThat(target.invocations).isEqualTo(2);
        }
    }

    @Test
    void testBooleanTargetNarrowsMaskWithoutMaterializingResult()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                        "positive",
                        new BoundSignature(BOOLEAN, List.of(LONG)),
                        strictSemantics(1),
                        new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                                TestScalarAdapterGenerator.class,
                                "positive",
                                MethodType.methodType(boolean.class, long.class))))
                .implementation();
        assertThat(function).isInstanceOf(MaskEvaluablePrimitiveFunction.class);

        try (Allocator allocator = new Allocator(createDefault())) {
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
            Mask dense = Mask.all(5);
            boolean denseResult = ((MaskEvaluablePrimitiveFunction) function).tryEvaluateTrueMaskInPlace(
                    List.of(Streams.ofValuesAndNulls(
                            new I64Vector(new long[] {-2, 7, 0, 11, 5}),
                            new BooleanVector(new boolean[] {false, false, false, true, false}))),
                    dense,
                    context);
            assertThat(denseResult).isTrue();
            assertThat(dense).containsExactly(1, 4);

            Mask sparse = Mask.sparse(new int[] {0, 1, 3, 4}, 5);
            boolean sparseResult = ((MaskEvaluablePrimitiveFunction) function).tryEvaluateFalseMaskInPlace(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-2, 7, 0, 11, -5}))),
                    sparse,
                    context);
            assertThat(sparseResult).isTrue();
            assertThat(sparse).containsExactly(0, 4);
        }
    }

    @Test
    void testNondeterministicZeroArgumentBooleanTargetRunsOncePerSelectedMaskRow()
            throws Throwable
    {
        CountingBooleanTarget target = new CountingBooleanTarget();
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                        "alternating",
                        new BoundSignature(BOOLEAN, List.of()),
                        new FunctionSemantics(false, List.of(), false, NEVER_FAILS),
                        new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                        CountingBooleanTarget.class,
                                        "nextValue",
                                        MethodType.methodType(boolean.class))
                                .bindTo(target)))
                .implementation();

        Mask mask = Mask.sparse(new int[] {1, 2, 4}, 6);
        try (Allocator allocator = new Allocator(createDefault())) {
            assertThat(((MaskEvaluablePrimitiveFunction) function).tryEvaluateTrueMaskInPlace(
                    List.of(), mask, new PrimitiveExecutionContext(allocator))).isTrue();
        }
        assertThat(mask).containsExactly(1, 4);
        assertThat(target.invocations).isEqualTo(3);
    }

    @Test
    void testNondeterministicTargetDoesNotCollapseRleRuns()
            throws Throwable
    {
        CountingTarget target = new CountingTarget();
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                        "next_value_with_input",
                        new BoundSignature(DOUBLE, List.of(LONG)),
                        new FunctionSemantics(false, List.of(RETURN_NULL_ON_NULL), false, NEVER_FAILS),
                        new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                        CountingTarget.class,
                                        "nextValueWithInput",
                                        MethodType.methodType(double.class, long.class))
                                .bindTo(target)))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(Streams.ofValues(new RleVector(new int[] {2, 3}, new I64Vector(new long[] {11, 22})))),
                    Mask.all(5),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(result.values()).isInstanceOf(F64Vector.class);
            assertThat(((F64Vector) result.values()).values()).containsExactly(1, 2, 3, 4, 5);
        }
        assertThat(target.invocations).isEqualTo(5);
    }

    @Test
    void testRegistryOwnedReferenceResultWritesDirectlyToVector()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "render_long",
                new BoundSignature(STRING_RESULT, List.of(LONG)),
                strictSemantics(1),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "renderLong",
                        MethodType.methodType(String.class, long.class))))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
            Streams dense = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {1, 22, 333}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    context);
            assertThat(strings((BinaryVector) dense.values())).containsExactly("v1", "v22", "v333");

            Streams sparse = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {1, 22, 333}))),
                    Mask.sparse(new int[] {1}, 3),
                    EnumSet.of(Stream.VALUES),
                    Streams.ofValues(binary("keep-left", "old", "keep-right")),
                    context);
            assertThat(strings((BinaryVector) sparse.values())).containsExactly("keep-left", "v22", "keep-right");

            Streams nullable = function.apply(
                    List.of(Streams.ofValuesAndNulls(
                            new I64Vector(new long[] {1, 22, 333}),
                            new BooleanVector(new boolean[] {false, true, false}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    context);
            assertThat(strings((BinaryVector) nullable.values())).containsExactly("v1", "", "v333");
            assertThat(((BooleanVector) nullable.get(Stream.NULLS)).values()).containsExactly(false, true, false);

            Streams valuesOnly = function.apply(
                    List.of(Streams.ofValuesAndNulls(
                            new I64Vector(new long[] {1, 22, 333}),
                            new BooleanVector(new boolean[] {false, true, false}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    context);
            // VALUES beneath an unrequested semantic NULL are deliberately unspecified.
            assertThat(strings((BinaryVector) valuesOnly.values())).containsExactly("v1", "v22", "v333");

            Streams rle = function.apply(
                    List.of(Streams.ofValues(new RleVector(new int[] {2, 3}, new I64Vector(new long[] {1, 22})))),
                    Mask.all(5),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    context);
            assertThat(rle.values()).isInstanceOf(RleVector.class);
            RleVector encoded = (RleVector) rle.values();
            assertThat(encoded.counts()).containsExactly(2, 3);
            assertThat(strings((BinaryVector) encoded.values())).containsExactly("v1", "v22");

            Streams reused = function.apply(
                    List.of(Streams.ofValues(new RleVector(new int[] {2, 3}, new I64Vector(new long[] {333, 1})))),
                    Mask.all(5),
                    EnumSet.of(Stream.VALUES),
                    rle,
                    context);
            assertThat(reused.values()).isInstanceOf(RleVector.class);
            RleVector reusedEncoding = (RleVector) reused.values();
            assertThat(reusedEncoding.counts()).containsExactly(2, 3);
            assertThat(strings((BinaryVector) reusedEncoding.values())).containsExactly("v333", "v1");
        }
    }

    @Test
    void testNullableReferenceResultWritesNullStreamWithoutPassingNullToWriter()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adaptNullableReferenceResult(
                "render_odd",
                new BoundSignature(STRING_RESULT, List.of(LONG)),
                new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), true, NEVER_FAILS),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "renderOdd",
                        MethodType.methodType(String.class, long.class))))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
            assertThat(function.requiredInputStreams(0, EnumSet.of(Stream.NULLS)))
                    .containsExactlyInAnyOrder(Stream.VALUES, Stream.NULLS);
            Streams nullsOnly = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {1, 2, 4, 5}))),
                    Mask.all(4),
                    EnumSet.of(Stream.NULLS),
                    Streams.empty(),
                    context);
            assertThat(nullsOnly.hasValues()).isFalse();
            assertThat(((BooleanVector) nullsOnly.get(Stream.NULLS)).values()).containsExactly(false, true, true, false);

            Streams dense = function.apply(
                    List.of(Streams.ofValuesAndNulls(
                            new I64Vector(new long[] {1, 2, 3, 5}),
                            new BooleanVector(new boolean[] {false, false, true, false}))),
                    Mask.all(4),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    context);
            assertThat(strings((BinaryVector) dense.values())).containsExactly("v1", "", "", "v5");
            assertThat(((BooleanVector) dense.get(Stream.NULLS)).values()).containsExactly(false, true, true, false);

            Streams dictionary = function.apply(
                    List.of(Streams.ofValues(new DictionaryVector(
                            new int[] {0, 1, 0, 2},
                            new I64Vector(new long[] {7, 8, 9})))),
                    Mask.all(4),
                    EnumSet.of(Stream.VALUES, Stream.NULLS),
                    Streams.empty(),
                    context);
            assertThat(strings((BinaryVector) dictionary.values())).containsExactly("v7", "", "v7", "v9");
            assertThat(((BooleanVector) dictionary.get(Stream.NULLS)).values()).containsExactly(false, true, false, false);
        }
    }

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
            assertThat(allocator.isSharedAllFalseBoolean(result.get(Stream.NULLS))).isTrue();
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

    @Test
    void testFallibleTargetOnlyObservesSelectedPositions()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "require_non_negative",
                new BoundSignature(LONG, List.of(LONG)),
                new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, MAY_FAIL),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "requireNonNegative",
                        MethodType.methodType(long.class, long.class))))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
            Streams result = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1, 7}))),
                    Mask.sparse(new int[] {1}, 2),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    context);
            assertThat(((I64Vector) result.values()).values()).containsExactly(0, 7);

            assertThatThrownBy(() -> function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1, 7}))),
                    Mask.all(2),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("negative value");
        }
    }

    @Test
    void testBoundLiteralMatchesRleArgumentAcrossEncodingsAndMasks()
            throws ReflectiveOperationException
    {
        MethodHandle target = MethodHandles.lookup().findStatic(
                TestScalarAdapterGenerator.class, "remainder", MethodType.methodType(long.class, long.class, long.class));
        ErrorValue divisionByZero = new ErrorValue("test", 3, "DIVISION_BY_ZERO", "USER_ERROR", "division by zero");
        ScalarFailureMapper mapper = failure -> {
            if (failure instanceof ArithmeticException) {
                return divisionByZero;
            }
            throw failure;
        };
        List<Vector> inputs = List.of(
                new I64Vector(new long[] {Long.MIN_VALUE, -101, 0, 101, Long.MAX_VALUE}),
                new DictionaryVector(new int[] {2, 0, 1, 2, 0}, new I64Vector(new long[] {-101, 0, 101})),
                new RleVector(new int[] {5}, new I64Vector(new long[] {-101})));
        for (long divisor : new long[] {100, 0}) {
            ScalarAdapterGenerator generator = new ScalarAdapterGenerator();
            PrimitiveFunction unbound = generator.adapt(
                    "rle_remainder",
                    new BoundSignature(LONG, List.of(LONG, LONG)),
                    new FunctionSemantics(true, Collections.nCopies(2, RETURN_NULL_ON_NULL), false, MAY_FAIL),
                    new ScalarMethodTarget(target, mapper)).implementation();
            PrimitiveFunction bound = generator.adapt(
                    "literal_remainder",
                    new BoundSignature(LONG, List.of(LONG)),
                    new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, MAY_FAIL),
                    new ScalarMethodTarget(MethodHandles.insertArguments(target, 1, divisor), mapper)).implementation();
            for (Vector input : inputs) {
                for (Mask mask : List.of(Mask.all(5), Mask.sparse(new int[] {0, 1, 3}, 5))) {
                    try (Allocator allocator = new Allocator(createDefault())) {
                        Streams values = Streams.ofValuesAndNulls(
                                input, new BooleanVector(new boolean[] {false, true, false, false, false}));
                        Streams constant = Streams.ofValues(new RleVector(new int[] {5}, new I64Vector(new long[] {divisor})));
                        PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
                        Streams unboundResult = unbound.apply(
                                List.of(values, constant), mask, EnumSet.allOf(Stream.class), Streams.empty(), context);
                        Streams boundResult = bound.apply(
                                List.of(values), mask, EnumSet.allOf(Stream.class), Streams.empty(), context);
                        for (int position : mask) {
                            boolean isNull = position == 1;
                            assertThat(VectorAccess.isNull(unboundResult.get(Stream.NULLS), position)).isEqualTo(isNull);
                            assertThat(VectorAccess.isNull(boundResult.get(Stream.NULLS), position)).isEqualTo(isNull);
                            boolean fails = !isNull && divisor == 0;
                            assertThat(((ErrorVector) unboundResult.get(Stream.ERRORS)).error(position))
                                    .isEqualTo(fails ? divisionByZero : null);
                            assertThat(((ErrorVector) boundResult.get(Stream.ERRORS)).error(position))
                                    .isEqualTo(fails ? divisionByZero : null);
                            if (!isNull && !fails) {
                                long expected = VectorAccess.longValue(input, position) % divisor;
                                assertThat(VectorAccess.longValue(unboundResult.values(), position)).isEqualTo(expected);
                                assertThat(VectorAccess.longValue(boundResult.values(), position)).isEqualTo(expected);
                            }
                        }
                    }
                }
            }
        }
    }

    private static long remainder(long value, long divisor)
    {
        return value % divisor;
    }

    @Test
    void testFallibleTargetDoesNotObserveNullWhenOnlyValuesRequested()
            throws Throwable
    {
        PrimitiveFunction function = requireNonNegativeFunction();

        assertThat(function.requiredInputStreams(0, EnumSet.of(Stream.VALUES)))
                .containsExactlyInAnyOrder(Stream.VALUES, Stream.NULLS, Stream.ERRORS);

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(Streams.ofValuesAndNulls(
                            new I64Vector(new long[] {-1, 7}),
                            new BooleanVector(new boolean[] {true, false}))),
                    Mask.all(2),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((I64Vector) result.values()).values()).containsExactly(0, 7);
        }
    }

    @Test
    void testFallibleTargetDoesNotObserveInputError()
            throws Throwable
    {
        PrimitiveFunction function = requireNonNegativeFunction();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1, 7}))
                            .with(Stream.ERRORS, new BooleanVector(new boolean[] {true, false}))),
                    Mask.all(2),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((I64Vector) result.values()).values()).containsExactly(0, 7);
        }
    }

    @Test
    void testDeclaredTargetFailuresBecomeRowLocalErrors()
            throws Throwable
    {
        PrimitiveFunction function = requireNonNegativeFunctionWithFailureMapping();

        try (Allocator allocator = new Allocator(createDefault())) {
            ErrorVector upstreamErrors = new ErrorVector(4);
            ErrorValue upstream = new ErrorValue("test", 2, "UPSTREAM", "USER_ERROR", "upstream failure");
            upstreamErrors.setError(2, upstream);
            Streams result = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1, 7, -2, -3}))
                            .with(Stream.ERRORS, upstreamErrors)),
                    Mask.all(4),
                    EnumSet.of(Stream.VALUES, Stream.ERRORS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((I64Vector) result.values()).values()).containsExactly(0, 7, 0, 0);
            ErrorVector errors = (ErrorVector) result.get(Stream.ERRORS);
            assertThat(errors.values()).containsExactly(true, false, true, true);
            assertThat(errors.error(0)).isEqualTo(NEGATIVE_VALUE);
            assertThat(errors.error(1)).isNull();
            assertThat(errors.error(2)).isEqualTo(upstream);
            assertThat(errors.error(3)).isEqualTo(NEGATIVE_VALUE);
        }
    }

    @Test
    void testMappedTargetFailureDoesNotEscapeValuesOnlyEvaluation()
            throws Throwable
    {
        PrimitiveFunction function = requireNonNegativeFunctionWithFailureMapping();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1, 7}))),
                    Mask.all(2),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(((I64Vector) result.values()).values()).containsExactly(0, 7);
            assertThat(result.has(Stream.ERRORS)).isFalse();
        }
    }

    @Test
    void testSparseCompanionStreamsPreserveUnselectedDiagnostics()
            throws Throwable
    {
        int size = 512;
        long[] values = new long[size];
        values[129] = -1;
        values[255] = -2;
        values[300] = -3;
        values[450] = 7;
        boolean[] nulls = new boolean[size];
        nulls[129] = true;
        ErrorValue upstream = new ErrorValue("test", 2, "UPSTREAM", "USER_ERROR", "upstream failure");
        ErrorVector inputErrors = new ErrorVector(size);
        inputErrors.setError(255, upstream);

        try (Allocator allocator = new Allocator(createDefault())) {
            Allocator.Context outputContext = new Allocator.Context("sparse-output");
            ErrorVector errors = allocator.allocate(outputContext, ErrorVector.class, size, ErrorVector::new);
            errors.setError(400, upstream);
            errors.setError(450, upstream);
            BooleanVector outputNulls = allocator.allocate(outputContext, BooleanVector.class, size, BooleanVector::new);
            outputNulls.values()[400] = true;
            Streams destination = Streams.ofValues(allocator.allocate(outputContext, I64Vector.class, size, I64Vector::new))
                    .with(Stream.NULLS, outputNulls)
                    .with(Stream.ERRORS, errors);
            PrimitiveFunction function = requireNonNegativeFunctionWithFailureMapping();
            PrimitiveExecutionContext execution = new PrimitiveExecutionContext(allocator);
            List<Streams> inputs = List.of(Streams.ofValues(new I64Vector(values))
                    .with(Stream.NULLS, new BooleanVector(nulls))
                    .with(Stream.ERRORS, inputErrors));
            Streams result = function.apply(
                    inputs,
                    Mask.sparse(new int[] {129, 255, 300, 450}, size),
                    EnumSet.allOf(Stream.class),
                    destination,
                    execution);

            ErrorVector resultErrors = (ErrorVector) result.get(Stream.ERRORS);
            assertThat(resultErrors.error(129)).isNull();
            assertThat(resultErrors.error(255)).isEqualTo(upstream);
            assertThat(resultErrors.error(300)).isEqualTo(NEGATIVE_VALUE);
            assertThat(resultErrors.error(400)).isEqualTo(upstream);
            assertThat(resultErrors.error(450)).isNull();
            assertThat(resultErrors.values()[129]).isFalse();
            assertThat(resultErrors.values()[450]).isFalse();
            assertThat(((BooleanVector) result.get(Stream.NULLS)).values()[129]).isTrue();
            assertThat(((BooleanVector) result.get(Stream.NULLS)).values()[400]).isTrue();
            assertThat(((I64Vector) result.values()).values()[450]).isEqualTo(7);

            // Reusing scratch must clear selected stale failures without erasing earlier unselected diagnostics.
            values[300] = 9;
            Streams updated = function.apply(
                    inputs,
                    Mask.sparse(new int[] {300}, size),
                    EnumSet.allOf(Stream.class),
                    result,
                    execution);
            ErrorVector updatedErrors = (ErrorVector) updated.get(Stream.ERRORS);
            assertThat(updatedErrors.error(300)).isNull();
            assertThat(updatedErrors.values()[300]).isFalse();
            assertThat(updatedErrors.error(255)).isEqualTo(upstream);
            assertThat(updatedErrors.error(400)).isEqualTo(upstream);
            assertThat(((I64Vector) updated.values()).values()[300]).isEqualTo(9);
            assertThat(((BooleanVector) updated.get(Stream.NULLS)).values()[129]).isTrue();
        }
    }

    @Test
    void testDenseMaskClearsSelectedErrorsInOversizedOutput()
            throws Throwable
    {
        assertDenseMaskClearsSelectedErrorsInOversizedOutput(256);
        assertDenseMaskClearsSelectedErrorsInOversizedOutput(1024);
    }

    private static void assertDenseMaskClearsSelectedErrorsInOversizedOutput(int inputSize)
            throws Throwable
    {
        int selectedSize = 256;
        int outputSize = 512;
        ErrorValue upstream = new ErrorValue("test", 2, "UPSTREAM", "USER_ERROR", "upstream failure");
        for (boolean hasInputError : List.of(false, true)) {
            ErrorVector inputErrors = new ErrorVector(inputSize);
            if (hasInputError) {
                inputErrors.setError(200, upstream);
            }
            try (Allocator allocator = new Allocator(createDefault())) {
                Allocator.Context outputContext = new Allocator.Context("oversized-error-output");
                ErrorVector errors = allocator.allocate(outputContext, ErrorVector.class, outputSize, ErrorVector::new);
                errors.setError(129, NEGATIVE_VALUE);
                errors.setError(400, upstream);
                Streams destination = Streams.ofValues(allocator.allocate(outputContext, I64Vector.class, outputSize, I64Vector::new))
                        .with(Stream.ERRORS, errors);
                Streams result = requireNonNegativeFunctionWithFailureMapping().apply(
                        List.of(Streams.ofValues(new I64Vector(inputSize)).with(Stream.ERRORS, inputErrors)),
                        Mask.all(selectedSize),
                        EnumSet.of(Stream.VALUES, Stream.ERRORS),
                        destination,
                        new PrimitiveExecutionContext(allocator));

                ErrorVector resultErrors = (ErrorVector) result.get(Stream.ERRORS);
                assertThat(resultErrors.length()).isGreaterThanOrEqualTo(Math.max(outputSize, inputSize));
                assertThat(resultErrors.error(129)).isNull();
                assertThat(resultErrors.values()[129]).isFalse();
                assertThat(resultErrors.error(400)).isEqualTo(upstream);
                assertThat(resultErrors.values()[400]).isTrue();
                assertThat(resultErrors.values()[200]).isEqualTo(hasInputError);
            }
        }
    }

    @Test
    void testUnrequestedErrorsStillValidateFailureMapper()
            throws Throwable
    {
        IllegalStateException fatal = new IllegalStateException("unmapped failure");
        PrimitiveFunction rejecting = requireNonNegativeFunctionWithFailureMapping(failure -> {
            throw fatal;
        });
        PrimitiveFunction invalid = requireNonNegativeFunctionWithFailureMapping(failure -> null);

        for (EnumSet<Stream> demand : List.of(EnumSet.of(Stream.VALUES), EnumSet.of(Stream.VALUES, Stream.ERRORS))) {
            try (Allocator allocator = new Allocator(createDefault())) {
                List<Streams> inputs = List.of(Streams.ofValues(new I64Vector(new long[] {-1})));
                PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
                assertThatThrownBy(() -> rejecting.apply(inputs, Mask.all(1), demand, Streams.empty(), context))
                        .isSameAs(fatal);
                assertThatThrownBy(() -> invalid.apply(inputs, Mask.all(1), demand, Streams.empty(), context))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("error is null");
            }
        }
    }

    @Test
    void testErrorOnlyDemandInvokesOnlyFallibleTargets()
            throws Throwable
    {
        PrimitiveFunction fallible = requireNonNegativeFunctionWithFailureMapping();
        assertThat(fallible.requiredInputStreams(0, EnumSet.of(Stream.ERRORS)))
                .containsExactlyInAnyOrder(Stream.VALUES, Stream.NULLS, Stream.ERRORS);

        CountingTarget target = new CountingTarget();
        PrimitiveFunction infallible = new ScalarAdapterGenerator().adapt(
                "counting",
                new BoundSignature(LONG, List.of(LONG)),
                strictSemantics(1),
                new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                CountingTarget.class,
                                "apply",
                                MethodType.methodType(long.class, long.class))
                        .bindTo(target)))
                .implementation();
        assertThat(infallible.requiredInputStreams(0, EnumSet.of(Stream.ERRORS))).containsExactly(Stream.ERRORS);

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams failures = fallible.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1, 7}))),
                    Mask.all(2),
                    EnumSet.of(Stream.ERRORS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(((ErrorVector) failures.get(Stream.ERRORS)).values()).containsExactly(true, false);

            ErrorVector upstream = new ErrorVector(2);
            upstream.setError(1, NEGATIVE_VALUE);
            Streams propagated = infallible.apply(
                    List.of(Streams.of(Stream.ERRORS, upstream)),
                    Mask.all(2),
                    EnumSet.of(Stream.ERRORS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            assertThat(((ErrorVector) propagated.get(Stream.ERRORS)).values()).containsExactly(false, true);
            assertThat(target.invocations).isZero();

            Streams presenceOnly = infallible.apply(
                    List.of(Streams.of(Stream.ERRORS, new BooleanVector(new boolean[] {true, false}))),
                    Mask.all(2),
                    EnumSet.of(Stream.ERRORS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));
            ErrorVector presenceErrors = (ErrorVector) presenceOnly.get(Stream.ERRORS);
            assertThat(presenceErrors.values()).containsExactly(true, false);
            assertThat(presenceErrors.error(0)).isNull();
            assertThat(presenceErrors.isAllFalse()).isFalse();
            assertThat(target.invocations).isZero();
        }
    }

    @Test
    void testReferenceResultWriterContinuesAfterMappedTargetFailure()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "render_non_negative",
                new BoundSignature(STRING_RESULT, List.of(LONG)),
                new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, MAY_FAIL),
                new ScalarMethodTarget(
                        MethodHandles.lookup().findStatic(
                                TestScalarAdapterGenerator.class,
                                "renderNonNegative",
                                MethodType.methodType(String.class, long.class)),
                        TestScalarAdapterGenerator::mapNegativeValue))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            Streams result = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1, 7, -2}))),
                    Mask.all(3),
                    EnumSet.of(Stream.VALUES, Stream.ERRORS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator));

            assertThat(strings((BinaryVector) result.values())).containsExactly("", "v7", "");
            ErrorVector errors = (ErrorVector) result.get(Stream.ERRORS);
            assertThat(errors.values()).containsExactly(true, false, true);
            assertThat(errors.error(0)).isEqualTo(NEGATIVE_VALUE);
            assertThat(errors.error(2)).isEqualTo(NEGATIVE_VALUE);
        }
    }

    @Test
    void testFailureMapperDoesNotClassifyResultWriterFailures()
            throws Throwable
    {
        TypeBinding failingResult = new ResultTypeBinding(
                new TypeIdentity("failing-result"),
                String.class,
                failingResultWriter());
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "render_long",
                new BoundSignature(failingResult, List.of(LONG)),
                new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, MAY_FAIL),
                new ScalarMethodTarget(
                        MethodHandles.lookup().findStatic(
                                TestScalarAdapterGenerator.class,
                                "renderLong",
                                MethodType.methodType(String.class, long.class)),
                        _ -> NEGATIVE_VALUE))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            assertThatThrownBy(() -> function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {7}))),
                    Mask.all(1),
                    EnumSet.of(Stream.VALUES, Stream.ERRORS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("writer failure");
        }
    }

    @Test
    void testFailureMapperIsIgnoredForInfallibleSemantics()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "require_non_negative",
                new BoundSignature(LONG, List.of(LONG)),
                strictSemantics(1),
                new ScalarMethodTarget(
                        MethodHandles.lookup().findStatic(
                                TestScalarAdapterGenerator.class,
                                "requireNonNegative",
                                MethodType.methodType(long.class, long.class)),
                        _ -> NEGATIVE_VALUE))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            assertThatThrownBy(() -> function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1}))),
                    Mask.all(1),
                    EnumSet.of(Stream.VALUES, Stream.ERRORS),
                    Streams.empty(),
                    new PrimitiveExecutionContext(allocator)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("negative value");
        }
    }

    private static PrimitiveFunction requireNonNegativeFunction()
            throws ReflectiveOperationException
    {
        return new ScalarAdapterGenerator().adapt(
                "require_non_negative",
                new BoundSignature(LONG, List.of(LONG)),
                new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, MAY_FAIL),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "requireNonNegative",
                        MethodType.methodType(long.class, long.class))))
                .implementation();
    }

    private static PrimitiveFunction requireNonNegativeFunctionWithFailureMapping()
            throws ReflectiveOperationException
    {
        return requireNonNegativeFunctionWithFailureMapping(TestScalarAdapterGenerator::mapNegativeValue);
    }

    private static PrimitiveFunction requireNonNegativeFunctionWithFailureMapping(ScalarFailureMapper failureMapper)
            throws ReflectiveOperationException
    {
        return new ScalarAdapterGenerator().adapt(
                "require_non_negative",
                new BoundSignature(LONG, List.of(LONG)),
                new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, MAY_FAIL),
                new ScalarMethodTarget(
                        MethodHandles.lookup().findStatic(
                                TestScalarAdapterGenerator.class,
                                "requireNonNegative",
                                MethodType.methodType(long.class, long.class)),
                        failureMapper))
                .implementation();
    }

    private static ErrorValue mapNegativeValue(Throwable failure)
            throws Throwable
    {
        if (failure instanceof IllegalArgumentException) {
            return NEGATIVE_VALUE;
        }
        throw failure;
    }

    @Test
    void testReferenceResultWriterRecoversAfterTargetFailure()
            throws Throwable
    {
        PrimitiveFunction function = new ScalarAdapterGenerator().adapt(
                "render_non_negative",
                new BoundSignature(STRING_RESULT, List.of(LONG)),
                new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, MAY_FAIL),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                        TestScalarAdapterGenerator.class,
                        "renderNonNegative",
                        MethodType.methodType(String.class, long.class))))
                .implementation();

        try (Allocator allocator = new Allocator(createDefault())) {
            PrimitiveExecutionContext context = new PrimitiveExecutionContext(allocator);
            assertThatThrownBy(() -> function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {-1}))),
                    Mask.all(1),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("negative value");

            Streams recovered = function.apply(
                    List.of(Streams.ofValues(new I64Vector(new long[] {7}))),
                    Mask.all(1),
                    EnumSet.of(Stream.VALUES),
                    Streams.empty(),
                    context);
            assertThat(strings((BinaryVector) recovered.values())).containsExactly("v7");
        }
    }

    private static FunctionSemantics strictSemantics(int arity)
    {
        return new FunctionSemantics(true, Collections.nCopies(arity, RETURN_NULL_ON_NULL), false, NEVER_FAILS);
    }

    private static double selectArithmetic(long left, double right, boolean add)
    {
        return add ? left + right : left - right;
    }

    private static boolean positive(long value)
    {
        return value > 0;
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

    private static String renderLong(long value)
    {
        return "v" + value;
    }

    private static String renderOdd(long value)
    {
        return (value & 1) == 0 ? null : renderLong(value);
    }

    private static String renderNonNegative(long value)
    {
        requireNonNegative(value);
        return renderLong(value);
    }

    private static long requireNonNegative(long value)
    {
        if (value < 0) {
            throw new IllegalArgumentException("negative value");
        }
        return value;
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

    private static List<String> strings(BinaryVector vector)
    {
        java.util.ArrayList<String> result = new java.util.ArrayList<>();
        for (int position = 0; position < vector.length(); position++) {
            result.add(new String(vector.data(), vector.startOffset(position), vector.length(position), UTF_8));
        }
        return result;
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

    private static ScalarResultWriterFactory resultWriter()
    {
        MethodHandle append;
        try {
            append = MethodHandles.lookup().findStatic(
                    TestScalarAdapterGenerator.class,
                    "appendString",
                    MethodType.methodType(void.class, ScalarResultWriter.class, int.class, String.class));
        }
        catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
        MethodHandle target = append;
        return new ScalarResultWriterFactory()
        {
            @Override
            public ScalarResultWriter createWriter()
            {
                return new StringResultWriter();
            }

            @Override
            public MethodHandle appendTarget()
            {
                return target;
            }
        };
    }

    private static ScalarResultWriterFactory failingResultWriter()
    {
        MethodHandle append;
        try {
            append = MethodHandles.lookup().findStatic(
                    TestScalarAdapterGenerator.class,
                    "failAppend",
                    MethodType.methodType(void.class, ScalarResultWriter.class, int.class, String.class));
        }
        catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
        MethodHandle target = append;
        return new ScalarResultWriterFactory()
        {
            @Override
            public ScalarResultWriter createWriter()
            {
                return new StringResultWriter();
            }

            @Override
            public MethodHandle appendTarget()
            {
                return target;
            }
        };
    }

    private static void failAppend(ScalarResultWriter writer, int position, String value)
    {
        throw new IllegalStateException("writer failure");
    }

    private static void appendString(ScalarResultWriter writer, int position, String value)
    {
        ((StringResultWriter) writer).append(position, value);
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

        public double nextValue()
        {
            return ++invocations;
        }

        public double nextValueWithInput(long ignored)
        {
            return ++invocations;
        }
    }

    private static final class CountingBooleanTarget
    {
        private int invocations;

        private boolean nextValue()
        {
            invocations++;
            return (invocations & 1) != 0;
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

    private record ResultTypeBinding(TypeIdentity identity, Class<?> carrierType, ScalarResultWriterFactory writerFactory)
            implements TypeBinding
    {
        @Override
        public Optional<ScalarResultWriterFactory> scalarResultWriterFactory()
        {
            return Optional.of(writerFactory);
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    }

    private static final class StringResultWriter
            implements ScalarResultWriter
    {
        private static final byte[] EMPTY = new byte[0];

        private VectorAllocator allocator;
        private BinaryVector proposed;
        private BinaryVector output;
        private Mask active;
        private int[] activePositions;
        private int activeIndex;
        private int nextPosition;
        private int positionCount;
        private int bytesUsed;

        @Override
        public void begin(VectorAllocator allocator, Vector proposed, int positionCount, Mask activeMask)
        {
            this.allocator = allocator;
            this.proposed = proposed instanceof BinaryVector binary ? binary : null;
            this.positionCount = positionCount;
            this.active = activeMask;
            this.activePositions = activeMask.selectedPositions();
            int proposedBytes = this.proposed == null ? 0 : this.proposed.offsets()[Math.min(positionCount, this.proposed.length())];
            output = BinaryVector.allocate(allocator, positionCount, Math.max(32, proposedBytes + positionCount * 8));
        }

        private void append(int position, String value)
        {
            fillUntil(position);
            write(value.getBytes(UTF_8));
            advanceActive(position);
        }

        @Override
        public Vector finish()
        {
            fillUntil(positionCount);
            BinaryVector result = output;
            if (proposed != null && allocator.owns(proposed)) {
                allocator.release(proposed);
            }
            clear();
            return result;
        }

        @Override
        public void abort()
        {
            if (allocator != null && output != null && allocator.owns(output)) {
                allocator.release(output);
            }
            clear();
        }

        private void fillUntil(int target)
        {
            while (nextPosition < target) {
                if (isActive(nextPosition)) {
                    write(EMPTY);
                    advanceActive(nextPosition - 1);
                }
                else if (proposed != null && nextPosition < proposed.length()) {
                    ensureCapacity(proposed.length(nextPosition));
                    output.setBytes(nextPosition, proposed.data(), proposed.startOffset(nextPosition), proposed.length(nextPosition));
                    bytesUsed += proposed.length(nextPosition);
                    nextPosition++;
                }
                else {
                    write(EMPTY);
                }
            }
        }

        private boolean isActive(int position)
        {
            return active.all() || (activeIndex < active.count() && activePositions[activeIndex] == position);
        }

        private void advanceActive(int position)
        {
            nextPosition = position + 1;
            if (!active.all()) {
                activeIndex++;
            }
        }

        private void write(byte[] value)
        {
            ensureCapacity(value.length);
            output.setBytes(nextPosition, value);
            bytesUsed += value.length;
            nextPosition++;
        }

        private void ensureCapacity(int additionalBytes)
        {
            if (bytesUsed + additionalBytes <= output.byteCapacity()) {
                return;
            }
            BinaryVector grown = BinaryVector.allocate(allocator, positionCount, Math.max(bytesUsed + additionalBytes, output.byteCapacity() * 2));
            System.arraycopy(output.offsets(), 0, grown.offsets(), 0, nextPosition + 1);
            System.arraycopy(output.data(), 0, grown.data(), 0, bytesUsed);
            allocator.release(output);
            output = grown;
        }

        private void clear()
        {
            allocator = null;
            proposed = null;
            output = null;
            active = null;
            activePositions = null;
            activeIndex = 0;
            nextPosition = 0;
            positionCount = 0;
            bytesUsed = 0;
        }
    }
}
