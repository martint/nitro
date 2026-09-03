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
package org.weakref.nitro;

import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.FunctionCapability;
import org.weakref.nitro.core.function.FunctionSemantics;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.function.scalar.AnnotatedScalarLoader;
import org.weakref.nitro.function.scalar.ScalarAdapterGenerator;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.function.scalar.ScalarMethodTarget;
import org.weakref.nitro.function.scalar.builtin.AddF64Optimization;
import org.weakref.nitro.function.scalar.builtin.AddI64Optimization;
import org.weakref.nitro.function.scalar.builtin.AndBoolean;
import org.weakref.nitro.function.scalar.builtin.ArrayElementI64;
import org.weakref.nitro.function.scalar.builtin.BigintAddExact;
import org.weakref.nitro.function.scalar.builtin.BigintDivide;
import org.weakref.nitro.function.scalar.builtin.BigintModulus;
import org.weakref.nitro.function.scalar.builtin.BigintRatio;
import org.weakref.nitro.function.scalar.builtin.BigintSubtractExact;
import org.weakref.nitro.function.scalar.builtin.BigintToIntegerExact;
import org.weakref.nitro.function.scalar.builtin.Cardinality;
import org.weakref.nitro.function.scalar.builtin.CoalesceI64;
import org.weakref.nitro.function.scalar.builtin.CoalesceI64Policy;
import org.weakref.nitro.function.scalar.builtin.ConcatUtf8;
import org.weakref.nitro.function.scalar.builtin.ContainsUtf8;
import org.weakref.nitro.function.scalar.builtin.ElementAtI64Utf8;
import org.weakref.nitro.function.scalar.builtin.EqualF64;
import org.weakref.nitro.function.scalar.builtin.EqualI64;
import org.weakref.nitro.function.scalar.builtin.EqualUtf8;
import org.weakref.nitro.function.scalar.builtin.IdenticalI64;
import org.weakref.nitro.function.scalar.builtin.IfF64;
import org.weakref.nitro.function.scalar.builtin.IfI64;
import org.weakref.nitro.function.scalar.builtin.IfUtf8;
import org.weakref.nitro.function.scalar.builtin.InUtf8;
import org.weakref.nitro.function.scalar.builtin.IsNullI32;
import org.weakref.nitro.function.scalar.builtin.IsNullI64;
import org.weakref.nitro.function.scalar.builtin.JoniRegexpPolicy;
import org.weakref.nitro.function.scalar.builtin.LengthUtf8;
import org.weakref.nitro.function.scalar.builtin.LessThanF64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualF64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualI64;
import org.weakref.nitro.function.scalar.builtin.LessThanOrEqualUtf8;
import org.weakref.nitro.function.scalar.builtin.LessThanUtf8;
import org.weakref.nitro.function.scalar.builtin.LikeUtf8;
import org.weakref.nitro.function.scalar.builtin.LikeUtf8Policy;
import org.weakref.nitro.function.scalar.builtin.MapContainsKeyUtf8;
import org.weakref.nitro.function.scalar.builtin.MapEntries;
import org.weakref.nitro.function.scalar.builtin.MaterializeLongCarrier;
import org.weakref.nitro.function.scalar.builtin.MultiplyF64Optimization;
import org.weakref.nitro.function.scalar.builtin.MultiplyI64Optimization;
import org.weakref.nitro.function.scalar.builtin.NotBooleanOptimization;
import org.weakref.nitro.function.scalar.builtin.NullI64;
import org.weakref.nitro.function.scalar.builtin.OrBoolean;
import org.weakref.nitro.function.scalar.builtin.RegexpReplaceUtf8;
import org.weakref.nitro.function.scalar.builtin.RegexpReplaceUtf8Policy;
import org.weakref.nitro.function.scalar.builtin.RoundedBigintRatio;
import org.weakref.nitro.function.scalar.builtin.ScaledRoundedBigintRatio;
import org.weakref.nitro.function.scalar.builtin.StartsWithUtf8;
import org.weakref.nitro.function.scalar.builtin.SubstringUtf8;
import org.weakref.nitro.function.scalar.builtin.SubtractF64Optimization;
import org.weakref.nitro.function.scalar.builtin.SubtractI64Optimization;
import org.weakref.nitro.function.scalar.builtin.UpperUtf8;
import org.weakref.nitro.function.scalar.builtin.Utf8BinaryDispatchPolicy;
import org.weakref.nitro.function.scalar.builtin.VarcharToBigint;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.stream.Stream;

import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;

public final class TestPrimitiveFunctions
{
    private TestPrimitiveFunctions() {}

    public static PrimitiveRegistry primitiveRegistry()
    {
        AnnotatedScalarLoader scalarLoader = new AnnotatedScalarLoader();
        PrimitiveRegistry primitiveRegistry = new PrimitiveRegistry();
        Utf8BinaryDispatchPolicy utf8Policy = Utf8BinaryDispatchPolicy.fromSystemProperties();
        for (Class<?> functionClass : List.of(
                ArrayElementI64.class,
                Cardinality.class,
                BigintToIntegerExact.class,
                MaterializeLongCarrier.class,
                VarcharToBigint.class,
                CoalesceI64.class,
                ConcatUtf8.class,
                ContainsUtf8.class,
                ElementAtI64Utf8.class,
                EqualF64.class,
                EqualI64.class,
                EqualUtf8.class,
                LessThanF64.class,
                LessThanOrEqualF64.class,
                LessThanOrEqualI64.class,
                LessThanOrEqualUtf8.class,
                IfF64.class,
                IfI64.class,
                IfUtf8.class,
                IdenticalI64.class,
                IsNullI32.class,
                IsNullI64.class,
                InUtf8.class,
                LessThanI64.class,
                LikeUtf8.class,
                LessThanUtf8.class,
                LengthUtf8.class,
                MapContainsKeyUtf8.class,
                NullI64.class,
                BigintAddExact.class,
                BigintSubtractExact.class,
                BigintDivide.class,
                BigintRatio.class,
                RoundedBigintRatio.class,
                ScaledRoundedBigintRatio.class,
                BigintModulus.class,
                AndBoolean.class,
                OrBoolean.class,
                RegexpReplaceUtf8.class,
                StartsWithUtf8.class,
                SubstringUtf8.class,
                UpperUtf8.class)) {
            primitiveRegistry.register(load(scalarLoader, functionClass, utf8Policy));
        }
        primitiveRegistry.register("map_keys", new MapEntries(MapEntries.Entry.KEY));
        primitiveRegistry.register("map_values", new MapEntries(MapEntries.Entry.VALUE));
        TypeBinding bigint = new TestingTypeBinding(new TypeIdentity("test-bigint"), long.class);
        TypeBinding doubleType = new TestingTypeBinding(new TypeIdentity("test-double"), double.class);
        primitiveRegistry.register(generatedBinary("add", "addBigint", bigint, new AddI64Optimization()));
        primitiveRegistry.register(generatedBinary("subtract", "subtractBigint", bigint, new SubtractI64Optimization()));
        primitiveRegistry.register(generatedBinary("multiply", "multiplyBigint", bigint, new MultiplyI64Optimization()));
        primitiveRegistry.register(generatedBinary("add_f64", "addDouble", doubleType, new AddF64Optimization()));
        primitiveRegistry.register(generatedBinary("subtract_f64", "subtractDouble", doubleType, new SubtractF64Optimization()));
        primitiveRegistry.register(generatedBinary("multiply_f64", "multiplyDouble", doubleType, new MultiplyF64Optimization()));
        primitiveRegistry.register(generatedBooleanUnary("not", "notBoolean", new NotBooleanOptimization()));
        primitiveRegistry.register(generatedDoubleUnary("round_f64", "roundDouble"));
        primitiveRegistry.register(generatedBigintToDoubleCast());
        primitiveRegistry.register(generatedYearOfDate());
        return primitiveRegistry;
    }

    private static ScalarDescriptor generatedBigintToDoubleCast()
    {
        TypeBinding bigint = new TestingTypeBinding(new TypeIdentity("test-bigint"), long.class);
        TypeBinding doubleType = new TestingTypeBinding(new TypeIdentity("test-double"), double.class);
        try {
            return new ScalarAdapterGenerator().adapt(
                    "cast_bigint_to_double",
                    new BoundSignature(doubleType, List.of(bigint)),
                    new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, NEVER_FAILS),
                    new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                            TestPrimitiveFunctions.class,
                            "bigintToDouble",
                            MethodType.methodType(double.class, long.class))));
        }
        catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private static double bigintToDouble(long value)
    {
        return value;
    }

    private static ScalarDescriptor generatedDoubleUnary(String name, String methodName)
    {
        TypeBinding doubleType = new TestingTypeBinding(new TypeIdentity("test-double"), double.class);
        try {
            return new ScalarAdapterGenerator().adapt(
                    name,
                    new BoundSignature(doubleType, List.of(doubleType)),
                    new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, NEVER_FAILS),
                    new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                            TestPrimitiveFunctions.class,
                            methodName,
                            MethodType.methodType(double.class, double.class))));
        }
        catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private static double roundDouble(double value)
    {
        return Math.copySign(Math.floor(Math.abs(value) + 0.5), value);
    }

    private static ScalarDescriptor generatedBooleanUnary(String name, String methodName, FunctionCapability capability)
    {
        TypeBinding booleanType = new TestingTypeBinding(new TypeIdentity("test-boolean"), boolean.class);
        ScalarDescriptor generated;
        try {
            generated = new ScalarAdapterGenerator().adapt(
                    name,
                    new BoundSignature(booleanType, List.of(booleanType)),
                    new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, NEVER_FAILS),
                    new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                            TestPrimitiveFunctions.class,
                            methodName,
                            MethodType.methodType(boolean.class, boolean.class))));
        }
        catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new ExceptionInInitializerError(exception);
        }
        return new ScalarDescriptor(
                generated.name(),
                generated.deterministic(),
                generated.implementation(),
                Stream.concat(generated.capabilities().stream(), Stream.of(capability)).toList());
    }

    private static boolean notBoolean(boolean value)
    {
        return !value;
    }

    private static ScalarDescriptor generatedBinary(String name, String methodName, TypeBinding type, FunctionCapability capability)
    {
        Class<?> carrier = type.carrierType();
        ScalarDescriptor generated;
        try {
            generated = new ScalarAdapterGenerator().adapt(
                    name,
                    new BoundSignature(type, List.of(type, type)),
                    new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL, RETURN_NULL_ON_NULL), false, NEVER_FAILS),
                    new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                            TestPrimitiveFunctions.class,
                            methodName,
                            MethodType.methodType(carrier, carrier, carrier))));
        }
        catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new ExceptionInInitializerError(exception);
        }
        return new ScalarDescriptor(
                generated.name(),
                generated.deterministic(),
                generated.implementation(),
                Stream.concat(generated.capabilities().stream(), Stream.of(capability)).toList());
    }

    private static double addDouble(double left, double right)
    {
        return left + right;
    }

    private static double subtractDouble(double left, double right)
    {
        return left - right;
    }

    private static double multiplyDouble(double left, double right)
    {
        return left * right;
    }

    private static long subtractBigint(long left, long right)
    {
        return left - right;
    }

    private static long multiplyBigint(long left, long right)
    {
        return left * right;
    }

    private static long addBigint(long left, long right)
    {
        return left + right;
    }

    private static ScalarDescriptor generatedYearOfDate()
    {
        TypeBinding date = new TestingTypeBinding(new TypeIdentity("test-date"), long.class);
        TypeBinding bigint = new TestingTypeBinding(new TypeIdentity("test-bigint"), long.class);
        try {
            return new ScalarAdapterGenerator().adapt(
                    "year_of_date",
                    new BoundSignature(bigint, List.of(date)),
                    new FunctionSemantics(true, List.of(RETURN_NULL_ON_NULL), false, NEVER_FAILS),
                    new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                            TestPrimitiveFunctions.class,
                            "yearOfEpochDay",
                            MethodType.methodType(long.class, long.class))));
        }
        catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private static long yearOfEpochDay(long epochDay)
    {
        long z = epochDay + 719468;
        long era = Math.floorDiv(z, 146097);
        long dayOfEra = z - era * 146097;
        long yearOfEra = (dayOfEra - dayOfEra / 1460 + dayOfEra / 36524 - dayOfEra / 146096) / 365;
        long year = yearOfEra + era * 400;
        long dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100);
        long monthPrime = (5 * dayOfYear + 2) / 153;
        long month = monthPrime < 10 ? monthPrime + 3 : monthPrime - 9;
        return month <= 2 ? year + 1 : year;
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

    private static ScalarDescriptor load(
            AnnotatedScalarLoader scalarLoader,
            Class<?> functionClass,
            Utf8BinaryDispatchPolicy utf8Policy)
    {
        if (functionClass == LikeUtf8.class) {
            return scalarLoader.load(new LikeUtf8(LikeUtf8Policy.fromSystemProperties()));
        }
        if (functionClass == CoalesceI64.class) {
            return scalarLoader.load(new CoalesceI64(CoalesceI64Policy.fromSystemProperties()));
        }
        if (functionClass == RegexpReplaceUtf8.class) {
            return scalarLoader.load(new RegexpReplaceUtf8(
                    RegexpReplaceUtf8Policy.fromSystemProperties(),
                    JoniRegexpPolicy.fromSystemProperties()));
        }
        if (functionClass == EqualUtf8.class ||
                functionClass == LessThanUtf8.class ||
                functionClass == LessThanOrEqualUtf8.class ||
                functionClass == StartsWithUtf8.class ||
                functionClass == ContainsUtf8.class ||
                functionClass == InUtf8.class) {
            if (functionClass == EqualUtf8.class) {
                return scalarLoader.load(new EqualUtf8(utf8Policy));
            }
            if (functionClass == LessThanUtf8.class) {
                return scalarLoader.load(new LessThanUtf8(utf8Policy));
            }
            if (functionClass == LessThanOrEqualUtf8.class) {
                return scalarLoader.load(new LessThanOrEqualUtf8(utf8Policy));
            }
            if (functionClass == StartsWithUtf8.class) {
                return scalarLoader.load(new StartsWithUtf8(utf8Policy));
            }
            if (functionClass == ContainsUtf8.class) {
                return scalarLoader.load(new ContainsUtf8(utf8Policy));
            }
            return scalarLoader.load(new InUtf8(utf8Policy));
        }
        return scalarLoader.load(functionClass);
    }
}
