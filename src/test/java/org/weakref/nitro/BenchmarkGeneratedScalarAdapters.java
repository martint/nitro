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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.FunctionSemantics;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarAdapterGenerator;
import org.weakref.nitro.function.scalar.ScalarMethodTarget;
import org.weakref.nitro.function.scalar.builtin.HandwrittenBigintAdd;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;

/**
 * Compares framework-generated scalar loops with representative handwritten fixed-width batch implementations.
 * The benchmark exercises the complete {@link PrimitiveFunction} contract, including mask and encoding dispatch,
 * while reusing output storage to model evaluator steady state.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 8, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 750, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkGeneratedScalarAdapters.POSITION_COUNT)
public class BenchmarkGeneratedScalarAdapters
{
    static final int POSITION_COUNT = 8192;
    private static final TypeBinding LONG = new BenchmarkTypeBinding("benchmark:long", long.class);
    private static final TypeBinding DOUBLE = new BenchmarkTypeBinding("benchmark:double", double.class);
    private static final Set<Stream> VALUES = Set.of(Stream.VALUES);

    @Param({"flat", "dictionary", "rle"})
    private String encoding;

    @Param({"dense", "sparse"})
    private String selection;

    private EngineResources resources;
    private Allocator allocator;
    private PrimitiveExecutionContext context;
    private Mask mask;
    private List<Streams> binaryInputs;
    private List<Streams> compositeInputs;
    private List<Streams> materializedAddInputs;
    private List<Streams> unaryInput;
    private Streams longOutput;
    private Streams doubleOutput;
    private PrimitiveFunction nativeAdd;
    private PrimitiveFunction generatedAdd;
    private PrimitiveFunction generatedBoundAdd;
    private PrimitiveFunction generatedCast;
    private PrimitiveFunction generatedBoundCast;
    private PrimitiveFunction generatedMultiply;
    private PrimitiveFunction generatedDirectComposite;
    private PrimitiveFunction generatedComposedComposite;
    private Streams intermediateOutput;

    @Setup
    public void setup()
            throws ReflectiveOperationException
    {
        resources = EngineResources.createDefault();
        allocator = new Allocator(resources);
        context = new PrimitiveExecutionContext(allocator);
        mask = createMask(selection);

        SplittableRandom random = new SplittableRandom(42);
        long[] left = new long[POSITION_COUNT];
        long[] right = new long[POSITION_COUNT];
        for (int position = 0; position < POSITION_COUNT; position++) {
            left[position] = random.nextLong();
            right[position] = random.nextLong();
        }
        Vector leftVector = encode(encoding, left);
        Vector rightVector = encode(encoding, right);
        long[] offset = new long[POSITION_COUNT];
        for (int position = 0; position < POSITION_COUNT; position++) {
            offset[position] = random.nextLong();
        }
        Vector offsetVector = encode(encoding, offset);
        binaryInputs = List.of(Streams.ofValues(leftVector), Streams.ofValues(rightVector));
        compositeInputs = List.of(Streams.ofValues(leftVector), Streams.ofValues(rightVector), Streams.ofValues(offsetVector));
        unaryInput = List.of(Streams.ofValues(leftVector));
        longOutput = Streams.ofValues(new I64Vector(POSITION_COUNT));
        intermediateOutput = Streams.ofValues(new I64Vector(POSITION_COUNT));
        materializedAddInputs = List.of(intermediateOutput, Streams.ofValues(offsetVector));
        doubleOutput = Streams.ofValues(new F64Vector(POSITION_COUNT));

        nativeAdd = new HandwrittenBigintAdd();
        ScalarAdapterGenerator generator = new ScalarAdapterGenerator();
        generatedAdd = generator.adapt(
                "generated_add",
                new BoundSignature(LONG, List.of(LONG, LONG)),
                strictSemantics(2),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                                BenchmarkGeneratedScalarAdapters.class,
                                "add",
                                MethodType.methodType(long.class, long.class, long.class))))
                .implementation();
        generatedCast = generator.adapt(
                "generated_cast",
                new BoundSignature(DOUBLE, List.of(LONG)),
                strictSemantics(1),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                                BenchmarkGeneratedScalarAdapters.class,
                                "cast",
                                MethodType.methodType(double.class, long.class))))
                .implementation();
        generatedMultiply = generator.adapt(
                "generated_multiply",
                new BoundSignature(LONG, List.of(LONG, LONG)),
                strictSemantics(2),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                                BenchmarkGeneratedScalarAdapters.class,
                                "multiply",
                                MethodType.methodType(long.class, long.class, long.class))))
                .implementation();
        generatedDirectComposite = generator.adapt(
                "generated_direct_composite",
                new BoundSignature(LONG, List.of(LONG, LONG, LONG)),
                strictSemantics(3),
                new ScalarMethodTarget(MethodHandles.lookup().findStatic(
                                BenchmarkGeneratedScalarAdapters.class,
                                "multiplyAdd",
                                MethodType.methodType(long.class, long.class, long.class, long.class))))
                .implementation();
        generatedComposedComposite = generator.adapt(
                "generated_composed_composite",
                new BoundSignature(LONG, List.of(LONG, LONG, LONG)),
                strictSemantics(3),
                new ScalarMethodTarget(MethodHandles.collectArguments(
                        MethodHandles.lookup().findStatic(
                                BenchmarkGeneratedScalarAdapters.class,
                                "add",
                                MethodType.methodType(long.class, long.class, long.class)),
                        0,
                        MethodHandles.lookup().findStatic(
                                BenchmarkGeneratedScalarAdapters.class,
                                "multiply",
                                MethodType.methodType(long.class, long.class, long.class)))))
                .implementation();
        ScalarTargets scalarTargets = new ScalarTargets();
        generatedBoundAdd = generator.adapt(
                "generated_bound_add",
                new BoundSignature(LONG, List.of(LONG, LONG)),
                strictSemantics(2),
                new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                ScalarTargets.class,
                                "add",
                                MethodType.methodType(long.class, long.class, long.class))
                        .bindTo(scalarTargets)))
                .implementation();
        generatedBoundCast = generator.adapt(
                "generated_bound_cast",
                new BoundSignature(DOUBLE, List.of(LONG)),
                strictSemantics(1),
                new ScalarMethodTarget(MethodHandles.lookup().findVirtual(
                                ScalarTargets.class,
                                "cast",
                                MethodType.methodType(double.class, long.class))
                        .bindTo(scalarTargets)))
                .implementation();
    }

    @TearDown
    public void tearDown()
    {
        allocator.close();
        resources.close();
    }

    @Benchmark
    public Streams nativeAdd()
    {
        return nativeAdd.apply(binaryInputs, mask, VALUES, longOutput, context);
    }

    @Benchmark
    public Streams generatedAdd()
    {
        return generatedAdd.apply(binaryInputs, mask, VALUES, longOutput, context);
    }

    @Benchmark
    public Streams generatedBoundAdd()
    {
        return generatedBoundAdd.apply(binaryInputs, mask, VALUES, longOutput, context);
    }

    @Benchmark
    public Streams generatedCast()
    {
        return generatedCast.apply(unaryInput, mask, VALUES, doubleOutput, context);
    }

    @Benchmark
    public Streams generatedBoundCast()
    {
        return generatedBoundCast.apply(unaryInput, mask, VALUES, doubleOutput, context);
    }

    @Benchmark
    public Streams generatedDirectComposite()
    {
        return generatedDirectComposite.apply(compositeInputs, mask, VALUES, longOutput, context);
    }

    @Benchmark
    public Streams generatedComposedComposite()
    {
        return generatedComposedComposite.apply(compositeInputs, mask, VALUES, longOutput, context);
    }

    @Benchmark
    public Streams generatedMaterializedComposite()
    {
        generatedMultiply.apply(binaryInputs, mask, VALUES, intermediateOutput, context);
        return generatedAdd.apply(materializedAddInputs, mask, VALUES, longOutput, context);
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkGeneratedScalarAdapters.class).run();
    }

    private static FunctionSemantics strictSemantics(int arity)
    {
        return new FunctionSemantics(true, Collections.nCopies(arity, RETURN_NULL_ON_NULL), false, NEVER_FAILS);
    }

    private static Mask createMask(String selection)
    {
        if (selection.equals("dense")) {
            return Mask.all(POSITION_COUNT);
        }
        int[] positions = new int[POSITION_COUNT / 8];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = index * 8;
        }
        return Mask.sparse(positions, POSITION_COUNT);
    }

    private static Vector encode(String encoding, long[] source)
    {
        if (encoding.equals("flat")) {
            return new I64Vector(source);
        }
        if (encoding.equals("dictionary")) {
            int dictionarySize = 64;
            long[] dictionary = new long[dictionarySize];
            System.arraycopy(source, 0, dictionary, 0, dictionarySize);
            int[] ids = new int[POSITION_COUNT];
            for (int position = 0; position < POSITION_COUNT; position++) {
                ids[position] = position % dictionarySize;
            }
            return new DictionaryVector(ids, new I64Vector(dictionary));
        }
        if (encoding.equals("rle")) {
            return new RleVector(new int[] {POSITION_COUNT}, new I64Vector(new long[] {source[0]}));
        }
        throw new IllegalArgumentException("Unknown encoding: " + encoding);
    }

    private static long add(long left, long right)
    {
        return left + right;
    }

    private static long multiply(long left, long right)
    {
        return left * right;
    }

    private static long multiplyAdd(long left, long right, long offset)
    {
        return left * right + offset;
    }

    private static double cast(long value)
    {
        return (double) value;
    }

    private static final class ScalarTargets
    {
        public long add(long left, long right)
        {
            return left + right;
        }

        public double cast(long value)
        {
            return (double) value;
        }
    }

    private record BenchmarkTypeBinding(TypeIdentity identity, Class<?> carrierType)
            implements TypeBinding
    {
        private BenchmarkTypeBinding(String identity, Class<?> carrierType)
        {
            this(new TypeIdentity(identity), carrierType);
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }
    }
}
