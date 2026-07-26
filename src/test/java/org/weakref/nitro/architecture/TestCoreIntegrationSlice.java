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
package org.weakref.nitro.architecture;

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.execution.ExecutionContext;
import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.execution.ExecutionPolicy;
import org.weakref.nitro.core.execution.MemoryReservation;
import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.FunctionIdentity;
import org.weakref.nitro.core.function.FunctionRegistry;
import org.weakref.nitro.core.function.FunctionSemantics;
import org.weakref.nitro.core.function.InvocationConvention;
import org.weakref.nitro.core.function.ResolvedCall;
import org.weakref.nitro.core.function.mask.DirectMaskInputProvider;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.DriverResult;
import org.weakref.nitro.execution.OperatorExecutionDriver;
import org.weakref.nitro.function.scalar.builtin.AddI64;
import org.weakref.nitro.function.scalar.builtin.IsNullI64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SingleBatchOperator;
import org.weakref.nitro.operator.evaluator.PrimitiveCallSiteBinder;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.PrimitiveInvocationBinding;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.AllMask;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;
import org.weakref.nitro.operator.source.BatchSourceOperator;
import org.weakref.nitro.operator.source.compatibility.NativeSourceOperatorIngress;
import org.weakref.nitro.operator.source.compatibility.OperatorBatchSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCoreIntegrationSlice
{
    private static final TypeBinding BIGINT = new TestingTypeBinding(new TypeIdentity("testing:bigint"), long.class);
    private static final TypeBinding BOOLEAN = new TestingTypeBinding(new TypeIdentity("testing:boolean"), boolean.class);
    private static final FunctionIdentity LESS_THAN = new FunctionIdentity("testing:less-than");
    private static final FunctionIdentity ADD = new FunctionIdentity("isolated:add");

    @Test
    void testResolvedScanFilterProjectIslandOwnsSchemaAndLifecycle()
            throws ReflectiveOperationException, IOException
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Schema inputSchema = new Schema(List.of(new Field("value", BIGINT, false)));
        Schema outputSchema = new Schema(List.of(new Field("result", BIGINT, false)));

        PrimitiveFunction isolatedAdd = isolatedAdd();
        assertThat(isolatedAdd.getClass().getClassLoader()).isNotSameAs(getClass().getClassLoader());
        PrimitiveFunction boundAdd = new PrimitiveCallSiteBinder().bind(isolatedAdd);
        assertThat(boundAdd.getClass().getDeclaredField("target").getType()).isEqualTo(PrimitiveFunction.class);
        assertThat(boundAdd.getClass().getName()).doesNotContain(isolatedAdd.getClass().getName());

        ResolvedCall lessThan = resolvedCall(LESS_THAN, BOOLEAN, List.of(BIGINT, BIGINT), new LessThanI64(), true);
        ResolvedCall add = resolvedCall(ADD, BIGINT, List.of(BIGINT, BIGINT), boundAdd, true);
        FunctionRegistry registry = (identity, argumentTypes, convention) -> {
            if (convention != InvocationConvention.BATCH_SCALAR) {
                throw new IllegalArgumentException("Unsupported invocation convention: " + convention);
            }
            ResolvedCall call = identity.equals(LESS_THAN) ? lessThan : identity.equals(ADD) ? add : null;
            if (call == null || !call.signature().argumentTypes().equals(argumentTypes)) {
                throw new IllegalArgumentException("Unknown function binding: " + identity);
            }
            return call;
        };

        Variable threshold = new Variable(0);
        Variable predicate = new Variable(1);
        ResolvedCall resolvedLessThan = registry.resolve(LESS_THAN, List.of(BIGINT, BIGINT), InvocationConvention.BATCH_SCALAR);
        EvaluationPlan filterPlan = new EvaluationPlan(
                List.of(
                        new Assignment(threshold, new Literal(4L), AllMask.ALL),
                        new Assignment(
                                predicate,
                                new Call(resolvedLessThan, List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(threshold, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(predicate, Stream.VALUES)));

        Variable increment = new Variable(2);
        Variable result = new Variable(3);
        ResolvedCall resolvedAdd = registry.resolve(ADD, List.of(BIGINT, BIGINT), InvocationConvention.BATCH_SCALAR);
        EvaluationPlan projectPlan = new EvaluationPlan(
                List.of(
                        new Assignment(increment, new Literal(10L), AllMask.ALL),
                        new Assignment(
                                result,
                                new Call(resolvedAdd, List.of(
                                        new Reference(new Input(0), Stream.VALUES),
                                        new Reference(increment, Stream.VALUES))),
                                AllMask.ALL)),
                List.of(new Reference(result, Stream.VALUES)));

        AtomicInteger sourceCloses = new AtomicInteger();
        Operator nativeSource = new TrackingOperator(
                new SingleBatchOperator(
                        inputSchema,
                        Mask.all(4),
                        () -> new Output[] {Output.of(Streams.ofValues(new I64Vector(new long[] {1, 2, 3, 4})))}),
                sourceCloses);
        Operator ingress = new BatchSourceOperator(new OperatorBatchSource(nativeSource), new NativeSourceOperatorIngress());
        Operator island = new ProjectOperator(
                allocator,
                projectPlan,
                new PrimitiveRegistry(),
                new FilterOperator(
                        ingress,
                        filterPlan,
                        new PrimitiveRegistry(),
                        new Reference(predicate, Stream.VALUES),
                        allocator,
                        allocator.engineResources().operatorResources().filter()),
                outputSchema);

        assertThat(ingress.outputSchema()).isEqualTo(inputSchema);
        assertThat(island.outputSchema()).isEqualTo(outputSchema);

        TestingExecutionContext context = new TestingExecutionContext();
        AtomicInteger rootCloses = new AtomicInteger();
        Operator trackedIsland = new TrackingOperator(island, rootCloses);
        List<Long> results = new ArrayList<>();
        Batch[] escapedBatch = new Batch[1];
        try (OperatorExecutionDriver driver = new OperatorExecutionDriver(trackedIsland, allocator, context)) {
            assertThat(driver.processNext(batch -> {
                escapedBatch[0] = batch;
                Mask mask = batch.borrowMask();
                I64Vector values = (I64Vector) batch.output(0).borrow(Stream.VALUES, mask);
                for (int index = 0; index < mask.count(); index++) {
                    results.add(values.values()[mask.position(index)]);
                }
            })).isEqualTo(DriverResult.OUTPUT);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.FINISHED);
            assertThat(driver.isFinished()).isTrue();
        }

        assertThat(results).containsExactly(11L, 12L, 13L);
        assertThat(rootCloses).hasValue(1);
        assertThat(sourceCloses).hasValue(1);
        assertThatThrownBy(escapedBatch[0]::borrowMask)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testDriverYieldsBeforeStartingExecution()
    {
        AtomicBoolean yield = new AtomicBoolean(true);
        TestingExecutionContext context = new TestingExecutionContext(yield);
        Operator source = new SingleBatchOperator(0, Mask.all(0), () -> new Output[0]);
        try (OperatorExecutionDriver driver = new OperatorExecutionDriver(source, new Allocator(EngineResources.createDefault()), context)) {
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.YIELDED);
            yield.set(false);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.OUTPUT);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.FINISHED);
        }
    }

    @Test
    void testDriverExposesHostMemoryBackpressure()
    {
        CompletableFuture<Void> continuation = new CompletableFuture<>();
        MemoryReservation memory = new MemoryReservation()
        {
            private long reserved;

            @Override
            public CompletionStage<Void> reserve(long bytes)
            {
                reserved += bytes;
                return continuation;
            }

            @Override
            public void release(long bytes)
            {
                reserved -= bytes;
            }

            @Override
            public long reservedBytes()
            {
                return reserved;
            }
        };
        try (Allocator allocator = new Allocator(EngineResources.createDefault(), memory)) {
            allocator.allocate(new Allocator.Context("test"), I64Vector.class, 8, I64Vector::new);
            Operator source = new SingleBatchOperator(0, Mask.all(0), () -> new Output[0]);
            try (OperatorExecutionDriver driver = new OperatorExecutionDriver(source, allocator, new TestingExecutionContext())) {
                assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.BLOCKED);
                assertThat(driver.blocked()).contains(continuation);

                continuation.complete(null);
                assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.OUTPUT);
            }
        }
        assertThat(memory.reservedBytes()).isZero();
    }

    @Test
    void testResolvedFunctionCapabilityCrossesIsolatedClassLoader()
            throws ReflectiveOperationException, IOException
    {
        DirectMaskInputProvider isolatedCapability = isolatedDirectMaskInput();
        assertThat(isolatedCapability.getClass().getClassLoader()).isNotSameAs(getClass().getClassLoader());

        PrimitiveFunction implementation = new IsNullI64();
        ResolvedCall resolvedCall = new ResolvedCall(
                new FunctionIdentity("isolated:direct-mask-input"),
                new BoundSignature(BOOLEAN, List.of(BIGINT)),
                new FunctionSemantics(
                        implementation.deterministic(),
                        List.of(FunctionSemantics.ArgumentNullConvention.CALLED_ON_NULL),
                        false,
                        FunctionSemantics.FailureConvention.NEVER_FAILS),
                List.of(),
                new PrimitiveInvocationBinding(implementation, List.of(isolatedCapability)));
        Call call = new Call(resolvedCall, List.of(new Reference(new Input(0), Stream.VALUES)));

        assertThat(new PrimitiveRegistry().capabilityOrNull(call, DirectMaskInputProvider.class))
                .isSameAs(isolatedCapability);
        assertThat(isolatedCapability.argumentCount()).isEqualTo(1);
        assertThat(isolatedCapability.argumentIndex()).isZero();
        assertThat(isolatedCapability.inputComponent()).isEqualTo(DirectMaskInputProvider.InputComponent.NULLS);
    }

    private static ResolvedCall resolvedCall(
            FunctionIdentity identity,
            TypeBinding result,
            List<TypeBinding> arguments,
            PrimitiveFunction implementation,
            boolean nullableResult)
    {
        return new ResolvedCall(
                identity,
                new BoundSignature(result, arguments),
                new FunctionSemantics(
                        implementation.deterministic(),
                        java.util.Collections.nCopies(arguments.size(), FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL),
                        nullableResult,
                        FunctionSemantics.FailureConvention.NEVER_FAILS),
                List.of(),
                new PrimitiveInvocationBinding(implementation));
    }

    private static PrimitiveFunction isolatedAdd()
            throws IOException, ReflectiveOperationException
    {
        String name = IsolatedAdd.class.getName();
        String resource = "/" + name.replace('.', '/') + ".class";
        byte[] bytes;
        try (var input = TestCoreIntegrationSlice.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("Missing class bytes: " + resource);
            }
            bytes = input.readAllBytes();
        }
        ClassLoader loader = new ClassLoader(TestCoreIntegrationSlice.class.getClassLoader())
        {
            @Override
            protected Class<?> loadClass(String requestedName, boolean resolve)
                    throws ClassNotFoundException
            {
                synchronized (getClassLoadingLock(requestedName)) {
                    if (!requestedName.equals(name)) {
                        return super.loadClass(requestedName, resolve);
                    }
                    Class<?> loaded = findLoadedClass(requestedName);
                    if (loaded == null) {
                        loaded = defineClass(requestedName, bytes, 0, bytes.length);
                    }
                    if (resolve) {
                        resolveClass(loaded);
                    }
                    return loaded;
                }
            }
        };
        return (PrimitiveFunction) loader.loadClass(name).getConstructor().newInstance();
    }

    private static DirectMaskInputProvider isolatedDirectMaskInput()
            throws IOException, ReflectiveOperationException
    {
        String name = IsolatedDirectMaskInput.class.getName();
        String resource = "/" + name.replace('.', '/') + ".class";
        byte[] bytes;
        try (var input = TestCoreIntegrationSlice.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("Missing class bytes: " + resource);
            }
            bytes = input.readAllBytes();
        }
        ClassLoader loader = new ClassLoader(TestCoreIntegrationSlice.class.getClassLoader())
        {
            @Override
            protected Class<?> loadClass(String requestedName, boolean resolve)
                    throws ClassNotFoundException
            {
                synchronized (getClassLoadingLock(requestedName)) {
                    if (!requestedName.equals(name)) {
                        return super.loadClass(requestedName, resolve);
                    }
                    Class<?> loaded = findLoadedClass(requestedName);
                    if (loaded == null) {
                        loaded = defineClass(requestedName, bytes, 0, bytes.length);
                    }
                    if (resolve) {
                        resolveClass(loaded);
                    }
                    return loaded;
                }
            }
        };
        return (DirectMaskInputProvider) loader.loadClass(name).getConstructor().newInstance();
    }

    public static final class IsolatedAdd
            implements PrimitiveFunction
    {
        private final AddI64 delegate = new AddI64();

        @Override
        public Set<Allocator.Context> allocationContexts()
        {
            return delegate.allocationContexts();
        }

        @Override
        public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
        {
            return delegate.requiredInputStreams(inputIndex, requestedOutputStreams);
        }

        @Override
        public Streams apply(
                List<Streams> inputs,
                Mask mask,
                Set<Stream> requestedStreams,
                Streams output,
                PrimitiveExecutionContext context)
        {
            return delegate.apply(inputs, mask, requestedStreams, output, context);
        }
    }

    public static final class IsolatedDirectMaskInput
            implements DirectMaskInputProvider
    {
        @Override
        public int argumentCount()
        {
            return 1;
        }

        @Override
        public int argumentIndex()
        {
            return 0;
        }

        @Override
        public InputComponent inputComponent()
        {
            return InputComponent.NULLS;
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

    private static final class TrackingOperator
            implements Operator
    {
        private final Operator delegate;
        private final AtomicInteger closeCount;

        private TrackingOperator(Operator delegate, AtomicInteger closeCount)
        {
            this.delegate = delegate;
            this.closeCount = closeCount;
        }

        @Override
        public int outputCount()
        {
            return delegate.outputCount();
        }

        @Override
        public Schema outputSchema()
        {
            return delegate.outputSchema();
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public Batch next()
        {
            return delegate.next();
        }

        @Override
        public void constrain(Mask mask)
        {
            delegate.constrain(mask);
        }

        @Override
        public boolean supportsRetainedBatches()
        {
            return delegate.supportsRetainedBatches();
        }

        @Override
        public boolean supportsStableBatchBorrow()
        {
            return delegate.supportsStableBatchBorrow();
        }

        @Override
        public boolean supportsConstrainedReborrow()
        {
            return delegate.supportsConstrainedReborrow();
        }

        @Override
        public void close()
        {
            closeCount.incrementAndGet();
            delegate.close();
        }
    }

    private static final class TestingExecutionContext
            implements ExecutionContext
    {
        private final AtomicBoolean yield;
        private final MemoryReservation memory = new MemoryReservation()
        {
            private long reserved;

            @Override
            public CompletionStage<Void> reserve(long bytes)
            {
                reserved += bytes;
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void release(long bytes)
            {
                reserved -= bytes;
            }

            @Override
            public long reservedBytes()
            {
                return reserved;
            }
        };

        private TestingExecutionContext()
        {
            this(new AtomicBoolean());
        }

        private TestingExecutionContext(AtomicBoolean yield)
        {
            this.yield = yield;
        }

        @Override
        public MemoryReservation memory()
        {
            return memory;
        }

        @Override
        public ExecutionPolicy policy()
        {
            return new ExecutionPolicy() {};
        }

        @Override
        public ExecutionDiagnostics diagnostics()
        {
            return (_, _) -> {};
        }

        @Override
        public boolean isYieldRequested()
        {
            return yield.get();
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public void requestMemoryRevocation() {}
    }
}
