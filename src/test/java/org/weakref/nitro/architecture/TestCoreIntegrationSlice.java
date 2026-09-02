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
import org.weakref.nitro.core.function.aggregation.AggregationArgument;
import org.weakref.nitro.core.function.aggregation.AggregationArgumentBinding;
import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationImplementationProvider;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateProvider;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateResolver;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTemplate;
import org.weakref.nitro.core.function.aggregation.ResolvedAggregation;
import org.weakref.nitro.core.function.mask.DirectMaskInputProvider;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.execution.DriverResult;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.execution.OperatorExecutionDriver;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.builtin.HandwrittenBigintAdd;
import org.weakref.nitro.function.scalar.builtin.IsNullI64;
import org.weakref.nitro.function.scalar.builtin.LessThanI64;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.FilterOperator;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.Output;
import org.weakref.nitro.operator.ProjectOperator;
import org.weakref.nitro.operator.SingleBatchOperator;
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

        ResolvedCall lessThan = resolvedCall(LESS_THAN, BOOLEAN, List.of(BIGINT, BIGINT), new LessThanI64(), true);
        ResolvedCall add = resolvedCall(ADD, BIGINT, List.of(BIGINT, BIGINT), isolatedAdd, true);
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
                        EngineResources.from(allocator).operatorResources().filter()),
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
    void testDriverResumesPullOperatorAfterMidPipelineYield()
    {
        AtomicBoolean yield = new AtomicBoolean();
        TestingExecutionContext context = new TestingExecutionContext(yield);
        Operator source = new CheckpointingOperator(context);
        try (OperatorExecutionDriver driver = new OperatorExecutionDriver(source, new Allocator(EngineResources.createDefault()), context)) {
            yield.set(true);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.YIELDED);

            yield.set(false);
            AtomicInteger outputs = new AtomicInteger();
            assertThat(driver.processNext(_ -> outputs.incrementAndGet())).isEqualTo(DriverResult.OUTPUT);
            assertThat(outputs).hasValue(1);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.FINISHED);
        }
    }

    @Test
    void testStackPreservingCheckpointContinuesCurrentPull()
    {
        AtomicBoolean yield = new AtomicBoolean(true);
        AtomicInteger parks = new AtomicInteger();
        TestingExecutionContext context = new TestingExecutionContext(yield, parks::incrementAndGet);
        Operator source = new CheckpointingOperator(context);
        try (OperatorExecutionDriver driver = new OperatorExecutionDriver(source, new Allocator(EngineResources.createDefault()), context)) {
            AtomicInteger outputs = new AtomicInteger();
            assertThat(driver.processNext(_ -> outputs.incrementAndGet())).isEqualTo(DriverResult.OUTPUT);
            assertThat(parks).hasValue(1);
            assertThat(outputs).hasValue(1);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.FINISHED);
        }
    }

    @Test
    void testDriverExposesMidPipelineBlockedContinuation()
    {
        CompletableFuture<Void> continuation = new CompletableFuture<>();
        TestingExecutionContext context = new TestingExecutionContext();
        Operator source = new AwaitingOperator(context, continuation);
        try (OperatorExecutionDriver driver = new OperatorExecutionDriver(source, new Allocator(EngineResources.createDefault()), context)) {
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.BLOCKED);
            assertThat(driver.blocked()).contains(continuation);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.BLOCKED);

            continuation.complete(null);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.OUTPUT);
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.FINISHED);
        }
    }

    @Test
    void testCancellationClosesBlockedPullGraph()
    {
        CompletableFuture<Void> continuation = new CompletableFuture<>();
        TestingExecutionContext context = new TestingExecutionContext();
        AwaitingOperator source = new AwaitingOperator(context, continuation);
        try (OperatorExecutionDriver driver = new OperatorExecutionDriver(source, new Allocator(EngineResources.createDefault()), context)) {
            assertThat(driver.processNext(_ -> {})).isEqualTo(DriverResult.BLOCKED);

            context.cancel();
            assertThatThrownBy(() -> driver.processNext(_ -> {}))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("execution is cancelled");
            assertThat(source.isClosed()).isTrue();
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

    @Test
    void testGroupedAggregationCapabilityLowersAcrossIsolatedClassLoader()
            throws ReflectiveOperationException, IOException
    {
        GroupedAggregationUpdateProvider provider = isolatedGroupedAggregationUpdateProvider();
        assertThat(provider.getClass().getClassLoader()).isNotSameAs(getClass().getClassLoader());

        PrimitiveFunction implementation = new IsNullI64();
        ResolvedCall resolvedCall = new ResolvedCall(
                new FunctionIdentity("isolated:grouped-update"),
                new BoundSignature(BIGINT, List.of(BIGINT)),
                new FunctionSemantics(
                        implementation.deterministic(),
                        List.of(FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL),
                        false,
                        FunctionSemantics.FailureConvention.NEVER_FAILS),
                List.of(),
                new PrimitiveInvocationBinding(implementation, List.of(provider)));

        GroupedAggregationUpdate update = new GroupedAggregationUpdateResolver()
                .resolve(resolvedCall, List.of(AggregationArgumentBinding.input(7)))
                .orElseThrow();
        assertThat(update).isEqualTo(GroupedAggregationUpdate.inputValue(7));
        assertThat(new GroupedAggregationUpdateResolver()
                .resolve(resolvedCall, List.of(AggregationArgumentBinding.computed())))
                .isEmpty();
        assertThat(new GroupedAggregationUpdateResolver()
                .resolveIntermediate(resolvedCall, AggregationArgumentBinding.input(9)))
                .contains(GroupedAggregationUpdate.inputValue(9));
    }

    @Test
    void testAggregationImplementationResolvesAcrossIsolatedClassLoader()
            throws ReflectiveOperationException, IOException
    {
        AggregationImplementation implementation = new TestingAggregationImplementation();
        AggregationImplementationProvider provider = isolatedAggregationImplementationProvider(BIGINT, implementation);
        assertThat(provider.getClass().getClassLoader()).isNotSameAs(getClass().getClassLoader());

        PrimitiveFunction scalarPlaceholder = new IsNullI64();
        ResolvedCall resolvedCall = new ResolvedCall(
                new FunctionIdentity("isolated:aggregate"),
                new BoundSignature(BIGINT, List.of(BIGINT)),
                new FunctionSemantics(
                        scalarPlaceholder.deterministic(),
                        List.of(FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL),
                        false,
                        FunctionSemantics.FailureConvention.NEVER_FAILS),
                List.of(),
                new PrimitiveInvocationBinding(scalarPlaceholder, List.of(provider)));

        AggregationImplementationProvider.Binding binding = ResolvedAggregation.resolve(
                        resolvedCall,
                        List.of(AggregationArgument.input()))
                .orElseThrow();
        assertThat(binding.intermediateType()).isSameAs(BIGINT);
        assertThat(binding.implementation()).isSameAs(implementation);
        assertThat(ResolvedAggregation.resolve(
                resolvedCall,
                List.of(AggregationArgument.computed()))).isEmpty();
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

    private static GroupedAggregationUpdateProvider isolatedGroupedAggregationUpdateProvider()
            throws IOException, ReflectiveOperationException
    {
        String name = IsolatedGroupedAggregationUpdateProvider.class.getName();
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
        return (GroupedAggregationUpdateProvider) loader.loadClass(name).getConstructor().newInstance();
    }

    private static AggregationImplementationProvider isolatedAggregationImplementationProvider(
            TypeBinding intermediateType,
            AggregationImplementation implementation)
            throws IOException, ReflectiveOperationException
    {
        String name = IsolatedAggregationImplementationProvider.class.getName();
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
        return (AggregationImplementationProvider) loader.loadClass(name)
                .getConstructor(TypeBinding.class, AggregationImplementation.class)
                .newInstance(intermediateType, implementation);
    }

    public static final class IsolatedAdd
            implements PrimitiveFunction
    {
        private final HandwrittenBigintAdd delegate = new HandwrittenBigintAdd();

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

    public static final class IsolatedGroupedAggregationUpdateProvider
            implements GroupedAggregationUpdateProvider
    {
        @Override
        public java.util.Optional<GroupedAggregationUpdateTemplate> update(List<AggregationArgument> arguments)
        {
            if (arguments.size() != 1 || arguments.getFirst().kind() != AggregationArgument.Kind.INPUT) {
                return java.util.Optional.empty();
            }
            return java.util.Optional.of(GroupedAggregationUpdateTemplate.inputValue(0));
        }

        @Override
        public java.util.Optional<GroupedAggregationUpdateTemplate> intermediateUpdate()
        {
            return java.util.Optional.of(GroupedAggregationUpdateTemplate.inputValue(0));
        }
    }

    public static final class IsolatedAggregationImplementationProvider
            implements AggregationImplementationProvider
    {
        private final TypeBinding intermediateType;
        private final AggregationImplementation implementation;

        public IsolatedAggregationImplementationProvider(
                TypeBinding intermediateType,
                AggregationImplementation implementation)
        {
            this.intermediateType = intermediateType;
            this.implementation = implementation;
        }

        @Override
        public java.util.Optional<Binding> bind(List<AggregationArgument> arguments)
        {
            if (arguments.size() != 1 || arguments.getFirst().kind() != AggregationArgument.Kind.INPUT) {
                return java.util.Optional.empty();
            }
            return java.util.Optional.of(new Binding(intermediateType, implementation));
        }
    }

    private static final class TestingAggregationImplementation
            implements AggregationImplementation
    {
        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addRawInput(Object state, org.weakref.nitro.data.Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, int group, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, org.weakref.nitro.data.Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams intermediate(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams result(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            throw new UnsupportedOperationException();
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
        private final Runnable stackPreservingYield;
        private boolean cancelled;
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
            this(yield, null);
        }

        private TestingExecutionContext(AtomicBoolean yield, Runnable stackPreservingYield)
        {
            this.yield = yield;
            this.stackPreservingYield = stackPreservingYield;
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
            return cancelled;
        }

        private void cancel()
        {
            cancelled = true;
        }

        @Override
        public void checkpoint()
        {
            if (stackPreservingYield != null && yield.compareAndSet(true, false)) {
                stackPreservingYield.run();
                return;
            }
            ExecutionContext.super.checkpoint();
        }

        @Override
        public void requestMemoryRevocation() {}
    }

    private static final class CheckpointingOperator
            implements Operator
    {
        private final ExecutionContext context;
        private boolean progressCommitted;
        private boolean outputProduced;

        private CheckpointingOperator(ExecutionContext context)
        {
            this.context = context;
        }

        @Override
        public int outputCount()
        {
            return 0;
        }

        @Override
        public boolean hasNext()
        {
            return !outputProduced;
        }

        @Override
        public Batch next()
        {
            if (!progressCommitted) {
                progressCommitted = true;
                context.checkpoint();
            }
            outputProduced = true;
            return new Batch(Mask.all(0), new Output[0]);
        }

        @Override
        public void constrain(Mask mask) {}

        @Override
        public void close() {}
    }

    private static final class AwaitingOperator
            implements Operator
    {
        private final ExecutionContext context;
        private final CompletionStage<Void> continuation;
        private boolean outputProduced;
        private boolean closed;

        private AwaitingOperator(ExecutionContext context, CompletionStage<Void> continuation)
        {
            this.context = context;
            this.continuation = continuation;
        }

        @Override
        public int outputCount()
        {
            return 0;
        }

        @Override
        public boolean hasNext()
        {
            return !outputProduced;
        }

        @Override
        public Batch next()
        {
            context.await(continuation);
            outputProduced = true;
            return new Batch(Mask.all(0), new Output[0]);
        }

        @Override
        public void constrain(Mask mask) {}

        @Override
        public void close()
        {
            closed = true;
        }

        private boolean isClosed()
        {
            return closed;
        }
    }
}
