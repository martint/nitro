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

import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.FunctionSemantics;
import org.weakref.nitro.core.function.NullPropagatingScalarInvocationProvider;
import org.weakref.nitro.core.function.ResolvedCall;
import org.weakref.nitro.core.function.ScalarInvocationProvider;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarAdapterGenerator;
import org.weakref.nitro.function.scalar.ScalarMethodTarget;
import org.weakref.nitro.operator.evaluator.PrimitiveRegistry;
import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Call;
import org.weakref.nitro.operator.evaluator.ir.EvaluationPlan;
import org.weakref.nitro.operator.evaluator.ir.Input;
import org.weakref.nitro.operator.evaluator.ir.Literal;
import org.weakref.nitro.operator.evaluator.ir.Reference;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.weakref.nitro.core.function.FunctionSemantics.ArgumentNullConvention.RETURN_NULL_ON_NULL;
import static org.weakref.nitro.core.function.FunctionSemantics.FailureConvention.NEVER_FAILS;

/**
 * Composes resolved scalar call trees into one generated batch adapter.
 *
 * <p>This is the generic complement to provider-authored projection lowering. It treats every scalar target as an
 * opaque exact call site, so neither this compiler nor the evaluator needs function-specific vocabulary. It admits
 * deterministic, non-failing trees over primitive stack carriers when every call either has strict non-null
 * semantics or explicitly supplies a non-null target with null-propagation delegated to the framework. Those
 * contracts make the root null exactly when any leaf is null and preserve invocation counts under encoded-domain
 * execution.
 */
final class GenericScalarProjectionCompiler
        implements AutoCloseable
{
    private final ScalarAdapterGenerator adapterGenerator = new ScalarAdapterGenerator();
    private final int dictionaryDomainMinimumReduction;
    private final Map<ExpressionKey, PrimitiveFunction> cache = new ConcurrentHashMap<>();

    GenericScalarProjectionCompiler(int dictionaryDomainMinimumReduction)
    {
        this.dictionaryDomainMinimumReduction = dictionaryDomainMinimumReduction;
    }

    Optional<FusedProjectionCompiler.CompiledMultiProjection> tryCompile(
            EvaluationPlan plan,
            PrimitiveRegistry primitiveRegistry,
            List<Reference> candidateOutputs)
    {
        Map<Integer, Assignment> assignments = new LinkedHashMap<>();
        for (Assignment assignment : plan.assignments()) {
            assignments.put(assignment.output().id(), assignment);
        }

        for (Reference output : candidateOutputs) {
            if (!(output.producer() instanceof Variable) || output.stream() != Stream.VALUES) {
                continue;
            }
            try {
                ScalarExpression expression = expression(output, null, assignments, primitiveRegistry, new HashSet<>());
                if (expression.operationCount() < 1 || expression.leaves().isEmpty()) {
                    continue;
                }
                ScalarExpression canonical = canonicalizeLeaves(expression);
                BoundSignature signature = new BoundSignature(
                        canonical.resultType(),
                        canonical.leaves().stream().map(Leaf::type).toList());
                FunctionSemantics semantics = new FunctionSemantics(
                        true,
                        canonical.leaves().stream().map(_ -> RETURN_NULL_ON_NULL).toList(),
                        false,
                        NEVER_FAILS);
                PrimitiveFunction function = cache.computeIfAbsent(canonical.key(), _ -> adapterGenerator.adapt(
                                "fused_scalar_expression",
                                signature,
                                semantics,
                                new ScalarMethodTarget(canonical.target()))
                        .implementation());
                FusedMultiProjection kernel = (inputs, mask, requestedStreams, context) -> new Streams[] {
                        function.apply(inputs, mask, requestedStreams, null, context)};
                return Optional.of(new FusedProjectionCompiler.CompiledMultiProjection(
                        kernel,
                        canonical.leaves().stream().map(Leaf::reference).toList(),
                        canonical.leaves().stream().map(leaf -> inputPhysicalType(leaf.type())).toList(),
                        canonical.leaves().stream().map(_ -> false).toList(),
                        List.of(output),
                        dictionaryDomainMinimumReduction,
                        FusedProjectionCompiler.CompilationKind.SCALAR_TARGET,
                        canonical.operationCount() == 1));
            }
            catch (Unsupported ignored) {
                // The ordinary evaluator remains authoritative for every unadmitted shape.
            }
        }
        return Optional.empty();
    }

    @Override
    public void close()
    {
        cache.clear();
    }

    private static ScalarExpression expression(
            Reference reference,
            TypeBinding expectedType,
            Map<Integer, Assignment> assignments,
            PrimitiveRegistry primitiveRegistry,
            Set<Integer> visitedVariables)
    {
        if (reference.stream() != Stream.VALUES) {
            throw new Unsupported();
        }
        if (reference.producer() instanceof Input) {
            if (expectedType == null || !supportedCarrier(expectedType.carrierType())) {
                throw new Unsupported();
            }
            return new ScalarExpression(
                    MethodHandles.identity(expectedType.carrierType()),
                    expectedType,
                    List.of(new Leaf(reference, expectedType)),
                    0,
                    new InputKey(reference, expectedType.carrierType()));
        }
        if (!(reference.producer() instanceof Variable variable)) {
            throw new Unsupported();
        }
        // PlanEvaluator memoizes an assignment. Do not turn a shared expression DAG into duplicated scalar calls;
        // direct bytecode generation with explicit locals is the correct later implementation for that shape.
        if (!visitedVariables.add(variable.id())) {
            throw new Unsupported();
        }
        Assignment assignment = assignments.get(variable.id());
        if (assignment == null) {
            throw new Unsupported();
        }
        if (assignment.operation() instanceof Literal literal) {
            if (expectedType == null || literal.value() == null || !supportedCarrier(expectedType.carrierType())) {
                throw new Unsupported();
            }
            return new ScalarExpression(
                    MethodHandles.constant(expectedType.carrierType(), constantValue(literal.value(), expectedType.carrierType())),
                    expectedType,
                    List.of(),
                    0,
                    new LiteralKey(literal.value(), expectedType.carrierType()));
        }
        if (!(assignment.operation() instanceof Call call) || call.resolvedCall() == null) {
            throw new Unsupported();
        }

        ResolvedCall resolvedCall = call.resolvedCall();
        BoundSignature signature = resolvedCall.signature();
        FunctionSemantics semantics = resolvedCall.semantics();
        ScalarInvocationProvider exactProvider = primitiveRegistry.capabilityOrNull(call, ScalarInvocationProvider.class);
        NullPropagatingScalarInvocationProvider nullPropagatingProvider =
                primitiveRegistry.capabilityOrNull(call, NullPropagatingScalarInvocationProvider.class);
        boolean frameworkManagedNulls = semantics.argumentNullConventions().stream()
                .allMatch(convention -> convention == RETURN_NULL_ON_NULL) && !semantics.nullableResult();
        MethodHandle invocationTarget = frameworkManagedNulls && exactProvider != null
                ? exactProvider.target()
                : nullPropagatingProvider == null ? null : nullPropagatingProvider.target(signature).orElse(null);
        if (invocationTarget == null ||
                !semantics.deterministic() ||
                semantics.failureConvention() != NEVER_FAILS ||
                !supportedCarrier(signature.resultType().carrierType()) ||
                signature.argumentTypes().stream().map(TypeBinding::carrierType).anyMatch(carrier -> !supportedCarrier(carrier)) ||
                call.arguments().size() != signature.argumentTypes().size() ||
                (expectedType != null && expectedType.carrierType() != signature.resultType().carrierType())) {
            throw new Unsupported();
        }

        MethodHandle target = invocationTarget;
        MethodType expectedTargetType = MethodType.methodType(
                signature.resultType().carrierType(),
                signature.argumentTypes().stream().map(TypeBinding::carrierType).toArray(Class<?>[]::new));
        if (!target.type().equals(expectedTargetType)) {
            throw new Unsupported();
        }

        List<ScalarExpression> arguments = new ArrayList<>(call.arguments().size());
        for (int index = 0; index < call.arguments().size(); index++) {
            arguments.add(expression(
                    call.arguments().get(index),
                    signature.argumentTypes().get(index),
                    assignments,
                    primitiveRegistry,
                    visitedVariables));
        }
        for (int index = arguments.size() - 1; index >= 0; index--) {
            target = MethodHandles.collectArguments(target, index, arguments.get(index).target());
        }
        List<Leaf> leaves = new ArrayList<>();
        int operationCount = 1;
        for (ScalarExpression argument : arguments) {
            leaves.addAll(argument.leaves());
            operationCount += argument.operationCount();
        }
        return new ScalarExpression(
                target,
                signature.resultType(),
                List.copyOf(leaves),
                operationCount,
                new CallKey(invocationTarget, arguments.stream().map(ScalarExpression::key).toList()));
    }

    private static ScalarExpression canonicalizeLeaves(ScalarExpression expression)
    {
        Map<Reference, Integer> positions = new LinkedHashMap<>();
        List<Leaf> leaves = new ArrayList<>();
        int[] reorder = new int[expression.leaves().size()];
        for (int index = 0; index < expression.leaves().size(); index++) {
            Leaf leaf = expression.leaves().get(index);
            Integer position = positions.get(leaf.reference());
            if (position == null) {
                position = leaves.size();
                positions.put(leaf.reference(), position);
                leaves.add(leaf);
            }
            else if (leaves.get(position).type().carrierType() != leaf.type().carrierType()) {
                throw new Unsupported();
            }
            reorder[index] = position;
        }
        MethodType canonicalType = MethodType.methodType(
                expression.resultType().carrierType(),
                leaves.stream().map(leaf -> leaf.type().carrierType()).toArray(Class<?>[]::new));
        MethodHandle target = Arrays.equals(reorder, identityReorder(reorder.length))
                ? expression.target()
                : MethodHandles.permuteArguments(expression.target(), canonicalType, reorder);
        return new ScalarExpression(target, expression.resultType(), List.copyOf(leaves), expression.operationCount(), expression.key());
    }

    private static int[] identityReorder(int size)
    {
        int[] identity = new int[size];
        for (int index = 0; index < size; index++) {
            identity[index] = index;
        }
        return identity;
    }

    private static Object constantValue(Object value, Class<?> carrier)
    {
        if (carrier == long.class && value instanceof Number number) {
            return number.longValue();
        }
        if (carrier == double.class && value instanceof Number number) {
            return number.doubleValue();
        }
        if (carrier == boolean.class && value instanceof Boolean) {
            return value;
        }
        throw new Unsupported();
    }

    private static boolean supportedCarrier(Class<?> carrier)
    {
        return carrier == long.class || carrier == double.class || carrier == boolean.class;
    }

    private static FusedProjectionCompiler.InputPhysicalType inputPhysicalType(TypeBinding type)
    {
        if (type.carrierType() == long.class) {
            return FusedProjectionCompiler.InputPhysicalType.LONG;
        }
        if (type.carrierType() == double.class) {
            return FusedProjectionCompiler.InputPhysicalType.DOUBLE;
        }
        if (type.carrierType() == boolean.class) {
            return FusedProjectionCompiler.InputPhysicalType.BOOLEAN;
        }
        throw new Unsupported();
    }

    private record Leaf(Reference reference, TypeBinding type) {}

    private sealed interface ExpressionKey
            permits InputKey, LiteralKey, CallKey {}

    private record InputKey(Reference reference, Class<?> carrier) implements ExpressionKey {}

    private record LiteralKey(Object value, Class<?> carrier) implements ExpressionKey {}

    private record CallKey(MethodHandle target, List<ExpressionKey> arguments) implements ExpressionKey
    {
        private CallKey
        {
            arguments = List.copyOf(arguments);
        }
    }

    private record ScalarExpression(
            MethodHandle target,
            TypeBinding resultType,
            List<Leaf> leaves,
            int operationCount,
            ExpressionKey key) {}

    private static final class Unsupported
            extends RuntimeException
    {
        private Unsupported()
        {
            super(null, null, false, false);
        }
    }
}
