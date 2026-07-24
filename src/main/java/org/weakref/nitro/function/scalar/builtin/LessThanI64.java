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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionProgram;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.operator.evaluator.MaskOutcome;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "lt")
public final class LessThanI64
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction, ProjectionCodeProvider
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("LessThanI64");

    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() != 2) {
            return Optional.empty();
        }
        var left = builder.argument(0, ProjectionCodeBuilder.ValueType.I64);
        var right = builder.argument(1, ProjectionCodeBuilder.ValueType.I64);
        return Optional.of(builder.program(
                List.of(ProjectionCodeBuilder.ValueType.I64, ProjectionCodeBuilder.ValueType.I64),
                builder.lessThan(left, right),
                builder.or(builder.isNull(0), builder.isNull(1))));
    }

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Set<Stream> requiredMaskInputStreams(int inputIndex)
    {
        return PrimitiveFunction.VALUES_AND_NULLS_INPUT_STREAMS;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for lt");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }
        Allocator.Context allocationContext = context.allocationContext("LessThanI64");

        var left = inputs.get(0).values();
        var right = inputs.get(1).values();
        var leftNulls = inputs.get(0).getOrNull(Stream.NULLS);
        var rightNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector existing = output != null && output.has(Stream.VALUES) ? output.values() : null;

        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector fast = NullFreeScalarKernels.compareLong(NullFreeScalarKernels.LESS_THAN, left, right, leftNulls, rightNulls, mask, existing, context.allocator(), allocationContext);
            if (fast != null) {
                return Streams.ofValues(fast);
            }
        }

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS) && !(VectorAccess.isAllFalseNulls(leftNulls) && VectorAccess.isAllFalseNulls(rightNulls))) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())));
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existing == null) {
            BooleanVector values = context.allocator().allocate(allocationContext, BooleanVector.class, RleVector.computeTargetRleLength(leftRle, rightRle), BooleanVector::new);
            return result.with(Stream.VALUES, I64BinaryDispatch.rleRleBoolean(leftRle, rightRle, values, LessThanI64::apply));
        }

        BooleanVector values = VectorAccess.writableBooleanVector(
                context.allocator(),
                allocationContext,
                existing,
                I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())));
        I64BinaryDispatch.applyBoolean(left, right, mask, values, LessThanI64::apply);
        return result.with(Stream.VALUES, values);
    }

    private static void applyNulls(Vector leftNulls, Vector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        VectorAccess.combineNullsOr(leftNulls, rightNulls, mask, outputNulls);
    }

    @Override
    public MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateMaskOutcome(inputs, mask, context, context.allocationContext("LessThanI64"), LessThanI64::apply);
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMask(inputs, mask, context, context.allocationContext("LessThanI64"), LessThanI64::apply, Mask.ComparisonOperator.LESS_THAN);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMask(inputs, mask, context, context.allocationContext("LessThanI64"), LessThanI64::apply, Mask.ComparisonOperator.LESS_THAN);
    }

    @Override
    public boolean requiresCompletedInputCompanionStreamsForMask()
    {
        // The comparison reads null/error streams directly (absent == all-false), so it does not need the
        // evaluator to materialize an all-false companion stream per operand per batch (e.g. for a constant).
        return false;
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMaskInPlace(inputs, mask, LessThanI64::apply, Mask.ComparisonOperator.LESS_THAN);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMaskInPlace(inputs, mask, LessThanI64::apply, Mask.ComparisonOperator.LESS_THAN);
    }

    private static boolean apply(long leftValue, long rightValue)
    {
        return leftValue < rightValue;
    }
}
