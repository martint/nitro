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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.operator.evaluator.MaskOutcome;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "lt")
public final class LessThanI64
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("LessThanI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public boolean requiresInputCompanionStreams()
    {
        return true;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for lt");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }
        Allocator.Context allocationContext = context.allocationContext("LessThanI64");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector leftNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector rightNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector existing = output != null && output.has(Stream.VALUES) ? output.values() : null;
        BooleanVector existingNulls = output != null && output.has(Stream.NULLS) ? (BooleanVector) output.get(Stream.NULLS) : null;

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    allocationContext,
                    existingNulls,
                    BooleanVector.class,
                    I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())),
                    BooleanVector::new);
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

        BooleanVector values = context.allocator().allocateOrGrow(
                allocationContext,
                existing instanceof BooleanVector vector ? vector : null,
                BooleanVector.class,
                I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())),
                BooleanVector::new);
        I64BinaryDispatch.applyBoolean(left, right, mask, values, LessThanI64::apply);
        return result.with(Stream.VALUES, values);
    }

    private static void applyNulls(Vector leftNulls, Vector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] nulls = outputNulls.values();
        java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            nulls[position] = isTrue(leftNulls, position) || isTrue(rightNulls, position);
        }
    }

    @Override
    public MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateMaskOutcome(inputs, mask, context, context.allocationContext("LessThanI64"), LessThanI64::apply);
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMask(inputs, mask, context, context.allocationContext("LessThanI64"), LessThanI64::apply);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMask(inputs, mask, context, context.allocationContext("LessThanI64"), LessThanI64::apply);
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMaskInPlace(inputs, mask, LessThanI64::apply);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMaskInPlace(inputs, mask, LessThanI64::apply);
    }

    private static boolean apply(long leftValue, long rightValue)
    {
        return leftValue < rightValue;
    }

    private static boolean isTrue(Vector vector, int position)
    {
        if (vector == null) {
            return false;
        }
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> isTrue(values.values(), values.ids()[position]);
            case RleVector values -> isTrue(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected boolean vector but got " + vector.getClass().getSimpleName());
        };
    }
}
