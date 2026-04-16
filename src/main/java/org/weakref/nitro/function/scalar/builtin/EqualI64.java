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

@ScalarFunction(name = "eq")
public final class EqualI64
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("EqualI64");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }
        Allocator.Context allocationContext = context.allocationContext("EqualI64");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector leftNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector rightNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector existing = output != null && output.has(Stream.VALUES) ? output.values() : null;

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    BinaryDispatchSupport.requiredLength(mask, Math.max(left.length(), right.length())));
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existing == null) {
            BooleanVector values = context.allocator().allocate(allocationContext, BooleanVector.class, RleVector.computeTargetRleLength(leftRle, rightRle), BooleanVector::new);
            return result.with(Stream.VALUES, I64BinaryDispatch.rleRleBoolean(leftRle, rightRle, values, EqualI64::compareEqual));
        }

        BooleanVector values = VectorAccess.writableBooleanVector(
                context.allocator(),
                allocationContext,
                existing,
                BinaryDispatchSupport.requiredLength(mask, Math.max(left.length(), right.length())));
        I64BinaryDispatch.applyBoolean(left, right, mask, values, EqualI64::compareEqual);
        return result.with(Stream.VALUES, values);
    }

    @Override
    public MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateMaskOutcome(inputs, mask, context, context.allocationContext("EqualI64"), EqualI64::compareEqual);
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMask(inputs, mask, context, context.allocationContext("EqualI64"), EqualI64::compareEqual);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMask(inputs, mask, context, context.allocationContext("EqualI64"), EqualI64::compareEqual);
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMaskInPlace(inputs, mask, EqualI64::compareEqual);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMaskInPlace(inputs, mask, EqualI64::compareEqual);
    }

    private static void applyNulls(Vector leftNulls, Vector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues leftNullValues = VectorAccess.booleanValues(leftNulls);
        VectorAccess.BooleanValues rightNullValues = VectorAccess.booleanValues(rightNulls);
        boolean[] nulls = outputNulls.values();
        java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            nulls[position] = leftNullValues.value(position) || rightNullValues.value(position);
        }
    }

    private static boolean compareEqual(long leftValue, long rightValue)
    {
        return leftValue == rightValue;
    }
}
