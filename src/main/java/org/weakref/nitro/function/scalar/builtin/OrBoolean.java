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
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "or", capabilities = OrBooleanOptimization.class)
public final class OrBoolean
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("OrBoolean");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for or");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector leftNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector rightNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector existingValues = output != null && output.has(Stream.VALUES) ? output.values() : null;
        VectorAccess.BooleanValues leftValues = VectorAccess.booleanValues(left);
        VectorAccess.BooleanValues rightValues = VectorAccess.booleanValues(right);

        int length = BooleanBinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length()));

        // Monomorphic null-free fast path: both operands flat and non-null, so a tight `|` loop over boolean[] that
        // C2 auto-vectorizes replaces four per-position accessor dispatches, and the whole NULLS pass is skipped
        // (an absent NULLS stream reads as all-false downstream).
        if (requestedStreams.contains(Stream.VALUES) && mask.all() && existingValues == null
                && left instanceof BooleanVector leftFlat && right instanceof BooleanVector rightFlat
                && VectorAccess.isAllFalseNulls(leftNulls) && VectorAccess.isAllFalseNulls(rightNulls)) {
            BooleanVector values = context.allocator().allocate(ALLOCATION_CONTEXT, BooleanVector.class, length, BooleanVector::new);
            orFlat(leftFlat.values(), rightFlat.values(), values.values(), length);
            return Streams.ofValues(values);
        }

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    length);
            applyNulls(leftValues, leftNulls, rightValues, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existingValues == null) {
            BooleanVector values = context.allocator().allocate(ALLOCATION_CONTEXT, BooleanVector.class, RleVector.computeTargetRleLength(leftRle, rightRle), BooleanVector::new);
            applyValues(leftValues, leftNulls, rightValues, rightNulls, mask, values);
            return result.with(Stream.VALUES, values);
        }

        BooleanVector outputValues = VectorAccess.writableBooleanVector(
                context.allocator(),
                ALLOCATION_CONTEXT,
                existingValues,
                length);
        applyValues(leftValues, leftNulls, rightValues, rightNulls, mask, outputValues);
        return result.with(Stream.VALUES, outputValues);
    }

    private static void orFlat(boolean[] left, boolean[] right, boolean[] output, int length)
    {
        for (int index = 0; index < length; index++) {
            output[index] = left[index] | right[index];
        }
    }

    private static void applyValues(VectorAccess.BooleanValues leftValues, Vector leftNulls, VectorAccess.BooleanValues rightValues, Vector rightNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues leftNullValues = VectorAccess.booleanValues(leftNulls);
        VectorAccess.BooleanValues rightNullValues = VectorAccess.booleanValues(rightNulls);
        boolean[] values = output.values();
        for (int position : mask) {
            boolean leftValue = leftValues.value(position);
            boolean rightValue = rightValues.value(position);
            boolean leftIsNull = leftNullValues.value(position);
            boolean rightIsNull = rightNullValues.value(position);
            values[position] = !leftIsNull && !rightIsNull
                    ? leftValue || rightValue
                    : (leftValue && !leftIsNull) || (rightValue && !rightIsNull);
        }
    }

    private static void applyNulls(VectorAccess.BooleanValues leftValues, Vector leftNulls, VectorAccess.BooleanValues rightValues, Vector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues leftNullValues = VectorAccess.booleanValues(leftNulls);
        VectorAccess.BooleanValues rightNullValues = VectorAccess.booleanValues(rightNulls);
        boolean[] nulls = outputNulls.values();
        java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            boolean leftValue = leftValues.value(position);
            boolean rightValue = rightValues.value(position);
            boolean leftIsNull = leftNullValues.value(position);
            boolean rightIsNull = rightNullValues.value(position);
            nulls[position] = (leftIsNull || rightIsNull) &&
                    !((leftValue && !leftIsNull) || (rightValue && !rightIsNull));
        }
    }

    private static boolean apply(boolean leftValue, boolean rightValue)
    {
        return leftValue || rightValue;
    }
}
