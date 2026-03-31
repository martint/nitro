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
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "or")
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
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);
        BooleanVector existingValues = output != null && output.has(Stream.VALUES) ? (BooleanVector) output.values() : null;
        BooleanVector existingNulls = output != null && output.has(Stream.NULLS) ? (BooleanVector) output.get(Stream.NULLS) : null;

        int length = BooleanBinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length()));
        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    existingNulls,
                    BooleanVector.class,
                    length,
                    BooleanVector::new);
            applyNulls(left, leftNulls, right, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existingValues == null) {
            BooleanVector values = context.allocator().allocate(ALLOCATION_CONTEXT, BooleanVector.class, RleVector.computeTargetRleLength(leftRle, rightRle), BooleanVector::new);
            applyValues(leftRle, leftNulls, rightRle, rightNulls, mask, values);
            return result.with(Stream.VALUES, values);
        }

        BooleanVector outputValues = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                existingValues,
                BooleanVector.class,
                length,
                BooleanVector::new);
        applyValues(left, leftNulls, right, rightNulls, mask, outputValues);
        return result.with(Stream.VALUES, outputValues);
    }

    private static void applyValues(Vector left, BooleanVector leftNulls, Vector right, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        boolean[] values = output.values();
        for (int position : mask) {
            boolean leftValue = booleanValue(left, position);
            boolean rightValue = booleanValue(right, position);
            boolean leftIsNull = isNull(leftNulls, position);
            boolean rightIsNull = isNull(rightNulls, position);
            values[position] = !leftIsNull && !rightIsNull
                    ? leftValue || rightValue
                    : (leftValue && !leftIsNull) || (rightValue && !rightIsNull);
        }
    }

    private static void applyNulls(Vector left, BooleanVector leftNulls, Vector right, BooleanVector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] nulls = outputNulls.values();
        java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            boolean leftValue = booleanValue(left, position);
            boolean rightValue = booleanValue(right, position);
            boolean leftIsNull = isNull(leftNulls, position);
            boolean rightIsNull = isNull(rightNulls, position);
            nulls[position] = (leftIsNull || rightIsNull) &&
                    !((leftValue && !leftIsNull) || (rightValue && !rightIsNull));
        }
    }

    private static boolean booleanValue(Vector values, int position)
    {
        return switch (values) {
            case BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> booleanValue(vector.values(), vector.ids()[position]);
            case RleVector vector -> booleanValue(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported or vector type: " + values.getClass().getSimpleName());
        };
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private static boolean apply(boolean leftValue, boolean rightValue)
    {
        return leftValue || rightValue;
    }
}
