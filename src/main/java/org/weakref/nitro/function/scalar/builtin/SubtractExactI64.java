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
import org.weakref.nitro.data.I64Vector;
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

@ScalarFunction(name = "subtract_exact")
public final class SubtractExactI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("SubtractExactI64");
    private static final Allocator.Context ERRORS_CONTEXT = new Allocator.Context("SubtractExactI64.errors");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT, ERRORS_CONTEXT);
    }

    @Override
    public boolean requiresInputCompanionStreams()
    {
        return true;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for subtract_exact");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        boolean requestErrors = requestedStreams.contains(Stream.ERRORS);
        Vector existingValues = output != null && output.has(Stream.VALUES) ? output.values() : null;
        BooleanVector existingNulls = output != null && output.has(Stream.NULLS) ? (BooleanVector) output.get(Stream.NULLS) : null;
        Vector existingErrors = output != null && output.has(Stream.ERRORS) ? output.get(Stream.ERRORS) : null;

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existingValues == null && existingErrors == null && !requestNulls && leftNulls == null && rightNulls == null) {
            if (requestValues && requestErrors) {
                int resultLength = RleVector.computeTargetRleLength(leftRle, rightRle);
                I64Vector values = context.allocator().allocate(ALLOCATION_CONTEXT, I64Vector.class, resultLength, I64Vector::new);
                BooleanVector errors = context.allocator().allocate(ERRORS_CONTEXT, BooleanVector.class, resultLength, BooleanVector::new);
                I64BinaryDispatch.RleWithErrors result = I64BinaryDispatch.rleRleLongWithErrors(leftRle, rightRle, values, errors, SubtractExactI64::apply);
                return Streams.ofValues(result.values()).with(Stream.ERRORS, result.errors());
            }
            if (requestErrors) {
                BooleanVector errors = context.allocator().allocate(ERRORS_CONTEXT, BooleanVector.class, RleVector.computeTargetRleLength(leftRle, rightRle), BooleanVector::new);
                return Streams.of(Stream.ERRORS, I64BinaryDispatch.rleRleErrorsOnly(leftRle, rightRle, errors, SubtractExactI64::apply));
            }
            if (requestValues) {
                I64Vector values = context.allocator().allocate(ALLOCATION_CONTEXT, I64Vector.class, RleVector.computeTargetRleLength(leftRle, rightRle), I64Vector::new);
                return Streams.ofValues(I64BinaryDispatch.rleRleLong(leftRle, rightRle, values, SubtractExactI64::result));
            }
            return Streams.empty();
        }

        int length = I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length()));
        Streams resultStreams = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    existingNulls,
                    BooleanVector.class,
                    length,
                    BooleanVector::new);
            applyNulls(leftNulls, rightNulls, mask, nulls);
            resultStreams = resultStreams.with(Stream.NULLS, nulls);
        }
        if (requestValues && requestErrors) {
            I64Vector result = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    existingValues instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    length,
                    I64Vector::new);
            BooleanVector errors = context.allocator().allocateOrGrow(
                    ERRORS_CONTEXT,
                    existingErrors instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    length,
                    BooleanVector::new);
            I64BinaryDispatch.applyLongWithErrors(left, right, mask, result, errors, SubtractExactI64::apply);
            resultStreams = Streams.ofValues(result).with(Stream.ERRORS, errors);
        }
        else if (requestErrors) {
            BooleanVector errors = context.allocator().allocateOrGrow(
                    ERRORS_CONTEXT,
                    existingErrors instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    length,
                    BooleanVector::new);
            I64BinaryDispatch.applyErrorsOnly(left, right, mask, errors, SubtractExactI64::apply);
            resultStreams = Streams.of(Stream.ERRORS, errors);
        }
        else if (requestValues) {
            I64Vector result = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    existingValues instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    length,
                    I64Vector::new);
            I64BinaryDispatch.applyLong(left, right, mask, result, SubtractExactI64::result);
            resultStreams = Streams.ofValues(result);
        }
        return resultStreams;
    }

    private static void applyNulls(BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] nulls = outputNulls.values();
        java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            nulls[position] = (leftNulls != null && leftNulls.values()[position]) ||
                    (rightNulls != null && rightNulls.values()[position]);
        }
    }

    private static void apply(long leftValue, long rightValue, long[] values, boolean[] errors, int position)
    {
        long result = leftValue - rightValue;
        if (values != null) {
            values[position] = result;
        }
        // HD 2-12: overflow iff the arguments have different signs and the result has the opposite sign of the left argument.
        errors[position] = ((leftValue ^ rightValue) & (leftValue ^ result)) < 0;
    }

    private static long result(long leftValue, long rightValue)
    {
        return leftValue - rightValue;
    }
}
