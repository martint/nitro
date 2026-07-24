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
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "divide")
public final class DivideI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("DivideI64");
    private static final Allocator.Context ERRORS_CONTEXT = new Allocator.Context("DivideI64.errors");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT, ERRORS_CONTEXT);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for divide");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestErrors = requestedStreams.contains(Stream.ERRORS);
        Vector existingValues = output != null && output.has(Stream.VALUES) ? output.values() : null;
        Vector existingErrors = output != null && output.has(Stream.ERRORS) ? output.get(Stream.ERRORS) : null;

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existingValues == null && existingErrors == null) {
            if (requestValues && requestErrors) {
                int resultLength = RleVector.computeTargetRleLength(leftRle, rightRle);
                I64Vector values = context.allocator().allocate(ALLOCATION_CONTEXT, I64Vector.class, resultLength, I64Vector::new);
                BooleanVector errors = context.allocator().allocate(ERRORS_CONTEXT, BooleanVector.class, resultLength, BooleanVector::new);
                I64BinaryDispatch.RleWithErrors result = I64BinaryDispatch.rleRleLongWithErrors(leftRle, rightRle, values, errors, DivideI64::apply);
                return Streams.ofValues(result.values()).with(Stream.ERRORS, result.errors());
            }
            if (requestErrors) {
                BooleanVector errors = context.allocator().allocate(ERRORS_CONTEXT, BooleanVector.class, RleVector.computeTargetRleLength(leftRle, rightRle), BooleanVector::new);
                return Streams.of(Stream.ERRORS, I64BinaryDispatch.rleRleErrorsOnly(leftRle, rightRle, errors, DivideI64::apply));
            }
            if (requestValues) {
                I64Vector values = context.allocator().allocate(ALLOCATION_CONTEXT, I64Vector.class, RleVector.computeTargetRleLength(leftRle, rightRle), I64Vector::new);
                return Streams.ofValues(I64BinaryDispatch.rleRleLong(leftRle, rightRle, values, DivideI64::result));
            }
            return Streams.empty();
        }

        int length = I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length()));
        Streams resultStreams = Streams.empty();
        if (requestValues && requestErrors) {
            I64Vector result = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    existingValues instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    length,
                    I64Vector::new);
            BooleanVector errors = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ERRORS_CONTEXT,
                    existingErrors,
                    length);
            I64BinaryDispatch.applyLongWithErrors(left, right, mask, result, errors, DivideI64::apply);
            resultStreams = Streams.ofValues(result).with(Stream.ERRORS, errors);
        }
        else if (requestErrors) {
            BooleanVector errors = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ERRORS_CONTEXT,
                    existingErrors,
                    length);
            I64BinaryDispatch.applyErrorsOnly(left, right, mask, errors, DivideI64::apply);
            resultStreams = Streams.of(Stream.ERRORS, errors);
        }
        else if (requestValues) {
            I64Vector result = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    existingValues instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    length,
                    I64Vector::new);
            I64BinaryDispatch.applyLong(left, right, mask, result, DivideI64::result);
            resultStreams = Streams.ofValues(result);
        }
        return resultStreams;
    }

    private static void apply(long leftValue, long rightValue, long[] values, boolean[] errors, int position)
    {
        if (values != null) {
            values[position] = result(leftValue, rightValue);
        }
        errors[position] = error(rightValue);
    }

    private static long result(long leftValue, long rightValue)
    {
        return rightValue != 0 ? leftValue / rightValue : 0;
    }

    private static boolean error(long rightValue)
    {
        return rightValue == 0;
    }
}
