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

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "add_exact")
public final class AddExactI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("AddExactI64");
    private static final Allocator.Context ERRORS_CONTEXT = new Allocator.Context("AddExactI64.errors");

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for add_exact");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector existingValues = output != null && output.has(Stream.VALUES) ? output.values() : null;
        Vector existingErrors = output != null && output.has(Stream.ERRORS) ? output.get(Stream.ERRORS) : null;

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existingValues == null && existingErrors == null) {
            return applyRleRle(leftRle, rightRle);
        }

        I64Vector result = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                existingValues instanceof I64Vector vector ? vector : null,
                I64Vector.class,
                requiredLength(mask, Math.max(left.length(), right.length())),
                I64Vector::new);
        BooleanVector errors = context.allocator().allocateOrGrow(
                ERRORS_CONTEXT,
                existingErrors instanceof BooleanVector vector ? vector : null,
                BooleanVector.class,
                requiredLength(mask, Math.max(left.length(), right.length())),
                BooleanVector::new);
        if (left instanceof RleVector leftRle && right instanceof I64Vector rightFlat) {
            applyLeftRleRightFlat(leftRle, rightFlat, mask, result, errors);
        }
        else if (left instanceof I64Vector leftFlat && right instanceof RleVector rightRle) {
            applyLeftFlatRightRle(leftFlat, rightRle, mask, result, errors);
        }
        else {
            applyFlatFlat((I64Vector) left, (I64Vector) right, mask, result, errors);
        }
        return Streams.ofValues(result).with(Stream.ERRORS, errors);
    }

    private static void applyFlatFlat(I64Vector left, I64Vector right, Mask mask, I64Vector output, BooleanVector errors)
    {
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                apply(left.values()[position], right.values()[position], output.values(), errors.values(), position);
            }
            return;
        }

        for (int position : mask) {
            apply(left.values()[position], right.values()[position], output.values(), errors.values(), position);
        }
    }

    private static void applyLeftRleRightFlat(RleVector left, I64Vector right, Mask mask, I64Vector output, BooleanVector errors)
    {
        long[] leftValues = ((I64Vector) left.values()).values();

        int position = 0;
        for (int run = 0; run < left.counts().length; run++) {
            int runLength = left.counts()[run];
            long leftValue = leftValues[run];

            for (int offset = 0; offset < runLength; offset++) {
                if (mask.all() || mask.contains(position)) {
                    apply(leftValue, right.values()[position], output.values(), errors.values(), position);
                }
                position++;
            }
        }
    }

    private static void applyLeftFlatRightRle(I64Vector left, RleVector right, Mask mask, I64Vector output, BooleanVector errors)
    {
        long[] rightValues = ((I64Vector) right.values()).values();

        int position = 0;
        for (int run = 0; run < right.counts().length; run++) {
            int runLength = right.counts()[run];
            long rightValue = rightValues[run];

            for (int offset = 0; offset < runLength; offset++) {
                if (mask.all() || mask.contains(position)) {
                    apply(left.values()[position], rightValue, output.values(), errors.values(), position);
                }
                position++;
            }
        }
    }

    private static void apply(long leftValue, long rightValue, long[] values, boolean[] errors, int position)
    {
        long result = leftValue + rightValue;
        values[position] = result;
        // HD 2-12: overflow iff both arguments have the opposite sign of the result.
        errors[position] = ((leftValue ^ result) & (rightValue ^ result)) < 0;
    }

    private static Streams applyRleRle(RleVector left, RleVector right)
    {
        long[] leftValues = ((I64Vector) left.values()).values();
        long[] rightValues = ((I64Vector) right.values()).values();

        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        long[] values = new long[counts.length];
        boolean[] errors = new boolean[counts.length];

        int outputIndex = 0;
        int leftIndex = 0;
        int rightIndex = 0;
        int leftCount = 0;
        int rightCount = 0;

        while (leftIndex < left.counts().length && rightIndex < right.counts().length) {
            if (leftCount == 0) {
                leftCount = left.counts()[leftIndex];
            }
            if (rightCount == 0) {
                rightCount = right.counts()[rightIndex];
            }

            int count = Math.min(leftCount, rightCount);
            long leftValue = leftValues[leftIndex];
            long rightValue = rightValues[rightIndex];
            counts[outputIndex] = count;
            apply(leftValue, rightValue, values, errors, outputIndex);
            outputIndex++;

            leftCount -= count;
            rightCount -= count;

            if (leftCount == 0) {
                leftIndex++;
            }
            if (rightCount == 0) {
                rightIndex++;
            }
        }

        return Streams.ofValues(new RleVector(counts, new I64Vector(values)))
                .with(Stream.ERRORS, new RleVector(counts, new BooleanVector(errors)));
    }

    private static int requiredLength(Mask mask, int defaultLength)
    {
        if (mask.none()) {
            return defaultLength;
        }
        return Math.max(defaultLength, mask.maxPosition() + 1);
    }
}
