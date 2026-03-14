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

@ScalarFunction(name = "add")
public final class AddI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("AddI64");

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for add");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector existing = output != null && output.has(Stream.VALUES) ? output.values() : null;

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existing == null) {
            return Streams.ofValues(applyRleRle(leftRle, rightRle));
        }

        I64Vector result = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                existing instanceof I64Vector vector ? vector : null,
                I64Vector.class,
                requiredLength(mask, Math.max(left.length(), right.length())),
                I64Vector::new);
        if (left instanceof RleVector leftRle && right instanceof I64Vector rightFlat) {
            applyLeftRleRightFlat(leftRle, rightFlat, mask, result);
        }
        else if (left instanceof I64Vector leftFlat && right instanceof RleVector rightRle) {
            applyLeftFlatRightRle(leftFlat, rightRle, mask, result);
        }
        else {
            applyFlatFlat((I64Vector) left, (I64Vector) right, mask, result);
        }
        return Streams.ofValues(result);
    }

    private static void applyFlatFlat(I64Vector left, I64Vector right, Mask mask, I64Vector output)
    {
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                output.values()[position] = apply(left.values()[position], right.values()[position]);
            }
            return;
        }

        for (int position : mask) {
            output.values()[position] = apply(left.values()[position], right.values()[position]);
        }
    }

    private static void applyLeftRleRightFlat(RleVector left, I64Vector right, Mask mask, I64Vector output)
    {
        long[] leftValues = ((I64Vector) left.values()).values();

        int position = 0;
        for (int run = 0; run < left.counts().length; run++) {
            int runLength = left.counts()[run];
            long leftValue = leftValues[run];

            for (int offset = 0; offset < runLength; offset++) {
                if (mask.all() || mask.contains(position)) {
                    output.values()[position] = apply(leftValue, right.values()[position]);
                }
                position++;
            }
        }
    }

    private static void applyLeftFlatRightRle(I64Vector left, RleVector right, Mask mask, I64Vector output)
    {
        long[] rightValues = ((I64Vector) right.values()).values();

        int position = 0;
        for (int run = 0; run < right.counts().length; run++) {
            int runLength = right.counts()[run];
            long rightValue = rightValues[run];

            for (int offset = 0; offset < runLength; offset++) {
                if (mask.all() || mask.contains(position)) {
                    output.values()[position] = apply(left.values()[position], rightValue);
                }
                position++;
            }
        }
    }

    private static RleVector applyRleRle(RleVector left, RleVector right)
    {
        long[] leftValues = ((I64Vector) left.values()).values();
        long[] rightValues = ((I64Vector) right.values()).values();

        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        long[] values = new long[counts.length];

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
            counts[outputIndex] = count;
            values[outputIndex] = apply(leftValues[leftIndex], rightValues[rightIndex]);
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

        return new RleVector(counts, new I64Vector(values));
    }

    private static long apply(long leftValue, long rightValue)
    {
        return leftValue + rightValue;
    }

    private static int requiredLength(Mask mask, int defaultLength)
    {
        if (mask.none()) {
            return defaultLength;
        }
        return Math.max(defaultLength, mask.maxPosition() + 1);
    }
}
