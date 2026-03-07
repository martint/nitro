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
package org.weakref.nitro.operator.evaluator.functions;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.FlatVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.EvaluationContext;
import org.weakref.nitro.operator.evaluator.Function;
import org.weakref.nitro.operator.evaluator.Result;

import static com.google.common.base.Preconditions.checkArgument;

public class SubtractI64
        implements Function
{
    private static final Allocator.Context CONTEXT = new Allocator.Context("SubtractI64");

    private final int left;
    private final int right;

    public SubtractI64(int left, int right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public Result apply(Result output, Mask mask, EvaluationContext context)
    {
        Vector leftVec = context.evaluate(left, mask).values();
        Vector rightVec = context.evaluate(right, mask).values();
        Vector out = output != null ? output.values() : null;
        return Result.of(apply(leftVec, rightVec, mask, out, context));
    }

    private Vector apply(Vector left, Vector right, Mask mask, Vector result, EvaluationContext context)
    {
        checkArgument(left.length() == right.length(), "Vectors must have the same length");

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle) {
            return applyRleRle(leftRle, rightRle, mask, result, context);
        }
        else if (left instanceof RleVector leftRle && right instanceof FlatVector rightFlat) {
            return applyRleFlat(leftRle, rightFlat, true, mask, result, context);
        }
        else if (left instanceof FlatVector leftFlat && right instanceof RleVector rightRle) {
            return applyRleFlat(rightRle, leftFlat, false, mask, result, context);
        }

        result = context.allocator().allocateOrGrow(CONTEXT, result, left.length(), I64Vector::new);
        return applyFlatFlat(left, right, mask, result);
    }

    private Vector applyFlatFlat(Vector left, Vector right, Mask mask, Vector result)
    {
        I64Vector leftFlat = (I64Vector) left;
        I64Vector rightFlat = (I64Vector) right;

        I64Vector output = (I64Vector) result;

        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                output.values()[position] = leftFlat.values()[position] - rightFlat.values()[position];
            }
        }
        else {
            for (int position : mask) {
                output.values()[position] = leftFlat.values()[position] - rightFlat.values()[position];
            }
        }
        return output;
    }

    private Vector applyRleFlat(RleVector rle, FlatVector flat, boolean rleIsLeft, Mask mask, Vector result, EvaluationContext context)
    {
        I64Vector output = (I64Vector) context.allocator().allocateOrGrow(CONTEXT, result, flat.length(), I64Vector::new);
        I64Vector flatValues = (I64Vector) flat;

        if (mask.all()) {
            int position = 0;
            for (int run = 0; run < rle.counts().length; run++) {
                int runLength = rle.counts()[run];
                long value = ((I64Vector) rle.values()).values()[run];

                for (int i = 0; i < runLength; i++) {
                    output.values()[position] = rleIsLeft
                            ? value - flatValues.values()[position]
                            : flatValues.values()[position] - value;
                    position++;
                }
            }
        }
        else {
            int position = 0;
            for (int run = 0; run < rle.counts().length; run++) {
                int runLength = rle.counts()[run];
                long value = ((I64Vector) rle.values()).values()[run];

                for (int i = 0; i < runLength; i++) {
                    if (mask.contains(position)) {
                        output.values()[position] = rleIsLeft
                                ? value - flatValues.values()[position]
                                : flatValues.values()[position] - value;
                    }
                    position++;
                }
            }
        }

        return output;
    }

    private RleVector applyRleRle(RleVector left, RleVector right, Mask mask, Vector result, EvaluationContext context)
    {
        int outputSize = RleVector.computeTargetRleLength(left, right);
        // TODO: if outputSize > some threshold, fall back to flat vectors

        int[] counts = new int[outputSize];
        I64Vector existingInner = result instanceof RleVector r ? (I64Vector) r.values() : null;
        I64Vector output = (I64Vector) context.allocator().allocateOrGrow(CONTEXT, existingInner, outputSize, I64Vector::new);

        I64Vector leftValues = (I64Vector) left.values();
        I64Vector rightValues = (I64Vector) right.values();

        int leftIndex = 0;
        int rightIndex = 0;

        int leftCount = 0;
        int rightCount = 0;

        int currentPosition = 0;
        for (int outputIndex = 0; outputIndex < outputSize; outputIndex++) {
            if (leftCount == 0) {
                leftCount = left.counts()[leftIndex];
            }
            if (rightCount == 0) {
                rightCount = right.counts()[rightIndex];
            }

            int count = Math.min(leftCount, rightCount);
            counts[outputIndex] = count;

            if (mask.anyTrue(currentPosition, currentPosition + count - 1)) {
                output.values()[outputIndex] = leftValues.values()[leftIndex] - rightValues.values()[rightIndex];
            }

            currentPosition += count;
            leftCount -= count;
            rightCount -= count;

            if (leftCount == 0) {
                leftIndex++;
            }
            if (rightCount == 0) {
                rightIndex++;
            }
        }

        return new RleVector(counts, output);
    }
}
