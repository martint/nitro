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

import org.weakref.nitro.data.FlatVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

import static com.google.common.base.Preconditions.checkArgument;
import static org.weakref.nitro.data.RleVector.computeTargetRleLength;

public class AddI64
{
    public Vector apply(Vector left, Vector right, Mask mask, Vector result)
    {
        checkArgument(left.length() == right.length(), "Vectors must have the same length");

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle) {
            return applyRleRle(leftRle, rightRle, mask);
        }
        else if (left instanceof RleVector leftRle && right instanceof FlatVector rightFlat) {
            return applyRleFlat(leftRle, rightFlat, mask);
        }
        else if (left instanceof FlatVector leftFlat && right instanceof RleVector rightRle) {
            return applyRleFlat(rightRle, leftFlat, mask);
        }

        return applyFlatFlat(left, right, mask, result);
    }

    private Vector applyFlatFlat(Vector left, Vector right, Mask mask, Vector result)
    {
        I64Vector leftFlat = (I64Vector) left;
        I64Vector rightFlat = (I64Vector) right;

        I64Vector output = (I64Vector) result;
        if (output == null) {
            // TODO: allocate from pool
            output = new I64Vector(left.length());
        }

        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                output.values()[position] = leftFlat.values()[position] + rightFlat.values()[position];
            }
        }
        else {
            for (int position : mask) {
                output.values()[position] = leftFlat.values()[position] + rightFlat.values()[position];
            }
        }
        return output;
    }

    private Vector applyRleFlat(RleVector rle, FlatVector flat, Mask mask)
    {
        // TODO: allocate from pool
        I64Vector output = new I64Vector(flat.length());
        I64Vector flatValues = (I64Vector) flat;

        if (mask.all()) {
            int position = 0;
            for (int run = 0; run < rle.counts().length; run++) {
                int runLength = rle.counts()[run];
                long value = ((I64Vector) rle.values()).values()[run];

                for (int i = 0; i < runLength; i++) {
                    output.values()[position] = value + flatValues.values()[position];
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
                        output.values()[position] = value + flatValues.values()[position];
                    }
                    position++;
                }
            }
        }

        return output;
    }

    private RleVector applyRleRle(RleVector left, RleVector right, Mask mask)
    {
        int outputSize = computeTargetRleLength(left, right);
        // TODO: if outputSize > some threshold, fall back to flat vectors

        // TODO: allocate from pool
        int[] counts = new int[outputSize];
        I64Vector output = new I64Vector(outputSize);

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
                output.values()[outputIndex] = leftValues.values()[leftIndex] + rightValues.values()[rightIndex];
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

//    private static FlatVector flatten(RleVector rle, Mask mask)
//    {
//        int[] counts = rle.counts();
//        I64Vector values = (I64Vector) rle.values();
//
//        I64Vector output = new I64Vector(rle.length());
//        int position = 0;
//        for (int run = 0; run < counts.length; run++) {
//            int count = counts[run];
//            long value = values.values()[run];
//            for (int i = 0; i < count; i++) {
//                if (mask.contains(position)) {
//                    output.values()[position++] = value;
//                }
//            }
//        }
//
//        return output;
//    }

//    public static void main()
//    {
//        RleVector left = new RleVector(new int[] {3, 1, 3}, new I64Vector(new long[] {1, 2, 3}));
//        RleVector right = new RleVector(new int[] {1, 4, 2}, new I64Vector(new long[] {10, 20, 30}));
//
//        AddI64 add = new AddI64();
//        Mask mask = Mask.sparse(new int[] {0, 1, 3, 0, 0, 0, 0}, 3);
////        Mask mask = Mask.all(left.length());
//
//        System.out.println("Left:              " + left);
//        System.out.println("Left FLAT:         " + flatten(left, mask));
//        System.out.println("Right:             " + right);
//        System.out.println("Right FLAT:        " + flatten(right, mask));
//        System.out.println("Mask:              " + mask);
//        System.out.println("RLE - RLE:         " + add.apply(left, right, mask, null));
//        System.out.println("FLAT - RLE:        " + add.apply(flatten(left, mask), right, mask, null));
//        System.out.println("RLE - FLAT:        " + add.apply(left, flatten(right, mask), mask, null));
//        System.out.println("FLAT - FLAT:       " + add.apply(flatten(left, mask), flatten(right, mask), mask, null));
//        System.out.println("RLE - RLE -> FLAT: " + flatten((RleVector) add.apply(left, right, mask, null), mask));
//    }
}
