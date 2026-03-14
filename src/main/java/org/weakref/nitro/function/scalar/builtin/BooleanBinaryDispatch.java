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

import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

import static com.google.common.base.Preconditions.checkArgument;

final class BooleanBinaryDispatch
{
    private BooleanBinaryDispatch() {}

    @FunctionalInterface
    interface Kernel
    {
        boolean apply(boolean left, boolean right);
    }

    @FunctionalInterface
    private interface BooleanPairConsumer
    {
        void accept(boolean left, boolean right, int position);
    }

    public static int requiredLength(Mask mask, int defaultLength)
    {
        if (mask.none()) {
            return defaultLength;
        }
        return Math.max(defaultLength, mask.maxPosition() + 1);
    }

    public static void apply(Vector left, Vector right, Mask mask, BooleanVector output, Kernel kernel)
    {
        boolean[] values = output.values();
        forEachPair(left, right, mask, (leftValue, rightValue, position) -> values[position] = kernel.apply(leftValue, rightValue));
    }

    public static RleVector rleRle(RleVector left, RleVector right, Kernel kernel)
    {
        boolean[] leftValues = ((BooleanVector) left.values()).values();
        boolean[] rightValues = ((BooleanVector) right.values()).values();

        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        boolean[] values = new boolean[counts.length];

        mergeRuns(left.counts(), right.counts(), (outputIndex, leftIndex, rightIndex, count) -> {
            counts[outputIndex] = count;
            values[outputIndex] = kernel.apply(leftValues[leftIndex], rightValues[rightIndex]);
        });
        return new RleVector(counts, new BooleanVector(values));
    }

    private static void forEachPair(Vector left, Vector right, Mask mask, BooleanPairConsumer consumer)
    {
        validateLength(left, mask);
        validateLength(right, mask);

        switch (left) {
            case BooleanVector leftFlat -> {
                switch (right) {
                    case BooleanVector rightFlat -> forEachFlatFlat(leftFlat.values(), rightFlat.values(), mask, consumer);
                    case RleVector rightRle -> forEachFlatRle(leftFlat.values(), rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            case RleVector leftRle -> {
                switch (right) {
                    case BooleanVector rightFlat -> forEachRleFlat(leftRle, rightFlat.values(), mask, consumer);
                    case RleVector rightRle -> forEachRleRle(leftRle, rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            default -> throw unsupported(left);
        }
    }

    private static void forEachFlatFlat(boolean[] left, boolean[] right, Mask mask, BooleanPairConsumer consumer)
    {
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], right[position], position);
        }
    }

    private static void forEachRleFlat(RleVector left, boolean[] right, Mask mask, BooleanPairConsumer consumer)
    {
        BooleanRleCursor cursor = new BooleanRleCursor(left);
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(cursor.valueAt(position), right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(cursor.valueAt(position), right[position], position);
        }
    }

    private static void forEachFlatRle(boolean[] left, RleVector right, Mask mask, BooleanPairConsumer consumer)
    {
        BooleanRleCursor cursor = new BooleanRleCursor(right);
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], cursor.valueAt(position), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], cursor.valueAt(position), position);
        }
    }

    private static void forEachRleRle(RleVector left, RleVector right, Mask mask, BooleanPairConsumer consumer)
    {
        BooleanRleCursor leftCursor = new BooleanRleCursor(left);
        BooleanRleCursor rightCursor = new BooleanRleCursor(right);
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftCursor.valueAt(position), rightCursor.valueAt(position), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftCursor.valueAt(position), rightCursor.valueAt(position), position);
        }
    }

    private static IllegalArgumentException unsupported(Vector vector)
    {
        return new IllegalArgumentException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }

    private static void validateLength(Vector vector, Mask mask)
    {
        if (mask.none()) {
            return;
        }

        int requiredLength = mask.maxPosition() + 1;
        checkArgument(vector.length() >= requiredLength, "Vector length %s is shorter than required length %s", vector.length(), requiredLength);
    }

    private static void mergeRuns(int[] leftCounts, int[] rightCounts, RunConsumer consumer)
    {
        int outputIndex = 0;
        int leftIndex = 0;
        int rightIndex = 0;
        int leftCount = 0;
        int rightCount = 0;

        while (leftIndex < leftCounts.length && rightIndex < rightCounts.length) {
            if (leftCount == 0) {
                leftCount = leftCounts[leftIndex];
            }
            if (rightCount == 0) {
                rightCount = rightCounts[rightIndex];
            }

            int count = Math.min(leftCount, rightCount);
            consumer.accept(outputIndex, leftIndex, rightIndex, count);
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
    }

    @FunctionalInterface
    private interface RunConsumer
    {
        void accept(int outputIndex, int leftIndex, int rightIndex, int count);
    }

    private static final class BooleanRleCursor
    {
        private final int[] counts;
        private final boolean[] values;
        private int runIndex;
        private int runEnd;

        private BooleanRleCursor(RleVector vector)
        {
            counts = vector.counts();
            values = ((BooleanVector) vector.values()).values();
            if (counts.length > 0) {
                runEnd = counts[0];
            }
        }

        private boolean valueAt(int position)
        {
            while (position >= runEnd) {
                runIndex++;
                runEnd += counts[runIndex];
            }
            return values[runIndex];
        }
    }
}
