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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

public final class I64BinaryVectorSupport
{
    private I64BinaryVectorSupport() {}

    @FunctionalInterface
    public interface LongBinaryOperation
    {
        long apply(long left, long right)
                throws Throwable;
    }

    @FunctionalInterface
    public interface LongComparisonOperation
    {
        boolean apply(long left, long right)
                throws Throwable;
    }

    public static Vector applyLongBinary(
            String functionName,
            Vector left,
            Vector right,
            Mask mask,
            Vector output,
            PrimitiveExecutionContext context,
            Allocator.Context allocationContext,
            LongBinaryOperation operation)
    {
        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && output == null) {
            return applyRleRleLong(functionName, leftRle, rightRle, operation);
        }

        I64Vector result = allocateLongOutput(output, mask, Math.max(left.length(), right.length()), context, allocationContext);
        applyLongToFlat(functionName, left, right, mask, result, operation);
        return result;
    }

    public static Vector applyLongComparison(
            String functionName,
            Vector left,
            Vector right,
            Mask mask,
            Vector output,
            PrimitiveExecutionContext context,
            Allocator.Context allocationContext,
            LongComparisonOperation operation)
    {
        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && output == null) {
            return applyRleRleComparison(functionName, leftRle, rightRle, operation);
        }

        BooleanVector result = allocateBooleanOutput(output, mask, Math.max(left.length(), right.length()), context, allocationContext);
        applyComparisonToFlat(functionName, left, right, mask, result, operation);
        return result;
    }

    public static long[] i64Values(Vector vector)
    {
        return ((I64Vector) vector).values();
    }

    private static I64Vector allocateLongOutput(Vector output, Mask mask, int defaultLength, PrimitiveExecutionContext context, Allocator.Context allocationContext)
    {
        I64Vector existing = output instanceof I64Vector vector ? vector : null;
        return (I64Vector) context.allocator().allocateOrGrow(allocationContext, existing, requiredLength(mask, defaultLength), I64Vector::new);
    }

    private static BooleanVector allocateBooleanOutput(Vector output, Mask mask, int defaultLength, PrimitiveExecutionContext context, Allocator.Context allocationContext)
    {
        BooleanVector existing = output instanceof BooleanVector vector ? vector : null;
        return (BooleanVector) context.allocator().allocateOrGrow(allocationContext, existing, requiredLength(mask, defaultLength), BooleanVector::new);
    }

    private static void applyLongToFlat(String functionName, Vector left, Vector right, Mask mask, I64Vector output, LongBinaryOperation operation)
    {
        I64ValueReader leftReader = new I64ValueReader(left);
        I64ValueReader rightReader = new I64ValueReader(right);
        try {
            for (int position : mask) {
                output.values()[position] = operation.apply(leftReader.valueAt(position), rightReader.valueAt(position));
            }
        }
        catch (Throwable throwable) {
            throw new RuntimeException("Error invoking scalar function " + functionName, throwable);
        }
    }

    private static void applyComparisonToFlat(String functionName, Vector left, Vector right, Mask mask, BooleanVector output, LongComparisonOperation operation)
    {
        I64ValueReader leftReader = new I64ValueReader(left);
        I64ValueReader rightReader = new I64ValueReader(right);
        try {
            for (int position : mask) {
                output.values()[position] = operation.apply(leftReader.valueAt(position), rightReader.valueAt(position));
            }
        }
        catch (Throwable throwable) {
            throw new RuntimeException("Error invoking scalar function " + functionName, throwable);
        }
    }

    private static Vector applyRleRleLong(String functionName, RleVector left, RleVector right, LongBinaryOperation operation)
    {
        I64ValueReader leftReader = new I64ValueReader(left.values());
        I64ValueReader rightReader = new I64ValueReader(right.values());

        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        long[] values = new long[counts.length];

        int outputIndex = 0;
        int leftIndex = 0;
        int rightIndex = 0;
        int leftCount = 0;
        int rightCount = 0;

        try {
            while (leftIndex < left.counts().length && rightIndex < right.counts().length) {
                if (leftCount == 0) {
                    leftCount = left.counts()[leftIndex];
                }
                if (rightCount == 0) {
                    rightCount = right.counts()[rightIndex];
                }

                int count = Math.min(leftCount, rightCount);
                counts[outputIndex] = count;
                values[outputIndex] = operation.apply(leftReader.valueAt(leftIndex), rightReader.valueAt(rightIndex));
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
        catch (Throwable throwable) {
            throw new RuntimeException("Error invoking scalar function " + functionName, throwable);
        }

        return new RleVector(counts, new I64Vector(values));
    }

    private static Vector applyRleRleComparison(String functionName, RleVector left, RleVector right, LongComparisonOperation operation)
    {
        I64ValueReader leftReader = new I64ValueReader(left.values());
        I64ValueReader rightReader = new I64ValueReader(right.values());

        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        boolean[] values = new boolean[counts.length];

        int outputIndex = 0;
        int leftIndex = 0;
        int rightIndex = 0;
        int leftCount = 0;
        int rightCount = 0;

        try {
            while (leftIndex < left.counts().length && rightIndex < right.counts().length) {
                if (leftCount == 0) {
                    leftCount = left.counts()[leftIndex];
                }
                if (rightCount == 0) {
                    rightCount = right.counts()[rightIndex];
                }

                int count = Math.min(leftCount, rightCount);
                counts[outputIndex] = count;
                values[outputIndex] = operation.apply(leftReader.valueAt(leftIndex), rightReader.valueAt(rightIndex));
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
        catch (Throwable throwable) {
            throw new RuntimeException("Error invoking scalar function " + functionName, throwable);
        }

        return new RleVector(counts, new BooleanVector(values));
    }

    private static int requiredLength(Mask mask, int defaultLength)
    {
        if (mask.none()) {
            return defaultLength;
        }
        return Math.max(defaultLength, mask.maxPosition() + 1);
    }

    private static final class I64ValueReader
    {
        private final long[] flatValues;
        private final int[] counts;
        private int runIndex;
        private int runEnd;

        private I64ValueReader(Vector vector)
        {
            if (vector instanceof RleVector rle) {
                flatValues = i64Values(rle.values());
                counts = rle.counts();
                if (counts.length > 0) {
                    runEnd = counts[0];
                }
            }
            else {
                flatValues = i64Values(vector);
                counts = null;
            }
        }

        public long valueAt(int position)
        {
            if (counts == null) {
                return flatValues[position];
            }

            while (position >= runEnd) {
                runIndex++;
                runEnd += counts[runIndex];
            }
            return flatValues[runIndex];
        }
    }
}
