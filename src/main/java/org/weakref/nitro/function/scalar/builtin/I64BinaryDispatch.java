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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;

final class I64BinaryDispatch
{
    private I64BinaryDispatch() {}

    @FunctionalInterface
    interface LongBinaryKernel
    {
        long apply(long left, long right);
    }

    @FunctionalInterface
    interface BooleanBinaryKernel
    {
        boolean apply(long left, long right);
    }

    @FunctionalInterface
    interface LongErrorKernel
    {
        void apply(long left, long right, long[] values, boolean[] errors, int position);
    }

    @FunctionalInterface
    interface LongPairConsumer
    {
        void accept(long left, long right, int position);
    }

    public static int requiredLength(Mask mask, int defaultLength)
    {
        return BinaryDispatchSupport.requiredLength(mask, defaultLength);
    }

    public static void applyLong(Vector left, Vector right, Mask mask, I64Vector output, LongBinaryKernel kernel)
    {
        long[] values = output.values();
        forEachPair(left, right, mask, (leftValue, rightValue, position) -> values[position] = kernel.apply(leftValue, rightValue));
    }

    public static void applyBoolean(Vector left, Vector right, Mask mask, BooleanVector output, BooleanBinaryKernel kernel)
    {
        boolean[] values = output.values();
        forEachPair(left, right, mask, (leftValue, rightValue, position) -> values[position] = kernel.apply(leftValue, rightValue));
    }

    public static void applyLongWithErrors(Vector left, Vector right, Mask mask, I64Vector output, BooleanVector errors, LongErrorKernel kernel)
    {
        long[] values = output.values();
        boolean[] errorValues = errors.values();
        forEachPair(left, right, mask, (leftValue, rightValue, position) -> kernel.apply(leftValue, rightValue, values, errorValues, position));
    }

    public static void applyErrorsOnly(Vector left, Vector right, Mask mask, BooleanVector errors, LongErrorKernel kernel)
    {
        boolean[] errorValues = errors.values();
        forEachPair(left, right, mask, (leftValue, rightValue, position) -> kernel.apply(leftValue, rightValue, null, errorValues, position));
    }

    public static RleVector rleRleLong(RleVector left, RleVector right, I64Vector output, LongBinaryKernel kernel)
    {
        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        long[] values = output.values();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());

        BinaryDispatchSupport.mergeRuns(left.counts(), right.counts(), (outputIndex, leftIndex, rightIndex, count) -> {
            counts[outputIndex] = count;
            values[outputIndex] = kernel.apply(leftValues.value(leftIndex), rightValues.value(rightIndex));
        });
        return new RleVector(counts, output);
    }

    public static RleVector rleRleBoolean(RleVector left, RleVector right, BooleanVector output, BooleanBinaryKernel kernel)
    {
        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        boolean[] values = output.values();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());

        BinaryDispatchSupport.mergeRuns(left.counts(), right.counts(), (outputIndex, leftIndex, rightIndex, count) -> {
            counts[outputIndex] = count;
            values[outputIndex] = kernel.apply(leftValues.value(leftIndex), rightValues.value(rightIndex));
        });
        return new RleVector(counts, output);
    }

    public static RleWithErrors rleRleLongWithErrors(RleVector left, RleVector right, I64Vector valuesOutput, BooleanVector errorsOutput, LongErrorKernel kernel)
    {
        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        long[] values = valuesOutput.values();
        boolean[] errors = errorsOutput.values();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());

        BinaryDispatchSupport.mergeRuns(left.counts(), right.counts(), (outputIndex, leftIndex, rightIndex, count) -> {
            counts[outputIndex] = count;
            kernel.apply(leftValues.value(leftIndex), rightValues.value(rightIndex), values, errors, outputIndex);
        });
        return new RleWithErrors(new RleVector(counts, valuesOutput), new RleVector(counts, errorsOutput));
    }

    public static RleVector rleRleErrorsOnly(RleVector left, RleVector right, BooleanVector errorsOutput, LongErrorKernel kernel)
    {
        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        boolean[] errors = errorsOutput.values();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());

        BinaryDispatchSupport.mergeRuns(left.counts(), right.counts(), (outputIndex, leftIndex, rightIndex, count) -> {
            counts[outputIndex] = count;
            kernel.apply(leftValues.value(leftIndex), rightValues.value(rightIndex), null, errors, outputIndex);
        });
        return new RleVector(counts, errorsOutput);
    }

    static void forEachPair(Vector left, Vector right, Mask mask, LongPairConsumer consumer)
    {
        BinaryDispatchSupport.validateLength(left, mask);
        BinaryDispatchSupport.validateLength(right, mask);

        switch (left) {
            case I32Vector leftFlat -> {
                switch (right) {
                    case I32Vector rightFlat -> forEachFlatFlat(leftFlat.values(), rightFlat.values(), mask, consumer);
                    case I64Vector rightFlat -> forEachFlatFlat(leftFlat.values(), rightFlat.values(), mask, consumer);
                    case DictionaryVector rightDictionary -> forEachFlatDictionary(leftFlat.values(), rightDictionary, mask, consumer);
                    case RleVector rightRle -> forEachFlatRle(leftFlat.values(), rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            case I64Vector leftFlat -> {
                switch (right) {
                    case I32Vector rightFlat -> forEachFlatFlat(leftFlat.values(), rightFlat.values(), mask, consumer);
                    case I64Vector rightFlat -> forEachFlatFlat(leftFlat.values(), rightFlat.values(), mask, consumer);
                    case DictionaryVector rightDictionary -> forEachFlatDictionary(leftFlat.values(), rightDictionary, mask, consumer);
                    case RleVector rightRle -> forEachFlatRle(leftFlat.values(), rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            case RleVector leftRle -> {
                switch (right) {
                    case I32Vector rightFlat -> forEachRleFlat(leftRle, rightFlat.values(), mask, consumer);
                    case I64Vector rightFlat -> forEachRleFlat(leftRle, rightFlat.values(), mask, consumer);
                    case DictionaryVector rightDictionary -> forEachRleDictionary(leftRle, rightDictionary, mask, consumer);
                    case RleVector rightRle -> forEachRleRle(leftRle, rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            case DictionaryVector leftDictionary -> {
                switch (right) {
                    case I32Vector rightFlat -> forEachDictionaryFlat(leftDictionary, rightFlat.values(), mask, consumer);
                    case I64Vector rightFlat -> forEachDictionaryFlat(leftDictionary, rightFlat.values(), mask, consumer);
                    case DictionaryVector rightDictionary -> forEachDictionaryDictionary(leftDictionary, rightDictionary, mask, consumer);
                    case RleVector rightRle -> forEachDictionaryRle(leftDictionary, rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            default -> throw unsupported(left);
        }
    }

    private static void forEachGeneric(Vector left, Vector right, Mask mask, LongPairConsumer consumer)
    {
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left);
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right);
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(position), rightValues.value(position), position);
            }
            return;
        }
        for (int position : mask) {
            consumer.accept(leftValues.value(position), rightValues.value(position), position);
        }
    }

    private static void forEachFlatFlat(int[] left, int[] right, Mask mask, LongPairConsumer consumer)
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

    private static void forEachFlatFlat(int[] left, long[] right, Mask mask, LongPairConsumer consumer)
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

    private static void forEachFlatFlat(long[] left, int[] right, Mask mask, LongPairConsumer consumer)
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

    private static void forEachFlatFlat(long[] left, long[] right, Mask mask, LongPairConsumer consumer)
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

    private static void forEachRleFlat(RleVector left, int[] right, Mask mask, LongPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor cursor = new BinaryDispatchSupport.RlePositionCursor(left.counts());
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(cursor.runIndexAt(position)), right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(cursor.runIndexAt(position)), right[position], position);
        }
    }

    private static void forEachRleFlat(RleVector left, long[] right, Mask mask, LongPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor cursor = new BinaryDispatchSupport.RlePositionCursor(left.counts());
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(cursor.runIndexAt(position)), right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(cursor.runIndexAt(position)), right[position], position);
        }
    }

    private static void forEachFlatRle(int[] left, RleVector right, Mask mask, LongPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor cursor = new BinaryDispatchSupport.RlePositionCursor(right.counts());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], rightValues.value(cursor.runIndexAt(position)), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], rightValues.value(cursor.runIndexAt(position)), position);
        }
    }

    private static void forEachFlatRle(long[] left, RleVector right, Mask mask, LongPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor cursor = new BinaryDispatchSupport.RlePositionCursor(right.counts());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], rightValues.value(cursor.runIndexAt(position)), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], rightValues.value(cursor.runIndexAt(position)), position);
        }
    }

    private static void forEachFlatDictionary(int[] left, DictionaryVector right, Mask mask, LongPairConsumer consumer)
    {
        int[] rightIds = right.ids();
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], rightValues.value(rightIds[position]), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], rightValues.value(rightIds[position]), position);
        }
    }

    private static void forEachFlatDictionary(long[] left, DictionaryVector right, Mask mask, LongPairConsumer consumer)
    {
        int[] rightIds = right.ids();
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], rightValues.value(rightIds[position]), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], rightValues.value(rightIds[position]), position);
        }
    }

    private static void forEachRleRle(RleVector left, RleVector right, Mask mask, LongPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor leftCursor = new BinaryDispatchSupport.RlePositionCursor(left.counts());
        BinaryDispatchSupport.RlePositionCursor rightCursor = new BinaryDispatchSupport.RlePositionCursor(right.counts());
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(leftCursor.runIndexAt(position)), rightValues.value(rightCursor.runIndexAt(position)), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(leftCursor.runIndexAt(position)), rightValues.value(rightCursor.runIndexAt(position)), position);
        }
    }

    private static void forEachRleDictionary(RleVector left, DictionaryVector right, Mask mask, LongPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor leftCursor = new BinaryDispatchSupport.RlePositionCursor(left.counts());
        int[] rightIds = right.ids();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(leftCursor.runIndexAt(position)), rightValues.value(rightIds[position]), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(leftCursor.runIndexAt(position)), rightValues.value(rightIds[position]), position);
        }
    }

    private static void forEachDictionaryFlat(DictionaryVector left, int[] right, Mask mask, LongPairConsumer consumer)
    {
        int[] leftIds = left.ids();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(leftIds[position]), right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(leftIds[position]), right[position], position);
        }
    }

    private static void forEachDictionaryFlat(DictionaryVector left, long[] right, Mask mask, LongPairConsumer consumer)
    {
        int[] leftIds = left.ids();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(leftIds[position]), right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(leftIds[position]), right[position], position);
        }
    }

    private static void forEachDictionaryRle(DictionaryVector left, RleVector right, Mask mask, LongPairConsumer consumer)
    {
        int[] leftIds = left.ids();
        BinaryDispatchSupport.RlePositionCursor rightCursor = new BinaryDispatchSupport.RlePositionCursor(right.counts());
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(leftIds[position]), rightValues.value(rightCursor.runIndexAt(position)), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(leftIds[position]), rightValues.value(rightCursor.runIndexAt(position)), position);
        }
    }

    private static void forEachDictionaryDictionary(DictionaryVector left, DictionaryVector right, Mask mask, LongPairConsumer consumer)
    {
        int[] leftIds = left.ids();
        int[] rightIds = right.ids();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(left.values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(right.values());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(leftValues.value(leftIds[position]), rightValues.value(rightIds[position]), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(leftValues.value(leftIds[position]), rightValues.value(rightIds[position]), position);
        }
    }

    private static IllegalArgumentException unsupported(Vector vector)
    {
        return new IllegalArgumentException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }

    public record RleWithErrors(RleVector values, RleVector errors) {}
}
