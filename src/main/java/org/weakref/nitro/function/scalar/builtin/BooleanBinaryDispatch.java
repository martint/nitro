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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

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
        return BinaryDispatchSupport.requiredLength(mask, defaultLength);
    }

    public static void apply(Vector left, Vector right, Mask mask, BooleanVector output, Kernel kernel)
    {
        boolean[] values = output.values();
        forEachPair(left, right, mask, (leftValue, rightValue, position) -> values[position] = kernel.apply(leftValue, rightValue));
    }

    public static RleVector rleRle(RleVector left, RleVector right, BooleanVector output, Kernel kernel)
    {
        int[] counts = new int[RleVector.computeTargetRleLength(left, right)];
        boolean[] values = output.values();

        BinaryDispatchSupport.mergeRuns(left.counts(), right.counts(), (outputIndex, leftIndex, rightIndex, count) -> {
            counts[outputIndex] = count;
            values[outputIndex] = kernel.apply(booleanValue(left.values(), leftIndex), booleanValue(right.values(), rightIndex));
        });
        return new RleVector(counts, output);
    }

    private static void forEachPair(Vector left, Vector right, Mask mask, BooleanPairConsumer consumer)
    {
        BinaryDispatchSupport.validateLength(left, mask);
        BinaryDispatchSupport.validateLength(right, mask);

        switch (left) {
            case BooleanVector leftFlat -> {
                switch (right) {
                    case BooleanVector rightFlat -> forEachFlatFlat(leftFlat.values(), rightFlat.values(), mask, consumer);
                    case DictionaryVector rightDictionary -> forEachFlatDictionary(leftFlat.values(), rightDictionary, mask, consumer);
                    case RleVector rightRle -> forEachFlatRle(leftFlat.values(), rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            case RleVector leftRle -> {
                switch (right) {
                    case BooleanVector rightFlat -> forEachRleFlat(leftRle, rightFlat.values(), mask, consumer);
                    case DictionaryVector rightDictionary -> forEachRleDictionary(leftRle, rightDictionary, mask, consumer);
                    case RleVector rightRle -> forEachRleRle(leftRle, rightRle, mask, consumer);
                    default -> throw unsupported(right);
                }
            }
            case DictionaryVector leftDictionary -> {
                switch (right) {
                    case BooleanVector rightFlat -> forEachDictionaryFlat(leftDictionary, rightFlat.values(), mask, consumer);
                    case DictionaryVector rightDictionary -> forEachDictionaryDictionary(leftDictionary, rightDictionary, mask, consumer);
                    case RleVector rightRle -> forEachDictionaryRle(leftDictionary, rightRle, mask, consumer);
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
        BinaryDispatchSupport.RlePositionCursor cursor = new BinaryDispatchSupport.RlePositionCursor(left.counts());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(booleanValue(left.values(), cursor.runIndexAt(position)), right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(booleanValue(left.values(), cursor.runIndexAt(position)), right[position], position);
        }
    }

    private static void forEachFlatRle(boolean[] left, RleVector right, Mask mask, BooleanPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor cursor = new BinaryDispatchSupport.RlePositionCursor(right.counts());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], booleanValue(right.values(), cursor.runIndexAt(position)), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], booleanValue(right.values(), cursor.runIndexAt(position)), position);
        }
    }

    private static void forEachFlatDictionary(boolean[] left, DictionaryVector right, Mask mask, BooleanPairConsumer consumer)
    {
        int[] rightIds = right.ids();
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(left[position], booleanValue(right.values(), rightIds[position]), position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(left[position], booleanValue(right.values(), rightIds[position]), position);
        }
    }

    private static void forEachRleRle(RleVector left, RleVector right, Mask mask, BooleanPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor leftCursor = new BinaryDispatchSupport.RlePositionCursor(left.counts());
        BinaryDispatchSupport.RlePositionCursor rightCursor = new BinaryDispatchSupport.RlePositionCursor(right.counts());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(
                        booleanValue(left.values(), leftCursor.runIndexAt(position)),
                        booleanValue(right.values(), rightCursor.runIndexAt(position)),
                        position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(
                    booleanValue(left.values(), leftCursor.runIndexAt(position)),
                    booleanValue(right.values(), rightCursor.runIndexAt(position)),
                    position);
        }
    }

    private static void forEachRleDictionary(RleVector left, DictionaryVector right, Mask mask, BooleanPairConsumer consumer)
    {
        BinaryDispatchSupport.RlePositionCursor leftCursor = new BinaryDispatchSupport.RlePositionCursor(left.counts());
        int[] rightIds = right.ids();
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(
                        booleanValue(left.values(), leftCursor.runIndexAt(position)),
                        booleanValue(right.values(), rightIds[position]),
                        position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(
                    booleanValue(left.values(), leftCursor.runIndexAt(position)),
                    booleanValue(right.values(), rightIds[position]),
                    position);
        }
    }

    private static void forEachDictionaryFlat(DictionaryVector left, boolean[] right, Mask mask, BooleanPairConsumer consumer)
    {
        int[] leftIds = left.ids();
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(booleanValue(left.values(), leftIds[position]), right[position], position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(booleanValue(left.values(), leftIds[position]), right[position], position);
        }
    }

    private static void forEachDictionaryRle(DictionaryVector left, RleVector right, Mask mask, BooleanPairConsumer consumer)
    {
        int[] leftIds = left.ids();
        BinaryDispatchSupport.RlePositionCursor rightCursor = new BinaryDispatchSupport.RlePositionCursor(right.counts());
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(
                        booleanValue(left.values(), leftIds[position]),
                        booleanValue(right.values(), rightCursor.runIndexAt(position)),
                        position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(
                    booleanValue(left.values(), leftIds[position]),
                    booleanValue(right.values(), rightCursor.runIndexAt(position)),
                    position);
        }
    }

    private static void forEachDictionaryDictionary(DictionaryVector left, DictionaryVector right, Mask mask, BooleanPairConsumer consumer)
    {
        int[] leftIds = left.ids();
        int[] rightIds = right.ids();
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                consumer.accept(
                        booleanValue(left.values(), leftIds[position]),
                        booleanValue(right.values(), rightIds[position]),
                        position);
            }
            return;
        }

        for (int position : mask) {
            consumer.accept(
                    booleanValue(left.values(), leftIds[position]),
                    booleanValue(right.values(), rightIds[position]),
                    position);
        }
    }

    private static boolean booleanValue(Vector vector, int position)
    {
        return switch (vector) {
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> booleanValue(values.values(), values.ids()[position]);
            case RleVector values -> booleanValue(values.values(), values.runIndex(position));
            default -> throw unsupported(vector);
        };
    }

    private static IllegalArgumentException unsupported(Vector vector)
    {
        return new IllegalArgumentException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }
}
