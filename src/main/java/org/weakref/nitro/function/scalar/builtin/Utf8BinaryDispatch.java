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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public final class Utf8BinaryDispatch
{
    private Utf8BinaryDispatch() {}

    public static Streams applyEquals(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.EQUALS, inputs, mask, requestedStreams, output, context);
    }

    public static Streams applyLessThan(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.LESS_THAN, inputs, mask, requestedStreams, output, context);
    }

    public static Streams applyStartsWith(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.STARTS_WITH, inputs, mask, requestedStreams, output, context);
    }

    public static Streams applyContains(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.CONTAINS, inputs, mask, requestedStreams, output, context);
    }

    private static Streams apply(String functionName, Allocator.Context allocationContext, Operation operation, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for %s", functionName);
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        checkArgument(left.length() == right.length(), "%s inputs must have the same logical length: left=%s right=%s", functionName, left.length(), right.length());
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);

        Streams result = Streams.empty();
        int requiredLength = Math.max(mask.maxPosition() + 1, left.length());
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector outputValues = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyValues(functionName, operation, left, right, leftNulls, rightNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyValues(String functionName, Operation operation, Vector left, Vector right, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        if (left instanceof BinaryVector leftValues && right instanceof BinaryVector rightValues) {
            applyFlatFlat(functionName, operation, leftValues, rightValues, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof BinaryVector leftValues && right instanceof DictionaryVector rightDictionary) {
            applyFlatDictionary(functionName, operation, leftValues, rightDictionary, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof DictionaryVector leftDictionary && right instanceof BinaryVector rightValues) {
            applyDictionaryFlat(functionName, operation, leftDictionary, rightValues, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof DictionaryVector leftDictionary && right instanceof DictionaryVector rightDictionary) {
            applyDictionaryDictionary(functionName, operation, leftDictionary, rightDictionary, leftNulls, rightNulls, mask, output);
            return;
        }
        throw new IllegalArgumentException("Unsupported " + functionName + " vector types: " + left.getClass().getSimpleName() + ", " + right.getClass().getSimpleName());
    }

    private static void applyFlatFlat(String functionName, Operation operation, BinaryVector left, BinaryVector right, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        boolean ascii = useAsciiFastPath(left, right);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, position, right, position, leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, position, right, position, leftNulls, rightNulls, position, ascii);
        }
    }

    private static void applyFlatDictionary(String functionName, Operation operation, BinaryVector left, DictionaryVector rightDictionary, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        boolean ascii = useAsciiFastPath(left, right);
        int[] rightIds = rightDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, position, right, rightIds[position], leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, position, right, rightIds[position], leftNulls, rightNulls, position, ascii);
        }
    }

    private static void applyDictionaryFlat(String functionName, Operation operation, DictionaryVector leftDictionary, BinaryVector right, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        boolean ascii = useAsciiFastPath(left, right);
        int[] leftIds = leftDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, position, leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, position, leftNulls, rightNulls, position, ascii);
        }
    }

    private static void applyDictionaryDictionary(String functionName, Operation operation, DictionaryVector leftDictionary, DictionaryVector rightDictionary, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        boolean ascii = useAsciiFastPath(left, right);
        int[] leftIds = leftDictionary.ids();
        int[] rightIds = rightDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, rightIds[position], leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, rightIds[position], leftNulls, rightNulls, position, ascii);
        }
    }

    private static boolean evaluate(String functionName, Operation operation, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition, BooleanVector leftNulls, BooleanVector rightNulls, int nullPosition, boolean ascii)
    {
        if (isNull(leftNulls, nullPosition) || isNull(rightNulls, nullPosition)) {
            return false;
        }
        return switch (operation) {
            case EQUALS -> compareEquals(functionName, left, leftPosition, right, rightPosition);
            case LESS_THAN -> compareLessThan(functionName, left, leftPosition, right, rightPosition, ascii);
            case STARTS_WITH -> compareStartsWith(functionName, left, leftPosition, right, rightPosition, ascii);
            case CONTAINS -> compareContains(functionName, left, leftPosition, right, rightPosition, ascii);
        };
    }

    private static boolean compareEquals(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        requireUtf8Traits(functionName, left, right);
        return binaryEquals(left, leftPosition, right, rightPosition);
    }

    private static boolean compareLessThan(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition, boolean ascii)
    {
        requireUtf8Traits(functionName, left, right);
        return binaryCompare(left, leftPosition, right, rightPosition) < 0;
    }

    private static boolean compareStartsWith(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition, boolean ascii)
    {
        requireUtf8Traits(functionName, left, right);
        return binaryStartsWith(left, leftPosition, right, rightPosition);
    }

    private static boolean compareContains(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition, boolean ascii)
    {
        requireUtf8Traits(functionName, left, right);
        return binaryContains(left, leftPosition, right, rightPosition);
    }

    private static void applyNulls(BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(leftNulls, position) || isNull(rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(leftNulls, position) || isNull(rightNulls, position);
        }
    }

    private static BinaryVector requireBinaryDictionary(String functionName, DictionaryVector vector)
    {
        checkArgument(vector.values() instanceof BinaryVector, "%s requires BinaryVector dictionary values", functionName);
        return (BinaryVector) vector.values();
    }

    private static void requireUtf8Traits(String functionName, BinaryVector left, BinaryVector right)
    {
        checkArgument(hasUtf8Traits(left) && hasUtf8Traits(right), "%s requires UTF8_STRING inputs", functionName);
    }

    private static boolean useAsciiFastPath(BinaryVector left, BinaryVector right)
    {
        return hasUtf8Traits(left) && hasUtf8Traits(right) &&
                left.hasTrait(BinaryVector.Trait.ASCII_ONLY) &&
                right.hasTrait(BinaryVector.Trait.ASCII_ONLY);
    }

    private static boolean hasUtf8Traits(BinaryVector vector)
    {
        return vector.hasTrait(BinaryVector.Trait.UTF8_STRING);
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private static boolean binaryEquals(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int leftLength = left.length(leftPosition);
        if (leftLength != right.length(rightPosition)) {
            return false;
        }
        byte[] leftData = left.data();
        byte[] rightData = right.data();
        int leftStart = left.startOffset(leftPosition);
        int rightStart = right.startOffset(rightPosition);
        for (int index = 0; index < leftLength; index++) {
            if (leftData[leftStart + index] != rightData[rightStart + index]) {
                return false;
            }
        }
        return true;
    }

    private static int binaryCompare(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int leftLength = left.length(leftPosition);
        int rightLength = right.length(rightPosition);
        int compareLength = Math.min(leftLength, rightLength);
        byte[] leftData = left.data();
        byte[] rightData = right.data();
        int leftStart = left.startOffset(leftPosition);
        int rightStart = right.startOffset(rightPosition);
        for (int index = 0; index < compareLength; index++) {
            int comparison = Byte.compare(leftData[leftStart + index], rightData[rightStart + index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    private static boolean binaryStartsWith(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int prefixLength = right.length(rightPosition);
        if (left.length(leftPosition) < prefixLength) {
            return false;
        }
        byte[] leftData = left.data();
        byte[] rightData = right.data();
        int leftStart = left.startOffset(leftPosition);
        int rightStart = right.startOffset(rightPosition);
        for (int index = 0; index < prefixLength; index++) {
            if (leftData[leftStart + index] != rightData[rightStart + index]) {
                return false;
            }
        }
        return true;
    }

    private static boolean binaryContains(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int haystackLength = left.length(leftPosition);
        int needleLength = right.length(rightPosition);
        if (needleLength == 0) {
            return true;
        }
        if (haystackLength < needleLength) {
            return false;
        }

        byte[] haystackData = left.data();
        byte[] needleData = right.data();
        int haystackStart = left.startOffset(leftPosition);
        int needleStart = right.startOffset(rightPosition);
        int lastStart = haystackLength - needleLength;
        for (int offset = 0; offset <= lastStart; offset++) {
            boolean match = true;
            for (int index = 0; index < needleLength; index++) {
                if (haystackData[haystackStart + offset + index] != needleData[needleStart + index]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }
        return false;
    }

    private enum Operation
    {
        EQUALS,
        LESS_THAN,
        STARTS_WITH,
        CONTAINS,
    }
}
