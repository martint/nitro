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
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "eq_utf8")
public final class EqualUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("EqualUtf8");

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq_utf8");
        if (!requestedStreams.contains(Stream.VALUES)) {
            return Streams.empty();
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        checkArgument(left.length() == right.length(), "eq_utf8 inputs must have the same logical length");
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);

        Streams result = Streams.empty();
        int requiredLength = Math.max(mask.maxPosition() + 1, left.length());
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            apply(left, right, leftNulls, rightNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void apply(Vector left, Vector right, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        if (left instanceof BinaryVector leftValues && right instanceof BinaryVector rightValues) {
            applyFlatFlat(leftValues, rightValues, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof BinaryVector leftValues && right instanceof DictionaryVector rightDictionary) {
            applyFlatDictionary(leftValues, rightDictionary, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof DictionaryVector leftDictionary && right instanceof BinaryVector rightValues) {
            applyDictionaryFlat(leftDictionary, rightValues, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof DictionaryVector leftDictionary && right instanceof DictionaryVector rightDictionary) {
            applyDictionaryDictionary(leftDictionary, rightDictionary, leftNulls, rightNulls, mask, output);
            return;
        }
        throw new IllegalArgumentException("Unsupported eq_utf8 vector types: " + left.getClass().getSimpleName() + ", " + right.getClass().getSimpleName());
    }

    private static void applyFlatFlat(BinaryVector left, BinaryVector right, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        boolean ascii = useAsciiFastPath(left, right);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    outputValues[position] = false;
                }
                else {
                    outputValues[position] = ascii ? binaryEquals(left, position, right, position) : utf8Equals(left, position, right, position);
                }
            }
            return;
        }
        for (int position : mask) {
            if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                outputValues[position] = false;
            }
            else {
                outputValues[position] = ascii ? binaryEquals(left, position, right, position) : utf8Equals(left, position, right, position);
            }
        }
    }

    private static void applyFlatDictionary(BinaryVector left, DictionaryVector rightDictionary, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector right = requireBinaryDictionary(rightDictionary);
        boolean ascii = useAsciiFastPath(left, right);
        int[] rightIds = rightDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    outputValues[position] = false;
                }
                else {
                    int rightPosition = rightIds[position];
                    outputValues[position] = ascii ? binaryEquals(left, position, right, rightPosition) : utf8Equals(left, position, right, rightPosition);
                }
            }
            return;
        }
        for (int position : mask) {
            if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                outputValues[position] = false;
            }
            else {
                int rightPosition = rightIds[position];
                outputValues[position] = ascii ? binaryEquals(left, position, right, rightPosition) : utf8Equals(left, position, right, rightPosition);
            }
        }
    }

    private static void applyDictionaryFlat(DictionaryVector leftDictionary, BinaryVector right, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(leftDictionary);
        boolean ascii = useAsciiFastPath(left, right);
        int[] leftIds = leftDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    outputValues[position] = false;
                }
                else {
                    int leftPosition = leftIds[position];
                    outputValues[position] = ascii ? binaryEquals(left, leftPosition, right, position) : utf8Equals(left, leftPosition, right, position);
                }
            }
            return;
        }
        for (int position : mask) {
            if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                outputValues[position] = false;
            }
            else {
                int leftPosition = leftIds[position];
                outputValues[position] = ascii ? binaryEquals(left, leftPosition, right, position) : utf8Equals(left, leftPosition, right, position);
            }
        }
    }

    private static void applyDictionaryDictionary(DictionaryVector leftDictionary, DictionaryVector rightDictionary, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(leftDictionary);
        BinaryVector right = requireBinaryDictionary(rightDictionary);
        boolean ascii = useAsciiFastPath(left, right);
        int[] leftIds = leftDictionary.ids();
        int[] rightIds = rightDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    outputValues[position] = false;
                }
                else {
                    int leftPosition = leftIds[position];
                    int rightPosition = rightIds[position];
                    outputValues[position] = ascii ? binaryEquals(left, leftPosition, right, rightPosition) : utf8Equals(left, leftPosition, right, rightPosition);
                }
            }
            return;
        }
        for (int position : mask) {
            if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                outputValues[position] = false;
            }
            else {
                int leftPosition = leftIds[position];
                int rightPosition = rightIds[position];
                outputValues[position] = ascii ? binaryEquals(left, leftPosition, right, rightPosition) : utf8Equals(left, leftPosition, right, rightPosition);
            }
        }
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

    private static BinaryVector requireBinaryDictionary(DictionaryVector vector)
    {
        checkArgument(vector.values() instanceof BinaryVector, "eq_utf8 requires BinaryVector dictionary values");
        return (BinaryVector) vector.values();
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

    private static boolean utf8Equals(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        checkArgument(hasUtf8Traits(left) && hasUtf8Traits(right), "eq_utf8 requires UTF8_STRING inputs");
        return left.utf8Value(leftPosition).equals(right.utf8Value(rightPosition));
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
}
