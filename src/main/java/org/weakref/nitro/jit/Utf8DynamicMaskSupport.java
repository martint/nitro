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
package org.weakref.nitro.jit;

import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

/**
 * Physical execution support for a compiled two-input UTF-8 equality expression.
 *
 * <p>The mutable bindings belong to one constructed evaluator program. Common flat/dictionary combinations resolve
 * their encoding once per batch; unusual compatible encodings use the general binary-region data API.
 */
final class Utf8DynamicMaskSupport
{
    private final Utf8DynamicMaskKernel kernel = Utf8DynamicMaskKernelGenerator.generate();

    boolean evaluate(Streams leftInput, Streams rightInput, Mask mask, boolean selectMatches)
    {
        Vector left = leftInput.values();
        Vector right = rightInput.values();
        if (!hasUtf8Trait(left) || !hasUtf8Trait(right)) {
            return false;
        }
        Vector leftNullVector = leftInput.getOrNull(Stream.NULLS);
        Vector rightNullVector = rightInput.getOrNull(Stream.NULLS);
        if ((leftNullVector != null && !(leftNullVector instanceof BooleanVector)) ||
                (rightNullVector != null && !(rightNullVector instanceof BooleanVector))) {
            retainGeneral(left, right, leftNullVector, rightNullVector, mask, selectMatches);
            return true;
        }
        boolean[] leftNulls = leftNullVector instanceof BooleanVector values ? values.values() : null;
        boolean[] rightNulls = rightNullVector instanceof BooleanVector values ? values.values() : null;
        if (left instanceof BinaryVector leftFlat && right instanceof BinaryVector rightFlat) {
            kernel.retainFlatFlat(
                    leftFlat.data(), leftFlat.offsets(), null, leftNulls,
                    rightFlat.data(), rightFlat.offsets(), null, rightNulls,
                    mask, selectMatches);
            return true;
        }
        if (left instanceof DictionaryVector leftEncoded &&
                leftEncoded.baseValues() instanceof BinaryVector leftBase &&
                leftEncoded.dictionaryDepth() == 1 &&
                right instanceof DictionaryVector rightEncoded &&
                rightEncoded.baseValues() instanceof BinaryVector rightBase &&
                rightEncoded.dictionaryDepth() == 1) {
            kernel.retainDictionaryDictionary(
                    leftBase.data(), leftBase.offsets(), leftEncoded.ids(), leftNulls,
                    rightBase.data(), rightBase.offsets(), rightEncoded.ids(), rightNulls,
                    mask, selectMatches);
            return true;
        }
        if (left instanceof DictionaryVector leftEncoded &&
                leftEncoded.baseValues() instanceof BinaryVector leftBase &&
                leftEncoded.dictionaryDepth() == 1 &&
                right instanceof BinaryVector rightFlat) {
            kernel.retainDictionaryFlat(
                    leftBase.data(), leftBase.offsets(), leftEncoded.ids(), leftNulls,
                    rightFlat.data(), rightFlat.offsets(), null, rightNulls,
                    mask, selectMatches);
            return true;
        }
        if (left instanceof BinaryVector leftFlat &&
                right instanceof DictionaryVector rightEncoded &&
                rightEncoded.baseValues() instanceof BinaryVector rightBase &&
                rightEncoded.dictionaryDepth() == 1) {
            kernel.retainFlatDictionary(
                    leftFlat.data(), leftFlat.offsets(), null, leftNulls,
                    rightBase.data(), rightBase.offsets(), rightEncoded.ids(), rightNulls,
                    mask, selectMatches);
            return true;
        }

        retainGeneral(left, right, leftNullVector, rightNullVector, mask, selectMatches);
        return true;
    }

    private static void retainGeneral(
            Vector left,
            Vector right,
            Vector leftNullVector,
            Vector rightNullVector,
            Mask mask,
            boolean selectMatches)
    {
        VectorAccess.BinaryRegions leftRegions = VectorAccess.binaryRegions(left);
        VectorAccess.BinaryRegions rightRegions = VectorAccess.binaryRegions(right);
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(leftNullVector);
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(rightNullVector);
        mask.retainIf(position -> {
            if (leftNulls.value(position) || rightNulls.value(position)) {
                return false;
            }
            int leftLength = leftRegions.length(position);
            int rightLength = rightRegions.length(position);
            return (leftLength == rightLength &&
                    java.util.Arrays.mismatch(
                            leftRegions.data(position),
                            leftRegions.offset(position),
                            leftRegions.offset(position) + leftLength,
                            rightRegions.data(position),
                            rightRegions.offset(position),
                            rightRegions.offset(position) + rightLength) == -1) == selectMatches;
        });
    }

    private static boolean hasUtf8Trait(Vector vector)
    {
        return switch (vector) {
            case BinaryVector binary -> binary.hasTrait(Utf8Traits.UTF8_STRING);
            case DictionaryVector dictionary -> hasUtf8Trait(dictionary.values());
            case org.weakref.nitro.data.RleVector rle -> hasUtf8Trait(rle.values());
            default -> false;
        };
    }
}
