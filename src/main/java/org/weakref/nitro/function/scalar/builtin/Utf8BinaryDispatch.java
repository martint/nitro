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
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public final class Utf8BinaryDispatch
{
    private static final jdk.incubator.vector.VectorSpecies<Byte> CONTAINS_SPECIES = jdk.incubator.vector.ByteVector.SPECIES_PREFERRED.length() <= Long.SIZE ? jdk.incubator.vector.ByteVector.SPECIES_PREFERRED : jdk.incubator.vector.ByteVector.SPECIES_512;

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
        int requiredLength = mask.none() ? 0 : mask.maxPosition() + 1;
        checkArgument(left.length() >= requiredLength, "%s left input length is too small for mask: left=%s required=%s", functionName, left.length(), requiredLength);
        checkArgument(right.length() >= requiredLength, "%s right input length is too small for mask: right=%s required=%s", functionName, right.length(), requiredLength);
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);

        Streams result = Streams.empty();
        int outputLength = Math.max(requiredLength, Math.max(left.length(), right.length()));
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    outputLength,
                    BooleanVector::new);
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector outputValues = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    outputLength,
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
        if (left instanceof BinaryVector leftValues && right instanceof RleVector rightRle) {
            applyFlatRle(functionName, operation, leftValues, rightRle, leftNulls, rightNulls, mask, output);
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
        if (left instanceof DictionaryVector leftDictionary && right instanceof RleVector rightRle) {
            applyDictionaryRle(functionName, operation, leftDictionary, rightRle, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof RleVector leftRle && right instanceof BinaryVector rightValues) {
            applyRleFlat(functionName, operation, leftRle, rightValues, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof RleVector leftRle && right instanceof DictionaryVector rightDictionary) {
            applyRleDictionary(functionName, operation, leftRle, rightDictionary, leftNulls, rightNulls, mask, output);
            return;
        }
        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle) {
            applyRleRle(functionName, operation, leftRle, rightRle, leftNulls, rightNulls, mask, output);
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

    private static void applyFlatRle(String functionName, Operation operation, BinaryVector left, RleVector rightRle, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        boolean ascii = useAsciiFastPath(left, right);
        boolean[] outputValues = output.values();
        int runIndex = 0;
        int runEnd = rightRle.counts()[0];
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                while (position >= runEnd) {
                    runIndex++;
                    runEnd += rightRle.counts()[runIndex];
                }
                outputValues[position] = evaluate(functionName, operation, left, position, right, runIndex, leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += rightRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, position, right, runIndex, leftNulls, rightNulls, position, ascii);
        }
    }

    private static void applyDictionaryRle(String functionName, Operation operation, DictionaryVector leftDictionary, RleVector rightRle, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        boolean ascii = useAsciiFastPath(left, right);
        int[] leftIds = leftDictionary.ids();
        boolean[] outputValues = output.values();
        int runIndex = 0;
        int runEnd = rightRle.counts()[0];
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                while (position >= runEnd) {
                    runIndex++;
                    runEnd += rightRle.counts()[runIndex];
                }
                outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, runIndex, leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += rightRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, runIndex, leftNulls, rightNulls, position, ascii);
        }
    }

    private static void applyRleFlat(String functionName, Operation operation, RleVector leftRle, BinaryVector right, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        boolean ascii = useAsciiFastPath(left, right);
        boolean[] outputValues = output.values();
        int runIndex = 0;
        int runEnd = leftRle.counts()[0];
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                while (position >= runEnd) {
                    runIndex++;
                    runEnd += leftRle.counts()[runIndex];
                }
                outputValues[position] = evaluate(functionName, operation, left, runIndex, right, position, leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += leftRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, runIndex, right, position, leftNulls, rightNulls, position, ascii);
        }
    }

    private static void applyRleDictionary(String functionName, Operation operation, RleVector leftRle, DictionaryVector rightDictionary, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        boolean ascii = useAsciiFastPath(left, right);
        int[] rightIds = rightDictionary.ids();
        boolean[] outputValues = output.values();
        int runIndex = 0;
        int runEnd = leftRle.counts()[0];
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                while (position >= runEnd) {
                    runIndex++;
                    runEnd += leftRle.counts()[runIndex];
                }
                outputValues[position] = evaluate(functionName, operation, left, runIndex, right, rightIds[position], leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += leftRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, runIndex, right, rightIds[position], leftNulls, rightNulls, position, ascii);
        }
    }

    private static void applyRleRle(String functionName, Operation operation, RleVector leftRle, RleVector rightRle, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        boolean ascii = useAsciiFastPath(left, right);
        boolean[] outputValues = output.values();
        int leftRunIndex = 0;
        int leftRunEnd = leftRle.counts()[0];
        int rightRunIndex = 0;
        int rightRunEnd = rightRle.counts()[0];
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                while (position >= leftRunEnd) {
                    leftRunIndex++;
                    leftRunEnd += leftRle.counts()[leftRunIndex];
                }
                while (position >= rightRunEnd) {
                    rightRunIndex++;
                    rightRunEnd += rightRle.counts()[rightRunIndex];
                }
                outputValues[position] = evaluate(functionName, operation, left, leftRunIndex, right, rightRunIndex, leftNulls, rightNulls, position, ascii);
            }
            return;
        }
        for (int position : mask) {
            while (position >= leftRunEnd) {
                leftRunIndex++;
                leftRunEnd += leftRle.counts()[leftRunIndex];
            }
            while (position >= rightRunEnd) {
                rightRunIndex++;
                rightRunEnd += rightRle.counts()[rightRunIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, leftRunIndex, right, rightRunIndex, leftNulls, rightNulls, position, ascii);
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

    private static BinaryVector requireBinaryRle(String functionName, RleVector vector)
    {
        checkArgument(vector.values() instanceof BinaryVector, "%s requires BinaryVector RLE values", functionName);
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
        if (needleLength == 1) {
            return binaryContainsSingleByte(haystackData, haystackStart, haystackLength, needleData[needleStart]);
        }
        return binaryContainsVectorized(haystackData, haystackStart, haystackLength, needleData, needleStart, needleLength);
    }

    private static boolean binaryContainsSingleByte(byte[] haystackData, int haystackStart, int haystackLength, byte needleByte)
    {
        int fullLength = CONTAINS_SPECIES.loopBound(haystackLength);
        int offset = 0;
        while (offset < fullLength) {
            jdk.incubator.vector.ByteVector haystack = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset);
            if (haystack.compare(jdk.incubator.vector.VectorOperators.EQ, needleByte).anyTrue()) {
                return true;
            }
            offset += CONTAINS_SPECIES.length();
        }
        if (offset < haystackLength) {
            jdk.incubator.vector.VectorMask<Byte> laneMask = CONTAINS_SPECIES.indexInRange(offset, haystackLength);
            jdk.incubator.vector.ByteVector haystack = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset, laneMask);
            if (haystack.compare(jdk.incubator.vector.VectorOperators.EQ, needleByte, laneMask).anyTrue()) {
                return true;
            }
        }
        return false;
    }

    private static boolean binaryContainsVectorized(byte[] haystackData, int haystackStart, int haystackLength, byte[] needleData, int needleStart, int needleLength)
    {
        int firstProbeOffset = 0;
        int secondProbeOffset = selectSecondProbeOffset(needleData, needleStart, needleLength);
        byte firstProbeByte = needleData[needleStart + firstProbeOffset];
        byte secondProbeByte = needleData[needleStart + secondProbeOffset];
        int lastStart = haystackLength - needleLength;
        int candidateCount = lastStart + 1;
        int fullLength = CONTAINS_SPECIES.loopBound(candidateCount);

        int offset = 0;
        while (offset < fullLength) {
            if (binaryContainsVectorizedChunk(haystackData, haystackStart, needleData, needleStart, needleLength, firstProbeOffset, secondProbeOffset, firstProbeByte, secondProbeByte, offset, null)) {
                return true;
            }
            offset += CONTAINS_SPECIES.length();
        }
        if (offset < candidateCount) {
            jdk.incubator.vector.VectorMask<Byte> laneMask = CONTAINS_SPECIES.indexInRange(offset, candidateCount);
            if (binaryContainsVectorizedChunk(haystackData, haystackStart, needleData, needleStart, needleLength, firstProbeOffset, secondProbeOffset, firstProbeByte, secondProbeByte, offset, laneMask)) {
                return true;
            }
        }
        return false;
    }

    private static boolean binaryContainsVectorizedChunk(
            byte[] haystackData,
            int haystackStart,
            byte[] needleData,
            int needleStart,
            int needleLength,
            int firstProbeOffset,
            int secondProbeOffset,
            byte firstProbeByte,
            byte secondProbeByte,
            int offset,
            jdk.incubator.vector.VectorMask<Byte> laneMask)
    {
        jdk.incubator.vector.ByteVector firstProbe;
        jdk.incubator.vector.ByteVector secondProbe;
        jdk.incubator.vector.VectorMask<Byte> candidateMask;
        if (laneMask == null) {
            firstProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset + firstProbeOffset);
            secondProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset + secondProbeOffset);
            candidateMask = firstProbe.compare(jdk.incubator.vector.VectorOperators.EQ, firstProbeByte)
                    .and(secondProbe.compare(jdk.incubator.vector.VectorOperators.EQ, secondProbeByte));
        }
        else {
            firstProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset + firstProbeOffset, laneMask);
            secondProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset + secondProbeOffset, laneMask);
            candidateMask = firstProbe.compare(jdk.incubator.vector.VectorOperators.EQ, firstProbeByte, laneMask)
                    .and(secondProbe.compare(jdk.incubator.vector.VectorOperators.EQ, secondProbeByte, laneMask));
        }
        if (!candidateMask.anyTrue()) {
            return false;
        }

        long candidateBits = candidateMask.toLong();
        while (candidateBits != 0) {
            int lane = Long.numberOfTrailingZeros(candidateBits);
            if (binaryMatchesAt(haystackData, haystackStart + offset + lane, needleData, needleStart, needleLength)) {
                return true;
            }
            candidateBits &= candidateBits - 1;
        }
        return false;
    }

    private static int selectSecondProbeOffset(byte[] needleData, int needleStart, int needleLength)
    {
        byte firstByte = needleData[needleStart];
        for (int offset = needleLength - 1; offset > 0; offset--) {
            if (needleData[needleStart + offset] != firstByte) {
                return offset;
            }
        }
        return needleLength - 1;
    }

    private static boolean binaryMatchesAt(byte[] haystackData, int haystackStart, byte[] needleData, int needleStart, int needleLength)
    {
        return Arrays.mismatch(
                haystackData,
                haystackStart,
                haystackStart + needleLength,
                needleData,
                needleStart,
                needleStart + needleLength) < 0;
    }

    private enum Operation
    {
        EQUALS,
        LESS_THAN,
        STARTS_WITH,
        CONTAINS,
    }
}
