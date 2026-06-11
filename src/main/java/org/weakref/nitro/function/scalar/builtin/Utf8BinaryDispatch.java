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

    public static Mask tryEvaluateEqualsTrueMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateEqualsMask(functionName, allocationContext, inputs, mask, context, true);
    }

    public static Mask tryEvaluateEqualsFalseMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateEqualsMask(functionName, allocationContext, inputs, mask, context, false);
    }

    public static Streams applyInSet(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for %s", functionName);
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector left = inputs.getFirst().values();
        Vector leftNullVector = inputs.getFirst().getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(leftNullVector);
        int requiredLength = mask.none() ? 0 : mask.maxPosition() + 1;
        checkArgument(left.length() >= requiredLength, "%s left input length is too small for mask: left=%s required=%s", functionName, left.length(), requiredLength);

        Streams result = Streams.empty();
        int outputLength = Math.max(requiredLength, left.length());
        if (requestedStreams.contains(Stream.NULLS) && leftNullVector != null) {
            result = result.with(Stream.NULLS, leftNullVector);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            Vector outputValues = tryApplyInSetSpecializedValues(functionName, allocationContext, inputs, mask, output == null ? null : output.getOrNull(Stream.VALUES), outputLength, context);
            if (outputValues == null) {
                BooleanVector booleanOutputValues = VectorAccess.writableBooleanVector(
                        context.allocator(),
                        allocationContext,
                        output != null && output.has(Stream.VALUES) ? output.values() : null,
                        outputLength);
                applyInSetValues(functionName, inputs, leftNulls, mask, booleanOutputValues);
                outputValues = booleanOutputValues;
            }
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    public static Mask tryEvaluateInSetTrueMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateInSetMask(functionName, allocationContext, inputs, mask, context, true);
    }

    public static Mask tryEvaluateInSetFalseMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateInSetMask(functionName, allocationContext, inputs, mask, context, false);
    }

    public static boolean tryEvaluateInSetTrueMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateInSetMaskInPlace(functionName, inputs, mask, true);
    }

    public static boolean tryEvaluateInSetFalseMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateInSetMaskInPlace(functionName, inputs, mask, false);
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
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));

        Streams result = Streams.empty();
        int outputLength = Math.max(requiredLength, Math.max(left.length(), right.length()));
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    outputLength);
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            Vector outputValues = tryApplySpecializedValues(
                    functionName,
                    allocationContext,
                    operation,
                    left,
                    right,
                    leftNulls,
                    rightNulls,
                    mask,
                    output == null ? null : output.getOrNull(Stream.VALUES),
                    outputLength,
                    context);
            if (outputValues == null) {
                BooleanVector booleanOutputValues = VectorAccess.writableBooleanVector(
                        context.allocator(),
                        allocationContext,
                        output != null && output.has(Stream.VALUES) ? output.values() : null,
                        outputLength);
                applyValues(functionName, operation, left, right, leftNulls, rightNulls, mask, booleanOutputValues);
                outputValues = booleanOutputValues;
            }
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static Mask tryEvaluateInSetMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for %s", functionName);
        Vector left = inputs.getFirst().values();
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.getFirst().getOrNull(Stream.NULLS));

        if (left instanceof DictionaryVector leftDictionary && allSingleValueRle(functionName, inputs.subList(1, inputs.size()))) {
            return evaluateInSetDictionaryMask(functionName, allocationContext, leftDictionary, leftNulls, inputs.subList(1, inputs.size()), mask, context, selectMatches);
        }
        return null;
    }

    private static Mask tryEvaluateEqualsMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for %s", functionName);

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));

        if (right instanceof RleVector rightRle && rightRle.counts().length == 1 && left instanceof DictionaryVector leftDictionary) {
            return evaluateEqualsDictionarySingleValueMask(functionName, allocationContext, leftDictionary, rightRle, leftNulls, rightNulls, mask, context, selectMatches);
        }
        if (left instanceof RleVector leftRle && leftRle.counts().length == 1 && right instanceof DictionaryVector rightDictionary) {
            return evaluateEqualsSingleValueDictionaryMask(functionName, allocationContext, leftRle, rightDictionary, leftNulls, rightNulls, mask, context, selectMatches);
        }
        return null;
    }

    private static Vector tryApplySpecializedValues(
            String functionName,
            Allocator.Context allocationContext,
            Operation operation,
            Vector left,
            Vector right,
            VectorAccess.BooleanValues leftNulls,
            VectorAccess.BooleanValues rightNulls,
            Mask mask,
            Vector existing,
            int outputLength,
            PrimitiveExecutionContext context)
    {
        if (operation == Operation.EQUALS && right instanceof RleVector rightRle && rightRle.counts().length == 1) {
            if (left instanceof DictionaryVector leftDictionary) {
                return applyEqualsDictionarySingleValue(functionName, allocationContext, leftDictionary, rightRle, leftNulls, rightNulls, mask, existing, outputLength, context);
            }
        }
        if (operation == Operation.EQUALS && left instanceof RleVector leftRle && leftRle.counts().length == 1) {
            if (right instanceof DictionaryVector rightDictionary) {
                return applyEqualsSingleValueDictionary(functionName, allocationContext, leftRle, rightDictionary, leftNulls, rightNulls, mask, existing, outputLength, context);
            }
        }
        return null;
    }

    private static Vector tryApplyInSetSpecializedValues(
            String functionName,
            Allocator.Context allocationContext,
            List<Streams> inputs,
            Mask mask,
            Vector existing,
            int outputLength,
            PrimitiveExecutionContext context)
    {
        Vector left = inputs.getFirst().values();
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.getFirst().getOrNull(Stream.NULLS));
        if (left instanceof DictionaryVector leftDictionary && allSingleValueRle(functionName, inputs.subList(1, inputs.size()))) {
            return applyInSetDictionary(functionName, allocationContext, leftDictionary, leftNulls, inputs.subList(1, inputs.size()), mask, existing, outputLength, context);
        }
        return null;
    }

    private static void applyValues(String functionName, Operation operation, Vector left, Vector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        if (operation == Operation.CONTAINS && right instanceof RleVector rightRle && rightRle.counts().length == 1) {
            if (left instanceof BinaryVector leftValues) {
                applyContainsFlatSingleNeedle(functionName, leftValues, rightRle, leftNulls, rightNulls, mask, output);
                return;
            }
            if (left instanceof DictionaryVector leftDictionary) {
                applyContainsDictionarySingleNeedle(functionName, leftDictionary, rightRle, leftNulls, rightNulls, mask, output);
                return;
            }
        }
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
        applyGeneric(functionName, operation, left, right, leftNulls, rightNulls, mask, output);
    }

    private static void applyGeneric(String functionName, Operation operation, Vector left, Vector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        requireUtf8Traits(functionName, left, right);
        VectorAccess.BinaryValues leftValues = VectorAccess.binaryValues(left);
        VectorAccess.BinaryValues rightValues = VectorAccess.binaryValues(right);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluateGeneric(operation, leftValues, rightValues, leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluateGeneric(operation, leftValues, rightValues, leftNulls, rightNulls, position);
        }
    }

    private static boolean evaluateGeneric(Operation operation, VectorAccess.BinaryValues leftValues, VectorAccess.BinaryValues rightValues, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, int position)
    {
        if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
            return false;
        }
        VectorAccess.BinarySlice left = leftValues.value(position);
        VectorAccess.BinarySlice right = rightValues.value(position);
        return switch (operation) {
            case EQUALS -> binarySliceEquals(left, right);
            case LESS_THAN -> binarySliceCompare(left, right) < 0;
            case STARTS_WITH -> binarySliceStartsWith(left, right);
            case CONTAINS -> binarySliceContains(left, right);
        };
    }

    private static void applyContainsFlatSingleNeedle(String functionName, BinaryVector left, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireUtf8Traits(functionName, left, right);

        ContainsNeedle needle = compileContainsNeedle(right, 0);
        boolean needleNull = isNull(rightNulls, 0);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = !needleNull &&
                        !isNull(leftNulls, position) &&
                        binaryContains(left, position, needle);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = !needleNull &&
                    !isNull(leftNulls, position) &&
                    binaryContains(left, position, needle);
        }
    }

    private static void applyContainsDictionarySingleNeedle(String functionName, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireUtf8Traits(functionName, left, right);

        ContainsNeedle needle = compileContainsNeedle(right, 0);
        boolean needleNull = isNull(rightNulls, 0);
        int[] leftIds = leftDictionary.ids();
        boolean[] dictionaryMatches = new boolean[left.length()];
        for (int index = 0; index < dictionaryMatches.length; index++) {
            dictionaryMatches[index] = !needleNull && binaryContains(left, index, needle);
        }

        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = !isNull(leftNulls, position) && dictionaryMatches[leftIds[position]];
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = !isNull(leftNulls, position) && dictionaryMatches[leftIds[position]];
        }
    }

    private static Vector applyEqualsDictionarySingleValue(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, Vector existing, int outputLength, PrimitiveExecutionContext context)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireUtf8Traits(functionName, left, right);

        boolean literalNull = isNull(rightNulls, 0);
        int[] leftIds = leftDictionary.ids();
        if (literalNull) {
            return existing != null ? existing : writableBooleanOutput(allocationContext, context, null, outputLength);
        }

        boolean[] dictionaryMatches = new boolean[left.length()];
        for (int index = 0; index < dictionaryMatches.length; index++) {
            dictionaryMatches[index] = binaryEquals(left, index, right, 0);
        }
        BooleanVector dictionaryValues = context.allocator().allocate(
                allocationContext,
                BooleanVector.class,
                dictionaryMatches.length,
                BooleanVector::new);
        System.arraycopy(dictionaryMatches, 0, dictionaryValues.values(), 0, dictionaryMatches.length);
        return context.allocator().allocateDictionary(allocationContext, leftIds, dictionaryValues);
    }

    private static Mask evaluateEqualsDictionarySingleValueMask(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireUtf8Traits(functionName, left, right);

        if (isNull(rightNulls, 0)) {
            return context.allocator().allocateSparseMask(allocationContext, new int[0], 0, mask.size());
        }

        int[] leftIds = leftDictionary.ids();
        boolean[] dictionaryMatches = new boolean[left.length()];
        for (int index = 0; index < dictionaryMatches.length; index++) {
            dictionaryMatches[index] = binaryEquals(left, index, right, 0);
        }

        int selectedCount = 0;
        for (int position : mask) {
            if (!isNull(leftNulls, position) && dictionaryMatches[leftIds[position]] == selectMatches) {
                selectedCount++;
            }
        }

        int[] positions = new int[selectedCount];
        int outputIndex = 0;
        for (int position : mask) {
            if (!isNull(leftNulls, position) && dictionaryMatches[leftIds[position]] == selectMatches) {
                positions[outputIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
    }

    private static Vector applyEqualsSingleValueDictionary(String functionName, Allocator.Context allocationContext, RleVector leftRle, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, Vector existing, int outputLength, PrimitiveExecutionContext context)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        requireUtf8Traits(functionName, left, right);

        boolean literalNull = isNull(leftNulls, 0);
        int[] rightIds = rightDictionary.ids();
        if (literalNull) {
            return existing != null ? existing : writableBooleanOutput(allocationContext, context, null, outputLength);
        }

        boolean[] dictionaryMatches = new boolean[right.length()];
        for (int index = 0; index < dictionaryMatches.length; index++) {
            dictionaryMatches[index] = binaryEquals(left, 0, right, index);
        }
        BooleanVector dictionaryValues = context.allocator().allocate(
                allocationContext,
                BooleanVector.class,
                dictionaryMatches.length,
                BooleanVector::new);
        System.arraycopy(dictionaryMatches, 0, dictionaryValues.values(), 0, dictionaryMatches.length);
        return context.allocator().allocateDictionary(allocationContext, rightIds, dictionaryValues);
    }

    private static Mask evaluateEqualsSingleValueDictionaryMask(String functionName, Allocator.Context allocationContext, RleVector leftRle, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        requireUtf8Traits(functionName, left, right);

        if (isNull(leftNulls, 0)) {
            return context.allocator().allocateSparseMask(allocationContext, new int[0], 0, mask.size());
        }

        int[] rightIds = rightDictionary.ids();
        boolean[] dictionaryMatches = new boolean[right.length()];
        for (int index = 0; index < dictionaryMatches.length; index++) {
            dictionaryMatches[index] = binaryEquals(left, 0, right, index);
        }

        int selectedCount = 0;
        for (int position : mask) {
            if (!isNull(rightNulls, position) && dictionaryMatches[rightIds[position]] == selectMatches) {
                selectedCount++;
            }
        }

        int[] positions = new int[selectedCount];
        int outputIndex = 0;
        for (int position : mask) {
            if (!isNull(rightNulls, position) && dictionaryMatches[rightIds[position]] == selectMatches) {
                positions[outputIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
    }

    private static Vector applyInSetDictionary(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, VectorAccess.BooleanValues leftNulls, List<Streams> literalInputs, Mask mask, Vector existing, int outputLength, PrimitiveExecutionContext context)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector[] literals = literalVectors(functionName, literalInputs);
        boolean[] dictionaryMatches = evaluateDictionaryMembership(left, literals);

        BooleanVector dictionaryValues = context.allocator().allocate(
                allocationContext,
                BooleanVector.class,
                dictionaryMatches.length,
                BooleanVector::new);
        System.arraycopy(dictionaryMatches, 0, dictionaryValues.values(), 0, dictionaryMatches.length);
        return context.allocator().allocateDictionary(allocationContext, leftDictionary.ids(), dictionaryValues);
    }

    private static Mask evaluateInSetDictionaryMask(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, VectorAccess.BooleanValues leftNulls, List<Streams> literalInputs, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector[] literals = literalVectors(functionName, literalInputs);
        boolean[] dictionaryMatches = evaluateDictionaryMembership(left, literals);

        int[] ids = leftDictionary.ids();
        int selectedCount = 0;
        for (int position : mask) {
            if (!isNull(leftNulls, position) && dictionaryMatches[ids[position]] == selectMatches) {
                selectedCount++;
            }
        }

        int[] positions = new int[selectedCount];
        int outputIndex = 0;
        for (int position : mask) {
            if (!isNull(leftNulls, position) && dictionaryMatches[ids[position]] == selectMatches) {
                positions[outputIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
    }

    private static boolean tryEvaluateInSetMaskInPlace(String functionName, List<Streams> inputs, Mask mask, boolean selectMatches)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for %s", functionName);
        Vector leftValues = inputs.getFirst().values();
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.getFirst().getOrNull(Stream.NULLS));

        if (!(leftValues instanceof DictionaryVector leftDictionary) || !allSingleValueRle(functionName, inputs.subList(1, inputs.size()))) {
            return false;
        }

        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector[] literals = literalVectors(functionName, inputs.subList(1, inputs.size()));
        boolean[] dictionaryMatches = evaluateDictionaryMembership(left, literals);
        int[] ids = leftDictionary.ids();
        mask.retainIf(position -> !leftNulls.value(position) && dictionaryMatches[ids[position]] == selectMatches);
        return true;
    }

    private static void applyInSetValues(String functionName, List<Streams> inputs, VectorAccess.BooleanValues leftNulls, Mask mask, BooleanVector output)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for %s", functionName);
        Vector leftValues = inputs.getFirst().values();
        BinaryVector[] literals = literalVectors(functionName, inputs.subList(1, inputs.size()));
        boolean[] outputValues = output.values();

        switch (leftValues) {
            case BinaryVector left -> {
                if (mask.all()) {
                    for (int position = 0; position < mask.size(); position++) {
                        outputValues[position] = !isNull(leftNulls, position) && matchesAny(left, position, literals);
                    }
                    return;
                }
                for (int position : mask) {
                    outputValues[position] = !isNull(leftNulls, position) && matchesAny(left, position, literals);
                }
            }
            case DictionaryVector leftDictionary -> {
                BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
                boolean[] dictionaryMatches = evaluateDictionaryMembership(left, literals);
                int[] ids = leftDictionary.ids();
                if (mask.all()) {
                    for (int position = 0; position < mask.size(); position++) {
                        outputValues[position] = !isNull(leftNulls, position) && dictionaryMatches[ids[position]];
                    }
                    return;
                }
                for (int position : mask) {
                    outputValues[position] = !isNull(leftNulls, position) && dictionaryMatches[ids[position]];
                }
            }
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector or DictionaryVector UTF-8 input");
        }
    }

    private static boolean[] evaluateDictionaryMembership(BinaryVector dictionary, BinaryVector[] literals)
    {
        boolean[] dictionaryMatches = new boolean[dictionary.length()];
        for (int index = 0; index < dictionaryMatches.length; index++) {
            dictionaryMatches[index] = matchesAny(dictionary, index, literals);
        }
        return dictionaryMatches;
    }

    private static boolean matchesAny(BinaryVector values, int position, BinaryVector[] literals)
    {
        for (BinaryVector literal : literals) {
            if (binaryEquals(values, position, literal, 0)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allSingleValueRle(String functionName, List<Streams> literalInputs)
    {
        for (Streams input : literalInputs) {
            if (!(input.values() instanceof RleVector rle) || rle.counts().length != 1) {
                return false;
            }
            requireBinaryRle(functionName, rle);
        }
        return true;
    }

    private static BinaryVector[] literalVectors(String functionName, List<Streams> literalInputs)
    {
        BinaryVector[] literals = new BinaryVector[literalInputs.size()];
        for (int index = 0; index < literalInputs.size(); index++) {
            RleVector rle = (RleVector) literalInputs.get(index).values();
            literals[index] = requireBinaryRle(functionName, rle);
            requireUtf8Traits(functionName, literals[index], literals[index]);
        }
        return literals;
    }

    private static void applyFlatFlat(String functionName, Operation operation, BinaryVector left, BinaryVector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyFlatDictionary(String functionName, Operation operation, BinaryVector left, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyDictionaryFlat(String functionName, Operation operation, DictionaryVector leftDictionary, BinaryVector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyDictionaryDictionary(String functionName, Operation operation, DictionaryVector leftDictionary, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyFlatRle(String functionName, Operation operation, BinaryVector left, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyDictionaryRle(String functionName, Operation operation, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyRleFlat(String functionName, Operation operation, RleVector leftRle, BinaryVector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyRleDictionary(String functionName, Operation operation, RleVector leftRle, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static void applyRleRle(String functionName, Operation operation, RleVector leftRle, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private static boolean evaluate(String functionName, Operation operation, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, int nullPosition, boolean ascii)
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

    private static void applyNulls(VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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
        return requireBinaryValues(functionName, vector.values());
    }

    private static BinaryVector requireBinaryRle(String functionName, RleVector vector)
    {
        return requireBinaryValues(functionName, vector.values());
    }

    private static BinaryVector requireBinaryValues(String functionName, Vector vector)
    {
        return switch (vector) {
            case BinaryVector values -> values;
            case DictionaryVector values -> materializeBinaryValues(functionName, values);
            case RleVector values -> materializeBinaryValues(functionName, values);
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
        };
    }

    private static BinaryVector materializeBinaryValues(String functionName, Vector vector)
    {
        int length = vector.length();
        int totalBytes = 0;
        for (int position = 0; position < length; position++) {
            totalBytes += binaryLength(functionName, vector, position);
        }

        BinaryVector materialized = new BinaryVector(length, totalBytes);
        copyBinaryTraits(vector, materialized);
        for (int position = 0; position < length; position++) {
            copyBinaryBytes(functionName, vector, position, materialized, position);
        }
        return materialized;
    }

    private static int binaryLength(String functionName, Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.length(position);
            case DictionaryVector values -> binaryLength(functionName, values.values(), values.ids()[position]);
            case RleVector values -> binaryLength(functionName, values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
        };
    }

    private static void copyBinaryBytes(String functionName, Vector source, int sourcePosition, BinaryVector target, int targetPosition)
    {
        switch (source) {
            case BinaryVector values -> target.setBytes(targetPosition, values.data(), values.startOffset(sourcePosition), values.length(sourcePosition));
            case DictionaryVector values -> copyBinaryBytes(functionName, values.values(), values.ids()[sourcePosition], target, targetPosition);
            case RleVector values -> copyBinaryBytes(functionName, values.values(), values.runIndex(sourcePosition), target, targetPosition);
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
        }
    }

    private static void copyBinaryTraits(Vector source, BinaryVector target)
    {
        switch (source) {
            case BinaryVector values -> {
                if (values.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING)) {
                    target.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
                }
                if (values.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)) {
                    target.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
                }
            }
            case DictionaryVector values -> copyBinaryTraits(values.values(), target);
            case RleVector values -> copyBinaryTraits(values.values(), target);
            default -> throw new IllegalArgumentException("Expected binary-backed vector but found " + source.getClass().getSimpleName());
        }
    }

    private static void requireUtf8Traits(String functionName, Vector left, Vector right)
    {
        checkArgument(hasUtf8Traits(left) && hasUtf8Traits(right), "%s requires UTF8_STRING inputs", functionName);
    }

    private static void requireUtf8Traits(String functionName, BinaryVector left, BinaryVector right)
    {
        checkArgument(hasUtf8Traits(left) && hasUtf8Traits(right), "%s requires UTF8_STRING inputs", functionName);
    }

    private static boolean hasUtf8Traits(Vector vector)
    {
        return switch (vector) {
            case BinaryVector values -> hasUtf8Traits(values);
            case DictionaryVector values -> hasUtf8Traits(values.values());
            case RleVector values -> hasUtf8Traits(values.values());
            default -> false;
        };
    }

    private static boolean useAsciiFastPath(BinaryVector left, BinaryVector right)
    {
        return hasUtf8Traits(left) && hasUtf8Traits(right) &&
                left.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY) &&
                right.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
    }

    private static boolean hasUtf8Traits(BinaryVector vector)
    {
        return vector.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
    }

    private static boolean isNull(VectorAccess.BooleanValues nulls, int position)
    {
        return nulls != null && nulls.value(position);
    }

    private static BooleanVector writableBooleanOutput(Allocator.Context allocationContext, PrimitiveExecutionContext context, Vector existing, int outputLength)
    {
        return VectorAccess.writableBooleanVector(context.allocator(), allocationContext, existing, outputLength);
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
        return binaryContains(left, leftPosition, compileContainsNeedle(right, rightPosition));
    }

    private static boolean binaryContains(BinaryVector haystack, int haystackPosition, ContainsNeedle needle)
    {
        int haystackLength = haystack.length(haystackPosition);
        if (needle.length() == 0) {
            return true;
        }
        if (haystackLength < needle.length()) {
            return false;
        }

        byte[] haystackData = haystack.data();
        int haystackStart = haystack.startOffset(haystackPosition);
        if (needle.length() == 1) {
            return binaryContainsSingleByte(haystackData, haystackStart, haystackLength, needle.firstProbeByte());
        }
        return binaryContainsVectorized(
                haystackData,
                haystackStart,
                haystackLength,
                needle.data(),
                needle.start(),
                needle.length(),
                needle.firstProbeOffset(),
                needle.secondProbeOffset(),
                needle.firstProbeByte(),
                needle.secondProbeByte());
    }

    private static boolean binarySliceEquals(VectorAccess.BinarySlice left, VectorAccess.BinarySlice right)
    {
        if (left.length() != right.length()) {
            return false;
        }
        for (int index = 0; index < left.length(); index++) {
            if (left.data()[left.offset() + index] != right.data()[right.offset() + index]) {
                return false;
            }
        }
        return true;
    }

    private static int binarySliceCompare(VectorAccess.BinarySlice left, VectorAccess.BinarySlice right)
    {
        int compareLength = Math.min(left.length(), right.length());
        for (int index = 0; index < compareLength; index++) {
            int comparison = Byte.compare(left.data()[left.offset() + index], right.data()[right.offset() + index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length(), right.length());
    }

    private static boolean binarySliceStartsWith(VectorAccess.BinarySlice left, VectorAccess.BinarySlice right)
    {
        if (left.length() < right.length()) {
            return false;
        }
        for (int index = 0; index < right.length(); index++) {
            if (left.data()[left.offset() + index] != right.data()[right.offset() + index]) {
                return false;
            }
        }
        return true;
    }

    private static boolean binarySliceContains(VectorAccess.BinarySlice haystack, VectorAccess.BinarySlice needle)
    {
        if (needle.length() == 0) {
            return true;
        }
        if (haystack.length() < needle.length()) {
            return false;
        }
        int lastStart = haystack.length() - needle.length();
        for (int start = 0; start <= lastStart; start++) {
            boolean matches = true;
            for (int index = 0; index < needle.length(); index++) {
                if (haystack.data()[haystack.offset() + start + index] != needle.data()[needle.offset() + index]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return true;
            }
        }
        return false;
    }

    /** Precompile a containment needle from raw bytes, for repeated probes (the compiled LIKE '%literal%' mask). */
    public static ContainsNeedle containsNeedle(byte[] needle)
    {
        int firstProbeOffset = 0;
        int secondProbeOffset = needle.length <= 1 ? 0 : selectSecondProbeOffset(needle, 0, needle.length);
        byte firstProbeByte = needle.length == 0 ? 0 : needle[firstProbeOffset];
        byte secondProbeByte = needle.length <= 1 ? firstProbeByte : needle[secondProbeOffset];
        return new ContainsNeedle(needle, 0, needle.length, firstProbeOffset, secondProbeOffset, firstProbeByte, secondProbeByte);
    }

    /** Vectorized containment of a precompiled needle in {@code data[offset, offset+length)}. */
    public static boolean contains(byte[] data, int offset, int length, ContainsNeedle needle)
    {
        if (needle.length() == 0) {
            return true;
        }
        if (length < needle.length()) {
            return false;
        }
        if (needle.length() == 1) {
            return binaryContainsSingleByte(data, offset, length, needle.firstProbeByte());
        }
        return binaryContainsVectorized(
                data,
                offset,
                length,
                needle.data(),
                needle.start(),
                needle.length(),
                needle.firstProbeOffset(),
                needle.secondProbeOffset(),
                needle.firstProbeByte(),
                needle.secondProbeByte());
    }

    /**
     * Sweep containment over a concatenated row buffer: select the rows of {@code [0, rowCount)} whose
     * {@code [offsets[row], offsets[row + 1])} slice contains the needle, writing them to {@code selection}
     * (ascending, deduplicated) and returning the count. One SIMD pass over the whole region -- candidate hits
     * are mapped back to rows through the monotone offsets with a forward-walking pointer, and a textual match
     * spanning a row boundary is rejected by the containment check -- where the per-row form restarts the
     * vector loop (and pays its setup) for every row, most shorter than one vector.
     */
    public static int containsSweep(byte[] data, int[] offsets, int rowCount, ContainsNeedle needle, int[] selection)
    {
        int needleLength = needle.length();
        if (needleLength == 0) {
            for (int row = 0; row < rowCount; row++) {
                selection[row] = row;
            }
            return rowCount;
        }
        int regionStart = offsets[0];
        int candidateCount = offsets[rowCount] - needleLength - regionStart + 1;
        if (candidateCount <= 0) {
            return 0;
        }
        byte[] needleData = needle.data();
        int needleStart = needle.start();
        int firstProbeOffset = needle.firstProbeOffset();
        int secondProbeOffset = needle.secondProbeOffset();
        byte firstProbeByte = needle.firstProbeByte();
        byte secondProbeByte = needle.secondProbeByte();
        int selected = 0;
        int row = 0;
        int fullLength = CONTAINS_SPECIES.loopBound(candidateCount);
        int offset = 0;
        while (offset < fullLength) {
            jdk.incubator.vector.ByteVector firstProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, data, regionStart + offset + firstProbeOffset);
            jdk.incubator.vector.ByteVector secondProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, data, regionStart + offset + secondProbeOffset);
            long candidateBits = firstProbe.eq(firstProbeByte).toLong() & secondProbe.eq(secondProbeByte).toLong();
            while (candidateBits != 0) {
                int lane = Long.numberOfTrailingZeros(candidateBits);
                candidateBits &= candidateBits - 1;
                int position = regionStart + offset + lane;
                if (binaryMatchesAt(data, position, needleData, needleStart, needleLength)) {
                    while (offsets[row + 1] <= position) {
                        row++;
                    }
                    if (position + needleLength <= offsets[row + 1] && (selected == 0 || selection[selected - 1] != row)) {
                        selection[selected++] = row;
                    }
                }
            }
            offset += CONTAINS_SPECIES.length();
        }
        for (int candidate = offset; candidate < candidateCount; candidate++) {
            int position = regionStart + candidate;
            if (data[position + firstProbeOffset] == firstProbeByte &&
                    data[position + secondProbeOffset] == secondProbeByte &&
                    binaryMatchesAt(data, position, needleData, needleStart, needleLength)) {
                while (offsets[row + 1] <= position) {
                    row++;
                }
                if (position + needleLength <= offsets[row + 1] && (selected == 0 || selection[selected - 1] != row)) {
                    selection[selected++] = row;
                }
            }
        }
        return selected;
    }

    private static ContainsNeedle compileContainsNeedle(BinaryVector needleVector, int needlePosition)
    {
        byte[] needleData = needleVector.data();
        int needleStart = needleVector.startOffset(needlePosition);
        int needleLength = needleVector.length(needlePosition);
        int firstProbeOffset = 0;
        int secondProbeOffset = needleLength <= 1 ? 0 : selectSecondProbeOffset(needleData, needleStart, needleLength);
        byte firstProbeByte = needleLength == 0 ? 0 : needleData[needleStart + firstProbeOffset];
        byte secondProbeByte = needleLength <= 1 ? firstProbeByte : needleData[needleStart + secondProbeOffset];
        return new ContainsNeedle(needleData, needleStart, needleLength, firstProbeOffset, secondProbeOffset, firstProbeByte, secondProbeByte);
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

    private static boolean binaryContainsVectorized(
            byte[] haystackData,
            int haystackStart,
            int haystackLength,
            byte[] needleData,
            int needleStart,
            int needleLength,
            int firstProbeOffset,
            int secondProbeOffset,
            byte firstProbeByte,
            byte secondProbeByte)
    {
        int lastStart = haystackLength - needleLength;
        int candidateCount = lastStart + 1;
        int fullLength = CONTAINS_SPECIES.loopBound(candidateCount);

        int offset = 0;
        while (offset < fullLength) {
            if (binaryContainsVectorizedChunk(haystackData, haystackStart, needleData, needleStart, needleLength, firstProbeOffset, secondProbeOffset, firstProbeByte, secondProbeByte, offset)) {
                return true;
            }
            offset += CONTAINS_SPECIES.length();
        }

        for (int candidate = offset; candidate < candidateCount; candidate++) {
            if (haystackData[haystackStart + candidate + firstProbeOffset] == firstProbeByte &&
                    haystackData[haystackStart + candidate + secondProbeOffset] == secondProbeByte &&
                    binaryMatchesAt(haystackData, haystackStart + candidate, needleData, needleStart, needleLength)) {
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
            int offset)
    {
        jdk.incubator.vector.ByteVector firstProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset + firstProbeOffset);
        jdk.incubator.vector.ByteVector secondProbe = jdk.incubator.vector.ByteVector.fromArray(CONTAINS_SPECIES, haystackData, haystackStart + offset + secondProbeOffset);
        long candidateBits = firstProbe.eq(firstProbeByte).toLong() & secondProbe.eq(secondProbeByte).toLong();
        if (candidateBits == 0) {
            return false;
        }
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

    public record ContainsNeedle(
            byte[] data,
            int start,
            int length,
            int firstProbeOffset,
            int secondProbeOffset,
            byte firstProbeByte,
            byte secondProbeByte)
    {
    }
}
