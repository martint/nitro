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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class Utf8BinaryDispatch
{
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VectorSpecies<Byte> FIXED_BINARY_BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_PREFERRED;
    private static final jdk.incubator.vector.VectorSpecies<Byte> CONTAINS_SPECIES = jdk.incubator.vector.ByteVector.SPECIES_PREFERRED.length() <= Long.SIZE ? jdk.incubator.vector.ByteVector.SPECIES_PREFERRED : jdk.incubator.vector.ByteVector.SPECIES_512;
    private final Utf8BinaryDispatchPolicy policy;

    public Utf8BinaryDispatch(Utf8BinaryDispatchPolicy policy)
    {
        this.policy = requireNonNull(policy, "policy is null");
    }

    public Streams applyEquals(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.EQUALS, inputs, mask, requestedStreams, output, context);
    }

    public Streams applyLessThan(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.LESS_THAN, inputs, mask, requestedStreams, output, context);
    }

    public Streams applyLessThanOrEqual(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.LESS_THAN_OR_EQUAL, inputs, mask, requestedStreams, output, context);
    }

    public Streams applyStartsWith(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.STARTS_WITH, inputs, mask, requestedStreams, output, context);
    }

    public Streams applyContains(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        return apply(functionName, allocationContext, Operation.CONTAINS, inputs, mask, requestedStreams, output, context);
    }

    public Mask tryEvaluateEqualsTrueMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateEqualsMask(functionName, allocationContext, inputs, mask, context, true);
    }

    public Mask tryEvaluateEqualsFalseMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateEqualsMask(functionName, allocationContext, inputs, mask, context, false);
    }

    public boolean tryEvaluateEqualsTrueMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateEqualsMaskInPlace(functionName, inputs, mask, true);
    }

    public boolean tryEvaluateEqualsFalseMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateEqualsMaskInPlace(functionName, inputs, mask, false);
    }

    public Mask tryEvaluateLessThanTrueMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateComparisonMask(functionName, allocationContext, Operation.LESS_THAN, inputs, mask, context, true);
    }

    public Mask tryEvaluateLessThanFalseMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateComparisonMask(functionName, allocationContext, Operation.LESS_THAN, inputs, mask, context, false);
    }

    public boolean tryEvaluateLessThanTrueMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateComparisonMaskInPlace(functionName, Operation.LESS_THAN, inputs, mask, true);
    }

    public boolean tryEvaluateLessThanFalseMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateComparisonMaskInPlace(functionName, Operation.LESS_THAN, inputs, mask, false);
    }

    public Mask tryEvaluateLessThanOrEqualTrueMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateComparisonMask(functionName, allocationContext, Operation.LESS_THAN_OR_EQUAL, inputs, mask, context, true);
    }

    public Mask tryEvaluateLessThanOrEqualFalseMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateComparisonMask(functionName, allocationContext, Operation.LESS_THAN_OR_EQUAL, inputs, mask, context, false);
    }

    public boolean tryEvaluateLessThanOrEqualTrueMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateComparisonMaskInPlace(functionName, Operation.LESS_THAN_OR_EQUAL, inputs, mask, true);
    }

    public boolean tryEvaluateLessThanOrEqualFalseMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateComparisonMaskInPlace(functionName, Operation.LESS_THAN_OR_EQUAL, inputs, mask, false);
    }

    public Streams applyInSet(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
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

    public Mask tryEvaluateInSetTrueMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateInSetMask(functionName, allocationContext, inputs, mask, context, true);
    }

    public Mask tryEvaluateInSetFalseMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryEvaluateInSetMask(functionName, allocationContext, inputs, mask, context, false);
    }

    public boolean tryEvaluateInSetTrueMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateInSetMaskInPlace(functionName, inputs, mask, true);
    }

    public boolean tryEvaluateInSetFalseMaskInPlace(String functionName, List<Streams> inputs, Mask mask)
    {
        return tryEvaluateInSetMaskInPlace(functionName, inputs, mask, false);
    }

    private Streams apply(String functionName, Allocator.Context allocationContext, Operation operation, List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
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
            // Logical UTF-8 semantics are established by the registry binding. Runtime traits describe narrower
            // value properties, such as ASCII-only content; they are not a second type-admission mechanism.
            requireBinaryVectors(functionName, left, right);
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
                applyValues(
                        functionName,
                        operation,
                        left,
                        right,
                        leftNulls,
                        rightNulls,
                        operation == Operation.CONTAINS && VectorAccess.isAllFalseNulls(inputs.getFirst().getOrNull(Stream.NULLS)),
                        mask,
                        booleanOutputValues);
                outputValues = booleanOutputValues;
            }
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private Mask tryEvaluateInSetMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for %s", functionName);
        Vector left = inputs.getFirst().values();
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.getFirst().getOrNull(Stream.NULLS));

        if (left instanceof DictionaryVector leftDictionary &&
                dictionaryMembershipWorthwhile(leftDictionary, mask.selectedCount()) &&
                allSingleValueRle(functionName, inputs.subList(1, inputs.size()))) {
            return evaluateInSetDictionaryMask(functionName, allocationContext, leftDictionary, leftNulls, inputs.subList(1, inputs.size()), mask, context, selectMatches);
        }
        return null;
    }

    private Mask tryEvaluateEqualsMask(String functionName, Allocator.Context allocationContext, List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for %s", functionName);

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));

        if (right instanceof RleVector rightRle && rightRle.counts().length == 1 && left instanceof DictionaryVector leftDictionary) {
            if (!dictionaryMembershipWorthwhile(leftDictionary, mask.selectedCount())) {
                return evaluateEqualsOversizedDictionarySingleValueMask(
                        functionName, allocationContext, leftDictionary, rightRle, leftNulls, rightNulls, mask, context, selectMatches);
            }
            return evaluateEqualsDictionarySingleValueMask(functionName, allocationContext, leftDictionary, rightRle, leftNulls, rightNulls, mask, context, selectMatches);
        }
        if (left instanceof RleVector leftRle && leftRle.counts().length == 1 && right instanceof DictionaryVector rightDictionary) {
            return evaluateEqualsSingleValueDictionaryMask(functionName, allocationContext, leftRle, rightDictionary, leftNulls, rightNulls, mask, context, selectMatches);
        }
        return null;
    }

    private boolean tryEvaluateEqualsMaskInPlace(String functionName, List<Streams> inputs, Mask mask, boolean selectMatches)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for %s", functionName);

        // Keep the dictionary-vs-literal membership specialization for that common filter shape. Column-to-column
        // equality cannot use it and previously fell through to materializing a BooleanVector before classifying it.
        if (tryEvaluateInSetMaskInPlace(functionName, inputs, mask, selectMatches)) {
            return true;
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        if (!hasBinaryShape(left) || !hasBinaryShape(right)) {
            return false;
        }
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));
        if (policy.flatSingleValueEqualsMask() && retainFlatSingleValueEquals(
                functionName, left, right, leftNulls, rightNulls, mask, selectMatches)) {
            return true;
        }
        if (policy.flattenedDictionaryEquals() && retainFlattenedDictionaryEquals(left, right, leftNulls, rightNulls, mask, selectMatches)) {
            return true;
        }
        mask.retainIf(position ->
                        !isNull(leftNulls, position) &&
                        !isNull(rightNulls, position) &&
                        binaryEquals(functionName, left, position, right, position) == selectMatches);
        return true;
    }

    private Mask tryEvaluateComparisonMask(
            String functionName,
            Allocator.Context allocationContext,
            Operation operation,
            List<Streams> inputs,
            Mask mask,
            PrimitiveExecutionContext context,
            boolean selectMatches)
    {
        Mask result = context.allocator().copyMask(allocationContext, mask);
        if (tryEvaluateComparisonMaskInPlace(functionName, operation, inputs, result, selectMatches)) {
            return result;
        }
        context.allocator().release(allocationContext, result);
        return null;
    }

    private boolean tryEvaluateComparisonMaskInPlace(
            String functionName,
            Operation operation,
            List<Streams> inputs,
            Mask mask,
            boolean selectMatches)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for %s", functionName);
        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        requireBinaryVectors(functionName, left, right);
        Vector leftNullVector = inputs.get(0).getOrNull(Stream.NULLS);
        Vector rightNullVector = inputs.get(1).getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(leftNullVector);
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(rightNullVector);

        if (left instanceof RleVector leftRle && leftRle.counts().length == 1 &&
                leftRle.values() instanceof BinaryVector leftValue &&
                right instanceof RleVector rightRle && rightRle.counts().length == 1 &&
                rightRle.values() instanceof BinaryVector rightValue &&
                VectorAccess.isAllFalseNulls(leftNullVector) &&
                VectorAccess.isAllFalseNulls(rightNullVector)) {
            boolean matches = evaluate(functionName, operation, leftValue, 0, rightValue, 0, null, null, 0);
            if (matches != selectMatches) {
                mask.clear(mask.size());
            }
            return true;
        }

        if (left instanceof DictionaryVector dictionary &&
                dictionary.values() instanceof BinaryVector dictionaryValues &&
                right instanceof RleVector rle && rle.counts().length == 1 &&
                rle.values() instanceof BinaryVector literal) {
            boolean[] matches = new boolean[dictionaryValues.length()];
            for (int entry = 0; entry < matches.length; entry++) {
                matches[entry] = evaluate(functionName, operation, dictionaryValues, entry, literal, 0, null, null, 0);
            }
            int[] ids = dictionary.ids();
            if (VectorAccess.isAllFalseNulls(leftNullVector) && VectorAccess.isAllFalseNulls(rightNullVector)) {
                mask.retainDictionaryComparison(ids, matches, null, selectMatches);
                return true;
            }
            mask.retainIf(position ->
                    !isNull(leftNulls, position) &&
                    !isNull(rightNulls, position) &&
                    matches[ids[position]] == selectMatches);
            return true;
        }
        if (left instanceof RleVector rle && rle.counts().length == 1 &&
                rle.values() instanceof BinaryVector literal &&
                right instanceof DictionaryVector dictionary &&
                dictionary.values() instanceof BinaryVector dictionaryValues) {
            boolean[] matches = new boolean[dictionaryValues.length()];
            for (int entry = 0; entry < matches.length; entry++) {
                matches[entry] = evaluate(functionName, operation, literal, 0, dictionaryValues, entry, null, null, 0);
            }
            int[] ids = dictionary.ids();
            if (VectorAccess.isAllFalseNulls(leftNullVector) && VectorAccess.isAllFalseNulls(rightNullVector)) {
                mask.retainDictionaryComparison(ids, matches, null, selectMatches);
                return true;
            }
            mask.retainIf(position ->
                    !isNull(leftNulls, position) &&
                    !isNull(rightNulls, position) &&
                    matches[ids[position]] == selectMatches);
            return true;
        }

        VectorAccess.BinaryValues leftValues = VectorAccess.binaryValues(left);
        VectorAccess.BinaryValues rightValues = VectorAccess.binaryValues(right);
        mask.retainIf(position ->
                !isNull(leftNulls, position) &&
                !isNull(rightNulls, position) &&
                evaluateGeneric(operation, leftValues, rightValues, leftNulls, rightNulls, position) == selectMatches);
        return true;
    }

    private boolean retainFlatSingleValueEquals(
            String functionName,
            Vector left,
            Vector right,
            VectorAccess.BooleanValues leftNulls,
            VectorAccess.BooleanValues rightNulls,
            Mask mask,
            boolean selectMatches)
    {
        if (left instanceof BinaryVector leftValues && right instanceof RleVector rightRle && rightRle.counts().length == 1) {
            BinaryVector literal = requireBinaryRle(functionName, rightRle);
            mask.retainIf(position ->
                    !isNull(leftNulls, position) &&
                    !isNull(rightNulls, position) &&
                    binaryEquals(leftValues, position, literal, 0) == selectMatches);
            return true;
        }
        if (left instanceof RleVector leftRle && leftRle.counts().length == 1 && right instanceof BinaryVector rightValues) {
            BinaryVector literal = requireBinaryRle(functionName, leftRle);
            mask.retainIf(position ->
                    !isNull(leftNulls, position) &&
                    !isNull(rightNulls, position) &&
                    binaryEquals(literal, 0, rightValues, position) == selectMatches);
            return true;
        }
        return false;
    }

    private boolean retainFlattenedDictionaryEquals(
            Vector left,
            Vector right,
            VectorAccess.BooleanValues leftNulls,
            VectorAccess.BooleanValues rightNulls,
            Mask mask,
            boolean selectMatches)
    {
        if (left instanceof DictionaryVector leftDictionary &&
                leftDictionary.baseValues() instanceof BinaryVector leftValues &&
                right instanceof DictionaryVector rightDictionary &&
                rightDictionary.baseValues() instanceof BinaryVector rightValues) {
            int leftDepth = dictionaryDepth(leftDictionary);
            int rightDepth = dictionaryDepth(rightDictionary);
            mask.retainIf(position ->
                    !isNull(leftNulls, position) &&
                    !isNull(rightNulls, position) &&
                    binaryEquals(leftValues, basePosition(leftDictionary, position, leftDepth), rightValues, basePosition(rightDictionary, position, rightDepth)) == selectMatches);
            return true;
        }
        if (left instanceof DictionaryVector leftDictionary &&
                leftDictionary.baseValues() instanceof BinaryVector leftValues &&
                right instanceof BinaryVector rightValues) {
            int leftDepth = dictionaryDepth(leftDictionary);
            mask.retainIf(position ->
                    !isNull(leftNulls, position) &&
                    !isNull(rightNulls, position) &&
                    binaryEquals(leftValues, basePosition(leftDictionary, position, leftDepth), rightValues, position) == selectMatches);
            return true;
        }
        if (left instanceof BinaryVector leftValues &&
                right instanceof DictionaryVector rightDictionary &&
                rightDictionary.baseValues() instanceof BinaryVector rightValues) {
            int rightDepth = dictionaryDepth(rightDictionary);
            mask.retainIf(position ->
                    !isNull(leftNulls, position) &&
                    !isNull(rightNulls, position) &&
                    binaryEquals(leftValues, position, rightValues, basePosition(rightDictionary, position, rightDepth)) == selectMatches);
            return true;
        }
        return false;
    }

    private int dictionaryDepth(DictionaryVector dictionary)
    {
        return dictionary.dictionaryDepth();
    }

    private int basePosition(DictionaryVector dictionary, int position, int depth)
    {
        if (!policy.directDictionaryPath()) {
            return dictionary.basePosition(position);
        }
        return dictionary.basePosition(position, depth);
    }

    private Mask evaluateEqualsOversizedDictionarySingleValueMask(
            String functionName,
            Allocator.Context allocationContext,
            DictionaryVector left,
            RleVector right,
            VectorAccess.BooleanValues leftNulls,
            VectorAccess.BooleanValues rightNulls,
            Mask mask,
            PrimitiveExecutionContext context,
            boolean selectMatches)
    {
        BinaryVector literal = requireBinaryRle(functionName, right);
        int selectedCount = 0;
        for (int position : mask) {
            if (!isNull(leftNulls, position) && !isNull(rightNulls, position) && binaryEquals(left, position, literal) == selectMatches) {
                selectedCount++;
            }
        }
        int[] positions = new int[selectedCount];
        int outputIndex = 0;
        for (int position : mask) {
            if (!isNull(leftNulls, position) && !isNull(rightNulls, position) && binaryEquals(left, position, literal) == selectMatches) {
                positions[outputIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
    }

    private Vector tryApplySpecializedValues(
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

    private Vector tryApplyInSetSpecializedValues(
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
        if (left instanceof DictionaryVector leftDictionary &&
                dictionaryMembershipWorthwhile(leftDictionary, mask.selectedCount()) &&
                allSingleValueRle(functionName, inputs.subList(1, inputs.size()))) {
            return applyInSetDictionary(functionName, allocationContext, leftDictionary, leftNulls, inputs.subList(1, inputs.size()), mask, existing, outputLength, context);
        }
        return null;
    }

    private void applyValues(String functionName, Operation operation, Vector left, Vector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, boolean leftNullFree, Mask mask, BooleanVector output)
    {
        if (operation == Operation.CONTAINS && right instanceof RleVector rightRle && rightRle.counts().length == 1) {
            if (left instanceof BinaryVector leftValues) {
                applyContainsFlatSingleNeedle(functionName, leftValues, rightRle, leftNulls, rightNulls, leftNullFree, mask, output);
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

    private void applyGeneric(String functionName, Operation operation, Vector left, Vector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        requireBinaryVectors(functionName, left, right);
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

    private boolean evaluateGeneric(Operation operation, VectorAccess.BinaryValues leftValues, VectorAccess.BinaryValues rightValues, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, int position)
    {
        if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
            return false;
        }
        VectorAccess.BinarySlice left = leftValues.value(position);
        VectorAccess.BinarySlice right = rightValues.value(position);
        return switch (operation) {
            case EQUALS -> binarySliceEquals(left, right);
            case LESS_THAN -> binarySliceCompare(left, right) < 0;
            case LESS_THAN_OR_EQUAL -> binarySliceCompare(left, right) <= 0;
            case STARTS_WITH -> binarySliceStartsWith(left, right);
            case CONTAINS -> binarySliceContains(left, right);
        };
    }

    private void applyContainsFlatSingleNeedle(String functionName, BinaryVector left, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, boolean leftNullFree, Mask mask, BooleanVector output)
    {
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireBinaryVectors(functionName, left, right);

        ContainsNeedle needle = compileContainsNeedle(right, 0);
        boolean needleNull = isNull(rightNulls, 0);
        boolean[] outputValues = output.values();
        if (mask.all() && leftNullFree && !needleNull && needle.length() > 1) {
            containsSweep(left.data(), left.offsets(), mask.size(), needle, outputValues);
            return;
        }
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

    private void applyContainsDictionarySingleNeedle(String functionName, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireBinaryVectors(functionName, left, right);

        ContainsNeedle needle = compileContainsNeedle(right, 0);
        boolean needleNull = isNull(rightNulls, 0);
        int[] leftIds = leftDictionary.ids();
        // After an earlier conjunct has made the mask sparse, probing only the surviving dictionary ids is cheaper
        // than scanning every dictionary entry in every batch.  It also avoids the per-invocation membership array.
        // Dense masks retain the dictionary-wide pass so repeated ids amortize the textual search as before.
        if (policy.sparseDictionaryContains() && !mask.all() && mask.selectedCount() < left.length()) {
            boolean[] outputValues = output.values();
            for (int position : mask) {
                outputValues[position] = !needleNull &&
                        !isNull(leftNulls, position) &&
                        binaryContains(left, leftIds[position], needle);
            }
            return;
        }
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

    private Vector applyEqualsDictionarySingleValue(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, Vector existing, int outputLength, PrimitiveExecutionContext context)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireBinaryVectors(functionName, left, right);

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

    private Mask evaluateEqualsDictionarySingleValueMask(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        requireBinaryVectors(functionName, left, right);

        if (isNull(rightNulls, 0)) {
            return context.allocator().allocateEmptyMask(allocationContext, mask.size());
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

    private Vector applyEqualsSingleValueDictionary(String functionName, Allocator.Context allocationContext, RleVector leftRle, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, Vector existing, int outputLength, PrimitiveExecutionContext context)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        requireBinaryVectors(functionName, left, right);

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

    private Mask evaluateEqualsSingleValueDictionaryMask(String functionName, Allocator.Context allocationContext, RleVector leftRle, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        requireBinaryVectors(functionName, left, right);

        if (isNull(leftNulls, 0)) {
            return context.allocator().allocateEmptyMask(allocationContext, mask.size());
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

    private Vector applyInSetDictionary(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, VectorAccess.BooleanValues leftNulls, List<Streams> literalInputs, Mask mask, Vector existing, int outputLength, PrimitiveExecutionContext context)
    {
        if (leftDictionary.values() instanceof DictionaryVector nestedDictionary && nestedDictionary.values() instanceof BinaryVector nestedValues) {
            BinaryVector[] literals = literalVectors(functionName, literalInputs);
            boolean[] nestedMatches = evaluateDictionaryMembership(nestedValues, literals);
            int[] nestedIds = nestedDictionary.ids();
            BooleanVector dictionaryValues = context.allocator().allocate(
                    allocationContext,
                    BooleanVector.class,
                    nestedDictionary.length(),
                    BooleanVector::new);
            boolean[] dictionaryMatches = dictionaryValues.values();
            for (int index = 0; index < nestedDictionary.length(); index++) {
                dictionaryMatches[index] = nestedMatches[nestedIds[index]];
            }
            return context.allocator().allocateDictionary(allocationContext, leftDictionary.ids(), dictionaryValues);
        }

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

    private Mask evaluateInSetDictionaryMask(String functionName, Allocator.Context allocationContext, DictionaryVector leftDictionary, VectorAccess.BooleanValues leftNulls, List<Streams> literalInputs, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        if (leftDictionary.values() instanceof DictionaryVector nestedDictionary && nestedDictionary.values() instanceof BinaryVector nestedValues) {
            BinaryVector[] literals = literalVectors(functionName, literalInputs);
            boolean[] nestedMatches = evaluateDictionaryMembership(nestedValues, literals);

            int[] ids = leftDictionary.ids();
            int[] nestedIds = nestedDictionary.ids();
            int selectedCount = 0;
            for (int position : mask) {
                if (!isNull(leftNulls, position) && nestedMatches[nestedIds[ids[position]]] == selectMatches) {
                    selectedCount++;
                }
            }

            int[] positions = new int[selectedCount];
            int outputIndex = 0;
            for (int position : mask) {
                if (!isNull(leftNulls, position) && nestedMatches[nestedIds[ids[position]]] == selectMatches) {
                    positions[outputIndex++] = position;
                }
            }
            return context.allocator().allocateSparseMask(allocationContext, positions, outputIndex, mask.size());
        }

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

    private boolean tryEvaluateInSetMaskInPlace(String functionName, List<Streams> inputs, Mask mask, boolean selectMatches)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for %s", functionName);
        Vector leftValues = inputs.getFirst().values();
        Vector leftNullVector = inputs.getFirst().getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(leftNullVector);

        if (leftValues instanceof BinaryVector left &&
                allSingleValueRle(functionName, inputs.subList(1, inputs.size())) &&
                tryRetainFlatPackedShortInSet(
                        left,
                        literalVectors(functionName, inputs.subList(1, inputs.size())),
                        leftNulls,
                        VectorAccess.isAllFalseNulls(leftNullVector),
                        mask,
                        selectMatches)) {
            return true;
        }

        if (!(leftValues instanceof DictionaryVector leftDictionary) ||
                !dictionaryMembershipWorthwhile(leftDictionary, mask.selectedCount()) ||
                !allSingleValueRle(functionName, inputs.subList(1, inputs.size()))) {
            return false;
        }

        BinaryVector[] literals = literalVectors(functionName, inputs.subList(1, inputs.size()));
        if (leftDictionary.values() instanceof DictionaryVector nestedDictionary &&
                nestedDictionary.values() instanceof BinaryVector nestedValues) {
            boolean[] nestedMatches = evaluateDictionaryMembership(nestedValues, literals);
            int[] ids = leftDictionary.ids();
            int[] nestedIds = nestedDictionary.ids();
            mask.retainIf(position ->
                    !leftNulls.value(position) && nestedMatches[nestedIds[ids[position]]] == selectMatches);
            return true;
        }

        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        boolean[] dictionaryMatches = evaluateDictionaryMembership(left, literals);
        int[] ids = leftDictionary.ids();
        int singleMatchingId = policy.directSingleDictionaryMatch() ? singleMatchingId(dictionaryMatches) : -1;
        if (singleMatchingId >= 0 && policy.monomorphicDictionaryMask() && VectorAccess.isAllFalseNulls(leftNullVector)) {
            mask.retainDictionaryIdComparison(ids, singleMatchingId, null, selectMatches);
        }
        else if (singleMatchingId >= 0 && policy.monomorphicDictionaryMask() && leftNullVector instanceof BooleanVector booleanNulls) {
            mask.retainDictionaryIdComparison(ids, singleMatchingId, booleanNulls.values(), selectMatches);
        }
        else if (policy.monomorphicDictionaryMask() && VectorAccess.isAllFalseNulls(leftNullVector)) {
            mask.retainDictionaryComparison(ids, dictionaryMatches, null, selectMatches);
        }
        else if (policy.monomorphicDictionaryMask() && leftNullVector instanceof BooleanVector booleanNulls) {
            mask.retainDictionaryComparison(ids, dictionaryMatches, booleanNulls.values(), selectMatches);
        }
        else {
            mask.retainIf(position -> !leftNulls.value(position) && dictionaryMatches[ids[position]] == selectMatches);
        }
        return true;
    }

    private int singleMatchingId(boolean[] matches)
    {
        int matchingId = -1;
        for (int index = 0; index < matches.length; index++) {
            if (!matches[index]) {
                continue;
            }
            if (matchingId >= 0) {
                return -1;
            }
            matchingId = index;
        }
        return matchingId;
    }

    private boolean tryRetainFlatPackedShortInSet(
            BinaryVector left,
            BinaryVector[] literals,
            VectorAccess.BooleanValues leftNulls,
            boolean leftNullFree,
            Mask mask,
            boolean selectMatches)
    {
        int literalLength = commonPackedLiteralLength(literals);
        if (literalLength < 0) {
            return false;
        }

        long literal0 = packedLiteral(literals, 0);
        long literal1 = packedLiteral(literals, 1);
        long literal2 = packedLiteral(literals, 2);
        long literal3 = packedLiteral(literals, 3);
        long literal4 = packedLiteral(literals, 4);
        long literal5 = packedLiteral(literals, 5);
        long literal6 = packedLiteral(literals, 6);
        long literal7 = packedLiteral(literals, 7);
        byte[] data = left.data();
        if (leftNullFree &&
                mask.all() &&
                literalLength == Short.BYTES &&
                left.fixedWidth().orElse(-1) == Short.BYTES) {
            retainDenseFixedShortInSet(
                    left,
                    mask,
                    selectMatches,
                    literals.length,
                    (short) literal0,
                    (short) literal1,
                    (short) literal2,
                    (short) literal3,
                    (short) literal4,
                    (short) literal5,
                    (short) literal6,
                    (short) literal7);
            return true;
        }
        boolean dense = mask.all();
        int iterations = dense ? mask.size() : mask.selectedCount();
        int[] positions = dense ? mask.positionsArrayForOverwrite(iterations) : mask.selectedPositions();
        int retained = 0;
        if (leftNullFree) {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : positions[index];
                if (packedMatchesAt(
                        left,
                        data,
                        position,
                        literalLength,
                        literal0,
                        literal1,
                        literal2,
                        literal3,
                        literal4,
                        literal5,
                        literal6,
                        literal7) == selectMatches) {
                    positions[retained++] = position;
                }
            }
        }
        else {
            for (int index = 0; index < iterations; index++) {
                int position = dense ? index : positions[index];
                if (!leftNulls.value(position) &&
                        packedMatchesAt(
                                left,
                                data,
                                position,
                                literalLength,
                                literal0,
                                literal1,
                                literal2,
                                literal3,
                                literal4,
                                literal5,
                                literal6,
                                literal7) == selectMatches) {
                    positions[retained++] = position;
                }
            }
        }
        mask.finishRetain(retained);
        return true;
    }

    private void retainDenseFixedShortInSet(
            BinaryVector values,
            Mask mask,
            boolean selectMatches,
            int literalCount,
            short literal0,
            short literal1,
            short literal2,
            short literal3,
            short literal4,
            short literal5,
            short literal6,
            short literal7)
    {
        int size = mask.size();
        int[] positions = mask.positionsArrayForOverwrite(size);
        byte[] data = values.data();
        int byteOffset = values.startOffset(0);
        int retained = 0;
        int position = 0;
        int vectorLimit = SHORT_SPECIES.loopBound(size);
        long laneMask = SHORT_SPECIES.length() == Long.SIZE ? -1L : (1L << SHORT_SPECIES.length()) - 1;
        for (; position < vectorLimit; position += SHORT_SPECIES.length()) {
            ShortVector vector = ByteVector.fromArray(FIXED_BINARY_BYTE_SPECIES, data, byteOffset + position * Short.BYTES).reinterpretAsShorts();
            long matches = packedShortMatches(
                    vector,
                    literalCount,
                    literal0,
                    literal1,
                    literal2,
                    literal3,
                    literal4,
                    literal5,
                    literal6,
                    literal7).toLong();
            if (!selectMatches) {
                matches = ~matches & laneMask;
            }
            while (matches != 0) {
                int lane = Long.numberOfTrailingZeros(matches);
                positions[retained++] = position + lane;
                matches &= matches - 1;
            }
        }
        for (; position < size; position++) {
            long value = packShort(data, byteOffset + position * Short.BYTES, Short.BYTES);
            if (packedShortScalarMatches(
                    value,
                    literalCount,
                    literal0,
                    literal1,
                    literal2,
                    literal3,
                    literal4,
                    literal5,
                    literal6,
                    literal7) == selectMatches) {
                positions[retained++] = position;
            }
        }
        mask.finishRetain(retained);
    }

    private boolean packedShortScalarMatches(
            long value,
            int literalCount,
            short literal0,
            short literal1,
            short literal2,
            short literal3,
            short literal4,
            short literal5,
            short literal6,
            short literal7)
    {
        return value == (literal0 & 0xFFFFL) ||
                (literalCount > 1 && value == (literal1 & 0xFFFFL)) ||
                (literalCount > 2 && value == (literal2 & 0xFFFFL)) ||
                (literalCount > 3 && value == (literal3 & 0xFFFFL)) ||
                (literalCount > 4 && value == (literal4 & 0xFFFFL)) ||
                (literalCount > 5 && value == (literal5 & 0xFFFFL)) ||
                (literalCount > 6 && value == (literal6 & 0xFFFFL)) ||
                (literalCount > 7 && value == (literal7 & 0xFFFFL));
    }

    private VectorMask<Short> packedShortMatches(
            ShortVector values,
            int literalCount,
            short literal0,
            short literal1,
            short literal2,
            short literal3,
            short literal4,
            short literal5,
            short literal6,
            short literal7)
    {
        VectorMask<Short> matches = values.eq(nativeShort(literal0));
        if (literalCount > 1) {
            matches = matches.or(values.eq(nativeShort(literal1)));
        }
        if (literalCount > 2) {
            matches = matches.or(values.eq(nativeShort(literal2)));
        }
        if (literalCount > 3) {
            matches = matches.or(values.eq(nativeShort(literal3)));
        }
        if (literalCount > 4) {
            matches = matches.or(values.eq(nativeShort(literal4)));
        }
        if (literalCount > 5) {
            matches = matches.or(values.eq(nativeShort(literal5)));
        }
        if (literalCount > 6) {
            matches = matches.or(values.eq(nativeShort(literal6)));
        }
        if (literalCount > 7) {
            matches = matches.or(values.eq(nativeShort(literal7)));
        }
        return matches;
    }

    private short nativeShort(short littleEndian)
    {
        return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? littleEndian : Short.reverseBytes(littleEndian);
    }

    private boolean packedMatchesAt(
            BinaryVector values,
            byte[] data,
            int position,
            int literalLength,
            long literal0,
            long literal1,
            long literal2,
            long literal3,
            long literal4,
            long literal5,
            long literal6,
            long literal7)
    {
        return values.length(position) == literalLength &&
                packedMatches(
                        packShort(data, values.startOffset(position), literalLength),
                        literal0,
                        literal1,
                        literal2,
                        literal3,
                        literal4,
                        literal5,
                        literal6,
                        literal7);
    }

    private void applyInSetValues(String functionName, List<Streams> inputs, VectorAccess.BooleanValues leftNulls, Mask mask, BooleanVector output)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for %s", functionName);
        Vector leftValues = inputs.getFirst().values();
        BinaryVector[] literals = literalVectors(functionName, inputs.subList(1, inputs.size()));
        boolean[] outputValues = output.values();

        switch (leftValues) {
            case BinaryVector left -> {
                if (tryApplyFlatPackedShortInSet(left, literals, leftNulls, mask, outputValues)) {
                    return;
                }
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
                if (!dictionaryMembershipWorthwhile(leftDictionary, mask.selectedCount())) {
                    for (int position : mask) {
                        outputValues[position] = !isNull(leftNulls, position) && matchesAny(leftDictionary, position, literals);
                    }
                    return;
                }
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

    /**
     * Compares a flat vector with a small same-width set using one packed load per row. Seven bytes is the largest
     * admitted width so {@link Long#MIN_VALUE} is an impossible packed value and can safely fill unused literal
     * lanes. The decision is made once per batch; the row loop contains no literal-count or representation dispatch.
     */
    private boolean tryApplyFlatPackedShortInSet(
            BinaryVector left,
            BinaryVector[] literals,
            VectorAccess.BooleanValues leftNulls,
            Mask mask,
            boolean[] outputValues)
    {
        int literalLength = commonPackedLiteralLength(literals);
        if (literalLength < 0) {
            return false;
        }

        long literal0 = packedLiteral(literals, 0);
        long literal1 = packedLiteral(literals, 1);
        long literal2 = packedLiteral(literals, 2);
        long literal3 = packedLiteral(literals, 3);
        long literal4 = packedLiteral(literals, 4);
        long literal5 = packedLiteral(literals, 5);
        long literal6 = packedLiteral(literals, 6);
        long literal7 = packedLiteral(literals, 7);
        byte[] data = left.data();

        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = !isNull(leftNulls, position) &&
                        left.length(position) == literalLength &&
                        packedMatches(
                                packShort(data, left.startOffset(position), literalLength),
                                literal0,
                                literal1,
                                literal2,
                                literal3,
                                literal4,
                                literal5,
                                literal6,
                                literal7);
            }
            return true;
        }
        for (int position : mask) {
            outputValues[position] = !isNull(leftNulls, position) &&
                    left.length(position) == literalLength &&
                    packedMatches(
                            packShort(data, left.startOffset(position), literalLength),
                            literal0,
                            literal1,
                            literal2,
                            literal3,
                            literal4,
                            literal5,
                            literal6,
                            literal7);
        }
        return true;
    }

    private int commonPackedLiteralLength(BinaryVector[] literals)
    {
        if (literals.length == 0 || literals.length > 8) {
            return -1;
        }
        int length = literals[0].length(0);
        if (length > 7) {
            return -1;
        }
        for (int index = 1; index < literals.length; index++) {
            if (literals[index].length(0) != length) {
                return -1;
            }
        }
        return length;
    }

    private long packedLiteral(BinaryVector[] literals, int index)
    {
        if (index >= literals.length) {
            return Long.MIN_VALUE;
        }
        BinaryVector literal = literals[index];
        return packShort(literal.data(), literal.startOffset(0), literal.length(0));
    }

    private boolean packedMatches(
            long value,
            long literal0,
            long literal1,
            long literal2,
            long literal3,
            long literal4,
            long literal5,
            long literal6,
            long literal7)
    {
        // Deliberately use non-short-circuit OR so C2 emits comparisons and combines their flags without one
        // unpredictable branch per literal.
        return (value == literal0) |
                (value == literal1) |
                (value == literal2) |
                (value == literal3) |
                (value == literal4) |
                (value == literal5) |
                (value == literal6) |
                (value == literal7);
    }

    private boolean[] evaluateDictionaryMembership(BinaryVector dictionary, BinaryVector[] literals)
    {
        boolean[] dictionaryMatches = new boolean[dictionary.length()];
        // Fast path: when every IN literal is at most 8 bytes (zip codes, state codes, and most short-string sets),
        // pack each literal into a little-endian long and test each dictionary entry with a length check plus a few
        // long compares instead of a per-literal byte loop. This turns the per-entry cost from O(literals x length)
        // byte compares into a handful of long compares -- decisive on a large dictionary re-tested every batch
        // (TPC-DS q08's ca_zip IN filter is ~65% of the query: a ~3600-entry dictionary times ~150 batches).
        if (allShort(literals)) {
            int literalCount = literals.length;
            long[] packedLiterals = new long[literalCount];
            int[] literalLengths = new int[literalCount];
            for (int index = 0; index < literalCount; index++) {
                int length = literals[index].length(0);
                literalLengths[index] = length;
                packedLiterals[index] = packShort(literals[index].data(), literals[index].startOffset(0), length);
            }
            byte[] data = dictionary.data();
            for (int index = 0; index < dictionaryMatches.length; index++) {
                int length = dictionary.length(index);
                if (length <= 8) {
                    long value = packShort(data, dictionary.startOffset(index), length);
                    for (int literal = 0; literal < literalCount; literal++) {
                        if (packedLiterals[literal] == value && literalLengths[literal] == length) {
                            dictionaryMatches[index] = true;
                            break;
                        }
                    }
                }
            }
            return dictionaryMatches;
        }
        for (int index = 0; index < dictionaryMatches.length; index++) {
            dictionaryMatches[index] = matchesAny(dictionary, index, literals);
        }
        return dictionaryMatches;
    }

    private boolean dictionaryMembershipWorthwhile(DictionaryVector dictionary, int activeRows)
    {
        Vector values = dictionary.values();
        while (values instanceof DictionaryVector nested) {
            values = nested.values();
        }
        return values instanceof BinaryVector binary && binary.length() <= activeRows;
    }

    private boolean matchesAny(DictionaryVector dictionary, int position, BinaryVector[] literals)
    {
        Vector values = dictionary.values();
        int dictionaryPosition = dictionary.ids()[position];
        while (values instanceof DictionaryVector nested) {
            dictionaryPosition = nested.ids()[dictionaryPosition];
            values = nested.values();
        }
        return matchesAny((BinaryVector) values, dictionaryPosition, literals);
    }

    private boolean binaryEquals(DictionaryVector dictionary, int position, BinaryVector literal)
    {
        Vector values = dictionary.values();
        int dictionaryPosition = dictionary.ids()[position];
        while (values instanceof DictionaryVector nested) {
            dictionaryPosition = nested.ids()[dictionaryPosition];
            values = nested.values();
        }
        return binaryEquals((BinaryVector) values, dictionaryPosition, literal, 0);
    }

    private boolean allShort(BinaryVector[] literals)
    {
        for (BinaryVector literal : literals) {
            if (literal.length(0) > 8) {
                return false;
            }
        }
        return true;
    }

    /** Pack up to 8 bytes into a little-endian long (low byte first); the caller compares length separately. */
    private long packShort(byte[] data, int start, int length)
    {
        long value = 0;
        for (int index = 0; index < length; index++) {
            value |= (data[start + index] & 0xFFL) << (8 * index);
        }
        return value;
    }

    private boolean matchesAny(BinaryVector values, int position, BinaryVector[] literals)
    {
        for (BinaryVector literal : literals) {
            if (binaryEquals(values, position, literal, 0)) {
                return true;
            }
        }
        return false;
    }

    private boolean allSingleValueRle(String functionName, List<Streams> literalInputs)
    {
        for (Streams input : literalInputs) {
            if (!(input.values() instanceof RleVector rle) || rle.counts().length != 1) {
                return false;
            }
            requireBinaryRle(functionName, rle);
        }
        return true;
    }

    private BinaryVector[] literalVectors(String functionName, List<Streams> literalInputs)
    {
        BinaryVector[] literals = new BinaryVector[literalInputs.size()];
        for (int index = 0; index < literalInputs.size(); index++) {
            RleVector rle = (RleVector) literalInputs.get(index).values();
            literals[index] = requireBinaryRle(functionName, rle);
            requireBinaryVectors(functionName, literals[index], literals[index]);
        }
        return literals;
    }

    private void applyFlatFlat(String functionName, Operation operation, BinaryVector left, BinaryVector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, position, right, position, leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, position, right, position, leftNulls, rightNulls, position);
        }
    }

    private void applyFlatDictionary(String functionName, Operation operation, BinaryVector left, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        int[] rightIds = rightDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, position, right, rightIds[position], leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, position, right, rightIds[position], leftNulls, rightNulls, position);
        }
    }

    private void applyDictionaryFlat(String functionName, Operation operation, DictionaryVector leftDictionary, BinaryVector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        int[] leftIds = leftDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, position, leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, position, leftNulls, rightNulls, position);
        }
    }

    private void applyDictionaryDictionary(String functionName, Operation operation, DictionaryVector leftDictionary, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
        int[] leftIds = leftDictionary.ids();
        int[] rightIds = rightDictionary.ids();
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, rightIds[position], leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, rightIds[position], leftNulls, rightNulls, position);
        }
    }

    private void applyFlatRle(String functionName, Operation operation, BinaryVector left, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector right = requireBinaryRle(functionName, rightRle);
        boolean[] outputValues = output.values();
        int runIndex = 0;
        int runEnd = rightRle.counts()[0];
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                while (position >= runEnd) {
                    runIndex++;
                    runEnd += rightRle.counts()[runIndex];
                }
                outputValues[position] = evaluate(functionName, operation, left, position, right, runIndex, leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += rightRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, position, right, runIndex, leftNulls, rightNulls, position);
        }
    }

    private void applyDictionaryRle(String functionName, Operation operation, DictionaryVector leftDictionary, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryDictionary(functionName, leftDictionary);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
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
                outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, runIndex, leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += rightRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, leftIds[position], right, runIndex, leftNulls, rightNulls, position);
        }
    }

    private void applyRleFlat(String functionName, Operation operation, RleVector leftRle, BinaryVector right, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        boolean[] outputValues = output.values();
        int runIndex = 0;
        int runEnd = leftRle.counts()[0];
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                while (position >= runEnd) {
                    runIndex++;
                    runEnd += leftRle.counts()[runIndex];
                }
                outputValues[position] = evaluate(functionName, operation, left, runIndex, right, position, leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += leftRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, runIndex, right, position, leftNulls, rightNulls, position);
        }
    }

    private void applyRleDictionary(String functionName, Operation operation, RleVector leftRle, DictionaryVector rightDictionary, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryDictionary(functionName, rightDictionary);
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
                outputValues[position] = evaluate(functionName, operation, left, runIndex, right, rightIds[position], leftNulls, rightNulls, position);
            }
            return;
        }
        for (int position : mask) {
            while (position >= runEnd) {
                runIndex++;
                runEnd += leftRle.counts()[runIndex];
            }
            outputValues[position] = evaluate(functionName, operation, left, runIndex, right, rightIds[position], leftNulls, rightNulls, position);
        }
    }

    private void applyRleRle(String functionName, Operation operation, RleVector leftRle, RleVector rightRle, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
    {
        BinaryVector left = requireBinaryRle(functionName, leftRle);
        BinaryVector right = requireBinaryRle(functionName, rightRle);
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
                outputValues[position] = evaluate(functionName, operation, left, leftRunIndex, right, rightRunIndex, leftNulls, rightNulls, position);
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
            outputValues[position] = evaluate(functionName, operation, left, leftRunIndex, right, rightRunIndex, leftNulls, rightNulls, position);
        }
    }

    private boolean evaluate(String functionName, Operation operation, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition, VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, int nullPosition)
    {
        if (isNull(leftNulls, nullPosition) || isNull(rightNulls, nullPosition)) {
            return false;
        }
        return switch (operation) {
            case EQUALS -> compareEquals(functionName, left, leftPosition, right, rightPosition);
            case LESS_THAN -> compareLessThan(functionName, left, leftPosition, right, rightPosition);
            case LESS_THAN_OR_EQUAL -> compareLessThanOrEqual(functionName, left, leftPosition, right, rightPosition);
            case STARTS_WITH -> compareStartsWith(functionName, left, leftPosition, right, rightPosition);
            case CONTAINS -> compareContains(functionName, left, leftPosition, right, rightPosition);
        };
    }

    // Logical type admission happens when the registry binds this implementation; row kernels only inspect bytes.
    private boolean compareEquals(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        return binaryEquals(left, leftPosition, right, rightPosition);
    }

    private boolean compareLessThan(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        return binaryCompare(left, leftPosition, right, rightPosition) < 0;
    }

    private boolean compareLessThanOrEqual(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        return binaryCompare(left, leftPosition, right, rightPosition) <= 0;
    }

    private boolean compareStartsWith(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        return binaryStartsWith(left, leftPosition, right, rightPosition);
    }

    private boolean compareContains(String functionName, BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        return binaryContains(left, leftPosition, right, rightPosition);
    }

    private void applyNulls(VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls, Mask mask, BooleanVector output)
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

    private BinaryVector requireBinaryDictionary(String functionName, DictionaryVector vector)
    {
        return requireBinaryValues(functionName, vector.values());
    }

    private BinaryVector requireBinaryRle(String functionName, RleVector vector)
    {
        return requireBinaryValues(functionName, vector.values());
    }

    private BinaryVector requireBinaryValues(String functionName, Vector vector)
    {
        return switch (vector) {
            case BinaryVector values -> values;
            case DictionaryVector values -> materializeBinaryValues(functionName, values);
            case RleVector values -> materializeBinaryValues(functionName, values);
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
        };
    }

    private BinaryVector materializeBinaryValues(String functionName, Vector vector)
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

    private int binaryLength(String functionName, Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector values -> values.length(position);
            case DictionaryVector values -> binaryLength(functionName, values.values(), values.ids()[position]);
            case RleVector values -> binaryLength(functionName, values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
        };
    }

    private void copyBinaryBytes(String functionName, Vector source, int sourcePosition, BinaryVector target, int targetPosition)
    {
        switch (source) {
            case BinaryVector values -> target.setBytes(targetPosition, values.data(), values.startOffset(sourcePosition), values.length(sourcePosition));
            case DictionaryVector values -> copyBinaryBytes(functionName, values.values(), values.ids()[sourcePosition], target, targetPosition);
            case RleVector values -> copyBinaryBytes(functionName, values.values(), values.runIndex(sourcePosition), target, targetPosition);
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
        }
    }

    private void copyBinaryTraits(Vector source, BinaryVector target)
    {
        switch (source) {
            case BinaryVector values -> {
                if (values.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID)) {
                    target.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_VALID);
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

    private void requireBinaryVectors(String functionName, Vector left, Vector right)
    {
        checkArgument(hasBinaryShape(left) && hasBinaryShape(right), "%s requires binary-backed inputs", functionName);
    }

    private void requireBinaryVectors(String functionName, BinaryVector left, BinaryVector right)
    {
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
    }

    private boolean hasBinaryShape(Vector vector)
    {
        return switch (vector) {
            case BinaryVector values -> hasBinaryShape(values);
            case DictionaryVector values -> hasBinaryShape(values.values());
            case RleVector values -> hasBinaryShape(values.values());
            default -> false;
        };
    }

    private boolean hasBinaryShape(BinaryVector vector)
    {
        return true;
    }

    private boolean isNull(VectorAccess.BooleanValues nulls, int position)
    {
        return nulls != null && nulls.value(position);
    }

    private BooleanVector writableBooleanOutput(Allocator.Context allocationContext, PrimitiveExecutionContext context, Vector existing, int outputLength)
    {
        return VectorAccess.writableBooleanVector(context.allocator(), allocationContext, existing, outputLength);
    }

    private boolean binaryEquals(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int leftLength = left.length(leftPosition);
        if (leftLength != right.length(rightPosition)) {
            return false;
        }
        byte[] leftData = left.data();
        byte[] rightData = right.data();
        int leftStart = left.startOffset(leftPosition);
        int rightStart = right.startOffset(rightPosition);
        if (policy.wordEquals()) {
            if (leftLength >= 16) {
                return Arrays.mismatch(leftData, leftStart, leftStart + leftLength, rightData, rightStart, rightStart + leftLength) == -1;
            }
            int index = 0;
            while (index + Long.BYTES <= leftLength) {
                if ((long) LONG_HANDLE.get(leftData, leftStart + index) != (long) LONG_HANDLE.get(rightData, rightStart + index)) {
                    return false;
                }
                index += Long.BYTES;
            }
            int remaining = leftLength - index;
            if (remaining == 0) {
                return true;
            }
            if (leftStart + index + Long.BYTES <= leftData.length && rightStart + index + Long.BYTES <= rightData.length) {
                long mask = (1L << (remaining << 3)) - 1;
                return (((long) LONG_HANDLE.get(leftData, leftStart + index) ^
                        (long) LONG_HANDLE.get(rightData, rightStart + index)) & mask) == 0;
            }
            for (; index < leftLength; index++) {
                if (leftData[leftStart + index] != rightData[rightStart + index]) {
                    return false;
                }
            }
            return true;
        }
        for (int index = 0; index < leftLength; index++) {
            if (leftData[leftStart + index] != rightData[rightStart + index]) {
                return false;
            }
        }
        return true;
    }

    private boolean binaryEquals(String functionName, Vector left, int leftPosition, Vector right, int rightPosition)
    {
        return switch (left) {
            case BinaryVector leftValues -> switch (right) {
                case BinaryVector rightValues -> binaryEquals(leftValues, leftPosition, rightValues, rightPosition);
                case DictionaryVector rightValues -> binaryEquals(functionName, leftValues, leftPosition, rightValues.values(), rightValues.ids()[rightPosition]);
                case RleVector rightValues -> binaryEquals(functionName, leftValues, leftPosition, rightValues.values(), rightValues.runIndex(rightPosition));
                default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
            };
            case DictionaryVector leftValues -> binaryEquals(functionName, leftValues.values(), leftValues.ids()[leftPosition], right, rightPosition);
            case RleVector leftValues -> binaryEquals(functionName, leftValues.values(), leftValues.runIndex(leftPosition), right, rightPosition);
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector-compatible UTF-8 inputs");
        };
    }

    private int binaryCompare(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int leftLength = left.length(leftPosition);
        int rightLength = right.length(rightPosition);
        int compareLength = Math.min(leftLength, rightLength);
        byte[] leftData = left.data();
        byte[] rightData = right.data();
        int leftStart = left.startOffset(leftPosition);
        int rightStart = right.startOffset(rightPosition);
        for (int index = 0; index < compareLength; index++) {
            int comparison = Byte.compareUnsigned(leftData[leftStart + index], rightData[rightStart + index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    private boolean binaryStartsWith(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
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

    private boolean binaryContains(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        return binaryContains(left, leftPosition, compileContainsNeedle(right, rightPosition));
    }

    private boolean binaryContains(BinaryVector haystack, int haystackPosition, ContainsNeedle needle)
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

    private boolean binarySliceEquals(VectorAccess.BinarySlice left, VectorAccess.BinarySlice right)
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

    private int binarySliceCompare(VectorAccess.BinarySlice left, VectorAccess.BinarySlice right)
    {
        int compareLength = Math.min(left.length(), right.length());
        for (int index = 0; index < compareLength; index++) {
            int comparison = Byte.compareUnsigned(left.data()[left.offset() + index], right.data()[right.offset() + index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length(), right.length());
    }

    private boolean binarySliceStartsWith(VectorAccess.BinarySlice left, VectorAccess.BinarySlice right)
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

    private boolean binarySliceContains(VectorAccess.BinarySlice haystack, VectorAccess.BinarySlice needle)
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

    /** Dense boolean-output form of {@link #containsSweep(byte[], int[], int, ContainsNeedle, int[])}. */
    public static void containsSweep(byte[] data, int[] offsets, int rowCount, ContainsNeedle needle, boolean[] matches)
    {
        Arrays.fill(matches, 0, rowCount, false);
        int needleLength = needle.length();
        if (needleLength == 0) {
            Arrays.fill(matches, 0, rowCount, true);
            return;
        }
        int regionStart = offsets[0];
        int candidateCount = offsets[rowCount] - needleLength - regionStart + 1;
        if (candidateCount <= 0) {
            return;
        }
        byte[] needleData = needle.data();
        int needleStart = needle.start();
        int firstProbeOffset = needle.firstProbeOffset();
        int secondProbeOffset = needle.secondProbeOffset();
        byte firstProbeByte = needle.firstProbeByte();
        byte secondProbeByte = needle.secondProbeByte();
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
                    if (position + needleLength <= offsets[row + 1]) {
                        matches[row] = true;
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
                if (position + needleLength <= offsets[row + 1]) {
                    matches[row] = true;
                }
            }
        }
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
        LESS_THAN_OR_EQUAL,
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
