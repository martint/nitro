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
import org.weakref.nitro.data.ErrorValue;
import org.weakref.nitro.data.ErrorVector;
import org.weakref.nitro.data.ErrorVectors;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.List;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A one-input LIKE primitive whose constant pattern is bound by the function registry. This avoids materializing
 * and decoding a constant vector at execution time and lets host engines use Nitro's encoding-aware implementation
 * without teaching the evaluator about the host engine's pattern representation.
 */
@ScalarFunction(name = "bound_like_utf8")
public final class BoundLikeUtf8
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("BoundLikeUtf8");
    private final LikeUtf8.Pattern pattern;
    private final Utf8BinaryDispatch.ContainsNeedle containsNeedle;

    public BoundLikeUtf8(String pattern)
    {
        this(pattern, LikeUtf8Policy.defaults());
    }

    public BoundLikeUtf8(String pattern, LikeUtf8Policy policy)
    {
        String patternText = requireNonNull(pattern, "pattern is null");
        this.pattern = LikeUtf8.compilePattern(patternText, requireNonNull(policy, "policy is null"));
        this.containsNeedle = compileContainsNeedle(patternText);
    }

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        if (inputIndex != 0) {
            throw new IllegalArgumentException("Unexpected argument index for bound_like_utf8: " + inputIndex);
        }
        return requestedOutputStreams.isEmpty() ? NO_INPUT_STREAMS : ALL_INPUT_STREAMS;
    }

    @Override
    public Set<Stream> requiredMaskInputStreams(int inputIndex)
    {
        if (inputIndex != 0) {
            throw new IllegalArgumentException("Unexpected argument index for bound_like_utf8: " + inputIndex);
        }
        return ALL_INPUT_STREAMS;
    }

    @Override
    public boolean requiresCompletedInputCompanionStreamsForMask()
    {
        return false;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        if (inputs.size() != 1) {
            throw new IllegalArgumentException("Unexpected argument count for bound_like_utf8");
        }
        if (requestedStreams.isEmpty()) {
            return Streams.empty();
        }

        Streams input = inputs.getFirst();
        Vector inputValues = input.values();
        Vector inputNullVector = input.getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(inputNullVector);
        Vector inputErrorVector = input.getOrNull(Stream.ERRORS);
        VectorAccess.BooleanValues inputErrors = VectorAccess.booleanValues(inputErrorVector);
        int length = inputValues.length();

        BooleanVector values = requestedStreams.contains(Stream.VALUES)
                ? VectorAccess.writableBooleanVector(context.allocator(), allocationContext, output == null ? null : output.getOrNull(Stream.VALUES), length)
                : null;
        BooleanVector nulls = requestedStreams.contains(Stream.NULLS)
                ? VectorAccess.writableBooleanVector(context.allocator(), allocationContext, output == null ? null : output.getOrNull(Stream.NULLS), length)
                : null;
        Vector errors = requestedStreams.contains(Stream.ERRORS)
                ? prepareErrors(inputErrorVector, output == null ? null : output.getOrNull(Stream.ERRORS), length, context)
                : null;

        if (values != null) {
            writeMatches(
                    inputValues,
                    mask,
                    inputNulls,
                    inputErrors,
                    VectorAccess.isAllFalseNulls(inputNullVector) && VectorAccess.isAllFalseNulls(inputErrorVector),
                    values.values());
        }
        for (int position : mask) {
            boolean failed = inputErrors.value(position);
            boolean isNull = inputNulls.value(position);
            if (nulls != null) {
                nulls.values()[position] = isNull;
            }
            if (errors instanceof ErrorVector diagnosticErrors) {
                ErrorValue error = failed ? ErrorVectors.errorAt(inputErrorVector, position) : null;
                if (error != null) {
                    diagnosticErrors.setError(position, error);
                }
                else {
                    diagnosticErrors.clearError(position);
                    diagnosticErrors.values()[position] = failed;
                }
            }
            else if (errors instanceof BooleanVector booleanErrors) {
                booleanErrors.values()[position] = failed;
            }
        }

        Streams result = Streams.empty();
        if (values != null) {
            result = result.with(Stream.VALUES, values);
        }
        if (nulls != null) {
            result = result.with(Stream.NULLS, nulls);
        }
        if (errors != null) {
            result = result.with(Stream.ERRORS, errors);
        }
        return result;
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return evaluateMask(inputs, mask, context, true);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return evaluateMask(inputs, mask, context, false);
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryRetainMask(inputs, mask, true);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return tryRetainMask(inputs, mask, false);
    }

    private Mask evaluateMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, boolean selectMatches)
    {
        if (!supportsDirectMask(inputs)) {
            return null;
        }
        Mask result = context.allocator().copyMask(allocationContext, mask);
        if (!tryRetainMask(inputs, result, selectMatches)) {
            context.allocator().release(allocationContext, result);
            return null;
        }
        return result;
    }

    private boolean tryRetainMask(List<Streams> inputs, Mask mask, boolean selectMatches)
    {
        if (inputs.size() != 1) {
            throw new IllegalArgumentException("Unexpected argument count for bound_like_utf8");
        }
        Streams input = inputs.getFirst();
        Vector values = input.values();
        Vector nullVector = input.getOrNull(Stream.NULLS);
        Vector errorVector = input.getOrNull(Stream.ERRORS);
        VectorAccess.BooleanValues nulls = VectorAccess.booleanValues(nullVector);
        VectorAccess.BooleanValues errors = VectorAccess.booleanValues(errorVector);

        switch (values) {
            case DictionaryVector dictionary when dictionary.values() instanceof BinaryVector entries -> {
                boolean[] entryMatches = new boolean[entries.length()];
                for (int entry = 0; entry < entries.length(); entry++) {
                    entryMatches[entry] = matches(entries, entry);
                }
                int[] ids = dictionary.ids();
                if (VectorAccess.isAllFalseNulls(nullVector) && VectorAccess.isAllFalseNulls(errorVector)) {
                    mask.retainDictionaryComparison(ids, entryMatches, null, selectMatches);
                    return true;
                }
                mask.retainIf(position ->
                        !nulls.value(position) &&
                        !errors.value(position) &&
                        entryMatches[ids[position]] == selectMatches);
                return true;
            }
            case RleVector rle when rle.values() instanceof BinaryVector entries -> {
                if (rle.counts().length == 1 &&
                        VectorAccess.isAllFalseNulls(nullVector) &&
                        VectorAccess.isAllFalseNulls(errorVector)) {
                    if (matches(entries, 0) != selectMatches) {
                        mask.clear(mask.size());
                    }
                    return true;
                }
                mask.retainIf(position ->
                        !nulls.value(position) &&
                        !errors.value(position) &&
                        matches(entries, rle.runIndex(position)) == selectMatches);
                return true;
            }
            default -> {
                // Flat LIKE already has a vectorized contains sweep. Materializing that result is cheaper than
                // replacing it with scalar predicate calls merely to avoid the BooleanVector.
                return false;
            }
        }
    }

    private static boolean supportsDirectMask(List<Streams> inputs)
    {
        if (inputs.size() != 1 || !inputs.getFirst().has(Stream.VALUES)) {
            return false;
        }
        return switch (inputs.getFirst().values()) {
            case DictionaryVector dictionary -> dictionary.values() instanceof BinaryVector;
            case RleVector rle -> rle.values() instanceof BinaryVector;
            default -> false;
        };
    }

    private Vector prepareErrors(Vector inputErrors, Vector existing, int length, PrimitiveExecutionContext context)
    {
        boolean hasDiagnostics = ErrorVectors.hasDiagnostics(inputErrors);
        if (!hasDiagnostics && VectorAccess.isAllFalseNulls(inputErrors)) {
            return context.allocator().borrowAllFalseBoolean(allocationContext, length);
        }
        if (hasDiagnostics || existing instanceof ErrorVector) {
            return context.allocator().allocateOrGrow(
                    allocationContext,
                    existing instanceof ErrorVector errors ? errors : null,
                    ErrorVector.class,
                    length,
                    ErrorVector::new);
        }
        return VectorAccess.writableBooleanVector(context.allocator(), allocationContext, existing, length);
    }

    private void writeMatches(
            Vector input,
            Mask mask,
            VectorAccess.BooleanValues nulls,
            VectorAccess.BooleanValues errors,
            boolean clean,
            boolean[] output)
    {
        switch (input) {
            case BinaryVector binary -> {
                if (containsNeedle != null && clean && mask.all() && containsNeedle.length() > 1) {
                    Utf8BinaryDispatch.containsSweep(binary.data(), binary.offsets(), mask.size(), containsNeedle, output);
                    return;
                }
                for (int position : mask) {
                    if (!nulls.value(position) && !errors.value(position)) {
                        output[position] = matches(binary, position);
                    }
                }
            }
            case DictionaryVector dictionary when dictionary.values() instanceof BinaryVector entries -> {
                boolean[] entryMatches = new boolean[entries.length()];
                for (int entry = 0; entry < entries.length(); entry++) {
                    entryMatches[entry] = matches(entries, entry);
                }
                for (int position : mask) {
                    if (!nulls.value(position) && !errors.value(position)) {
                        output[position] = entryMatches[dictionary.ids()[position]];
                    }
                }
            }
            case RleVector rle when rle.values() instanceof BinaryVector entries -> {
                for (int position : mask) {
                    if (!nulls.value(position) && !errors.value(position)) {
                        int entry = rle.runIndex(position);
                        output[position] = matches(entries, entry);
                    }
                }
            }
            default -> throw new IllegalArgumentException("Unsupported bound_like_utf8 input vector: " + input.getClass().getSimpleName());
        }
    }

    private boolean matches(BinaryVector input, int position)
    {
        if (containsNeedle != null) {
            return Utf8BinaryDispatch.contains(input.data(), input.startOffset(position), input.length(position), containsNeedle);
        }
        return pattern.matches(input.data(), input.startOffset(position), input.length(position));
    }

    private static Utf8BinaryDispatch.ContainsNeedle compileContainsNeedle(String pattern)
    {
        if (pattern.length() < 2 || pattern.charAt(0) != '%' || pattern.charAt(pattern.length() - 1) != '%') {
            return null;
        }
        String literal = pattern.substring(1, pattern.length() - 1);
        if (literal.indexOf('%') >= 0) {
            return null;
        }
        return Utf8BinaryDispatch.containsNeedle(literal.getBytes(UTF_8));
    }
}
