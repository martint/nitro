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
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A one-input LIKE primitive whose constant pattern is bound by the function registry. This avoids materializing
 * and decoding a constant vector at execution time and lets host engines use Nitro's encoding-aware implementation
 * without teaching the evaluator about the host engine's pattern representation.
 */
@ScalarFunction(name = "bound_like_utf8")
public final class BoundLikeUtf8
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("BoundLikeUtf8");
    private final LikeUtf8.Pattern pattern;

    public BoundLikeUtf8(String pattern)
    {
        this(pattern, LikeUtf8Policy.defaults());
    }

    public BoundLikeUtf8(String pattern, LikeUtf8Policy policy)
    {
        this.pattern = LikeUtf8.compilePattern(requireNonNull(pattern, "pattern is null"), requireNonNull(policy, "policy is null"));
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
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(input.getOrNull(Stream.NULLS));
        Vector inputErrorVector = input.getOrNull(Stream.ERRORS);
        VectorAccess.BooleanValues inputErrors = VectorAccess.booleanValues(inputErrorVector);
        int length = inputValues.length();

        BooleanVector values = requestedStreams.contains(Stream.VALUES)
                ? VectorAccess.writableBooleanVector(context.allocator(), allocationContext, output == null ? null : output.getOrNull(Stream.VALUES), length)
                : null;
        BooleanVector nulls = requestedStreams.contains(Stream.NULLS)
                ? VectorAccess.writableBooleanVector(context.allocator(), allocationContext, output == null ? null : output.getOrNull(Stream.NULLS), length)
                : null;
        ErrorVector errors = requestedStreams.contains(Stream.ERRORS)
                ? context.allocator().allocateOrGrow(
                        allocationContext,
                        output != null && output.getOrNull(Stream.ERRORS) instanceof ErrorVector existing ? existing : null,
                        ErrorVector.class,
                        length,
                        ErrorVector::new)
                : null;

        if (values != null) {
            writeMatches(inputValues, mask, inputNulls, inputErrors, values.values());
        }
        for (int position : mask) {
            boolean failed = inputErrors.value(position);
            boolean isNull = inputNulls.value(position);
            if (nulls != null) {
                nulls.values()[position] = isNull;
            }
            if (errors != null) {
                ErrorValue error = failed ? ErrorVectors.errorAt(inputErrorVector, position) : null;
                if (error != null) {
                    errors.setError(position, error);
                }
                else {
                    errors.clearError(position);
                    errors.values()[position] = failed;
                }
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

    private void writeMatches(
            Vector input,
            Mask mask,
            VectorAccess.BooleanValues nulls,
            VectorAccess.BooleanValues errors,
            boolean[] output)
    {
        switch (input) {
            case BinaryVector binary -> {
                for (int position : mask) {
                    if (!nulls.value(position) && !errors.value(position)) {
                        output[position] = pattern.matches(binary.data(), binary.startOffset(position), binary.length(position));
                    }
                }
            }
            case DictionaryVector dictionary when dictionary.values() instanceof BinaryVector entries -> {
                boolean[] entryMatches = new boolean[entries.length()];
                for (int entry = 0; entry < entries.length(); entry++) {
                    entryMatches[entry] = pattern.matches(entries.data(), entries.startOffset(entry), entries.length(entry));
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
                        output[position] = pattern.matches(entries.data(), entries.startOffset(entry), entries.length(entry));
                    }
                }
            }
            default -> throw new IllegalArgumentException("Unsupported bound_like_utf8 input vector: " + input.getClass().getSimpleName());
        }
    }
}
