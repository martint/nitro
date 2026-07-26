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
package org.weakref.nitro.function.scalar;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.util.List;
import java.util.Set;

/**
 * Primitive execution contract for normalized evaluator calls.
 * <p>
 * Implementations may reuse the supplied output streams and must preserve
 * positions outside the requested mask when doing so.
 */
@FunctionalInterface
public interface PrimitiveFunction
{
    Set<Stream> NO_INPUT_STREAMS = Set.of();
    Set<Stream> VALUES_INPUT_STREAMS = Set.of(Stream.VALUES);
    Set<Stream> NULLS_INPUT_STREAMS = Set.of(Stream.NULLS);
    Set<Stream> ERRORS_INPUT_STREAMS = Set.of(Stream.ERRORS);
    Set<Stream> VALUES_AND_NULLS_INPUT_STREAMS = Set.of(Stream.VALUES, Stream.NULLS);
    Set<Stream> VALUES_AND_ERRORS_INPUT_STREAMS = Set.of(Stream.VALUES, Stream.ERRORS);
    Set<Stream> NULLS_AND_ERRORS_INPUT_STREAMS = Set.of(Stream.NULLS, Stream.ERRORS);
    Set<Stream> ALL_INPUT_STREAMS = Set.of(Stream.VALUES, Stream.NULLS, Stream.ERRORS);

    Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context);

    default Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        // Functions commonly derive their nulls and errors streams from the input values (e.g. divide-by-zero
        // detection), so request the input values whenever any output stream is requested. Functions that can
        // produce a requested output without the input values should override this to narrow the request.
        return requestedOutputStreams.isEmpty() ? NO_INPUT_STREAMS : VALUES_INPUT_STREAMS;
    }

    default Set<Stream> requiredMaskInputStreams(int inputIndex)
    {
        return VALUES_INPUT_STREAMS;
    }

    default boolean deterministic()
    {
        return true;
    }

    /**
     * Returns {@code true} when this function propagates nulls strictly: the output is null at exactly the
     * positions where any input value is null, and the output value at a non-null position depends only on the
     * input values (never on which positions are null). This lets the evaluator peel a dictionary-encoded input
     * even when the input's NULLS stream carries a different dictionary than its VALUES stream — the transform
     * runs over the distinct base values while the original NULLS stream passes straight through.
     */
    default boolean propagatesNulls()
    {
        return false;
    }

    default Set<Allocator.Context> allocationContexts()
    {
        return Set.of();
    }

    static Set<Stream> inputStreams(boolean values, boolean nulls, boolean errors)
    {
        int flags = 0;
        if (values) {
            flags |= 1;
        }
        if (nulls) {
            flags |= 1 << 1;
        }
        if (errors) {
            flags |= 1 << 2;
        }
        return switch (flags) {
            case 0 -> NO_INPUT_STREAMS;
            case 1 -> VALUES_INPUT_STREAMS;
            case 2 -> NULLS_INPUT_STREAMS;
            case 3 -> VALUES_AND_NULLS_INPUT_STREAMS;
            case 4 -> ERRORS_INPUT_STREAMS;
            case 5 -> VALUES_AND_ERRORS_INPUT_STREAMS;
            case 6 -> NULLS_AND_ERRORS_INPUT_STREAMS;
            case 7 -> ALL_INPUT_STREAMS;
            default -> throw new IllegalArgumentException("Unsupported input stream flags: " + flags);
        };
    }

    static Set<Stream> valuesOnlyWhenRequested(Set<Stream> requestedOutputStreams)
    {
        return requestedOutputStreams.contains(Stream.VALUES) ? VALUES_INPUT_STREAMS : NO_INPUT_STREAMS;
    }

    static Set<Stream> nullsOnlyWhenRequested(Set<Stream> requestedOutputStreams)
    {
        return requestedOutputStreams.contains(Stream.VALUES) || requestedOutputStreams.contains(Stream.NULLS) ? NULLS_INPUT_STREAMS : NO_INPUT_STREAMS;
    }

    static Set<Stream> valuesAndNullsWhenRequested(Set<Stream> requestedOutputStreams)
    {
        return requestedOutputStreams.contains(Stream.VALUES) || requestedOutputStreams.contains(Stream.NULLS) ? VALUES_AND_NULLS_INPUT_STREAMS : NO_INPUT_STREAMS;
    }

    static Set<Stream> valuesAlwaysNullsWhenRequested(Set<Stream> requestedOutputStreams)
    {
        return inputStreams(
                requestedOutputStreams.contains(Stream.VALUES) || requestedOutputStreams.contains(Stream.NULLS) || requestedOutputStreams.contains(Stream.ERRORS),
                requestedOutputStreams.contains(Stream.NULLS),
                false);
    }
}
