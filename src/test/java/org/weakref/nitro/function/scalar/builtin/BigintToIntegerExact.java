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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;

/** Checked physical narrowing used by legacy operator benchmarks; logical cast selection belongs to a registry. */
@ScalarFunction(name = "cast_bigint_to_integer")
public final class BigintToIntegerExact
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("BigintToIntegerExact");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.inputStreams(
                requestedOutputStreams.contains(Stream.VALUES),
                requestedOutputStreams.contains(Stream.NULLS),
                false);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for cast_bigint_to_integer");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Streams input = inputs.get(0);
        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS) && input.has(Stream.NULLS)) {
            result = result.with(Stream.NULLS, input.get(Stream.NULLS));
        }

        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        I32Vector outputValues = context.allocator().allocateOrGrow(
                allocationContext,
                output != null && output.has(Stream.VALUES) && output.values() instanceof I32Vector vector ? vector : null,
                I32Vector.class,
                mask.maxPosition() + 1,
                I32Vector::new);
        applyValues(input.values(), mask, outputValues);
        return result.with(Stream.VALUES, outputValues);
    }

    private static void applyValues(Vector values, Mask mask, I32Vector output)
    {
        VectorAccess.LongValues inputValues = VectorAccess.longValues(values);
        for (int position : mask) {
            output.values()[position] = toIntExact(inputValues.value(position));
        }
    }
}
