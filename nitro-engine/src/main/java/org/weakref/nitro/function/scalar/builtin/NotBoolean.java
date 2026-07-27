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
import org.weakref.nitro.data.BooleanVector;
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

@ScalarFunction(name = "not", capabilities = NotBooleanOptimization.class)
public final class NotBoolean
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("NotBoolean");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for not");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector inputValues = inputs.get(0).values();
        Vector inputNulls = inputs.get(0).getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        int requiredLength = Math.max(mask.maxPosition() + 1, inputValues.length());

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            boolean[] nulls = outputNulls.values();
            java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
            for (int position : mask) {
                nulls[position] = inputNullValues.value(position);
            }
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        BooleanVector outputValues = VectorAccess.writableBooleanVector(
                context.allocator(),
                allocationContext,
                output != null ? output.getOrNull(Stream.VALUES) : null,
                requiredLength);
        boolean[] values = outputValues.values();
        for (int position : mask) {
            values[position] = !VectorAccess.booleanValues(inputValues).value(position);
        }
        return result.with(Stream.VALUES, outputValues);
    }
}
