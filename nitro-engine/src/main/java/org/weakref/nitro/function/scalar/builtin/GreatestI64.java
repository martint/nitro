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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "greatest_i64")
public final class GreatestI64
        implements PrimitiveFunction
{
    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(!inputs.isEmpty(), "greatest_i64 requires at least one argument");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        Allocator.Context allocationContext = context.allocationContext("GreatestI64");
        int requiredLength = mask.maxPosition() + 1;
        for (Streams input : inputs) {
            requiredLength = Math.max(requiredLength, input.values().length());
        }

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            boolean[] nullValues = outputNulls.values();
            List<VectorAccess.BooleanValues> inputNulls = inputs.stream()
                    .map(input -> VectorAccess.booleanValues(input.getOrNull(Stream.NULLS)))
                    .toList();
            for (int position : mask) {
                boolean isNull = false;
                for (VectorAccess.BooleanValues nulls : inputNulls) {
                    isNull |= nulls.value(position);
                }
                nullValues[position] = isNull;
            }
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestValues) {
            return result;
        }

        I64Vector values = context.allocator().allocateOrGrow(
                allocationContext,
                output != null && output.getOrNull(Stream.VALUES) instanceof I64Vector vector ? vector : null,
                I64Vector.class,
                requiredLength,
                I64Vector::new);
        List<VectorAccess.LongValues> arguments = inputs.stream()
                .map(Streams::values)
                .map(VectorAccess::longValues)
                .toList();
        long[] outputValues = values.values();
        for (int position : mask) {
            long greatest = arguments.getFirst().value(position);
            for (int argument = 1; argument < arguments.size(); argument++) {
                greatest = Math.max(greatest, arguments.get(argument).value(position));
            }
            outputValues[position] = greatest;
        }
        return result.with(Stream.VALUES, values);
    }
}
