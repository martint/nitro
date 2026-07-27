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
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "lt_f64", capabilities = LessThanF64Optimization.class)
public final class LessThanF64
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("LessThanF64");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for lt_f64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector fast = NullFreeScalarKernels.compareDouble(NullFreeScalarKernels.LESS_THAN, inputs.get(0).values(), inputs.get(1).values(), inputs.get(0).getOrNull(Stream.NULLS), inputs.get(1).getOrNull(Stream.NULLS), mask, output != null ? output.getOrNull(Stream.VALUES) : null, context.allocator(), allocationContext);
            if (fast != null) {
                return Streams.ofValues(fast);
            }
        }

        VectorAccess.DoubleValues leftValues = VectorAccess.doubleValues(inputs.get(0).values());
        VectorAccess.DoubleValues rightValues = VectorAccess.doubleValues(inputs.get(1).values());
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(inputs.get(0).values().length(), inputs.get(1).values().length()));

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            boolean[] nulls = outputNulls.values();
            Arrays.fill(nulls, 0, outputNulls.length(), false);
            for (int position : mask) {
                nulls[position] = leftNulls.value(position) || rightNulls.value(position);
            }
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        BooleanVector values = VectorAccess.writableBooleanVector(
                context.allocator(),
                allocationContext,
                output != null && output.getOrNull(Stream.VALUES) != outputNulls ? output.getOrNull(Stream.VALUES) : null,
                requiredLength);
        boolean[] outputValues = values.values();
        for (int position : mask) {
            outputValues[position] = leftValues.value(position) < rightValues.value(position);
        }
        return result.with(Stream.VALUES, values);
    }
}
