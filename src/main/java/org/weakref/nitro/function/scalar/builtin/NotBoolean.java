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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "not")
public final class NotBoolean
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("NotBoolean");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
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
        BooleanVector inputNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, inputValues.length());

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.getOrNull(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            boolean[] nulls = outputNulls.values();
            java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
            if (inputNulls != null) {
                boolean[] inputNullValues = inputNulls.values();
                for (int position : mask) {
                    nulls[position] = inputNullValues[position];
                }
            }
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        BooleanVector outputValues = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                output != null && output.getOrNull(Stream.VALUES) instanceof BooleanVector vector ? vector : null,
                BooleanVector.class,
                requiredLength,
                BooleanVector::new);
        boolean[] values = outputValues.values();
        for (int position : mask) {
            values[position] = !booleanValue(inputValues, position);
        }
        return result.with(Stream.VALUES, outputValues);
    }

    private static boolean booleanValue(Vector values, int position)
    {
        return switch (values) {
            case BooleanVector vector -> vector.values()[position];
            case DictionaryVector vector -> booleanValue(vector.values(), vector.ids()[position]);
            case RleVector vector -> booleanValue(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported not vector type: " + values.getClass().getSimpleName());
        };
    }
}
