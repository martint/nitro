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
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
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

@ScalarFunction(name = "divide_scale_i64")
public final class DivideScaleI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("DivideScaleI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public boolean requiresInputCompanionStreams()
    {
        return true;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 3, "Unexpected argument count for divide_scale_i64");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }
        Allocator.Context allocationContext = context.allocationContext("DivideScaleI64");

        Vector numeratorValues = inputs.get(0).values();
        Vector denominatorValues = inputs.get(1).values();
        Vector scaleValues = inputs.get(2).values();
        Vector numeratorNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector denominatorNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector scaleNulls = inputs.get(2).getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(numeratorValues.length(), Math.max(denominatorValues.length(), scaleValues.length())));

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.getOrNull(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            boolean[] nullValues = nulls.values();
            for (int position : mask) {
                nullValues[position] = isNull(numeratorNulls, position) || isNull(denominatorNulls, position) || isNull(scaleNulls, position) || value(denominatorValues, position) == 0;
            }
            result = result.with(Stream.NULLS, nulls);
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
        long[] outputValues = values.values();
        for (int position : mask) {
            long denominator = value(denominatorValues, position);
            outputValues[position] = denominator == 0 ? 0 : (value(numeratorValues, position) * value(scaleValues, position)) / denominator;
        }
        return result.with(Stream.VALUES, values);
    }

    private static long value(Vector vector, int position)
    {
        return switch (vector) {
            case I64Vector values -> values.values()[position];
            case I32Vector values -> values.values()[position];
            case DictionaryVector values -> value(values.values(), values.ids()[position]);
            case RleVector values -> value(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but found " + vector.getClass().getSimpleName());
        };
    }

    private static boolean isNull(Vector nulls, int position)
    {
        return switch (nulls) {
            case null -> false;
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> isNull(values.values(), values.ids()[position]);
            case RleVector values -> isNull(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected boolean-backed null vector but found " + nulls.getClass().getSimpleName());
        };
    }
}
