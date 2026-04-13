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

@ScalarFunction(name = "divide_round_i64")
public final class DivideRoundI64
        implements PrimitiveFunction
{
    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for divide_round_i64");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        Allocator.Context allocationContext = context.allocationContext("DivideRoundI64");
        Vector numeratorValues = inputs.get(0).values();
        Vector denominatorValues = inputs.get(1).values();
        Vector numeratorNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector denominatorNulls = inputs.get(1).getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(numeratorValues.length(), denominatorValues.length()));

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            boolean[] nullValues = nulls.values();
            VectorAccess.BooleanValues numeratorNullValues = VectorAccess.booleanValues(numeratorNulls);
            VectorAccess.BooleanValues denominatorNullValues = VectorAccess.booleanValues(denominatorNulls);
            for (int position : mask) {
                nullValues[position] = numeratorNullValues.value(position) || denominatorNullValues.value(position) || value(denominatorValues, position) == 0;
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
            outputValues[position] = denominator == 0 ? 0 : roundDivide(value(numeratorValues, position), denominator);
        }
        return result.with(Stream.VALUES, values);
    }

    private static long roundDivide(long numerator, long denominator)
    {
        long positiveNumerator = numerator >= 0 ? numerator : -numerator;
        long positiveDenominator = denominator >= 0 ? denominator : -denominator;
        long rounded = (positiveNumerator + (positiveDenominator / 2)) / positiveDenominator;
        return (numerator < 0) ^ (denominator < 0) ? -rounded : rounded;
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

}
