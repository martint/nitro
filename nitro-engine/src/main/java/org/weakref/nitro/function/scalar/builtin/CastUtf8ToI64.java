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
import org.weakref.nitro.data.I64Vector;
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

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "cast_utf8_to_i64")
public final class CastUtf8ToI64
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("CastUtf8ToI64");

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
        checkArgument(inputs.size() == 1, "Unexpected argument count for cast_utf8_to_i64");

        Vector values = inputs.getFirst().values();
        Vector inputNulls = inputs.getFirst().getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, values.length());
        Streams result = Streams.empty();

        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            copyNulls(inputNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            apply(values, inputNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void apply(Vector values, Vector inputNulls, Mask mask, I64Vector output)
    {
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNullValues.value(position) ? 0 : parse(values, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNullValues.value(position) ? 0 : parse(values, position);
        }
    }

    private static long parse(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> parse(vector.data(), vector.startOffset(position), vector.endOffset(position));
            case DictionaryVector vector -> parse(vector.values(), vector.ids()[position]);
            case RleVector vector -> parse(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported cast_utf8_to_i64 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static long parse(byte[] data, int offset, int end)
    {
        if (offset == end) {
            throw new IllegalArgumentException("Cannot cast empty UTF-8 string to i64");
        }

        int index = offset;
        boolean negative = false;
        byte first = data[index];
        if (first == '-') {
            negative = true;
            index++;
        }
        else if (first == '+') {
            index++;
        }
        if (index == end) {
            throw new IllegalArgumentException("Cannot cast sign-only UTF-8 string to i64");
        }

        long value = 0;
        for (; index < end; index++) {
            int digit = data[index] - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("Cannot cast non-numeric UTF-8 string to i64");
            }
            value = Math.addExact(Math.multiplyExact(value, 10), digit);
        }
        return negative ? Math.negateExact(value) : value;
    }

    private static void copyNulls(Vector inputNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNullValues.value(position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNullValues.value(position);
        }
    }
}
