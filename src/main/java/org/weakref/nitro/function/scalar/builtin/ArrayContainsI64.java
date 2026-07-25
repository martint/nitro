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
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "array_contains_i64")
public final class ArrayContainsI64
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("ArrayContainsI64");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for array_contains_i64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector arrayInput = inputs.get(0).values();
        Vector needleInput = inputs.get(1).values();
        checkArgument(arrayInput.length() == needleInput.length(), "array_contains_i64 inputs must have the same logical length");

        ArrayVector arrays = requireArrayVector(arrayInput);
        NeedleAccess needles = needleAccess("array_contains_i64", needleInput);
        Vector arrayNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector needleNulls = inputs.get(1).getOrNull(Stream.NULLS);

        Streams result = Streams.empty();
        int requiredLength = Math.max(mask.maxPosition() + 1, arrayInput.length());
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            applyNulls(arrayNulls, needleNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector outputValues = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.VALUES) ? output.get(Stream.VALUES) : null,
                    requiredLength);
            applyValues(arrays, arrayInput, needles, arrayNulls, needleNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyValues(ArrayVector arrays, Vector arrayInput, NeedleAccess needles, Vector arrayNulls, Vector needleNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues arrayNullValues = VectorAccess.booleanValues(arrayNulls);
        VectorAccess.BooleanValues needleNullValues = VectorAccess.booleanValues(needleNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = arrayNullValues.value(position) || needleNullValues.value(position)
                        ? false
                        : contains(arrays, arrayInput, position, needles.value(position));
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = arrayNullValues.value(position) || needleNullValues.value(position)
                    ? false
                    : contains(arrays, arrayInput, position, needles.value(position));
        }
    }

    private static boolean contains(ArrayVector arrays, Vector arrayInput, int position, long needle)
    {
        int arrayPosition = switch (arrayInput) {
            case DictionaryVector vector -> vector.ids()[position];
            default -> position;
        };
        I64Vector elementValues = (I64Vector) arrays.elementValues();
        BooleanVector elementNulls = arrays.elementNulls();
        for (int elementIndex = arrays.startOffset(arrayPosition); elementIndex < arrays.endOffset(arrayPosition); elementIndex++) {
            if (elementNulls != null && elementNulls.values()[elementIndex]) {
                continue;
            }
            if (elementValues.values()[elementIndex] == needle) {
                return true;
            }
        }
        return false;
    }

    private static void applyNulls(Vector arrayNulls, Vector needleNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues arrayNullValues = VectorAccess.booleanValues(arrayNulls);
        VectorAccess.BooleanValues needleNullValues = VectorAccess.booleanValues(needleNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = arrayNullValues.value(position) || needleNullValues.value(position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = arrayNullValues.value(position) || needleNullValues.value(position);
        }
    }

    private static ArrayVector requireArrayVector(Vector vector)
    {
        return switch (vector) {
            case ArrayVector arrays -> arrays;
            case DictionaryVector dictionary when dictionary.values() instanceof ArrayVector arrays -> arrays;
            default -> throw new IllegalArgumentException("array_contains_i64 requires ArrayVector input");
        };
    }

    private static NeedleAccess needleAccess(String functionName, Vector vector)
    {
        return switch (vector) {
            case I64Vector values -> new NeedleAccess(values, false, null);
            case DictionaryVector dictionary when dictionary.values() instanceof I64Vector values -> new NeedleAccess(values, true, dictionary.ids());
            default -> throw new IllegalArgumentException(functionName + " requires I64Vector needle input");
        };
    }

    private record NeedleAccess(I64Vector values, boolean dictionary, int[] ids)
    {
        private long value(int position)
        {
            return values.values()[dictionary ? ids[position] : position];
        }
    }
}
