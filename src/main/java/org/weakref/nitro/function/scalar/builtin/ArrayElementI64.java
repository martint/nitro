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
import static java.lang.Math.toIntExact;

@ScalarFunction(name = "array_element_i64")
public final class ArrayElementI64
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("ArrayElementI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.inputStreams(
                requestedOutputStreams.contains(Stream.VALUES) || requestedOutputStreams.contains(Stream.NULLS) || requestedOutputStreams.contains(Stream.ERRORS),
                requestedOutputStreams.contains(Stream.VALUES) || requestedOutputStreams.contains(Stream.NULLS),
                requestedOutputStreams.contains(Stream.ERRORS));
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for array_element_i64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS) && !requestedStreams.contains(Stream.ERRORS)) {
            return Streams.empty();
        }

        Vector arrayInput = inputs.get(0).values();
        Vector indexInput = inputs.get(1).values();
        checkArgument(arrayInput.length() == indexInput.length(), "array_element_i64 inputs must have the same logical length");

        ArrayVector arrays = requireArrayVector(arrayInput);
        I64Vector elementValues = requireI64Vector("array_element_i64", arrays.elementValues());
        Vector elementNulls = arrays.elementNulls();
        Vector elementErrors = arrays.elementStreamOrNull(Stream.ERRORS);
        IndexAccess indexes = indexAccess("array_element_i64", indexInput);
        Vector arrayNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector indexNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector arrayErrors = inputs.get(0).getOrNull(Stream.ERRORS);
        Vector indexErrors = inputs.get(1).getOrNull(Stream.ERRORS);

        Streams result = Streams.empty();
        int requiredLength = Math.max(mask.maxPosition() + 1, arrayInput.length());
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            applyNulls(arrays, arrayInput, elementNulls, indexes, arrayNulls, indexNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            applyValues(arrays, arrayInput, elementValues, elementNulls, indexes, arrayNulls, indexNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        if (requestedStreams.contains(Stream.ERRORS) && (arrayErrors != null || indexErrors != null || elementErrors != null)) {
            BooleanVector outputErrors = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.ERRORS) ? output.get(Stream.ERRORS) : null,
                    requiredLength);
            applyErrors(arrays, arrayInput, elementErrors, indexes, arrayErrors, indexErrors, arrayNulls, indexNulls, mask, outputErrors);
            result = result.with(Stream.ERRORS, outputErrors);
        }
        return result;
    }

    private static void applyValues(ArrayVector arrays, Vector arrayInput, I64Vector elementValues, Vector elementNulls, IndexAccess indexes, Vector arrayNulls, Vector indexNulls, Mask mask, I64Vector output)
    {
        VectorAccess.BooleanValues arrayNullValues = VectorAccess.booleanValues(arrayNulls);
        VectorAccess.BooleanValues indexNullValues = VectorAccess.booleanValues(indexNulls);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = arrayNullValues.value(position) || indexNullValues.value(position)
                        ? 0
                        : lookupArrayElement(arrays, arrayInput, elementValues, elementNulls, indexes, position).value();
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = arrayNullValues.value(position) || indexNullValues.value(position)
                    ? 0
                    : lookupArrayElement(arrays, arrayInput, elementValues, elementNulls, indexes, position).value();
        }
    }

    private static void applyNulls(ArrayVector arrays, Vector arrayInput, Vector elementNulls, IndexAccess indexes, Vector arrayNulls, Vector indexNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues arrayNullValues = VectorAccess.booleanValues(arrayNulls);
        VectorAccess.BooleanValues indexNullValues = VectorAccess.booleanValues(indexNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = arrayNullValues.value(position) || indexNullValues.value(position)
                        || lookupArrayElement(arrays, arrayInput, null, elementNulls, indexes, position).nullValue();
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = arrayNullValues.value(position) || indexNullValues.value(position)
                    || lookupArrayElement(arrays, arrayInput, null, elementNulls, indexes, position).nullValue();
        }
    }

    private static void applyErrors(ArrayVector arrays, Vector arrayInput, Vector elementErrors, IndexAccess indexes, Vector arrayErrors, Vector indexErrors, Vector arrayNulls, Vector indexNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues arrayErrorValues = VectorAccess.booleanValues(arrayErrors);
        VectorAccess.BooleanValues indexErrorValues = VectorAccess.booleanValues(indexErrors);
        VectorAccess.BooleanValues arrayNullValues = VectorAccess.booleanValues(arrayNulls);
        VectorAccess.BooleanValues indexNullValues = VectorAccess.booleanValues(indexNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = arrayErrorValues.value(position)
                        || indexErrorValues.value(position)
                        || (!arrayNullValues.value(position) && !indexNullValues.value(position) && lookupArrayElementError(arrays, arrayInput, elementErrors, indexes, position));
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = arrayErrorValues.value(position)
                    || indexErrorValues.value(position)
                    || (!arrayNullValues.value(position) && !indexNullValues.value(position) && lookupArrayElementError(arrays, arrayInput, elementErrors, indexes, position));
        }
    }

    private static ArrayElementResult lookupArrayElement(ArrayVector arrays, Vector arrayInput, I64Vector elementValues, Vector elementNulls, IndexAccess indexes, int position)
    {
        VectorAccess.BooleanValues elementNullValues = VectorAccess.booleanValues(elementNulls);
        int arrayPosition = switch (arrayInput) {
            case DictionaryVector vector -> vector.ids()[position];
            default -> position;
        };
        long index = indexes.value(position);
        if (index < 0 || index >= arrays.length(arrayPosition)) {
            return new ArrayElementResult(0, true);
        }
        int elementPosition = arrays.startOffset(arrayPosition) + toIntExact(index);
        boolean nullValue = elementNullValues.value(elementPosition);
        long value = elementValues == null || nullValue ? 0 : elementValues.values()[elementPosition];
        return new ArrayElementResult(value, nullValue);
    }

    private static boolean lookupArrayElementError(ArrayVector arrays, Vector arrayInput, Vector elementErrors, IndexAccess indexes, int position)
    {
        VectorAccess.BooleanValues elementErrorValues = VectorAccess.booleanValues(elementErrors);
        int arrayPosition = switch (arrayInput) {
            case DictionaryVector vector -> vector.ids()[position];
            default -> position;
        };
        long index = indexes.value(position);
        if (index < 0 || index >= arrays.length(arrayPosition)) {
            return false;
        }
        int elementPosition = arrays.startOffset(arrayPosition) + toIntExact(index);
        return elementErrorValues.value(elementPosition);
    }

    private static ArrayVector requireArrayVector(Vector vector)
    {
        return switch (vector) {
            case ArrayVector arrays -> arrays;
            case DictionaryVector dictionary when dictionary.values() instanceof ArrayVector arrays -> arrays;
            default -> throw new IllegalArgumentException("array_element_i64 requires ArrayVector input");
        };
    }

    private static I64Vector requireI64Vector(String functionName, Vector vector)
    {
        checkArgument(vector instanceof I64Vector, "%s requires I64Vector values", functionName);
        return (I64Vector) vector;
    }

    private static IndexAccess indexAccess(String functionName, Vector vector)
    {
        return switch (vector) {
            case I64Vector values -> new IndexAccess(values, false, null);
            case DictionaryVector dictionary when dictionary.values() instanceof I64Vector values -> new IndexAccess(values, true, dictionary.ids());
            default -> throw new IllegalArgumentException(functionName + " requires I64Vector index input");
        };
    }

    private record IndexAccess(I64Vector values, boolean dictionary, int[] ids)
    {
        private long value(int position)
        {
            return values.values()[dictionary ? ids[position] : position];
        }
    }

    private record ArrayElementResult(long value, boolean nullValue) {}
}
