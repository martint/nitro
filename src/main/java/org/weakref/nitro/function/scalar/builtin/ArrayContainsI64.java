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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "array_contains_i64")
public final class ArrayContainsI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ArrayContainsI64");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for array_contains_i64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector arrayInput = inputs.get(0).values();
        Vector needleInput = inputs.get(1).values();
        checkArgument(arrayInput.length() == needleInput.length(), "array_contains_i64 inputs must have the same logical length");

        ArrayVector arrays = requireArrayVector(arrayInput);
        NeedleAccess needles = needleAccess("array_contains_i64", needleInput);
        BooleanVector arrayNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector needleNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);

        Streams result = Streams.empty();
        int requiredLength = Math.max(mask.maxPosition() + 1, arrayInput.length());
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyNulls(arrayNulls, needleNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.get(Stream.VALUES) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyValues(arrays, arrayInput, needles, arrayNulls, needleNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyValues(ArrayVector arrays, Vector arrayInput, NeedleAccess needles, BooleanVector arrayNulls, BooleanVector needleNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(arrayNulls, position) || isNull(needleNulls, position)
                        ? false
                        : contains(arrays, arrayInput, position, needles.value(position));
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(arrayNulls, position) || isNull(needleNulls, position)
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

    private static void applyNulls(BooleanVector arrayNulls, BooleanVector needleNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(arrayNulls, position) || isNull(needleNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(arrayNulls, position) || isNull(needleNulls, position);
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

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private record NeedleAccess(I64Vector values, boolean dictionary, int[] ids)
    {
        private long value(int position)
        {
            return values.values()[dictionary ? ids[position] : position];
        }
    }
}
