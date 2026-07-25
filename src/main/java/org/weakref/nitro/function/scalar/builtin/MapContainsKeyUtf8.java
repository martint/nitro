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
import org.weakref.nitro.data.MapVector;
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

@ScalarFunction(name = "map_contains_key_utf8")
public final class MapContainsKeyUtf8
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("MapContainsKeyUtf8");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for map_contains_key_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS) && !requestedStreams.contains(Stream.ERRORS)) {
            return Streams.empty();
        }

        Vector mapInput = inputs.get(0).values();
        Vector keyInput = inputs.get(1).values();
        checkArgument(mapInput.length() == keyInput.length(), "map_contains_key_utf8 inputs must have the same logical length");

        MapVector maps = requireMapVector(mapInput);
        BinaryVector mapKeys = requireUtf8Binary("map_contains_key_utf8", maps.keyValues());
        KeyAccess keys = keyAccess("map_contains_key_utf8", keyInput);
        Vector mapNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector keyNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector mapErrors = inputs.get(0).getOrNull(Stream.ERRORS);
        Vector keyErrors = inputs.get(1).getOrNull(Stream.ERRORS);

        Streams result = Streams.empty();
        int requiredLength = Math.max(mask.maxPosition() + 1, mapInput.length());
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            applyNulls(mapNulls, keyNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector outputValues = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.VALUES) ? output.values() : null,
                    requiredLength);
            applyValues(maps, mapInput, mapKeys, keys, mapNulls, keyNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        if (requestedStreams.contains(Stream.ERRORS) && (mapErrors != null || keyErrors != null)) {
            BooleanVector outputErrors = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.ERRORS) ? output.get(Stream.ERRORS) : null,
                    requiredLength);
            applyErrors(mapErrors, keyErrors, mask, outputErrors);
            result = result.with(Stream.ERRORS, outputErrors);
        }
        return result;
    }

    private static void applyValues(MapVector maps, Vector mapInput, BinaryVector mapKeys, KeyAccess keys, Vector mapNulls, Vector keyNulls, Mask mask, BooleanVector output)
    {
        boolean ascii = mapKeys.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY) && keys.values().hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        VectorAccess.BooleanValues mapNullValues = VectorAccess.booleanValues(mapNulls);
        VectorAccess.BooleanValues keyNullValues = VectorAccess.booleanValues(keyNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = mapNullValues.value(position) || keyNullValues.value(position) ? false : containsKey(maps, mapInput, position, mapKeys, keys.values(), keys.position(position), ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = mapNullValues.value(position) || keyNullValues.value(position) ? false : containsKey(maps, mapInput, position, mapKeys, keys.values(), keys.position(position), ascii);
        }
    }

    private static boolean containsKey(MapVector maps, Vector mapInput, int position, BinaryVector mapKeys, BinaryVector lookupKeys, int lookupPosition, boolean ascii)
    {
        int mapPosition = switch (mapInput) {
            case DictionaryVector vector -> vector.ids()[position];
            default -> position;
        };
        for (int entryIndex = maps.startOffset(mapPosition); entryIndex < maps.endOffset(mapPosition); entryIndex++) {
            if (binaryEquals(mapKeys, entryIndex, lookupKeys, lookupPosition)) {
                return true;
            }
        }
        return false;
    }

    private static void applyNulls(Vector mapNulls, Vector keyNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues mapNullValues = VectorAccess.booleanValues(mapNulls);
        VectorAccess.BooleanValues keyNullValues = VectorAccess.booleanValues(keyNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = mapNullValues.value(position) || keyNullValues.value(position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = mapNullValues.value(position) || keyNullValues.value(position);
        }
    }

    private static void applyErrors(Vector mapErrors, Vector keyErrors, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues mapErrorValues = VectorAccess.booleanValues(mapErrors);
        VectorAccess.BooleanValues keyErrorValues = VectorAccess.booleanValues(keyErrors);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = mapErrorValues.value(position) || keyErrorValues.value(position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = mapErrorValues.value(position) || keyErrorValues.value(position);
        }
    }

    private static MapVector requireMapVector(Vector vector)
    {
        return switch (vector) {
            case MapVector maps -> maps;
            case DictionaryVector dictionary when dictionary.values() instanceof MapVector maps -> maps;
            default -> throw new IllegalArgumentException("map_contains_key_utf8 requires MapVector input");
        };
    }

    private static KeyAccess keyAccess(String functionName, Vector vector)
    {
        return switch (vector) {
            case BinaryVector values -> new KeyAccess(values, false, null);
            case DictionaryVector dictionary when dictionary.values() instanceof BinaryVector values -> new KeyAccess(values, true, dictionary.ids());
            default -> throw new IllegalArgumentException(functionName + " requires BinaryVector key input");
        };
    }

    private static BinaryVector requireUtf8Binary(String functionName, Vector vector)
    {
        checkArgument(vector instanceof BinaryVector, "%s requires BinaryVector inputs", functionName);
        BinaryVector values = (BinaryVector) vector;
        checkArgument(values.hasTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING), "%s requires UTF8_STRING inputs", functionName);
        return values;
    }

    private static boolean binaryEquals(BinaryVector left, int leftPosition, BinaryVector right, int rightPosition)
    {
        int leftLength = left.length(leftPosition);
        if (leftLength != right.length(rightPosition)) {
            return false;
        }
        byte[] leftData = left.data();
        byte[] rightData = right.data();
        int leftStart = left.startOffset(leftPosition);
        int rightStart = right.startOffset(rightPosition);
        for (int index = 0; index < leftLength; index++) {
            if (leftData[leftStart + index] != rightData[rightStart + index]) {
                return false;
            }
        }
        return true;
    }

    private record KeyAccess(BinaryVector values, boolean dictionary, int[] ids)
    {
        private int position(int position)
        {
            return dictionary ? ids[position] : position;
        }
    }
}
