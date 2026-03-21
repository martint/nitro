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
import org.weakref.nitro.data.MapVector;
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

@ScalarFunction(name = "element_at_i64_utf8")
public final class ElementAtI64Utf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ElementAtI64Utf8");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for element_at_i64_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS) && !requestedStreams.contains(Stream.ERRORS)) {
            return Streams.empty();
        }

        Vector mapInput = inputs.get(0).values();
        Vector keyInput = inputs.get(1).values();
        checkArgument(mapInput.length() == keyInput.length(), "element_at_i64_utf8 inputs must have the same logical length");

        MapVector maps = requireMapVector(mapInput);
        BinaryVector mapKeys = requireUtf8Binary("element_at_i64_utf8", maps.keyValues());
        I64Vector mapValues = (I64Vector) maps.valueValues();
        BooleanVector mapValueNulls = (BooleanVector) maps.valueStreamOrNull(Stream.NULLS);
        KeyAccess keys = keyAccess("element_at_i64_utf8", keyInput);
        BooleanVector mapNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector keyNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);
        BooleanVector mapErrors = (BooleanVector) inputs.get(0).getOrNull(Stream.ERRORS);
        BooleanVector keyErrors = (BooleanVector) inputs.get(1).getOrNull(Stream.ERRORS);

        Streams result = Streams.empty();
        int requiredLength = Math.max(mask.maxPosition() + 1, mapInput.length());
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyNulls(maps, mapInput, mapKeys, mapValues, mapValueNulls, keys, mapNulls, keyNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            applyValues(maps, mapInput, mapKeys, mapValues, mapValueNulls, keys, mapNulls, keyNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        if (requestedStreams.contains(Stream.ERRORS) && (mapErrors != null || keyErrors != null)) {
            BooleanVector outputErrors = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.ERRORS) && output.get(Stream.ERRORS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyErrors(mapErrors, keyErrors, mask, outputErrors);
            result = result.with(Stream.ERRORS, outputErrors);
        }
        return result;
    }

    private static void applyValues(MapVector maps, Vector mapInput, BinaryVector mapKeys, I64Vector mapValues, BooleanVector mapValueNulls, KeyAccess keys, BooleanVector mapNulls, BooleanVector keyNulls, Mask mask, I64Vector output)
    {
        boolean ascii = mapKeys.hasTrait(BinaryVector.Trait.ASCII_ONLY) && keys.values().hasTrait(BinaryVector.Trait.ASCII_ONLY);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(mapNulls, position) || isNull(keyNulls, position) ? 0 : lookupValue(maps, mapInput, position, mapKeys, mapValues, mapValueNulls, keys.values(), keys.position(position), ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(mapNulls, position) || isNull(keyNulls, position) ? 0 : lookupValue(maps, mapInput, position, mapKeys, mapValues, mapValueNulls, keys.values(), keys.position(position), ascii);
        }
    }

    private static void applyNulls(MapVector maps, Vector mapInput, BinaryVector mapKeys, I64Vector mapValues, BooleanVector mapValueNulls, KeyAccess keys, BooleanVector mapNulls, BooleanVector keyNulls, Mask mask, BooleanVector output)
    {
        boolean ascii = mapKeys.hasTrait(BinaryVector.Trait.ASCII_ONLY) && keys.values().hasTrait(BinaryVector.Trait.ASCII_ONLY);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(mapNulls, position) || isNull(keyNulls, position) || isLookupNull(maps, mapInput, position, mapKeys, mapValues, mapValueNulls, keys.values(), keys.position(position), ascii);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(mapNulls, position) || isNull(keyNulls, position) || isLookupNull(maps, mapInput, position, mapKeys, mapValues, mapValueNulls, keys.values(), keys.position(position), ascii);
        }
    }

    private static boolean isLookupNull(MapVector maps, Vector mapInput, int position, BinaryVector mapKeys, I64Vector mapValues, BooleanVector mapValueNulls, BinaryVector lookupKeys, int lookupPosition, boolean ascii)
    {
        return findEntry(maps, mapInput, position, mapKeys, mapValues, mapValueNulls, lookupKeys, lookupPosition, ascii).nullValue();
    }

    private static long lookupValue(MapVector maps, Vector mapInput, int position, BinaryVector mapKeys, I64Vector mapValues, BooleanVector mapValueNulls, BinaryVector lookupKeys, int lookupPosition, boolean ascii)
    {
        return findEntry(maps, mapInput, position, mapKeys, mapValues, mapValueNulls, lookupKeys, lookupPosition, ascii).value();
    }

    private static LookupResult findEntry(MapVector maps, Vector mapInput, int position, BinaryVector mapKeys, I64Vector mapValues, BooleanVector mapValueNulls, BinaryVector lookupKeys, int lookupPosition, boolean ascii)
    {
        int mapPosition = switch (mapInput) {
            case DictionaryVector vector -> vector.ids()[position];
            default -> position;
        };
        for (int entryIndex = maps.startOffset(mapPosition); entryIndex < maps.endOffset(mapPosition); entryIndex++) {
            if (binaryEquals(mapKeys, entryIndex, lookupKeys, lookupPosition)) {
                boolean nullValue = mapValueNulls != null && mapValueNulls.values()[entryIndex];
                return new LookupResult(nullValue ? 0 : mapValues.values()[entryIndex], nullValue);
            }
        }
        return new LookupResult(0, true);
    }

    private static MapVector requireMapVector(Vector vector)
    {
        return switch (vector) {
            case MapVector maps -> maps;
            case DictionaryVector dictionary when dictionary.values() instanceof MapVector maps -> maps;
            default -> throw new IllegalArgumentException("element_at_i64_utf8 requires MapVector input");
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
        checkArgument(values.hasTrait(BinaryVector.Trait.UTF8_STRING), "%s requires UTF8_STRING inputs", functionName);
        return values;
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private static void applyErrors(BooleanVector mapErrors, BooleanVector keyErrors, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isError(mapErrors, position) || isError(keyErrors, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isError(mapErrors, position) || isError(keyErrors, position);
        }
    }

    private static boolean isError(BooleanVector errors, int position)
    {
        return errors != null && errors.values()[position];
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

    private record LookupResult(long value, boolean nullValue) {}
}
