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

@ScalarFunction(name = "map_keys")
public final class MapKeys
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("MapKeys");

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
        checkArgument(inputs.size() == 1, "Unexpected argument count for map_keys");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector input = inputs.getFirst().values();
        BooleanVector inputNulls = (BooleanVector) inputs.getFirst().getOrNull(Stream.NULLS);
        Streams result = Streams.empty();
        int requiredLength = input.length();
        boolean reuseOutput = mask.all();
        Streams evaluationOutput = reuseOutput ? output : null;

        if (requestedStreams.contains(Stream.VALUES)) {
            result = result.with(Stream.VALUES, mapKeys(input, evaluationOutput, context, mask, requiredLength));
        }
        if (requestedStreams.contains(Stream.NULLS) && inputNulls != null) {
            result = result.with(Stream.NULLS, copyNulls(inputNulls, evaluationOutput, context, mask, requiredLength));
        }
        return result;
    }

    private static DictionaryVector dictionaryKeys(DictionaryVector dictionary, MapVector maps, Streams output, PrimitiveExecutionContext context, Mask mask, int requiredLength)
    {
        if (!mask.all()) {
            return null;
        }
        DictionaryVector existing = output != null && output.has(Stream.VALUES) && output.values() instanceof DictionaryVector vector ? vector : null;
        int[] ids = existing != null ? existing.ids() : new int[requiredLength];
        System.arraycopy(dictionary.ids(), 0, ids, 0, dictionary.length());
        return context.allocator().allocateDictionary(ALLOCATION_CONTEXT, ids, arrayKeys(maps, null, context, Mask.all(maps.length()), maps.length()));
    }

    private static ArrayVector arrayKeys(MapVector maps, Streams output, PrimitiveExecutionContext context, Mask mask, int requiredLength)
    {
        ArrayVector arrays = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                output != null && output.has(Stream.VALUES) && output.values() instanceof ArrayVector vector ? vector : null,
                ArrayVector.class,
                requiredLength,
                ArrayVector::new);
        System.arraycopy(maps.offsets(), 0, arrays.offsets(), 0, maps.length() + 1);
        arrays.setElements(context.allocator().copyStreams(ALLOCATION_CONTEXT, maps.keys()));
        return arrays;
    }

    private static Vector mapKeys(Vector input, Streams output, PrimitiveExecutionContext context, Mask mask, int requiredLength)
    {
        if (input instanceof MapVector maps) {
            if (!mask.all()) {
                return selectedArray(maps, context, mask, requiredLength, selectedPositions(mask), maps.keys());
            }
            return arrayKeys(maps, output, context, mask, requiredLength);
        }
        if (input instanceof DictionaryVector dictionary) {
            checkArgument(dictionary.values() instanceof MapVector, "map_keys requires MapVector dictionary values");
            if (mask.all()) {
                return dictionaryKeys(dictionary, (MapVector) dictionary.values(), output, context, mask, requiredLength);
            }
            return selectedArray(
                    (MapVector) dictionary.values(),
                    context,
                    mask,
                    requiredLength,
                    selectedDictionaryPositions(dictionary, mask),
                    ((MapVector) dictionary.values()).keys());
        }
        throw new IllegalArgumentException("map_keys requires MapVector input");
    }

    private static ArrayVector selectedArray(MapVector maps, PrimitiveExecutionContext context, Mask mask, int requiredLength, int[] mapPositions, Streams elements)
    {
        ArrayVector arrays = context.allocator().allocateArray(ALLOCATION_CONTEXT, requiredLength);
        int currentOffset = 0;
        int selectedIndex = 0;
        for (int position = 0; position < requiredLength; position++) {
            if (selectedIndex < mask.selectedCount() && mask.position(selectedIndex) == position) {
                currentOffset += maps.length(mapPositions[selectedIndex]);
                selectedIndex++;
            }
            arrays.offsets()[position + 1] = currentOffset;
        }
        arrays.setElements(context.allocator().copyStreams(ALLOCATION_CONTEXT, elements, nestedPositions(maps.offsets(), mapPositions, currentOffset)));
        return arrays;
    }

    private static int[] selectedPositions(Mask mask)
    {
        int[] positions = new int[mask.selectedCount()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = mask.position(index);
        }
        return positions;
    }

    private static int[] selectedDictionaryPositions(DictionaryVector dictionary, Mask mask)
    {
        int[] positions = new int[mask.selectedCount()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = dictionary.ids()[mask.position(index)];
        }
        return positions;
    }

    private static int[] nestedPositions(int[] offsets, int[] positions, int totalEntries)
    {
        int[] entryPositions = new int[totalEntries];
        int next = 0;
        for (int position : positions) {
            for (int entry = offsets[position]; entry < offsets[position + 1]; entry++) {
                entryPositions[next++] = entry;
            }
        }
        return entryPositions;
    }

    private static BooleanVector copyNulls(BooleanVector inputNulls, Streams output, PrimitiveExecutionContext context, Mask mask, int requiredLength)
    {
        BooleanVector outputNulls = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                BooleanVector.class,
                requiredLength,
                BooleanVector::new);
        if (mask.all()) {
            System.arraycopy(inputNulls.values(), 0, outputNulls.values(), 0, inputNulls.length());
        }
        else {
            for (int position : mask) {
                outputNulls.values()[position] = inputNulls.values()[position];
            }
        }
        return outputNulls;
    }
}
