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

@ScalarFunction(name = "map_values")
public final class MapValues
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("MapValues");

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for map_values");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector input = inputs.getFirst().values();
        BooleanVector inputNulls = (BooleanVector) inputs.getFirst().getOrNull(Stream.NULLS);
        Streams result = Streams.empty();

        if (requestedStreams.contains(Stream.VALUES)) {
            result = result.with(Stream.VALUES, mapValues(input, output, context));
        }
        if (requestedStreams.contains(Stream.NULLS) && inputNulls != null) {
            result = result.with(Stream.NULLS, inputNulls);
        }
        return result;
    }

    private static Vector mapValues(Vector input, Streams output, PrimitiveExecutionContext context)
    {
        if (input instanceof MapVector maps) {
            return arrayValues(maps, output, context);
        }
        if (input instanceof DictionaryVector dictionary) {
            checkArgument(dictionary.values() instanceof MapVector, "map_values requires MapVector dictionary values");
            return new DictionaryVector(dictionary.ids(), arrayValues((MapVector) dictionary.values(), null, context));
        }
        throw new IllegalArgumentException("map_values requires MapVector input");
    }

    private static ArrayVector arrayValues(MapVector maps, Streams output, PrimitiveExecutionContext context)
    {
        ArrayVector arrays = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                output != null && output.has(Stream.VALUES) && output.values() instanceof ArrayVector vector ? vector : null,
                ArrayVector.class,
                maps.length(),
                ArrayVector::new);
        System.arraycopy(maps.offsets(), 0, arrays.offsets(), 0, maps.length() + 1);
        arrays.setElements(maps.values());
        return arrays;
    }
}
