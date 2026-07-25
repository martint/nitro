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
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "cardinality")
public final class Cardinality
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("Cardinality");

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
        checkArgument(inputs.size() == 1, "Unexpected argument count for cardinality");

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
        if (values instanceof ArrayVector arrayValues) {
            applyFlat(arrayValues, inputNulls, mask, output);
            return;
        }
        if (values instanceof MapVector mapValues) {
            applyFlat(mapValues, inputNulls, mask, output);
            return;
        }
        if (values instanceof DictionaryVector dictionaryValues) {
            checkArgument(dictionaryValues.values() instanceof ArrayVector || dictionaryValues.values() instanceof MapVector, "cardinality requires ArrayVector or MapVector dictionary values");
            applyDictionary(dictionaryValues.values(), dictionaryValues.ids(), inputNulls, mask, output);
            return;
        }
        throw new IllegalArgumentException("Unsupported cardinality vector type: " + values.getClass().getSimpleName());
    }

    private static void applyFlat(ArrayVector values, Vector inputNulls, Mask mask, I64Vector output)
    {
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNullValues.value(position) ? 0 : values.length(position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNullValues.value(position) ? 0 : values.length(position);
        }
    }

    private static void applyFlat(MapVector values, Vector inputNulls, Mask mask, I64Vector output)
    {
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNullValues.value(position) ? 0 : values.length(position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNullValues.value(position) ? 0 : values.length(position);
        }
    }

    private static void applyDictionary(Vector values, int[] ids, Vector inputNulls, Mask mask, I64Vector output)
    {
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNullValues.value(position) ? 0 : cardinalityLength(values, ids[position]);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNullValues.value(position) ? 0 : cardinalityLength(values, ids[position]);
        }
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

    private static int cardinalityLength(Vector values, int position)
    {
        return switch (values) {
            case ArrayVector vector -> vector.length(position);
            case MapVector vector -> vector.length(position);
            default -> throw new IllegalArgumentException("Unsupported cardinality vector type: " + values.getClass().getSimpleName());
        };
    }
}
