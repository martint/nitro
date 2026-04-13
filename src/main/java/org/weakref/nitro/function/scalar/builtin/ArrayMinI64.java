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

@ScalarFunction(name = "array_min_i64")
public final class ArrayMinI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ArrayMinI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for array_min_i64");

        Vector values = inputs.getFirst().values();
        Vector inputNulls = inputs.getFirst().getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, values.length());
        Streams result = Streams.empty();

        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            applyNulls(values, inputNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            applyValues(values, inputNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyValues(Vector values, Vector inputNulls, Mask mask, I64Vector output)
    {
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNullValues.value(position) ? 0 : min(values, position).value();
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNullValues.value(position) ? 0 : min(values, position).value();
        }
    }

    private static void applyNulls(Vector values, Vector inputNulls, Mask mask, BooleanVector output)
    {
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNullValues.value(position) || min(values, position).nullValue();
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNullValues.value(position) || min(values, position).nullValue();
        }
    }

    private static MinResult min(Vector values, int position)
    {
        if (values instanceof ArrayVector arrayValues) {
            return min(arrayValues, position);
        }
        if (values instanceof DictionaryVector dictionaryValues) {
            checkArgument(dictionaryValues.values() instanceof ArrayVector, "array_min_i64 requires ArrayVector dictionary values");
            return min((ArrayVector) dictionaryValues.values(), dictionaryValues.ids()[position]);
        }
        throw new IllegalArgumentException("Unsupported array_min_i64 vector type: " + values.getClass().getSimpleName());
    }

    private static MinResult min(ArrayVector values, int position)
    {
        I64Vector elementValues = (I64Vector) values.elementValues();
        VectorAccess.BooleanValues elementNulls = VectorAccess.booleanValues(values.elementNulls());

        long min = 0;
        boolean found = false;
        for (int elementIndex = values.startOffset(position); elementIndex < values.endOffset(position); elementIndex++) {
            if (elementNulls.value(elementIndex)) {
                continue;
            }
            long value = elementValues.values()[elementIndex];
            if (!found || value < min) {
                min = value;
                found = true;
            }
        }
        return new MinResult(min, !found);
    }

    private record MinResult(long value, boolean nullValue) {}
}
