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

@ScalarFunction(name = "array_sum_i64")
public final class ArraySumI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ArraySumI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for array_sum_i64");

        Vector values = inputs.getFirst().values();
        BooleanVector inputNulls = (BooleanVector) inputs.getFirst().getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, values.length());
        Streams result = Streams.empty();

        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            copyNulls(inputNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I64Vector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I64Vector vector ? vector : null,
                    I64Vector.class,
                    requiredLength,
                    I64Vector::new);
            apply(values, inputNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void apply(Vector values, BooleanVector inputNulls, Mask mask, I64Vector output)
    {
        if (values instanceof ArrayVector arrayValues) {
            applyFlat(arrayValues, inputNulls, mask, output);
            return;
        }
        if (values instanceof DictionaryVector dictionaryValues) {
            checkArgument(dictionaryValues.values() instanceof ArrayVector, "array_sum_i64 requires ArrayVector dictionary values");
            applyDictionary((ArrayVector) dictionaryValues.values(), dictionaryValues.ids(), inputNulls, mask, output);
            return;
        }
        throw new IllegalArgumentException("Unsupported array_sum_i64 vector type: " + values.getClass().getSimpleName());
    }

    private static void applyFlat(ArrayVector values, BooleanVector inputNulls, Mask mask, I64Vector output)
    {
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position) ? 0 : sum(values, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position) ? 0 : sum(values, position);
        }
    }

    private static void applyDictionary(ArrayVector values, int[] ids, BooleanVector inputNulls, Mask mask, I64Vector output)
    {
        long[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position) ? 0 : sum(values, ids[position]);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position) ? 0 : sum(values, ids[position]);
        }
    }

    private static long sum(ArrayVector values, int position)
    {
        I64Vector elementValues = (I64Vector) values.elementValues();
        BooleanVector elementNulls = values.elementNulls();
        long sum = 0;
        for (int elementIndex = values.startOffset(position); elementIndex < values.endOffset(position); elementIndex++) {
            if (elementNulls == null || !elementNulls.values()[elementIndex]) {
                sum += elementValues.values()[elementIndex];
            }
        }
        return sum;
    }

    private static void copyNulls(BooleanVector inputNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position);
        }
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }
}
