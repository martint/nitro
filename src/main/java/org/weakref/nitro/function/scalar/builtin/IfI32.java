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
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "if_i32")
public final class IfI32
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("IfI32");

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
        checkArgument(inputs.size() == 3, "Unexpected argument count for if_i32");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector condition = inputs.get(0).values();
        Vector conditionNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector trueValues = inputs.get(1).values();
        Vector falseValues = inputs.get(2).values();
        Vector trueNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector falseNulls = inputs.get(2).getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues conditionValues = VectorAccess.booleanValues(condition);
        VectorAccess.LongValues trueBranchValues = VectorAccess.longValues(trueValues);
        VectorAccess.LongValues falseBranchValues = VectorAccess.longValues(falseValues);
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(trueValues.length(), falseValues.length()));

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            I32Vector outputValues = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof I32Vector vector ? vector : null,
                    I32Vector.class,
                    requiredLength,
                    I32Vector::new);
            applyValues(conditionValues, conditionNulls, trueBranchValues, falseBranchValues, trueNulls, falseNulls, mask, outputValues, outputNulls);
            return result.with(Stream.VALUES, outputValues);
        }

        applyNulls(conditionValues, conditionNulls, trueNulls, falseNulls, mask, outputNulls);
        return result;
    }

    private static void applyValues(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, VectorAccess.LongValues trueValues, VectorAccess.LongValues falseValues, Vector trueNulls, Vector falseNulls, Mask mask, I32Vector outputValues, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues trueNullValues = VectorAccess.booleanValues(trueNulls);
        VectorAccess.BooleanValues falseNullValues = VectorAccess.booleanValues(falseNulls);
        for (int position : mask) {
            boolean takeTrue = !conditionNullValues.value(position) && conditionValues.value(position);
            VectorAccess.LongValues selectedValues = takeTrue ? trueValues : falseValues;
            VectorAccess.BooleanValues selectedNullValues = takeTrue ? trueNullValues : falseNullValues;
            if (selectedNullValues.value(position)) {
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
                continue;
            }

            outputValues.values()[position] = (int) selectedValues.value(position);
            if (outputNulls != null) {
                outputNulls.values()[position] = false;
            }
        }
    }

    private static void applyNulls(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, Vector trueNulls, Vector falseNulls, Mask mask, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues trueNullValues = VectorAccess.booleanValues(trueNulls);
        VectorAccess.BooleanValues falseNullValues = VectorAccess.booleanValues(falseNulls);
        boolean[] nulls = outputNulls.values();
        Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            boolean takeTrue = !conditionNullValues.value(position) && conditionValues.value(position);
            nulls[position] = takeTrue ? trueNullValues.value(position) : falseNullValues.value(position);
        }
    }

    private static boolean conditionValue(VectorAccess.BooleanValues values, Vector nulls, int position)
    {
        if (VectorAccess.isNull(nulls, position)) {
            return false;
        }
        return values.value(position);
    }
}
