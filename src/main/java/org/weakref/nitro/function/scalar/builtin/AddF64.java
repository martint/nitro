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
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "add_f64")
public final class AddF64
        implements PrimitiveFunction
{
    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for add_f64");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        Allocator.Context allocationContext = context.allocationContext("AddF64");
        VectorAccess.DoubleValues leftValues = VectorAccess.doubleValues(inputs.get(0).values());
        VectorAccess.DoubleValues rightValues = VectorAccess.doubleValues(inputs.get(1).values());
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(inputs.get(0).values().length(), inputs.get(1).values().length()));

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            boolean[] nullValues = nulls.values();
            for (int position : mask) {
                nullValues[position] = leftNulls.value(position) || rightNulls.value(position);
            }
            result = result.with(Stream.NULLS, nulls);
        }
        if (!requestValues) {
            return result;
        }

        F64Vector values = context.allocator().allocateOrGrow(
                allocationContext,
                output != null && output.getOrNull(Stream.VALUES) instanceof F64Vector vector ? vector : null,
                F64Vector.class,
                requiredLength,
                F64Vector::new);
        double[] outputValues = values.values();
        for (int position : mask) {
            outputValues[position] = leftValues.value(position) + rightValues.value(position);
        }
        return result.with(Stream.VALUES, values);
    }
}
