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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "divide_i64_to_f64")
public final class DivideI64ToF64
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
        checkArgument(inputs.size() == 2, "Unexpected argument count for divide_i64_to_f64");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        Allocator.Context allocationContext = context.allocationContext("DivideI64ToF64");
        VectorAccess.LongValues numeratorValues = VectorAccess.longValues(inputs.get(0).values());
        VectorAccess.LongValues denominatorValues = VectorAccess.longValues(inputs.get(1).values());
        VectorAccess.BooleanValues numeratorNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues denominatorNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));
        Vector numeratorValueVector = inputs.get(0).values();
        Vector denominatorValueVector = inputs.get(1).values();
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(numeratorValueVector.length(), denominatorValueVector.length()));

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = context.allocator().allocateOrGrow(
                    allocationContext,
                    output != null && output.getOrNull(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            boolean[] nullValues = nulls.values();
            for (int position : mask) {
                long denominator = denominatorValues.value(position);
                nullValues[position] = numeratorNulls.value(position) || denominatorNulls.value(position) || denominator == 0;
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
            long denominator = denominatorValues.value(position);
            outputValues[position] = denominator == 0 ? 0 : ((double) numeratorValues.value(position) / denominator);
        }
        return result.with(Stream.VALUES, values);
    }
}
