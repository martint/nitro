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
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/** Widen an integral column (I64 or I32 through the long view) to F64 for double arithmetic. */
@ScalarFunction(name = "cast_i64_to_f64")
public final class CastI64ToF64
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
        checkArgument(inputs.size() == 1, "Unexpected argument count for year_of_date");
        boolean requestValues = requestedStreams.contains(Stream.VALUES);
        boolean requestNulls = requestedStreams.contains(Stream.NULLS);
        if (!requestValues && !requestNulls) {
            return Streams.empty();
        }

        Allocator.Context allocationContext = context.allocationContext("CastI64ToF64");
        VectorAccess.LongValues epochDays = VectorAccess.longValues(inputs.getFirst().values());
        VectorAccess.BooleanValues inputNulls = VectorAccess.booleanValues(inputs.getFirst().getOrNull(Stream.NULLS));
        int requiredLength = Math.max(mask.maxPosition() + 1, inputs.getFirst().values().length());

        Streams result = Streams.empty();
        if (requestNulls) {
            BooleanVector nulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            boolean[] nullValues = nulls.values();
            for (int position : mask) {
                nullValues[position] = inputNulls.value(position);
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
            outputValues[position] = (double) epochDays.value(position);
        }
        return result.with(Stream.VALUES, values);
    }
}
