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
import org.weakref.nitro.data.BinaryDispatchSupport;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Null-safe equality for I64 physical carriers. Unlike ordinary equality, the result is never null:
 * two nulls are identical, and exactly one null is not identical.
 */
@ScalarFunction(name = "identical_i64")
public final class IdenticalI64
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("IdenticalI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for identical_i64");
        if (!requestedStreams.contains(Stream.VALUES)) {
            return Streams.empty();
        }

        Vector leftValuesVector = inputs.get(0).values();
        Vector rightValuesVector = inputs.get(1).values();
        VectorAccess.LongValues leftValues = VectorAccess.longValues(leftValuesVector);
        VectorAccess.LongValues rightValues = VectorAccess.longValues(rightValuesVector);
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));

        BooleanVector values = VectorAccess.writableBooleanVector(
                context.allocator(),
                context.allocationContext("IdenticalI64"),
                output != null ? output.getOrNull(Stream.VALUES) : null,
                BinaryDispatchSupport.requiredLength(mask, Math.max(leftValuesVector.length(), rightValuesVector.length())));
        boolean[] result = values.values();
        for (int position : mask) {
            boolean leftNull = leftNulls.value(position);
            boolean rightNull = rightNulls.value(position);
            result[position] = leftNull == rightNull && (leftNull || leftValues.value(position) == rightValues.value(position));
        }
        return Streams.ofValues(values);
    }
}
