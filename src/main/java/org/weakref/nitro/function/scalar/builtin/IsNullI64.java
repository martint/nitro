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

@ScalarFunction(name = "is_null_i64")
public final class IsNullI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("IsNullI64");

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
        checkArgument(inputs.size() == 1, "Unexpected argument count for is_null_i64");
        if (!requestedStreams.contains(Stream.VALUES)) {
            return Streams.empty();
        }

        BooleanVector inputNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        Vector inputValues = (Vector) inputs.get(0).getOrNull(Stream.VALUES);
        int requiredLength = mask.maxPosition() + 1;
        if (inputNulls != null) {
            requiredLength = Math.max(requiredLength, inputNulls.length());
        }
        if (inputValues != null) {
            requiredLength = Math.max(requiredLength, inputValues.length());
        }
        BooleanVector values = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                output != null && output.getOrNull(Stream.VALUES) instanceof BooleanVector vector ? vector : null,
                BooleanVector.class,
                requiredLength,
                BooleanVector::new);
        boolean[] outputValues = values.values();
        if (inputNulls == null) {
            for (int position : mask) {
                outputValues[position] = false;
            }
        }
        else {
            boolean[] inputNullValues = inputNulls.values();
            for (int position : mask) {
                outputValues[position] = inputNullValues[position];
            }
        }
        return Streams.ofValues(values);
    }
}
