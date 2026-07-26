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

@ScalarFunction(name = "is_null_i32", capabilities = {IsNullProjectionOptimization.class, IsNullDirectMaskOptimization.class})
public final class IsNullI32
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("IsNullI32");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.nullsOnlyWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for is_null_i32");
        if (!requestedStreams.contains(Stream.VALUES)) {
            return Streams.empty();
        }
        Allocator.Context allocationContext = context.allocationContext("IsNullI32");

        Vector inputNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector inputValues = inputs.get(0).getOrNull(Stream.VALUES);
        int requiredLength = mask.maxPosition() + 1;
        if (inputNulls != null) {
            requiredLength = Math.max(requiredLength, inputNulls.length());
        }
        if (inputValues != null) {
            requiredLength = Math.max(requiredLength, inputValues.length());
        }
        BooleanVector values = VectorAccess.writableBooleanVector(
                context.allocator(),
                allocationContext,
                output != null ? output.getOrNull(Stream.VALUES) : null,
                requiredLength);
        boolean[] outputValues = values.values();
        context.allocator().copyBooleanValues(inputNulls, mask, outputValues);
        return Streams.ofValues(values);
    }
}
