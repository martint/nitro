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
import org.weakref.nitro.data.I64BinaryDispatch;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
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

@ScalarFunction(name = "lte_i64")
public final class LessThanOrEqualI64
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("LessThanOrEqualI64");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for lte_i64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }
        Allocator.Context allocationContext = context.allocationContext("LessThanOrEqualI64");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector leftNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector rightNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector existing = output != null && output.has(Stream.VALUES) ? output.values() : null;

        if (requestedStreams.contains(Stream.VALUES)) {
            BooleanVector fast = NullFreeScalarKernels.compareLong(
                    NullFreeScalarKernels.LESS_THAN_OR_EQUAL,
                    left,
                    right,
                    leftNulls,
                    rightNulls,
                    mask,
                    existing,
                    context.allocator(),
                    allocationContext);
            if (fast != null) {
                return Streams.ofValues(fast);
            }
        }

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS) &&
                !(VectorAccess.isAllFalseNulls(leftNulls) && VectorAccess.isAllFalseNulls(rightNulls))) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())));
            VectorAccess.combineNullsOr(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existing == null) {
            BooleanVector values = context.allocator().allocate(
                    allocationContext,
                    BooleanVector.class,
                    RleVector.computeTargetRleLength(leftRle, rightRle),
                    BooleanVector::new);
            return result.with(Stream.VALUES, I64BinaryDispatch.rleRleBoolean(leftRle, rightRle, values, LessThanOrEqualI64::apply));
        }

        BooleanVector values = VectorAccess.writableBooleanVector(
                context.allocator(),
                allocationContext,
                existing,
                I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())));
        I64BinaryDispatch.applyBoolean(left, right, mask, values, LessThanOrEqualI64::apply);
        return result.with(Stream.VALUES, values);
    }

    private static boolean apply(long leftValue, long rightValue)
    {
        return leftValue <= rightValue;
    }
}
