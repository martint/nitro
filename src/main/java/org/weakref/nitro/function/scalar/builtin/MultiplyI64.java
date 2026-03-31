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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "multiply")
public final class MultiplyI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("MultiplyI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for multiply");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }
        Allocator.Context allocationContext = context.allocationContext("MultiplyI64");

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);
        Vector existingValues = output != null && output.has(Stream.VALUES) ? output.values() : null;
        BooleanVector existingNulls = output != null && output.has(Stream.NULLS) ? (BooleanVector) output.get(Stream.NULLS) : null;

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    allocationContext,
                    existingNulls,
                    BooleanVector.class,
                    I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())),
                    BooleanVector::new);
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existingValues == null && leftNulls == null && rightNulls == null) {
            I64Vector values = context.allocator().allocate(allocationContext, I64Vector.class, RleVector.computeTargetRleLength(leftRle, rightRle), I64Vector::new);
            return result.with(Stream.VALUES, I64BinaryDispatch.rleRleLong(leftRle, rightRle, values, MultiplyI64::apply));
        }

        I64Vector resultValues = context.allocator().allocateOrGrow(
                allocationContext,
                existingValues instanceof I64Vector vector ? vector : null,
                I64Vector.class,
                I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())),
                I64Vector::new);
        I64BinaryDispatch.applyLong(left, right, mask, resultValues, MultiplyI64::apply);
        return result(resultValues, outputNulls);
    }

    private static long apply(long leftValue, long rightValue)
    {
        return leftValue * rightValue;
    }

    private static void applyNulls(BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] nulls = outputNulls.values();
        java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            nulls[position] = (leftNulls != null && leftNulls.values()[position]) ||
                    (rightNulls != null && rightNulls.values()[position]);
        }
    }

    private static Streams result(I64Vector values, BooleanVector nulls)
    {
        if (nulls == null) {
            return Streams.ofValues(values);
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }
}
