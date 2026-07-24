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
import org.weakref.nitro.data.I64BinaryDispatch;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/** Multiplies two integer inputs, treating a null on either side as zero. */
@ScalarFunction(name = "multiply_null_as_zero_i64")
public final class MultiplyNullAsZeroI64
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("MultiplyNullAsZeroI64");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for multiply_null_as_zero_i64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        I64Vector values = context.allocator().allocateOrGrow(
                context.allocationContext("MultiplyNullAsZeroI64"),
                output != null && output.getOrNull(Stream.VALUES) instanceof I64Vector existing ? existing : null,
                I64Vector.class,
                I64BinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())),
                I64Vector::new);
        I64BinaryDispatch.applyLong(left, right, mask, values, (leftValue, rightValue) -> leftValue * rightValue);

        Vector leftNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector rightNulls = inputs.get(1).getOrNull(Stream.NULLS);
        boolean leftNullFree = VectorAccess.isAllFalseNulls(leftNulls);
        boolean rightNullFree = VectorAccess.isAllFalseNulls(rightNulls);
        if (!leftNullFree || !rightNullFree) {
            VectorAccess.BooleanValues leftNullValues = VectorAccess.booleanValues(leftNulls);
            VectorAccess.BooleanValues rightNullValues = VectorAccess.booleanValues(rightNulls);
            long[] outputValues = values.values();
            for (int position : mask) {
                if ((!leftNullFree && leftNullValues.value(position)) || (!rightNullFree && rightNullValues.value(position))) {
                    outputValues[position] = 0;
                }
            }
        }
        // The result is never null. PlanEvaluator synthesizes the requested all-false NULLS stream.
        return Streams.ofValues(values);
    }
}
