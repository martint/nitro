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

@ScalarFunction(name = "and")
public final class AndBoolean
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("AndBoolean");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for and");
        if (!requestedStreams.contains(Stream.VALUES)) {
            return Streams.empty();
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector existing = output != null && output.has(Stream.VALUES) ? output.values() : null;

        if (left instanceof RleVector leftRle && right instanceof RleVector rightRle && mask.all() && existing == null) {
            BooleanVector values = context.allocator().allocate(ALLOCATION_CONTEXT, BooleanVector.class, RleVector.computeTargetRleLength(leftRle, rightRle), BooleanVector::new);
            return Streams.of(Stream.VALUES, BooleanBinaryDispatch.rleRle(leftRle, rightRle, values, AndBoolean::apply));
        }

        BooleanVector result = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                existing instanceof BooleanVector vector ? vector : null,
                BooleanVector.class,
                BooleanBinaryDispatch.requiredLength(mask, Math.max(left.length(), right.length())),
                BooleanVector::new);
        BooleanBinaryDispatch.apply(left, right, mask, result, AndBoolean::apply);
        return Streams.of(Stream.VALUES, result);
    }

    private static boolean apply(boolean leftValue, boolean rightValue)
    {
        return leftValue && rightValue;
    }
}
