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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.operator.evaluator.MaskOutcome;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "eq")
public final class EqualI64
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("EqualI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq");
        if (!requestedStreams.contains(Stream.VALUES)) {
            return Streams.empty();
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();
        Vector existing = output != null && output.has(Stream.VALUES) ? output.values() : null;

        BooleanVector result = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                existing instanceof BooleanVector vector ? vector : null,
                BooleanVector.class,
                BinaryDispatchSupport.requiredLength(mask, Math.max(left.length(), right.length())),
                BooleanVector::new);
        applyIntegerEquality(left, right, mask, result);
        return Streams.of(Stream.VALUES, result);
    }

    @Override
    public MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateMaskOutcome(inputs, mask, context, ALLOCATION_CONTEXT, EqualI64::compareEqual);
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMask(inputs, mask, context, ALLOCATION_CONTEXT, EqualI64::compareEqual);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMask(inputs, mask, context, ALLOCATION_CONTEXT, EqualI64::compareEqual);
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateTrueMaskInPlace(inputs, mask, EqualI64::compareEqual);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        return LongComparisonMaskSupport.tryEvaluateFalseMaskInPlace(inputs, mask, EqualI64::compareEqual);
    }

    private static void applyIntegerEquality(Vector left, Vector right, Mask mask, BooleanVector output)
    {
        BinaryDispatchSupport.validateLength(left, mask);
        BinaryDispatchSupport.validateLength(right, mask);

        boolean[] values = output.values();
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                values[position] = integerValue(left, position) == integerValue(right, position);
            }
            return;
        }

        for (int position : mask) {
            values[position] = integerValue(left, position) == integerValue(right, position);
        }
    }

    private static long integerValue(Vector vector, int position)
    {
        return switch (vector) {
            case I32Vector values -> values.values()[position];
            case I64Vector values -> values.values()[position];
            case DictionaryVector values -> integerValue(values.values(), values.ids()[position]);
            case RleVector values -> integerValue(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but got " + vector.getClass().getSimpleName());
        };
    }

    private static boolean compareEqual(long leftValue, long rightValue)
    {
        return leftValue == rightValue;
    }
}
