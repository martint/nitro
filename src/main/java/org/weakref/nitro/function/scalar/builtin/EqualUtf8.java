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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "eq_utf8")
public final class EqualUtf8
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("EqualUtf8");

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
    public Set<Stream> requiredMaskInputStreams(int inputIndex)
    {
        return PrimitiveFunction.VALUES_AND_NULLS_INPUT_STREAMS;
    }

    @Override
    public boolean requiresCompletedInputCompanionStreamsForMask()
    {
        return false;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq_utf8");
        return Utf8BinaryDispatch.applyEquals("eq_utf8", ALLOCATION_CONTEXT, inputs, mask, requestedStreams, output, context);
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq_utf8");
        return Utf8BinaryDispatch.tryEvaluateEqualsTrueMask("eq_utf8", ALLOCATION_CONTEXT, inputs, mask, context);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq_utf8");
        return Utf8BinaryDispatch.tryEvaluateEqualsFalseMask("eq_utf8", ALLOCATION_CONTEXT, inputs, mask, context);
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq_utf8");
        return Utf8BinaryDispatch.tryEvaluateInSetTrueMaskInPlace("eq_utf8", inputs, mask);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for eq_utf8");
        return Utf8BinaryDispatch.tryEvaluateInSetFalseMaskInPlace("eq_utf8", inputs, mask);
    }
}
