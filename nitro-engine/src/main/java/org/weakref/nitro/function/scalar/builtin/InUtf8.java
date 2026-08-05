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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.function.scalar.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "in_utf8", capabilities = {InUtf8ProjectionOptimization.class, InUtf8SourceMaskOptimization.class})
public final class InUtf8
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("InUtf8");
    private final Utf8BinaryDispatch dispatch;

    public InUtf8()
    {
        this(Utf8BinaryDispatchPolicy.defaults());
    }

    public InUtf8(Utf8BinaryDispatchPolicy policy)
    {
        dispatch = new Utf8BinaryDispatch(policy);
    }

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
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return dispatch.applyInSet("in_utf8", allocationContext, inputs, mask, requestedStreams, output, context);
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return dispatch.tryEvaluateInSetTrueMask("in_utf8", allocationContext, inputs, mask, context);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return dispatch.tryEvaluateInSetFalseMask("in_utf8", allocationContext, inputs, mask, context);
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return dispatch.tryEvaluateInSetTrueMaskInPlace("in_utf8", inputs, mask);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return dispatch.tryEvaluateInSetFalseMaskInPlace("in_utf8", inputs, mask);
    }
}
