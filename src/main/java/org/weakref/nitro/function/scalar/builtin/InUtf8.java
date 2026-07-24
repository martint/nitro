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

import org.weakref.nitro.core.function.projection.ProjectionArgument;
import org.weakref.nitro.core.function.projection.ProjectionCodeBuilder;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;
import org.weakref.nitro.core.function.projection.ProjectionProgram;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "in_utf8")
public final class InUtf8
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction, ProjectionCodeProvider
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("InUtf8");

    @Override
    public Optional<ProjectionProgram> generate(ProjectionCodeBuilder builder, List<ProjectionArgument> arguments)
    {
        if (arguments.size() < 2 ||
                arguments.getFirst().kind() != ProjectionArgument.Kind.INPUT) {
            return Optional.empty();
        }
        for (int index = 1; index < arguments.size(); index++) {
            if (arguments.get(index).kind() != ProjectionArgument.Kind.LITERAL ||
                    !(arguments.get(index).literal() instanceof String)) {
                return Optional.empty();
            }
        }

        List<ProjectionCodeBuilder.ValueType> argumentTypes = new ArrayList<>(arguments.size());
        var input = builder.argument(0, ProjectionCodeBuilder.ValueType.UTF8);
        var matches = builder.utf8Equal(input, builder.argument(1, ProjectionCodeBuilder.ValueType.UTF8));
        argumentTypes.add(ProjectionCodeBuilder.ValueType.UTF8);
        argumentTypes.add(ProjectionCodeBuilder.ValueType.UTF8);
        for (int index = 2; index < arguments.size(); index++) {
            argumentTypes.add(ProjectionCodeBuilder.ValueType.UTF8);
            matches = builder.or(
                    matches,
                    builder.utf8Equal(input, builder.argument(index, ProjectionCodeBuilder.ValueType.UTF8)));
        }
        return Optional.of(builder.program(argumentTypes, matches, builder.isNull(0)));
    }

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
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return Utf8BinaryDispatch.applyInSet("in_utf8", ALLOCATION_CONTEXT, inputs, mask, requestedStreams, output, context);
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return Utf8BinaryDispatch.tryEvaluateInSetTrueMask("in_utf8", ALLOCATION_CONTEXT, inputs, mask, context);
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return Utf8BinaryDispatch.tryEvaluateInSetFalseMask("in_utf8", ALLOCATION_CONTEXT, inputs, mask, context);
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return Utf8BinaryDispatch.tryEvaluateInSetTrueMaskInPlace("in_utf8", inputs, mask);
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() >= 2, "Unexpected argument count for in_utf8");
        return Utf8BinaryDispatch.tryEvaluateInSetFalseMaskInPlace("in_utf8", inputs, mask);
    }
}
