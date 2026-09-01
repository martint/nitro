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
package org.weakref.nitro.operator.pattern;

import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationPositionAccumulator;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/// Executes a registry aggregation over match rows without introducing a host Page/Block boundary.
public final class RegisteredPatternAggregationFunction
        implements PatternAggregationFunction
{
    private final AggregationImplementation implementation;
    private final PatternAggregationInput input;
    private final int[] selectedPosition = new int[1];

    private Allocator allocator;
    private Allocator.Context allocationContext;
    private Object state;
    private Mask selectedMask;

    public RegisteredPatternAggregationFunction(AggregationImplementation implementation, PatternAggregationInput input)
    {
        this.implementation = requireNonNull(implementation, "implementation is null");
        this.input = requireNonNull(input, "input is null");
    }

    @Override
    public Streams evaluate(
            PatternEvaluationContext context,
            PatternAggregationRows rows,
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            int outputPosition,
            int outputSize)
    {
        requireNonNull(context, "context is null");
        requireNonNull(rows, "rows is null");
        requireNonNull(output, "output is null");
        initializeExecution(allocator, allocationContext);
        implementation.initialize(state, 0, 1);

        while (rows.advance()) {
            input.reset(context, rows.position(), rows.labelOrdinal());
            AggregationPositionAccumulator accumulator = implementation.bindRawInputPosition(state, 0, input);
            if (accumulator != null) {
                accumulator.add(input.physicalPosition());
            }
            else {
                addMaskedPosition();
            }
        }

        Streams result = implementation.copyResultPosition(
                0,
                0,
                state,
                output,
                outputPosition,
                outputSize,
                allocator,
                allocationContext);
        if (result != null) {
            return result;
        }

        Streams single = requireNonNull(
                implementation.result(0, state, null, allocator, allocationContext),
                "aggregation returned null");
        try {
            return allocator.copySinglePositionInto(
                    allocationContext,
                    single,
                    output,
                    0,
                    outputPosition,
                    outputSize);
        }
        finally {
            for (var stream : single.streams()) {
                allocator.release(allocationContext, single.get(stream));
            }
        }
    }

    private void initializeExecution(Allocator allocator, Allocator.Context allocationContext)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(allocationContext, "allocationContext is null");
        if (state == null) {
            this.allocator = allocator;
            this.allocationContext = allocationContext;
            state = implementation.allocate(new AggregationExecution(allocator, allocationContext, input.schema()), 1);
            return;
        }
        if (this.allocator != allocator || this.allocationContext != allocationContext) {
            throw new IllegalArgumentException("pattern aggregation cannot change allocator context");
        }
    }

    private void addMaskedPosition()
    {
        selectedPosition[0] = input.physicalPosition();
        if (selectedMask == null) {
            selectedMask = allocator.allocateSparseMask(allocationContext, selectedPosition, 1, input.physicalSize());
        }
        else {
            allocator.overwriteSparseMask(allocationContext, selectedMask, selectedPosition, 1, input.physicalSize());
        }
        implementation.addRawInput(state, 0, selectedMask, input);
    }
}
